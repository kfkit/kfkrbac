/*
 * Copyright 2026 Ivan Abramov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.kfkit.kfkrbac;

import static dev.kfkit.kfkrbac.Node.ADMIN;
import static dev.kfkit.kfkrbac.Node.ALICE;
import static dev.kfkit.kfkrbac.Node.BOB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Network;

/**
 * The command of the jar on a real broker: run inside the broker's
 * container with the script of the distribution, {@code kafka-run-class.sh
 * dev.kfkit.kfkrbac.RbacCommand}, as an operator runs it, against one node
 * that is broker and controller, of the release {@code -PbrokerVersion}
 * names. Each test has a node of its own.
 *
 * <p>What the command wrote is asked of the broker by clients of
 * {@code alice} and {@code bob}, users no ACL names; {@code admin} is a
 * super user. A record is in force on the broker, and listed by it, a
 * moment after the command that created or deleted it has returned, so the
 * first request and the first listing that depend on it are asked until
 * they show it, with a bound; what follows from the same record is asked
 * once. A request that is asked until it is refused is one the broker
 * answers at once whether it allows it or not.
 *
 * <p>The name ends in {@code BrokerIT} so that the jobs which run the
 * one-node tests on every supported release select it too.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class CommandBrokerIT {

    private static final String PREFIX = "billing.";
    private static final String TOPIC = "billing.orders";
    private static final String REPORTS = "reports";
    private static final String LEDGER = "ledger";
    /** A prefix no test makes anyone an owner of. */
    private static final String OUTSIDE = "payments.";

    private static final String NOT_COUNTED =
            "Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).\n";

    /** How long a record may take from the command's answer to being in force and listed. */
    private static final Duration IN_FORCE = Duration.ofSeconds(30);

    private Network network;
    private Node node;
    private Clients clients;

    @BeforeEach
    void start() {
        Node.requireDocker();
        network = Network.newNetwork();
        node = Node.combined(network);
        Node.start(node);
        clients = new Clients(node.bootstrap());
        clients.createTopics(TOPIC, REPORTS, LEDGER);
    }

    @AfterEach
    void stop(TestInfo test) {
        if (node != null) {
            node.close(test.getTestMethod().orElseThrow().getName());
        }
        if (network != null) {
            network.close();
        }
    }

    /** #18 AC1: bind, list, who-can, what-can and unbind. */
    @Test
    void whatBindWroteListPrintsAndTheAuthorizerHonoursAndUnbindRemovesItAndNothingElse() {
        AclBinding elsewhere = Clients.binding("DeveloperRead", BOB, OUTSIDE);
        AclBinding acl = Clients.allow(BOB, AclOperation.READ, TOPIC);
        clients.createAcls(elsewhere, acl);
        clients.produce(ADMIN, TOPIC).assertAllowed("he is a super user");
        clients.consume(ALICE, "billing.app", TOPIC).assertRefused("no binding and no ACL names her");

        node.command(ADMIN, "bind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", PREFIX)
                .assertPrinted("Bound DeveloperRead to User:alice on prefix billing.\n", "admin is a super user");

        awaitPrinted("""
                ROLE           MEMBER      ON
                DeveloperRead  User:alice  prefix billing.
                DeveloperRead  User:bob    prefix payments.
                """, "bind has written the binding", "list");
        Await.allowed(IN_FORCE, "the command bound her DeveloperRead on " + PREFIX,
                () -> clients.consume(ALICE, "billing.app", TOPIC));
        clients.produce(ALICE, TOPIC).assertRefused("DeveloperRead does not grant writing");
        clients.consume(ALICE, "payments.app", TOPIC).assertRefused("the group is outside " + PREFIX);
        assertEquals(Set.of(elsewhere, acl, Clients.binding("DeveloperRead", ALICE, PREFIX)),
                clients.describeAcls(AclBindingFilter.ANY), "the ACL records of the cluster after bind");

        // Each asked once: the broker has listed the binding.
        node.command(ADMIN, "list", "--member", "User:alice", "--output", "json").assertPrinted("""
                [
                  {"role":"DeveloperRead","member":"User:alice","resourceType":"TOPIC","name":"billing.","patternType":"PREFIXED"}
                ]
                """, "the binding is the one of hers");
        node.command(ADMIN, "who-can", "--operation", "Read", "--topic", TOPIC).assertPrinted("""
                Read on topic billing.orders:

                User:alice  allowed
                    allowed by binding DeveloperRead to User:alice on prefix billing.
                User:bob  allowed
                    allowed by ACL allow User:bob Read on topic billing.orders

                """ + NOT_COUNTED, "her binding and his ACL reach the topic, his binding does not");
        node.command(ADMIN, "what-can", "--principal", "User:alice").assertPrinted("""
                User:alice may:
                  topics with prefix billing.             Read, Describe  by binding DeveloperRead to User:alice on prefix billing.
                  groups with prefix billing.             Read, Describe  by binding DeveloperRead to User:alice on prefix billing.
                  transactional IDs with prefix billing.  Describe        by binding DeveloperRead to User:alice on prefix billing.

                """ + NOT_COUNTED, "that binding is all that names her");

        node.command(ADMIN, "unbind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", PREFIX)
                .assertPrinted("Unbound DeveloperRead to User:alice on prefix billing.\n", "the binding was there");

        // Asked with a request that is answered at once either way: her consumer has read the one record.
        Await.refused(IN_FORCE, "the command unbound her", () -> clients.describe(ALICE, TOPIC));
        clients.consume(ALICE, "billing.app", TOPIC).assertRefused("the command unbound her");
        awaitListed(Set.of(elsewhere, acl));
        awaitPrinted("""
                ROLE           MEMBER    ON
                DeveloperRead  User:bob  prefix payments.
                """, "unbind has removed her binding and no other", "list");
        node.command(ADMIN, "unbind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", PREFIX)
                .assertFailedSaying(1, "the binding is not there any more",
                        "error: no such binding: DeveloperRead to User:alice on prefix billing.");
        assertEquals(Set.of(elsewhere, acl), clients.describeAcls(AclBindingFilter.ANY),
                "the ACL records of the cluster after an unbind that found nothing");
    }

    /** #18 AC5. */
    @Test
    void aCallerTheClusterRefusesSeesTheReasonOfTheClusterAndStatusOne() {
        AclBinding owner = Clients.binding("ResourceOwner", BOB, PREFIX);
        AclBinding inside = Clients.binding("DeveloperRead", ALICE, PREFIX);

        node.command(ALICE, "list").assertFailedSaying(1, "no binding and no ACL lets her list the ACLs",
                "error: ClusterAuthorizationException");
        node.command(ALICE, "bind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", PREFIX)
                .assertFailedSaying(1, "no binding and no ACL lets her create one",
                        "error: ClusterAuthorizationException");
        node.command(ALICE, "group", "add", "--group", "payments", "--member", "User:alice")
                .assertFailedSaying(1, "no binding and no ACL lets her create a membership",
                        "error: ClusterAuthorizationException");
        assertEquals(Set.of(), clients.describeAcls(AclBindingFilter.ANY),
                "the ACL records of the cluster after alice was refused");

        clients.createAcls(owner);

        // The controller decides who may bind, a moment after it has the record that makes him an owner.
        Await.until(IN_FORCE,
                () -> node.command(BOB, "bind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", PREFIX),
                ran -> ran.status() == 0,
                last -> "expected bob to bind inside " + PREFIX + " within " + IN_FORCE.toSeconds()
                        + " s, he holds ResourceOwner there; " + last);

        // Each asked once: the controller has let him bind, so it knows what he owns.
        node.command(BOB, "bind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", OUTSIDE)
                .assertFailedSaying(1, "he owns " + PREFIX + ", not " + OUTSIDE,
                        "error: ClusterAuthorizationException: User:bob may not create DeveloperRead:User:alice on the"
                                + " TOPIC prefix '" + OUTSIDE + "'",
                        "none of the ResourceOwner bindings of User:bob covers that pattern",
                        "it owns the TOPIC prefix '" + PREFIX + "'");
        node.command(BOB, "group", "add", "--group", "payments", "--member", "User:bob")
                .assertFailedSaying(1, "a ResourceOwner does not decide who is in a group",
                        "error: ClusterAuthorizationException: User:bob may not create",
                        "a record of group membership is managed only by");
        node.command(ALICE, "unbind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", PREFIX)
                .assertFailedSaying(1, "DeveloperRead manages no access", "error: ClusterAuthorizationException");
        awaitListed(Set.of(owner, inside));
    }

    /** #18 AC6: nothing is sent to the cluster. */
    @Test
    void wrongUsageExitsWithTwoAndSaysHowTheVerbIsUsed() {
        String run = "Usage: kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand ";

        node.commandAsTyped().assertFailedSaying(2, "no verb", run + "<verb>", "Exit status:");
        node.command(ADMIN, "grant", "--role", "DeveloperRead")
                .assertFailedSaying(2, "there is no such verb", "Unknown verb: grant", run + "<verb>");
        node.command(ADMIN, "bind", "--role", "DeveloperRead", "--member", "User:alice")
                .assertFailedSaying(2, "a role for a pattern without a pattern",
                        "DeveloperRead is bound to a name pattern: give --prefix or --name", run + "bind ");
        node.command(ADMIN, "unbind", "--role", "Developer", "--member", "User:alice", "--prefix", PREFIX)
                .assertFailedSaying(2, "no such role", "There is no role Developer: the roles are DeveloperRead,",
                        run + "unbind ");
        node.command(ADMIN, "list", "--output", "yaml")
                .assertFailedSaying(2, "no such output", "--output is text or json, not yaml", run + "list ");
        node.command(ADMIN, "group").assertFailedSaying(2, "group is half a verb",
                "group is followed by add, remove or list", run + "group add ", run + "group list ");
        node.command(ADMIN, "group", "add", "--group", "payments", "--member", "Group:billing")
                .assertFailedSaying(2, "a group is not a member of a group",
                        "Not a membership: the member is a group, and a group is not a member of a group",
                        run + "group add ");
        node.command(ADMIN, "group", "remove", "--group", "payments")
                .assertFailedSaying(2, "nobody to remove", "--member is required", run + "group remove ");
        node.command(ADMIN, "who-can", "--topic", TOPIC)
                .assertFailedSaying(2, "no operation", "--operation is required", run + "who-can ");
        node.command(ADMIN, "what-can").assertFailedSaying(2, "no principal", "--principal is required",
                run + "what-can ");
        node.commandAsTyped("list")
                .assertFailedSaying(2, "no cluster to connect to", "--bootstrap-server is required", run + "list ");

        Ran help = node.commandAsTyped("--help").assertStatus(0, "help is asked for");
        for (String verb : new String[] {"bind", "unbind", "list", "group add", "group remove", "group list",
                "who-can", "what-can"}) {
            assertTrue(help.out().contains(run + verb + " --bootstrap-server"), () -> "--help shows " + verb + "; "
                    + help);
        }
        assertEquals(Set.of(), clients.describeAcls(AclBindingFilter.ANY),
                "the ACL records of the cluster after the command was used wrongly");
    }

    /** #17 AC9, which is its AC1 and AC2 on a real broker, with the memberships made by the group verbs. */
    @Test
    void aRoleBoundToAGroupIsHeldByItsMemberAndAnAclOfAGroupWorksForItsMember() {
        AclBinding binding = Clients.groupBinding("DeveloperRead", "payments", PREFIX);
        AclBinding acl = Clients.allowGroup("payments", AclOperation.READ, REPORTS, "*");
        AclBinding fromElsewhere = Clients.allowGroup("payments", AclOperation.READ, LEDGER, "192.0.2.7");
        clients.produce(ADMIN, TOPIC).assertAllowed("he is a super user");
        clients.produce(ADMIN, REPORTS).assertAllowed("he is a super user");
        clients.createAcls(acl, fromElsewhere);
        node.command(ADMIN, "bind", "--role", "DeveloperRead", "--member", "Group:payments", "--prefix", PREFIX)
                .assertPrinted("Bound DeveloperRead to Group:payments on prefix billing.\n", "admin is a super user");
        awaitListed(Set.of(binding, acl, fromElsewhere));
        clients.consume(ALICE, "billing.app", TOPIC).assertRefused("she is in no group, and no record names her");
        clients.read(ALICE, REPORTS).assertRefused("she is in no group, and no record names her");

        node.command(ADMIN, "group", "add", "--group", "payments", "--member", "User:alice")
                .assertPrinted("Added User:alice to Group:payments\n", "admin is a super user");

        Await.allowed(IN_FORCE, "she is in Group:payments, which holds DeveloperRead on " + PREFIX,
                () -> clients.consume(ALICE, "billing.app", TOPIC));
        Await.allowed(IN_FORCE, "she is in Group:payments, which an ACL allows to read " + REPORTS,
                () -> clients.read(ALICE, REPORTS));
        clients.consume(BOB, "billing.app", TOPIC).assertRefused("he is not in the group");
        clients.read(BOB, REPORTS).assertRefused("he is not in the group");
        clients.produce(ALICE, TOPIC).assertRefused("DeveloperRead does not grant writing");
        clients.describe(ALICE, LEDGER).assertRefused("the ACL of her group is for another host than hers");

        awaitPrinted("""
                GROUP     MEMBER
                payments  User:alice
                """, "group add has written the membership", "group", "list");
        // Each asked once: the broker has listed the membership.
        node.command(ADMIN, "group", "list", "--group", "payments", "--output", "json").assertPrinted("""
                [
                  {"group":"payments","member":"User:alice"}
                ]
                """, "she is the one member");
        node.command(ADMIN, "who-can", "--operation", "Read", "--topic", REPORTS).assertPrinted("""
                Read on topic reports:

                User:alice  allowed
                    allowed by ACL allow Group:payments Read on topic reports, as a member of Group:payments

                Some ACLs name a host: asked as from 192.0.2.1; --host asks as from another address.
                """ + NOT_COUNTED, "the ACL of her group reaches the topic");
        node.command(ADMIN, "who-can", "--operation", "Read", "--topic", TOPIC).assertPrinted("""
                Read on topic billing.orders:

                User:alice  allowed
                    allowed by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments

                Some ACLs name a host: asked as from 192.0.2.1; --host asks as from another address.
                """ + NOT_COUNTED, "the binding of her group reaches the topic");

        node.command(ADMIN, "group", "remove", "--group", "payments", "--member", "User:alice")
                .assertPrinted("Removed User:alice from Group:payments\n", "the membership was there");

        // Asked with requests that are answered at once either way: her consumer has read the one record.
        Await.refused(IN_FORCE, "she is not in the group any more", () -> clients.describe(ALICE, TOPIC));
        Await.refused(IN_FORCE, "she is not in the group any more", () -> clients.read(ALICE, REPORTS));
        clients.consume(ALICE, "billing.app", TOPIC).assertRefused("she is not in the group any more");
        awaitListed(Set.of(binding, acl, fromElsewhere));
        awaitPrinted("No groups.\n", "group remove has removed the one membership", "group", "list");
    }

    /** Runs the command as the super user until it prints exactly that, with a bound. */
    private void awaitPrinted(String expected, String why, String... typed) {
        Await.until(IN_FORCE, () -> node.command(ADMIN, typed),
                ran -> ran.status() == 0 && ran.out().equals(expected) && ran.err().isEmpty(),
                last -> "expected the command to print\n" + expected + "within " + IN_FORCE.toSeconds() + " s, " + why
                        + "; " + last);
    }

    private void awaitListed(Set<AclBinding> expected) {
        Await.until(IN_FORCE, () -> clients.describeAcls(AclBindingFilter.ANY), expected::equals,
                listed -> "expected the cluster to list " + expected + " within " + IN_FORCE.toSeconds()
                        + " s; the broker listed: " + listed);
    }
}
