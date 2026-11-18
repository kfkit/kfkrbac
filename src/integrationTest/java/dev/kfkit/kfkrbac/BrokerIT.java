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

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Network;

/**
 * The jar that ships as the authorizer of a real broker: one node that is
 * broker and controller, of the release {@code -PbrokerVersion} names. Each
 * test has a node of its own, so no test sees what another bound.
 *
 * <p>{@code alice} and {@code bob} are users no ACL names; {@code admin} is
 * a super user and creates the topics, the bindings and the ACLs. A binding
 * is in force on the broker a moment after {@code AdminClient} has created
 * it, so the first request that depends on it is asked until it goes
 * through, with a bound; what follows from the same record is asked once.
 *
 * <p>A request to create or delete ACLs is decided by the controller, which
 * on this node is an authorizer of its own next to the broker's and has a
 * record a moment after it was created, like the broker. So the first such
 * request that depends on a record is asked with a bound too.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class BrokerIT {

    private static final String PREFIX = "billing.";
    private static final String TOPIC = "billing.orders";
    private static final String OTHER_TOPIC = "billing.invoices";
    private static final String ELSEWHERE = "payments.orders";
    /** A prefix no test makes anyone an owner of. */
    private static final String OUTSIDE = "payments.";

    /** How long a binding or an ACL may take from {@code AdminClient}'s answer to being in force. */
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
        clients.createTopics(TOPIC, OTHER_TOPIC, ELSEWHERE);
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

    /** AC1. */
    @Test
    void aWriteBindingLetsProduceIdempotentlyAndInATransaction() {
        clients.produce(ALICE, TOPIC).assertRefused("no binding and no ACL names her");
        clients.produceInTransaction(ALICE, "billing.tx", TOPIC)
                .assertRefused("no binding and no ACL names her");

        clients.createAcls(Clients.binding("DeveloperWrite", ALICE, PREFIX));

        Await.allowed(IN_FORCE, "she holds DeveloperWrite on " + PREFIX, () -> clients.produce(ALICE, TOPIC));
        clients.produceInTransaction(ALICE, "billing.tx", TOPIC)
                .assertAllowed("she holds DeveloperWrite on " + PREFIX + ", which covers the transactional ID");

        clients.produceInTransaction(ALICE, "payments.tx", TOPIC)
                .assertRefused(TransactionalIdAuthorizationException.class,
                        "the transactional ID is outside " + PREFIX);
        clients.produce(ALICE, ELSEWHERE).assertRefused("the topic is outside " + PREFIX);
        clients.produce(BOB, TOPIC).assertRefused("the binding is hers, not his");
    }

    /** AC2. */
    @Test
    void oneReadBindingCoversTheTopicsAndTheGroupsOfThePrefix() {
        clients.produce(ADMIN, TOPIC).assertAllowed("he is a super user");
        clients.consume(ALICE, "billing.app", TOPIC).assertRefused("no binding and no ACL names her");

        clients.createAcls(Clients.binding("DeveloperRead", ALICE, PREFIX));

        Await.allowed(IN_FORCE, "she holds DeveloperRead on " + PREFIX,
                () -> clients.consume(ALICE, "billing.app", TOPIC));
        clients.consume(ALICE, "payments.app", TOPIC)
                .assertRefused(GroupAuthorizationException.class, "the group is outside " + PREFIX);
        clients.produce(ALICE, TOPIC).assertRefused("DeveloperRead does not grant writing");
    }

    /** AC3. */
    @Test
    void aDenyAclStopsWhatTheBindingAllows() {
        AclBinding binding = Clients.binding("DeveloperWrite", ALICE, PREFIX);
        clients.createAcls(binding);
        Await.allowed(IN_FORCE, "she holds DeveloperWrite on " + PREFIX, () -> clients.produce(ALICE, TOPIC));

        AclBinding deny = Clients.deny(ALICE, TOPIC);
        clients.createAcls(deny);

        Await.refused(IN_FORCE, "an ACL denies her that topic", () -> clients.produce(ALICE, TOPIC));
        // The binding stays next to the deny ACL.
        awaitListed(AclBindingFilter.ANY, Set.of(binding, deny));
        clients.produce(ALICE, OTHER_TOPIC)
                .assertAllowed("the binding is still in force and the deny ACL names another topic");
    }

    /** AC4. */
    @Test
    void aResourceOwnerBindsRolesInsideWhatItOwnsAndNothingElse() {
        AclBinding owner = Clients.binding("ResourceOwner", BOB, PREFIX);
        AclBinding inside = Clients.binding("DeveloperRead", ALICE, PREFIX);
        clients.produce(ADMIN, TOPIC).assertAllowed("he is a super user");
        clients.createAcls(BOB, inside)
                .assertRefused(ClusterAuthorizationException.class, "no binding and no ACL names him");
        clients.consume(ALICE, "billing.app", TOPIC).assertRefused("no binding and no ACL names her");

        clients.createAcls(owner);

        // The controller decides who may bind, a moment after it has the record that makes him an owner.
        Await.allowed(IN_FORCE, "he holds ResourceOwner on " + PREFIX + " and the binding is inside it",
                () -> clients.createAcls(BOB, inside));
        Await.allowed(IN_FORCE, "bob bound her DeveloperRead on " + PREFIX,
                () -> clients.consume(ALICE, "billing.app", TOPIC));

        // Each asked once: the controller has let him bind, so it knows what he owns.
        clients.createAcls(BOB, Clients.binding("DeveloperRead", ALICE, OUTSIDE))
                .assertRefusedSaying(ClusterAuthorizationException.class, "he owns " + PREFIX + ", not " + OUTSIDE,
                        "User:bob may not create DeveloperRead:User:alice on the TOPIC prefix '" + OUTSIDE + "'",
                        "none of the ResourceOwner bindings of User:bob covers that pattern",
                        "it owns the TOPIC prefix '" + PREFIX + "'");
        clients.createAcls(BOB, Clients.allow(ALICE, AclOperation.READ, TOPIC))
                .assertRefusedSaying(ClusterAuthorizationException.class,
                        "a ResourceOwner binds roles and creates no ordinary ACL, inside what it owns or not",
                        "User:bob may not create the ACL User:alice ALLOW READ from host * on the TOPIC name '"
                                + TOPIC + "'",
                        "it is an ordinary ACL, and a ResourceOwner manages role bindings only");
        clients.createAcls(BOB, Clients.clusterBinding("SystemAdmin", BOB))
                .assertRefusedSaying(ClusterAuthorizationException.class,
                        "a ResourceOwner binds no role to the cluster, and so cannot make itself a SystemAdmin",
                        "User:bob may not create SystemAdmin:User:bob on the CLUSTER name 'kafka-cluster'",
                        "a binding to the cluster is managed only by");
        awaitListed(AclBindingFilter.ANY, Set.of(owner, inside));
    }

    /** AC5. */
    @Test
    void aResourceOwnerDeletesTheBindingsInsideWhatItOwnsAndNoOther() {
        AclBinding owner = Clients.binding("ResourceOwner", BOB, PREFIX);
        AclBinding outside = Clients.binding("DeveloperRead", ALICE, OUTSIDE);
        AclBinding his = Clients.binding("DeveloperRead", ALICE, PREFIX);
        clients.createAcls(owner, outside);
        Await.allowed(IN_FORCE, "he holds ResourceOwner on " + PREFIX + " and the binding is inside it",
                () -> clients.createAcls(BOB, his));
        Await.allowed(IN_FORCE, "bob bound her DeveloperRead on " + PREFIX, () -> clients.describe(ALICE, TOPIC));

        // The controller deletes what his filter matches among the records it has, and it has the one he
        // has just created a moment after it answered him: until then the request goes through and
        // deletes nothing.
        Await.until(IN_FORCE, () -> clients.deleteAcls(BOB, his.toFilter()),
                deletion -> deletion.deleted().equals(Set.of(his)),
                last -> "expected bob to delete " + his + ", which he created inside " + PREFIX + ", within "
                        + IN_FORCE.toSeconds() + " s; the cluster's last answer: " + last);
        awaitListed(AclBindingFilter.ANY, Set.of(owner, outside));
        Await.refused(IN_FORCE, "bob deleted the binding that let her", () -> clients.describe(ALICE, TOPIC));

        // Asked once: the controller had the binding he created, so it has the one created before it.
        clients.deleteAcls(BOB, outside.toFilter()).answer()
                .assertRefusedSaying(ClusterAuthorizationException.class, "he owns " + PREFIX + ", not " + OUTSIDE,
                        "User:bob may not delete DeveloperRead:User:alice on the TOPIC prefix '" + OUTSIDE
                                + "', which the filter matches",
                        "none of the ResourceOwner bindings of User:bob covers that pattern",
                        "it owns the TOPIC prefix '" + PREFIX + "'");
        assertEquals(Set.of(owner, outside), clients.describeAcls(AclBindingFilter.ANY),
                "the ACLs of the cluster after bob was refused deleting " + outside);
    }

    /** AC6. */
    @Test
    void bindingsAreListedAsAclsWithTheRoleInThePrincipal() {
        AclBinding write = Clients.binding("DeveloperWrite", ALICE, PREFIX);
        AclBinding read = Clients.binding("DeveloperRead", BOB, PREFIX);
        assertEquals("DeveloperWrite:User:alice", write.entry().principal());
        assertEquals("DeveloperRead:User:bob", read.entry().principal());
        assertEquals(Set.of(), clients.describeAcls(AclBindingFilter.ANY), "the ACLs of a new cluster");

        clients.createAcls(write, read);

        // The broker that answers lists a record once it has it, a moment after it was created.
        awaitListed(AclBindingFilter.ANY, Set.of(write, read));
        awaitListed(new AclBindingFilter(ResourcePatternFilter.ANY, new AccessControlEntryFilter(
                "DeveloperRead:User:bob", null, AclOperation.ANY, AclPermissionType.ANY)), Set.of(read));
    }

    /** AC7. */
    @Test
    void bindingsAreInForceWhenTheBrokerIsBackAfterARestart() {
        AclBinding binding = Clients.binding("DeveloperWrite", ALICE, PREFIX);
        clients.createAcls(binding);
        Await.allowed(IN_FORCE, "she holds DeveloperWrite on " + PREFIX, () -> clients.produce(ALICE, TOPIC));

        node.restart();

        // Asked once, and first: nothing has spoken to the broker since it started.
        clients.produce(ALICE, TOPIC)
                .assertAllowed("the binding was created before the restart and the broker serves no request "
                        + "before it has loaded its ACL records");
        clients.produce(BOB, TOPIC).assertRefused("the binding is hers, not his");
        assertEquals(Set.of(binding), clients.describeAcls(AclBindingFilter.ANY),
                "the ACLs of the cluster after the restart");
    }

    /** Who may bind what: everyone else, nothing. */
    @Test
    void aPrincipalWithoutARoleThatManagesAccessCreatesAndDeletesNothing() {
        AclBinding read = Clients.binding("DeveloperRead", BOB, PREFIX);
        clients.createAcls(read);
        awaitListed(AclBindingFilter.ANY, Set.of(read));

        clients.createAcls(ALICE, Clients.binding("DeveloperWrite", ALICE, PREFIX))
                .assertRefused(ClusterAuthorizationException.class, "no binding and no ACL names her");
        clients.createAcls(ALICE, Clients.allow(ALICE, AclOperation.READ, TOPIC))
                .assertRefused(ClusterAuthorizationException.class, "no binding and no ACL names her");
        clients.deleteAcls(ALICE, read.toFilter()).answer()
                .assertRefused(ClusterAuthorizationException.class, "no binding and no ACL names her");
        // Whether the controller has his binding yet or not: DeveloperRead does not open the ACL API.
        clients.createAcls(BOB, Clients.binding("DeveloperWrite", BOB, PREFIX))
                .assertRefused(ClusterAuthorizationException.class,
                        "DeveloperRead is the only role he holds, and it manages no access");
        assertEquals(Set.of(read), clients.describeAcls(AclBindingFilter.ANY),
                "the ACLs of the cluster after alice and bob were refused");
    }

    /** Who may bind what: ACLs stay, and whoever they allow ALTER on the cluster manages ACLs as before. */
    @Test
    void aPrincipalAnAclAllowsToAlterTheClusterCreatesAndDeletesAclsAndBindings() {
        AclBinding acl = Clients.allow(ALICE, AclOperation.READ, TOPIC);
        AclBinding binding = Clients.binding("DeveloperWrite", ALICE, PREFIX);
        clients.createAcls(BOB, acl)
                .assertRefused(ClusterAuthorizationException.class, "no binding and no ACL names him");

        AclBinding alter = Clients.allowOnCluster(BOB, AclOperation.ALTER);
        clients.createAcls(alter);

        Await.allowed(IN_FORCE, "an ACL allows him ALTER on the cluster, and he holds no role",
                () -> clients.createAcls(BOB, acl));
        clients.createAcls(BOB, binding)
                .assertAllowed("an ACL allows him ALTER on the cluster, which is all a binding took before the jar");
        awaitListed(AclBindingFilter.ANY, Set.of(alter, acl, binding));

        // Asked once: the controller applies his filters to the records it has written.
        Deletion deletion = clients.deleteAcls(BOB, acl.toFilter(), binding.toFilter());
        deletion.answer().assertAllowed("an ACL allows him ALTER on the cluster");
        assertEquals(Set.of(acl, binding), deletion.deleted(), "what the cluster deleted for " + deletion);
        awaitListed(AclBindingFilter.ANY, Set.of(alter));
    }

    /** A role is not a way to become a super user. */
    @Test
    void ofTheRolesOnlySystemAdminReplacesAUsersScramCredentials() {
        AclBinding binding = Clients.binding("DeveloperRead", ALICE, PREFIX);
        clients.replaceScramCredentials(ALICE, ADMIN)
                .assertRefused(ClusterAuthorizationException.class, "no binding and no ACL names her");
        clients.replaceScramCredentials(BOB, ADMIN)
                .assertRefused(ClusterAuthorizationException.class, "no binding and no ACL names him");

        clients.createAcls(Clients.clusterBinding("ClusterAdmin", ALICE), Clients.clusterBinding("SystemAdmin", BOB));

        // The controller answers these, a moment after it has the two bindings. ClusterAdmin grants ALTER on
        // the cluster, so she then passes the broker's check before createAcls and is refused there, with
        // its reason: that tells the controller has her binding. SystemAdmin creates any record.
        Await.until(IN_FORCE, () -> clients.createAcls(ALICE, binding),
                answer -> answer.refusedSaying("User:alice may not create DeveloperRead:User:alice")
                        && answer.refusedSaying("User:alice is none of them"),
                last -> "expected " + last.request() + " to be refused within " + IN_FORCE.toSeconds()
                        + " s with the reason that she is none of those who create ACL records, she holds "
                        + "ClusterAdmin, which runs the cluster and does not manage access; the cluster's last "
                        + "answer: " + last);
        Await.allowed(IN_FORCE, "he holds SystemAdmin", () -> clients.createAcls(BOB, binding));

        // Each asked once.
        clients.replaceScramCredentials(ALICE, ADMIN)
                .assertRefused(ClusterAuthorizationException.class,
                        "ClusterAdmin grants ALTER on the cluster, and for this request only SystemAdmin, of "
                                + "the roles, is let through");
        assertEquals(Set.of(), clients.usersWithScramCredentials(),
                "the users with SCRAM credentials after alice was refused");
        clients.replaceScramCredentials(BOB, ADMIN).assertAllowed("he holds SystemAdmin");
        Await.until(IN_FORCE, clients::usersWithScramCredentials, Set.of(ADMIN)::equals,
                listed -> "expected " + ADMIN + " to be the one user with SCRAM credentials within "
                        + IN_FORCE.toSeconds() + " s, bob replaced them; the broker listed: " + listed);
    }

    private void awaitListed(AclBindingFilter filter, Set<AclBinding> expected) {
        Await.until(IN_FORCE, () -> clients.describeAcls(filter), expected::equals,
                listed -> "expected describeAcls(" + filter + ") to list " + expected + " within "
                        + IN_FORCE.toSeconds() + " s; the broker listed: " + listed);
    }
}
