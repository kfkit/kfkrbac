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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Network;

/**
 * {@code apply} and {@code import} on real brokers: the command runs inside
 * the broker's container with the script of the distribution, reads a file
 * that was put there, and what it wrote is listed by the broker and
 * honoured by its authorizer. One node that is broker and controller, of
 * the release {@code -PbrokerVersion} names; each test has its own, and the
 * round trip from one cluster to another has two.
 *
 * <p>The files of the two shapes {@code import} reads are the examples of
 * {@code docs/examples/}.
 *
 * <p>A record is listed by the broker a moment after the command that
 * created it has returned. {@code apply} compares the file with what the
 * broker lists, so before it runs a second time the listing is asked until
 * it shows what the first run did, with a bound.
 *
 * <p>The name ends in {@code BrokerIT} so that the jobs which run the
 * one-node tests on every supported release select it too.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class ApplyImportBrokerIT {

    private static final String PREFIX = "billing.";
    private static final String TOPIC = "billing.orders";
    private static final String ORDERS = "orders";

    private static final Duration IN_FORCE = Duration.ofSeconds(30);

    private final List<Network> networks = new ArrayList<>();
    private final List<Node> nodes = new ArrayList<>();
    private Node node;
    private Clients clients;

    @BeforeEach
    void start() {
        Node.requireDocker();
        node = cluster();
        clients = new Clients(node.bootstrap());
        clients.createTopics(TOPIC, ORDERS);
    }

    @AfterEach
    void stop(TestInfo test) {
        for (int i = 0; i < nodes.size(); i++) {
            nodes.get(i).close(test.getTestMethod().orElseThrow().getName() + "-cluster-" + (i + 1));
        }
        networks.forEach(Network::close);
    }

    /** #19 AC1. */
    @Test
    void theListOfAClusterAppliedToAnEmptyClusterMakesListThereTheSame() {
        AclBinding anAcl = Clients.allow(BOB, AclOperation.READ, TOPIC);
        clients.createAcls(anAcl);
        bind("--role", "DeveloperRead", "--member", "Group:payments", "--prefix", PREFIX);
        bind("--role", "DeveloperWrite", "--member", "User:bob", "--name", ORDERS);
        bind("--role", "DeveloperRead", "--member", "User:bob", "--prefix", PREFIX, "--group-only");
        bind("--role", "DeveloperRead", "--member", "User:bob", "--prefix", "orders-", "--subject");
        bind("--role", "ClusterAdmin", "--member", "User:bob");
        node.command(ADMIN, "group", "add", "--group", "payments", "--member", "User:alice")
                .assertPrinted("Added User:alice to Group:payments\n", "admin is a super user");
        String list = """
                ROLE            MEMBER          ON
                DeveloperRead   User:bob        subjects with prefix orders-
                DeveloperRead   Group:payments  prefix billing.
                DeveloperWrite  User:bob        name orders
                DeveloperRead   User:bob        groups with prefix billing.
                ClusterAdmin    User:bob        the cluster
                """;
        String groupList = """
                GROUP     MEMBER
                payments  User:alice
                """;
        awaitPrinted(node, list, "the bindings were bound", "list");
        awaitPrinted(node, groupList, "the member was added", "group", "list");
        String bindings = node.command(ADMIN, "list", "--output", "json").assertStatus(0, "it only lists").out();
        String groups = node.command(ADMIN, "group", "list", "--output", "json").assertStatus(0, "it only lists")
                .out();
        Set<AclBinding> records = clients.describeAcls(AclBindingFilter.ANY);
        assertEquals(7, records.size(), "five bindings, a membership and an ACL: " + records);

        Node empty = cluster();
        Clients there = new Clients(empty.bootstrap());
        there.createTopics(TOPIC);
        there.produce(ADMIN, TOPIC).assertAllowed("he is a super user");
        there.consume(ALICE, "billing.app", TOPIC).assertRefused("the cluster has no record at all");
        String file = empty.put("access.json", "{\"bindings\": " + bindings + ", \"groups\": " + groups + "}");

        empty.command(ADMIN, "apply", "--file", file).assertPrinted("""
                + membership of User:alice in Group:payments
                + binding DeveloperRead to User:bob on subjects with prefix orders-
                + binding DeveloperRead to Group:payments on prefix billing.
                + binding DeveloperWrite to User:bob on name orders
                + binding DeveloperRead to User:bob on groups with prefix billing.
                + binding ClusterAdmin to User:bob on the cluster
                Created 6, deleted 0.
                """, "the cluster was empty");

        awaitPrinted(empty, list, "apply has written the bindings of the file", "list");
        awaitPrinted(empty, groupList, "apply has written the memberships of the file", "group", "list");
        empty.command(ADMIN, "list", "--output", "json").assertPrinted(bindings, "the bindings are those of the file");
        empty.command(ADMIN, "group", "list", "--output", "json")
                .assertPrinted(groups, "the memberships are those of the file");
        Set<AclBinding> carried = new HashSet<>(records);
        carried.remove(anAcl);
        assertEquals(carried, there.describeAcls(AclBindingFilter.ANY),
                "the bindings and the membership of the first cluster, and not its ordinary ACL");
        Await.allowed(IN_FORCE, "she is in Group:payments there, which holds DeveloperRead on " + PREFIX,
                () -> there.consume(ALICE, "billing.app", TOPIC));

        // What list prints alone is a file too: the bindings, and the groups are not touched.
        String listed = empty.put("bindings.json", bindings);
        empty.command(ADMIN, "apply", "--file", listed)
                .assertPrinted("Nothing to change: the cluster has what the file has.\n",
                        "the bindings are those of the file already");
        assertEquals(carried, there.describeAcls(AclBindingFilter.ANY), "the ACL records after an apply of the same");
    }

    /** #19 AC2 and AC3. */
    @Test
    void applyPrintsTheDifferenceMakesExactlyItAndLeavesOrdinaryAclsAlone() {
        AclBinding stays = Clients.binding("DeveloperRead", ALICE, PREFIX);
        AclBinding goes = Clients.binding("DeveloperWrite", BOB, PREFIX);
        AclBinding member = Clients.membership(BOB, "audit");
        AclBinding anAcl = Clients.allow(BOB, AclOperation.READ, TOPIC);
        AclBinding aGroupAcl = Clients.allowGroup("audit", AclOperation.READ, ORDERS, "*");
        AclBinding comes = Clients.groupBinding("DeveloperRead", "payments", PREFIX);
        AclBinding joins = Clients.membership(ALICE, "payments");
        clients.createAcls(stays, goes, member, anAcl, aGroupAcl);
        awaitListed(clients, Set.of(stays, goes, member, anAcl, aGroupAcl));
        String file = node.put("access.json", """
                {
                  "bindings": [
                    {"role":"DeveloperRead","member":"User:alice","resourceType":"TOPIC","name":"billing.","patternType":"PREFIXED"},
                    {"role":"DeveloperRead","member":"Group:payments","resourceType":"TOPIC","name":"billing.","patternType":"PREFIXED"}
                  ],
                  "groups": [
                    {"group":"payments","member":"User:alice"}
                  ]
                }
                """);
        String difference = """
                + membership of User:alice in Group:payments
                + binding DeveloperRead to Group:payments on prefix billing.
                - membership of User:bob in Group:audit
                - binding DeveloperWrite to User:bob on prefix billing.
                """;

        node.command(ADMIN, "apply", "--file", file, "--dry-run")
                .assertPrinted(difference + "Would create 2 and delete 2. Nothing was changed.\n", "a dry run");
        assertEquals(Set.of(stays, goes, member, anAcl, aGroupAcl), clients.describeAcls(AclBindingFilter.ANY),
                "the ACL records after a dry run");

        node.command(ADMIN, "apply", "--file", file, "--no-delete").assertPrinted("""
                + membership of User:alice in Group:payments
                + binding DeveloperRead to Group:payments on prefix billing.
                  not in the file, and left: membership of User:bob in Group:audit
                  not in the file, and left: binding DeveloperWrite to User:bob on prefix billing.
                Created 2, deleted 0; 2 not in the file left as they are, as --no-delete says.
                """, "it only adds");
        awaitListed(clients, Set.of(stays, goes, member, anAcl, aGroupAcl, comes, joins));

        node.command(ADMIN, "apply", "--file", file).assertPrinted("""
                - membership of User:bob in Group:audit
                - binding DeveloperWrite to User:bob on prefix billing.
                Created 0, deleted 2.
                """, "what is not in the file goes");
        awaitListed(clients, Set.of(stays, comes, joins, anAcl, aGroupAcl));

        node.command(ADMIN, "apply", "--file", file)
                .assertPrinted("Nothing to change: the cluster has what the file has.\n", "a second apply");
        assertEquals(Set.of(stays, comes, joins, anAcl, aGroupAcl), clients.describeAcls(AclBindingFilter.ANY),
                "the ACL records after a second apply: the ordinary ACLs are where they were");
    }

    /** #19 AC4. */
    @Test
    void importOfBindingsOfNamedRolesWritesThemAndTheAuthorizerHonoursThem() {
        clients.produce(ADMIN, TOPIC).assertAllowed("he is a super user");
        clients.consume(ALICE, "billing.app", TOPIC).assertRefused("no binding and no ACL names her");
        String file = node.put("role-bindings.json", example("role-bindings.json"));

        Ran dry = node.command(ADMIN, "import", "--file", file, "--format", "role-bindings", "--cluster-id",
                "cluster-1", "--dry-run").assertStatus(0, "a dry run");
        assertEquals(Set.of(), clients.describeAcls(AclBindingFilter.ANY), "the ACL records after a dry run: " + dry);

        Ran imported = node.command(ADMIN, "import", "--file", file, "--format", "role-bindings", "--cluster-id",
                "cluster-1").assertStatus(0, "admin is a super user");

        assertEquals(dry.out().replace("Would write ", "Wrote ").replace(". Nothing was changed.", "."),
                imported.out(), "what the dry run said would be written");
        awaitPrinted(node, """
                ROLE            MEMBER          ON
                ResourceOwner   User:bob        subjects with prefix billing.
                DeveloperRead   User:alice      prefix billing.
                ResourceOwner   User:bob        prefix billing.
                DeveloperWrite  Group:payments  name orders
                ClusterAdmin    User:carol      the cluster
                DeveloperWrite  Group:payments  transactional ID billing-1
                """, "import has written the bindings of the file for cluster-1, and no other", "list");
        Await.allowed(IN_FORCE, "the import bound her DeveloperRead on " + PREFIX,
                () -> clients.consume(ALICE, "billing.app", TOPIC));
        clients.produce(ALICE, TOPIC).assertRefused("DeveloperRead does not grant writing");
    }

    /** #19 AC5. */
    @Test
    void importOfRolesThatAreSetsOfAclsMakesGroupsWhoseMembersAreAllowedWhatTheAclsAllowed() {
        clients.produce(ADMIN, ORDERS).assertAllowed("he is a super user");
        clients.consume(ALICE, "orders-app", ORDERS).assertRefused("no binding and no ACL names her");
        String file = node.put("role-acls.json", example("role-acls.json"));

        node.command(ADMIN, "import", "--file", file, "--format", "role-acls").assertPrinted("""
                + membership of User:alice in Group:orders-readers
                + membership of User:bob in Group:orders-readers
                + ACL allow Group:orders-readers Read on topic orders
                + ACL allow Group:orders-readers Read on groups with prefix orders-
                skipped [1] (line 10) role billing-writers: it has a DENY ACL ([1].acls[1] (line 15)), and a group cannot be denied: a DENY ACL names the users; nothing of the role is written
                Wrote 4 records; skipped 1.
                """, "admin is a super user");

        Set<AclBinding> written = Set.of(Clients.membership(ALICE, "orders-readers"),
                Clients.membership(BOB, "orders-readers"),
                Clients.allowGroup("orders-readers", AclOperation.READ, ORDERS, "*"),
                new AclBinding(new ResourcePattern(ResourceType.GROUP, "orders-", PatternType.PREFIXED),
                        new AccessControlEntry("Group:orders-readers", "*", AclOperation.READ,
                                AclPermissionType.ALLOW)));
        awaitListed(clients, written);
        Await.allowed(IN_FORCE, "she is in Group:orders-readers, which the ACLs of the role allow to read "
                + ORDERS + " in a group that starts with orders-", () -> clients.consume(ALICE, "orders-app", ORDERS));
        clients.read(BOB, ORDERS).assertAllowed("he is a member too");
        clients.consume(ALICE, "billing.app", ORDERS).assertRefused("the group is outside orders-");
        clients.produce(ALICE, ORDERS).assertRefused("the ACLs of the role allow reading");
        clients.produce(ALICE, TOPIC).assertRefused("the role with a DENY ACL was not written");

        // Whoever leaves the group loses what the role's ACLs allowed.
        node.command(ADMIN, "group", "remove", "--group", "orders-readers", "--member", "User:bob")
                .assertPrinted("Removed User:bob from Group:orders-readers\n", "the membership was there");
        Await.refused(IN_FORCE, "he is not in the group any more", () -> clients.read(BOB, ORDERS));
    }

    /** #19 AC7, with the file where the command runs. */
    @Test
    void aFileThatIsNotValidFailsWithThePlaceAndTheFieldAndStatusTwoAndWritesNothing() {
        String notJson = node.put("not.json", "{\"bindings\": [\n  {\"role\": }\n]}");
        String noName = node.put("no-name.json", "[{\"role\":\"DeveloperRead\",\"member\":\"User:alice\","
                + "\"resourceType\":\"TOPIC\",\"patternType\":\"PREFIXED\"}]");
        String noRole = node.put("no-role.json", "[\n  {\"principal\": \"User:alice\"}\n]");

        node.command(ADMIN, "apply", "--file", notJson).assertFailedSaying(2, "the file is not JSON",
                "error: " + notJson + ": line 2, column 12: not valid JSON: Unexpected character");
        node.command(ADMIN, "apply", "--file", noName).assertFailedSaying(2, "a binding without a name",
                "error: " + noName + ": line 1, column 2: [0]: \"name\" is required");
        node.command(ADMIN, "import", "--file", noRole, "--format", "role-bindings")
                .assertFailedSaying(2, "a binding without a role",
                        "error: " + noRole + ": line 2, column 3: [0]: \"role\" is required");
        node.command(ADMIN, "apply", "--file", "/opt/kfkrbac/missing.json")
                .assertFailedSaying(2, "no such file", "error: /opt/kfkrbac/missing.json: there is no such file");
        assertEquals(Set.of(), clients.describeAcls(AclBindingFilter.ANY),
                "the ACL records of the cluster after files that could not be read");
    }

    /** A cluster of one node on a network of its own. */
    private Node cluster() {
        Network network = Network.newNetwork();
        networks.add(network);
        Node started = Node.combined(network);
        nodes.add(started);
        Node.start(started);
        return started;
    }

    private void bind(String... options) {
        List<String> typed = new ArrayList<>(List.of("bind"));
        typed.addAll(List.of(options));
        node.command(ADMIN, typed.toArray(new String[0])).assertStatus(0, "admin is a super user");
    }

    private static String example(String name) {
        try {
            return Files.readString(Path.of("docs/examples").resolve(name));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Runs the command as the super user until it prints exactly that, with a bound. */
    private static void awaitPrinted(Node on, String expected, String why, String... typed) {
        Await.until(IN_FORCE, () -> on.command(ADMIN, typed),
                ran -> ran.status() == 0 && ran.out().equals(expected) && ran.err().isEmpty(),
                last -> "expected the command to print\n" + expected + "within " + IN_FORCE.toSeconds() + " s, " + why
                        + "; " + last);
    }

    private static void awaitListed(Clients of, Set<AclBinding> expected) {
        Await.until(IN_FORCE, () -> of.describeAcls(AclBindingFilter.ANY), expected::equals,
                listed -> "expected the cluster to list " + expected + " within " + IN_FORCE.toSeconds()
                        + " s; the broker listed: " + listed);
    }
}
