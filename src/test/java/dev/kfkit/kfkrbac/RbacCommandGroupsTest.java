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

import static dev.kfkit.kfkrbac.Commands.allow;
import static dev.kfkit.kfkrbac.Commands.authorizerAllows;
import static dev.kfkit.kfkrbac.Commands.record;
import static dev.kfkit.kfkrbac.Commands.run;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.acl;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.literal;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.prefixed;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Groups in the command, against ACL records held in memory: the
 * {@code group} verbs, and the members of groups in {@code who-can} and
 * {@code what-can}. What it writes is read back by the codec and by a real
 * {@link RbacAuthorizer}; what it answers is compared with what that
 * authorizer answers.
 */
class RbacCommandGroupsTest {

    private static final ResourcePattern BILLING = prefixed(TOPIC, "billing.");
    private static final ResourcePattern ORDERS = literal(TOPIC, "billing.orders");
    private static final ResourcePattern REPORTS = literal(TOPIC, "reports");
    private static final ResourcePattern THE_CLUSTER = literal(CLUSTER, "kafka-cluster");

    private static final AclBinding ALICE_IN_PAYMENTS = record("Member:User:alice", literal(TOPIC, "Group:payments"));
    private static final AclBinding CAROL_IN_PAYMENTS = record("Member:User:carol", literal(TOPIC, "Group:payments"));
    private static final AclBinding ALICE_IN_AUDIT = record("Member:User:alice", literal(TOPIC, "Group:audit"));
    private static final AclBinding DAVE_IN_AUDIT = record("Member:User:dave", literal(TOPIC, "Group:audit"));
    private static final AclBinding PAYMENTS_READ_BILLING = record("DeveloperRead:Group:payments", BILLING);
    private static final AclBinding AUDIT_READS_REPORTS = allow("Group:audit", REPORTS, READ);

    private static final String NOT_COUNTED =
            "Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).\n";

    private final FakeAclRecords cluster = new FakeAclRecords();

    // --- group add, group remove ------------------------------------------------------

    @Test
    void groupAddWritesTheMembershipTheAuthorizerHonoursAndGroupRemoveRemovesItAndNothingElse() {
        cluster.records.addAll(List.of(PAYMENTS_READ_BILLING, allow("Group:payments", REPORTS, READ),
                record("Member:User:carol", literal(TOPIC, "Group:audit"))));
        Set<AclBinding> before = new HashSet<>(cluster.records);
        assertFalse(authorizerAllows(cluster.records, "alice", READ, ORDERS));
        assertFalse(authorizerAllows(cluster.records, "alice", READ, REPORTS));

        Commands.Ran added = run(cluster, "group", "add", "--group", "payments", "--member", "User:alice");

        assertEquals(new Commands.Ran(0, "Added User:alice to Group:payments\n", ""), added);
        Set<AclBinding> withIt = new HashSet<>(before);
        withIt.add(ALICE_IN_PAYMENTS);
        assertEquals(withIt, cluster.records);
        assertEquals(new BindingCodec.Membership(new KafkaPrincipal("User", "alice"),
                new KafkaPrincipal("Group", "payments")), BindingCodec.decode(ALICE_IN_PAYMENTS));
        assertTrue(authorizerAllows(cluster.records, "alice", READ, ORDERS), "a role bound to her group");
        assertTrue(authorizerAllows(cluster.records, "alice", READ, REPORTS), "an ACL of her group");
        assertFalse(authorizerAllows(cluster.records, "alice", WRITE, ORDERS));
        assertFalse(authorizerAllows(cluster.records, "bob", READ, ORDERS), "he is not in the group");
        assertFalse(authorizerAllows(cluster.records, "bob", READ, REPORTS), "he is not in the group");
        assertFalse(authorizerAllows(cluster.records, "carol", READ, ORDERS), "she is in another group");

        Commands.Ran removed = run(cluster, "group", "remove", "--group=payments", "--member=User:alice");

        assertEquals(new Commands.Ran(0, "Removed User:alice from Group:payments\n", ""), removed);
        assertEquals(before, cluster.records);
        assertFalse(authorizerAllows(cluster.records, "alice", READ, ORDERS));
        assertFalse(authorizerAllows(cluster.records, "alice", READ, REPORTS));
    }

    @Test
    void groupAddOfWhoIsInTheGroupAlreadyChangesNothing() {
        cluster.records.add(ALICE_IN_PAYMENTS);

        Commands.Ran added = run(cluster, "group", "add", "--group", "payments", "--member", "User:alice");

        assertEquals(new Commands.Ran(0, "Added User:alice to Group:payments\n", ""), added);
        assertEquals(Set.of(ALICE_IN_PAYMENTS), cluster.records);
    }

    @Test
    void groupRemoveLeavesAMembershipThatDiffersInOneThingAndFailsWhenThereIsNone() {
        cluster.records.addAll(List.of(CAROL_IN_PAYMENTS, ALICE_IN_AUDIT, PAYMENTS_READ_BILLING,
                record("DeveloperRead:User:alice", literal(TOPIC, "payments")),
                allow("Group:payments", REPORTS, READ)));
        Set<AclBinding> before = new HashSet<>(cluster.records);

        Commands.Ran removed = run(cluster, "group", "remove", "--group", "payments", "--member", "User:alice");

        assertEquals(new Commands.Ran(1, "", "error: no such membership: User:alice in Group:payments\n"), removed);
        assertEquals(before, cluster.records);
        assertEquals(0, cluster.created);
    }

    // --- group list -------------------------------------------------------------------

    @Test
    void groupListPrintsEachMembershipOnALineAndNothingElse() {
        cluster.records.addAll(List.of(ALICE_IN_PAYMENTS, CAROL_IN_PAYMENTS, DAVE_IN_AUDIT, PAYMENTS_READ_BILLING,
                AUDIT_READS_REPORTS, allow("User:bob", ORDERS, READ)));

        assertEquals(new Commands.Ran(0, """
                GROUP     MEMBER
                audit     User:dave
                payments  User:alice
                payments  User:carol
                """, ""), run(cluster, "group", "list"));
        assertEquals(new Commands.Ran(0, """
                GROUP  MEMBER
                audit  User:dave
                """, ""), run(cluster, "group", "list", "--group", "audit"));
        assertEquals(new Commands.Ran(0, "Group:nobody has no members.\n", ""),
                run(cluster, "group", "list", "--group", "nobody"));
    }

    @Test
    void groupListAsJsonIsOneObjectForEachMembershipWithGroupAndMember() {
        assertEquals(new Commands.Ran(0, "No groups.\n", ""), run(cluster, "group", "list"));
        assertEquals(new Commands.Ran(0, "[]\n", ""), run(cluster, "group", "list", "--output", "json"));
        cluster.records.addAll(List.of(ALICE_IN_PAYMENTS, DAVE_IN_AUDIT, PAYMENTS_READ_BILLING,
                record("Member:User:\"eve\"", literal(TOPIC, "Group:back\\office"))));

        assertEquals(new Commands.Ran(0, """
                [
                  {"group":"audit","member":"User:dave"},
                  {"group":"back\\\\office","member":"User:\\"eve\\""},
                  {"group":"payments","member":"User:alice"}
                ]
                """, ""), run(cluster, "group", "list", "--output", "json"));
        assertEquals(new Commands.Ran(0, """
                [
                  {"group":"payments","member":"User:alice"}
                ]
                """, ""), run(cluster, "group", "list", "--output=json", "--group=payments"));
    }

    @Test
    void groupListLeavesOutARecordThatIsNotAMembershipAndSaysSo() {
        cluster.records.addAll(List.of(ALICE_IN_PAYMENTS,
                record("Member:Group:billing", literal(TOPIC, "Group:payments"))));

        Commands.Ran ran = run(cluster, "group", "list");

        assertEquals("""
                GROUP     MEMBER
                payments  User:alice
                """, ran.out());
        assertEquals("warning: not a group membership, puts nobody in a group: Member:Group:billing on topic"
                + " Group:payments: the member is a group, and a group is not a member of a group\n", ran.err());
    }

    // --- usage ------------------------------------------------------------------------

    static Stream<Arguments> wrongUsage() {
        return Stream.of(
                Arguments.of(List.of("group", "add", "--member", "User:alice"), RbacCommand.Verb.GROUP_ADD,
                        "--group is required"),
                Arguments.of(List.of("group", "add", "--group", "payments"), RbacCommand.Verb.GROUP_ADD,
                        "--member is required"),
                Arguments.of(List.of("group", "add", "--group", "", "--member", "User:alice"),
                        RbacCommand.Verb.GROUP_ADD, "The name of the group is empty"),
                Arguments.of(List.of("group", "add", "--group", "Group:payments", "--member", "User:alice"),
                        RbacCommand.Verb.GROUP_ADD,
                        "--group is the name of the group, without Group: in front: payments"),
                Arguments.of(List.of("group", "add", "--group", "payments", "--member", "alice"),
                        RbacCommand.Verb.GROUP_ADD,
                        "Not a membership: the member is not <type>:<name>, as in Member:User:alice"),
                Arguments.of(List.of("group", "add", "--group", "payments", "--member", "Group:billing"),
                        RbacCommand.Verb.GROUP_ADD,
                        "Not a membership: the member is a group, and a group is not a member of a group"),
                Arguments.of(List.of("group", "add", "--group", "payments", "--member", "User:alice", "--role",
                        "DeveloperRead"), RbacCommand.Verb.GROUP_ADD, "Unknown option: --role"),
                Arguments.of(List.of("group", "remove", "--group", "payments"), RbacCommand.Verb.GROUP_REMOVE,
                        "--member is required"),
                Arguments.of(List.of("group", "remove", "--group", "payments", "--member", "Group:billing"),
                        RbacCommand.Verb.GROUP_REMOVE,
                        "Not a membership: the member is a group, and a group is not a member of a group"),
                Arguments.of(List.of("group", "list", "--output", "yaml"), RbacCommand.Verb.GROUP_LIST,
                        "--output is text or json, not yaml"),
                Arguments.of(List.of("group", "list", "--group", "Group:payments"), RbacCommand.Verb.GROUP_LIST,
                        "--group is the name of the group, without Group: in front: payments"),
                Arguments.of(List.of("group", "list", "--member", "User:alice"), RbacCommand.Verb.GROUP_LIST,
                        "Unknown option: --member"));
    }

    @ParameterizedTest
    @MethodSource("wrongUsage")
    void wrongUsagePrintsWhatIsWrongAndTheUsageOfThatVerbAndExitsWithTwo(List<String> command,
            RbacCommand.Verb verb, String wrong) {
        cluster.records.add(ALICE_IN_PAYMENTS);

        Commands.Ran ran = run(cluster, command.toArray(new String[0]));

        assertEquals(new Commands.Ran(2, "", wrong + "\n\n" + verb.usage()), ran);
        assertEquals(0, cluster.described + cluster.created + cluster.deleted);
        assertFalse(cluster.closed, "the cluster is not reached");
        assertEquals(Set.of(ALICE_IN_PAYMENTS), cluster.records);
    }

    @Test
    void groupWithoutAddRemoveOrListIsWrongUsageAndPrintsTheUsageOfTheThree() {
        String usage = RbacCommand.Verb.GROUP_ADD.usage() + "\n" + RbacCommand.Verb.GROUP_REMOVE.usage() + "\n"
                + RbacCommand.Verb.GROUP_LIST.usage();

        assertEquals(new Commands.Ran(2, "", "group is followed by add, remove or list\n\n" + usage),
                Commands.ran(cluster, "group"));
        assertEquals(new Commands.Ran(2, "", "group is followed by add, remove or list, not by delete\n\n" + usage),
                run(cluster, "group", "delete", "--group", "payments", "--member", "User:alice"));
        assertEquals(new Commands.Ran(0, usage, ""), Commands.ran(cluster, "group", "--help"));
        assertEquals(0, cluster.described + cluster.created + cluster.deleted);
    }

    // --- who-can ----------------------------------------------------------------------

    @Test
    void whoCanNamesWhoHoldsARoleThroughAGroupAndWhoAnAclOfAGroupAllowsWithTheGroup() {
        cluster.records.addAll(List.of(PAYMENTS_READ_BILLING, ALICE_IN_PAYMENTS, CAROL_IN_PAYMENTS, DAVE_IN_AUDIT,
                allow("Group:audit", ORDERS, READ), record("DeveloperRead:User:bob", BILLING),
                acl("User:carol", ORDERS, READ, "*", DENY).toBinding(),
                record("Member:User:erin", literal(TOPIC, "Group:idle")),
                record("DeveloperRead:Group:empty", BILLING)));

        assertEquals(new Commands.Ran(0, """
                Read on topic billing.orders:

                User:alice  allowed
                    allowed by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
                User:bob  allowed
                    allowed by binding DeveloperRead to User:bob on prefix billing.
                User:carol  denied
                    denied by ACL deny User:carol Read on topic billing.orders
                    without that, allowed by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
                User:dave  allowed
                    allowed by ACL allow Group:audit Read on topic billing.orders, as a member of Group:audit

                """ + NOT_COUNTED, ""), run(cluster, "who-can", "--operation", "Read", "--topic", "billing.orders"));
    }

    @Test
    void whoCanNamesNoGroupAndNobodyForAGroupWithoutMembers() {
        cluster.records.addAll(List.of(PAYMENTS_READ_BILLING, allow("Group:audit", ORDERS, READ)));

        assertEquals(new Commands.Ran(0, """
                Read on topic billing.orders:

                Nobody, by the roles and the ACLs.

                """ + NOT_COUNTED, ""), run(cluster, "who-can", "--operation", "Read", "--topic", "billing.orders"));
    }

    @Test
    void whoCanHonoursTheHostOfAnAclOfAGroup() {
        cluster.records.addAll(List.of(DAVE_IN_AUDIT,
                acl("Group:audit", ORDERS, READ, "192.0.2.7", ALLOW).toBinding()));

        Commands.Ran elsewhere = run(cluster, "who-can", "--operation", "Read", "--topic", "billing.orders");
        Commands.Ran there = run(cluster, "who-can", "--operation", "Read", "--topic", "billing.orders", "--host",
                "192.0.2.7");

        assertTrue(elsewhere.out().contains("Nobody, by the roles and the ACLs.\n"), elsewhere.out());
        assertTrue(elsewhere.out().contains("Some ACLs name a host: asked as from 192.0.2.1;"), elsewhere.out());
        assertTrue(there.out().contains("""
                User:dave  allowed
                    allowed by ACL allow Group:audit Read on topic billing.orders from host 192.0.2.7, as a member of Group:audit
                """), there.out());
    }

    /**
     * The answer is the authorizer's: for every user, operation and
     * resource asked, "who can" says allowed exactly when an authorizer
     * that was fed the same records does, groups and all, and names a
     * reason for it.
     */
    @Test
    void whoCanAnswersWhatTheAuthorizerAnswersWithGroups() throws Exception {
        cluster.records.addAll(List.of(PAYMENTS_READ_BILLING, ALICE_IN_PAYMENTS, CAROL_IN_PAYMENTS, ALICE_IN_AUDIT,
                DAVE_IN_AUDIT, AUDIT_READS_REPORTS, allow("Group:audit", prefixed(GROUP, "billing."), DESCRIBE),
                record("ClusterAdmin:Group:operators", THE_CLUSTER),
                record("Member:User:frank", literal(TOPIC, "Group:operators")),
                record("DeveloperWrite:Group:payments", literal(TRANSACTIONAL_ID, "billing-1")),
                record("DeveloperWrite:User:bob", BILLING),
                allow("User:erin", ORDERS, READ),
                acl("User:carol", ORDERS, READ, "*", DENY).toBinding(),
                acl("User:frank", literal(TOPIC, "reports"), ALL, "*", DENY).toBinding(),
                record("Member:User:gina", literal(TOPIC, "Group:idle")),
                record("Member:Group:audit", literal(TOPIC, "Group:payments"))));
        List<ResourcePattern> resources = List.of(ORDERS, REPORTS, literal(TOPIC, "payments.orders"),
                literal(GROUP, "billing.app"), literal(TRANSACTIONAL_ID, "billing-1"), THE_CLUSTER);
        List<String> users = List.of("alice", "bob", "carol", "dave", "erin", "frank", "gina", "zoe");
        int allowed = 0;
        int throughAGroup = 0;
        try (Access access = new Access(cluster.records, null)) {
            for (ResourcePattern resource : resources) {
                for (AclOperation operation : Role.everyAclOperation()) {
                    Map<String, Boolean> said = new HashMap<>();
                    for (Access.Answer answer : access.whoCan(operation, resource)) {
                        assertEquals("User", answer.principal().getPrincipalType(),
                                () -> "no group is named: " + answer);
                        said.put(answer.principal().getName(), answer.allowed());
                        assertEquals(answer.allowed(), answer.deniedBy().isEmpty(),
                                () -> "a deny ACL is named exactly for who is denied: " + answer);
                        assertFalse(answer.allowedBy().isEmpty(), () -> "a reason is named: " + answer);
                        for (Access.Reason reason : answer.allowedBy()) {
                            if (reason.through() != null) {
                                assertTrue(access.groupsOf(answer.principal()).contains(reason.through()),
                                        () -> "the group is one of the principal's: " + answer);
                                throughAGroup++;
                            }
                        }
                    }
                    for (String user : users) {
                        boolean authorizer = authorizerAllows(cluster.records, user, operation, resource);
                        assertEquals(authorizer, said.getOrDefault(user, false),
                                () -> user + " " + operation + " " + resource);
                        allowed += authorizer ? 1 : 0;
                    }
                }
            }
        }
        int yes = allowed;
        int asked = resources.size() * Role.everyAclOperation().size() * users.size();
        assertTrue(yes > 30 && yes < asked - 30, () -> "both answers are exercised: " + yes + " of " + asked);
        int through = throughAGroup;
        assertTrue(through > 20, () -> "groups are exercised: " + through + " reasons through a group");
    }

    // --- what-can ---------------------------------------------------------------------

    @Test
    void whatCanListsWhatThePrincipalGetsThroughEachOfItsGroups() {
        cluster.records.addAll(List.of(PAYMENTS_READ_BILLING, ALICE_IN_PAYMENTS, ALICE_IN_AUDIT, DAVE_IN_AUDIT,
                AUDIT_READS_REPORTS, allow("User:alice", literal(TOPIC, "orders"), WRITE),
                record("DeveloperRead:Group:idle", prefixed(TOPIC, "payments.")),
                acl("User:alice", literal(TOPIC, "billing.secret"), READ, "*", DENY).toBinding()));

        assertEquals(new Commands.Ran(0, """
                User:alice is in Group:audit, Group:payments.
                User:alice may:
                  topics with prefix billing.             Read, Describe   by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
                  groups with prefix billing.             Read, Describe   by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
                  transactional IDs with prefix billing.  Describe         by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
                  topic orders                            Write, Describe  by ACL allow User:alice Write on topic orders
                  topic reports                           Read, Describe   by ACL allow Group:audit Read on topic reports, as a member of Group:audit
                but not, whatever allows it:
                  topic billing.secret  Read  by ACL deny User:alice Read on topic billing.secret

                """ + NOT_COUNTED, ""), run(cluster, "what-can", "--principal", "User:alice"));
        assertEquals(new Commands.Ran(0, """
                User:dave is in Group:audit.
                User:dave may:
                  topic reports  Read, Describe  by ACL allow Group:audit Read on topic reports, as a member of Group:audit

                """ + NOT_COUNTED, ""), run(cluster, "what-can", "--principal", "User:dave"));
        assertEquals(new Commands.Ran(0, """
                User:bob may do nothing, by the roles and the ACLs.

                """ + NOT_COUNTED, ""), run(cluster, "what-can", "--principal", "User:bob"));
    }

    @Test
    void whatCanOfAGroupIsWhatItGivesItsMembers() {
        cluster.records.addAll(List.of(ALICE_IN_AUDIT, DAVE_IN_AUDIT, AUDIT_READS_REPORTS,
                record("DeveloperRead:Group:idle", literal(TOPIC, "orders"))));

        assertEquals(new Commands.Ran(0, """
                Group:audit has the members User:alice, User:dave.
                A member of Group:audit may:
                  topic reports  Read, Describe  by ACL allow Group:audit Read on topic reports

                """ + NOT_COUNTED, ""), run(cluster, "what-can", "--principal", "Group:audit"));
        assertEquals(new Commands.Ran(0, """
                Group:idle has no members.
                A member of Group:idle may:
                  topic orders             Read, Describe  by binding DeveloperRead to Group:idle on name orders
                  group orders             Read, Describe  by binding DeveloperRead to Group:idle on name orders
                  transactional ID orders  Describe        by binding DeveloperRead to Group:idle on name orders

                """ + NOT_COUNTED, ""), run(cluster, "what-can", "--principal", "Group:idle"));
    }

    /**
     * The lines are the authorizer's answers: on each name asked, the
     * operations it allows a member are the ones the lines of the bindings
     * and the allow ACLs give there, through a group or not, less the ones
     * the lines of the deny ACLs take away there, and no other.
     */
    @Test
    void whatCanSaysOfEachNameWhatTheAuthorizerAnswersThereWithGroups() throws Exception {
        cluster.records.addAll(List.of(ALICE_IN_PAYMENTS, ALICE_IN_AUDIT, DAVE_IN_AUDIT,
                record("DeveloperRead:Group:payments", ORDERS), record("DeveloperWrite:Group:audit", ORDERS),
                record("DeveloperRead:Group:idle", literal(TOPIC, "billing.invoices")),
                allow("Group:audit", literal(TOPIC, "billing.invoices"), READ),
                acl("Group:payments", literal(GROUP, "billing.orders"), ALL, "192.0.2.8", ALLOW).toBinding(),
                acl("User:alice", ORDERS, WRITE, "*", DENY).toBinding(),
                acl("User:dave", literal(TOPIC, "billing.invoices"), ALL, "*", DENY).toBinding()));
        Set<String> all = new HashSet<>();
        Role.everyAclOperation().forEach(operation -> all.add(Wording.operation(operation)));
        Map<String, Set<String>> may = new HashMap<>();
        int throughAGroup = 0;
        try (Access access = new Access(cluster.records, InetAddress.getByName("192.0.2.9"))) {
            KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
            for (Access.Line line : access.granted(alice)) {
                may.computeIfAbsent(line.on(), on -> new HashSet<>())
                        .addAll(line.operations().equals("all") ? all : List.of(line.operations().split(", ")));
                throughAGroup += line.through() == null ? 0 : 1;
            }
            for (Access.Line line : access.denied(alice)) {
                may.computeIfAbsent(line.on(), on -> new HashSet<>())
                        .removeAll(line.operations().equals("all") ? all : List.of(line.operations().split(", ")));
            }
        }
        int allowed = 0;
        for (ResourcePattern resource : List.of(ORDERS, literal(TOPIC, "billing.invoices"),
                literal(GROUP, "billing.orders"), literal(TRANSACTIONAL_ID, "billing.orders"), THE_CLUSTER)) {
            for (AclOperation operation : Role.everyAclOperation()) {
                boolean authorizer = authorizerAllows(cluster.records, "alice", operation, resource, "192.0.2.9");
                assertEquals(authorizer,
                        may.getOrDefault(Wording.resources(resource), Set.of()).contains(Wording.operation(operation)),
                        () -> operation + " on " + resource);
                allowed += authorizer ? 1 : 0;
            }
        }
        assertEquals(Set.of("Read", "Describe"), may.get("topic billing.orders"));
        assertEquals(Set.of("Read", "Describe"), may.get("topic billing.invoices"));
        assertEquals(Set.of("Read", "Describe"), may.get("group billing.orders"));
        assertEquals(2 + 2 + 2 + 2, allowed);
        assertEquals(3 + 2 + 1, throughAGroup, "the lines of the two bindings and of the ACL of her groups");
    }

    // --- a DENY of a group, which nobody may create --------------------------------------

    @Test
    void aDenyOfAGroupThatIsThereAnywayTakesAwayWhatRolesAndGroupsGiveAndNothingAnAclAllowsByName() throws Exception {
        cluster.records.addAll(List.of(PAYMENTS_READ_BILLING, ALICE_IN_PAYMENTS, CAROL_IN_PAYMENTS,
                allow("User:carol", ORDERS, READ), acl("Group:payments", ORDERS, READ, "*", DENY).toBinding()));
        assertFalse(authorizerAllows(cluster.records, "alice", READ, ORDERS));
        assertTrue(authorizerAllows(cluster.records, "carol", READ, ORDERS));
        assertTrue(authorizerAllows(cluster.records, "alice", READ, literal(TOPIC, "billing.invoices")));

        Commands.Ran who = run(cluster, "who-can", "--operation", "Read", "--topic", "billing.orders");
        Commands.Ran what = run(cluster, "what-can", "--principal", "User:alice");

        String warning = "warning: a DENY for a group, takes away only what roles and groups give its members:"
                + " Group:payments on topic billing.orders: a group only adds access, and a DENY for a group cannot"
                + " take away what an ACL allows a member by name; deny the users by name instead\n";
        assertEquals(new Commands.Ran(0, """
                Read on topic billing.orders:

                User:alice  denied
                    denied by ACL deny Group:payments Read on topic billing.orders, as a member of Group:payments
                    without that, allowed by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
                User:carol  allowed
                    allowed by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
                    allowed by ACL allow User:carol Read on topic billing.orders

                """ + NOT_COUNTED, warning), who);
        assertEquals(new Commands.Ran(0, """
                User:alice is in Group:payments.
                User:alice may:
                  topics with prefix billing.             Read, Describe  by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
                  groups with prefix billing.             Read, Describe  by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
                  transactional IDs with prefix billing.  Describe        by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
                and not by a role or a group, only by an ACL that names User:alice:
                  topic billing.orders  Read  by ACL deny Group:payments Read on topic billing.orders, as a member of Group:payments

                """ + NOT_COUNTED, warning), what);
    }

    // --- reading changes nothing --------------------------------------------------------

    static Stream<List<String>> readingVerbs() {
        return Stream.of(
                List.of("group", "list"),
                List.of("group", "list", "--group", "payments", "--output", "json"),
                List.of("list"),
                List.of("who-can", "--operation", "Read", "--topic", "billing.orders"),
                List.of("who-can", "--operation", "Read", "--topic", "reports", "--host", "192.0.2.7"),
                List.of("what-can", "--principal", "User:alice"),
                List.of("what-can", "--principal", "Group:payments"));
    }

    @ParameterizedTest
    @MethodSource("readingVerbs")
    void aVerbThatOnlyReadsCreatesAndDeletesNothingWhereThereAreGroups(List<String> command) {
        cluster.records.addAll(List.of(PAYMENTS_READ_BILLING, ALICE_IN_PAYMENTS, CAROL_IN_PAYMENTS, ALICE_IN_AUDIT,
                AUDIT_READS_REPORTS, acl("User:carol", ORDERS, READ, "*", DENY).toBinding()));
        Set<AclBinding> before = new HashSet<>(cluster.records);

        Commands.Ran ran = run(cluster, command.toArray(new String[0]));

        assertEquals(0, ran.status(), ran.err());
        assertFalse(ran.out().isEmpty());
        assertEquals(1, cluster.described);
        assertEquals(0, cluster.created);
        assertEquals(0, cluster.deleted);
        assertEquals(before, cluster.records);
        assertTrue(cluster.closed);
    }
}
