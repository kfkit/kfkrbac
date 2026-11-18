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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The command, verb by verb, against ACL records held in memory. What it
 * writes is read back by the codec and by a real {@link RbacAuthorizer};
 * what it answers is compared with what that authorizer answers.
 */
class RbacCommandTest {

    private static final ResourcePattern BILLING = prefixed(TOPIC, "billing.");
    private static final ResourcePattern ORDERS = literal(TOPIC, "billing.orders");
    private static final ResourcePattern THE_CLUSTER = literal(CLUSTER, "kafka-cluster");

    private static final String NOT_COUNTED =
            "Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).\n";

    /** What one run of the command left behind. */
    private record Ran(int status, String out, String err) {
    }

    private final FakeAclRecords cluster = new FakeAclRecords();

    /** The properties the command connected with, or {@code null} when it did not connect. */
    private Properties connectedWith;

    // --- AC1: bind, list, the authorizer, unbind -----------------------------------

    @Test
    void whatBindWroteListPrintsAndTheAuthorizerHonoursAndUnbindRemovesItAndNothingElse() {
        cluster.records.add(binding("DeveloperWrite:User:bob", BILLING));
        cluster.records.add(allow("User:carol", ORDERS, READ));
        Set<AclBinding> before = new HashSet<>(cluster.records);
        assertFalse(authorizerAllows("alice", READ, ORDERS));

        Ran bound = run("bind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", "billing.");

        assertEquals(new Ran(0, "Bound DeveloperRead to User:alice on prefix billing.\n", ""), bound);
        Set<AclBinding> withIt = new HashSet<>(before);
        withIt.add(binding("DeveloperRead:User:alice", BILLING));
        assertEquals(withIt, cluster.records);
        assertEquals("""
                ROLE            MEMBER      ON
                DeveloperRead   User:alice  prefix billing.
                DeveloperWrite  User:bob    prefix billing.
                """, run("list").out());
        assertTrue(authorizerAllows("alice", READ, ORDERS));
        assertFalse(authorizerAllows("alice", WRITE, ORDERS));

        Ran unbound = run("unbind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", "billing.");

        assertEquals(new Ran(0, "Unbound DeveloperRead to User:alice on prefix billing.\n", ""), unbound);
        assertEquals(before, cluster.records);
        assertFalse(authorizerAllows("alice", READ, ORDERS));
    }

    static Stream<Arguments> bindings() {
        return Stream.of(
                Arguments.of(List.of("--role", "DeveloperRead", "--member", "User:alice", "--prefix", "billing."),
                        binding("DeveloperRead:User:alice", BILLING), RoleBinding.Target.NAMES, "billing."),
                Arguments.of(List.of("--role=DeveloperWrite", "--member=User:alice", "--name=orders"),
                        binding("DeveloperWrite:User:alice", literal(TOPIC, "orders")), RoleBinding.Target.NAMES,
                        "orders"),
                Arguments.of(List.of("--role", "ResourceOwner", "--member", "User:alice", "--name", "*"),
                        binding("ResourceOwner:User:alice", literal(TOPIC, "*")), RoleBinding.Target.NAMES, "*"),
                Arguments.of(List.of("--role", "DeveloperRead", "--member", "User:alice", "--prefix", "billing.",
                        "--group-only"),
                        binding("DeveloperRead:User:alice", prefixed(GROUP, "billing.")), RoleBinding.Target.GROUPS,
                        "billing."),
                Arguments.of(List.of("--role", "DeveloperWrite", "--member", "User:alice", "--name", "billing-1",
                        "--transactional-id-only"),
                        binding("DeveloperWrite:User:alice", literal(TRANSACTIONAL_ID, "billing-1")),
                        RoleBinding.Target.TRANSACTIONAL_IDS, "billing-1"),
                Arguments.of(List.of("--role", "DeveloperRead", "--member", "User:alice", "--prefix", "orders-",
                        "--subject"),
                        binding("DeveloperRead:User:alice", prefixed(TOPIC, "Subject:orders-")),
                        RoleBinding.Target.SUBJECTS, "orders-"),
                Arguments.of(List.of("--role", "DeveloperManage", "--member", "User:alice", "--name", "billing-sink",
                        "--connector"),
                        binding("DeveloperManage:User:alice", literal(TOPIC, "Connector:billing-sink")),
                        RoleBinding.Target.CONNECTORS, "billing-sink"),
                Arguments.of(List.of("--role", "ClusterAdmin", "--member", "User:alice"),
                        binding("ClusterAdmin:User:alice", THE_CLUSTER), RoleBinding.Target.CLUSTER,
                        "kafka-cluster"),
                Arguments.of(List.of("--role", "DeveloperRead", "--member", "Group:payments", "--prefix", "billing."),
                        binding("DeveloperRead:Group:payments", BILLING), RoleBinding.Target.NAMES, "billing."));
    }

    @ParameterizedTest
    @MethodSource("bindings")
    void bindWritesTheRecordOfTheFormatAndUnbindDeletesThatRecord(List<String> options, AclBinding record,
            RoleBinding.Target target, String name) {
        AclBinding other = binding("ResourceViewer:User:zoe", literal(TOPIC, "elsewhere"));
        cluster.records.add(other);

        assertEquals(0, run("bind", options).status());

        assertEquals(Set.of(other, record), cluster.records);
        RoleBinding read = (RoleBinding) BindingCodec.decode(record);
        assertEquals(target, read.target());
        assertEquals(name, read.name());
        assertEquals(1, cluster.created);
        assertEquals(0, cluster.deleted);

        assertEquals(0, run("unbind", options).status());

        assertEquals(Set.of(other), cluster.records);
    }

    @Test
    void unbindLeavesABindingThatDiffersInOneThing() {
        AclBinding onTheName = binding("DeveloperRead:User:alice", literal(TOPIC, "billing."));
        AclBinding ofBob = binding("DeveloperRead:User:bob", BILLING);
        AclBinding ofAnotherRole = binding("DeveloperWrite:User:alice", BILLING);
        AclBinding ofGroups = binding("DeveloperRead:User:alice", prefixed(GROUP, "billing."));
        AclBinding anAcl = allow("User:alice", BILLING, ALL);
        cluster.records.addAll(List.of(onTheName, ofBob, ofAnotherRole, ofGroups, anAcl,
                binding("DeveloperRead:User:alice", BILLING)));

        assertEquals(0, run("unbind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", "billing.")
                .status());

        assertEquals(Set.of(onTheName, ofBob, ofAnotherRole, ofGroups, anAcl), cluster.records);
    }

    @Test
    void unbindOfABindingThatIsNotThereFailsAndRemovesNothing() {
        cluster.records.add(binding("DeveloperRead:User:bob", BILLING));

        Ran ran = run("unbind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", "billing.");

        assertEquals(new Ran(1, "", "error: no such binding: DeveloperRead to User:alice on prefix billing.\n"),
                ran);
        assertEquals(Set.of(binding("DeveloperRead:User:bob", BILLING)), cluster.records);
    }

    // --- AC2: list ------------------------------------------------------------------

    @Test
    void listPrintsEachBindingOnALineAsABindingAndNoAcl() {
        everyKindOfBinding();
        cluster.records.add(allow("User:carol", ORDERS, READ));

        assertEquals(new Ran(0, """
                ROLE             MEMBER      ON
                ResourceOwner    User:dave   every name
                DeveloperManage  User:alice  connector billing-sink
                DeveloperRead    User:erin   subjects with prefix orders-
                DeveloperRead    User:alice  prefix billing.
                DeveloperWrite   User:bob    name orders
                DeveloperRead    User:alice  groups with prefix billing.
                ClusterAdmin     User:carol  the cluster
                DeveloperWrite   User:bob    transactional ID billing-1
                """, ""), run("list"));
    }

    @Test
    void listFiltersByRoleMemberPrefixAndName() {
        everyKindOfBinding();

        assertEquals("""
                ROLE           MEMBER      ON
                DeveloperRead  User:erin   subjects with prefix orders-
                DeveloperRead  User:alice  prefix billing.
                DeveloperRead  User:alice  groups with prefix billing.
                """, run("list", "--role", "DeveloperRead").out());
        assertEquals("""
                ROLE            MEMBER    ON
                DeveloperWrite  User:bob  name orders
                DeveloperWrite  User:bob  transactional ID billing-1
                """, run("list", "--member", "User:bob").out());
        assertEquals("""
                ROLE           MEMBER      ON
                DeveloperRead  User:alice  prefix billing.
                DeveloperRead  User:alice  groups with prefix billing.
                """, run("list", "--prefix", "billing.").out());
        assertEquals("""
                ROLE            MEMBER    ON
                DeveloperWrite  User:bob  name orders
                """, run("list", "--name", "orders").out());
        assertEquals("""
                ROLE            MEMBER    ON
                DeveloperWrite  User:bob  transactional ID billing-1
                """, run("list", "--role", "DeveloperWrite", "--member", "User:bob", "--name", "billing-1").out());
        assertEquals("No bindings.\n", run("list", "--member", "User:nobody").out());
        assertEquals("No bindings.\n", run("list", "--name", "kafka-cluster").out());
    }

    @Test
    void listAsJsonIsOneObjectForEachBindingWithTheFieldsOfItsRecord() {
        everyKindOfBinding();
        cluster.records.add(allow("User:carol", ORDERS, READ));

        assertEquals(new Ran(0, """
                [
                  {"role":"ResourceOwner","member":"User:dave","resourceType":"TOPIC","name":"*","patternType":"LITERAL"},
                  {"role":"DeveloperManage","member":"User:alice","resourceType":"TOPIC","name":"Connector:billing-sink","patternType":"LITERAL"},
                  {"role":"DeveloperRead","member":"User:erin","resourceType":"TOPIC","name":"Subject:orders-","patternType":"PREFIXED"},
                  {"role":"DeveloperRead","member":"User:alice","resourceType":"TOPIC","name":"billing.","patternType":"PREFIXED"},
                  {"role":"DeveloperWrite","member":"User:bob","resourceType":"TOPIC","name":"orders","patternType":"LITERAL"},
                  {"role":"DeveloperRead","member":"User:alice","resourceType":"GROUP","name":"billing.","patternType":"PREFIXED"},
                  {"role":"ClusterAdmin","member":"User:carol","resourceType":"CLUSTER","name":"kafka-cluster","patternType":"LITERAL"},
                  {"role":"DeveloperWrite","member":"User:bob","resourceType":"TRANSACTIONAL_ID","name":"billing-1","patternType":"LITERAL"}
                ]
                """, ""), run("list", "--output", "json"));
    }

    @Test
    void listAsJsonIsAnEmptyArrayWithoutBindingsAndEscapesWhatJsonMust() {
        assertEquals("[]\n", run("list", "--output", "json").out());

        cluster.records.add(binding("DeveloperRead:User:CN=alice,O=\"Billing\\Ltd\"", literal(GROUP, "a\tb")));

        assertEquals("""
                [
                  {"role":"DeveloperRead","member":"User:CN=alice,O=\\"Billing\\\\Ltd\\"","resourceType":"GROUP","name":"a\\u0009b","patternType":"LITERAL"}
                ]
                """, run("list", "--output", "json").out());
    }

    @Test
    void listSaysWhichRecordLooksLikeABindingAndIsNot() {
        cluster.records.add(binding("DeveloperRead:User:alice", BILLING));
        cluster.records.add(acl("DeveloperRead:User:bob", BILLING, READ, "*", ALLOW).toBinding());

        assertEquals(new Ran(0, """
                ROLE           MEMBER      ON
                DeveloperRead  User:alice  prefix billing.
                """,
                "warning: not a binding, grants nothing: DeveloperRead:User:bob on topics with prefix billing.:"
                        + " the operation is READ, a binding has ALL\n"),
                run("list"));
    }

    @Test
    void listSaysWhichRecordIsNotAMembershipAndWhichDeniesAGroup() {
        cluster.records.add(binding("DeveloperRead:User:alice", BILLING));
        cluster.records.add(binding("Member:User:alice", literal(TOPIC, "Group:payments")));
        cluster.records.add(binding("Member:Group:billing", literal(TOPIC, "Group:payments")));
        cluster.records.add(acl("Group:payments", BILLING, READ, "*", DENY).toBinding());
        cluster.records.add(acl("Group:payments", BILLING, READ, "*", ALLOW).toBinding());

        Ran ran = run("list");

        assertEquals(0, ran.status());
        List<String> warnings = ran.err().lines().toList();
        assertEquals(2, warnings.size(), ran.err());
        assertTrue(warnings.stream().anyMatch(line -> line.startsWith("warning: not a group membership, puts nobody"
                + " in a group: Member:Group:billing on ") && line.endsWith(": the member is a group, and a group"
                        + " is not a member of a group")), ran.err());
        assertTrue(warnings.stream().anyMatch(line -> line.startsWith("warning: a DENY for a group, takes away only"
                + " what roles and groups give its members: Group:payments on ")), ran.err());
    }

    // --- AC3: who-can ---------------------------------------------------------------

    @Test
    void whoCanNamesWhoHoldsARoleWhoAnAclAllowsAndWhomADenyAclStops() {
        accessToBilling();

        assertEquals(new Ran(0, """
                Read on topic billing.orders:

                User:alice  allowed
                    allowed by binding DeveloperRead to User:alice on prefix billing.
                User:bob  allowed
                    allowed by ACL allow User:bob Read on topic billing.orders
                User:carol  denied
                    denied by ACL deny User:carol Read on topic billing.orders
                    without that, allowed by binding DeveloperRead to User:carol on prefix billing.

                """ + NOT_COUNTED, ""),
                run("who-can", "--operation", "Read", "--topic", "billing.orders"));
    }

    @Test
    void whoCanNamesNobodyWhereNoRoleAndNoAclReaches() {
        accessToBilling();

        assertEquals("""
                Write on topic billing.orders:

                Nobody, by the roles and the ACLs.

                """ + NOT_COUNTED, run("who-can", "--operation", "Write", "--topic", "billing.orders").out());
        assertEquals("""
                Read on group billing.orders:

                User:alice  allowed
                    allowed by binding DeveloperRead to User:alice on prefix billing.
                User:carol  allowed
                    allowed by binding DeveloperRead to User:carol on prefix billing.

                """ + NOT_COUNTED, run("who-can", "--operation", "read", "--group", "billing.orders").out());
    }

    @Test
    void whoCanOnTheClusterAndOnATransactionalId() {
        cluster.records.add(binding("ClusterAdmin:User:carol", THE_CLUSTER));
        cluster.records.add(binding("DeveloperWrite:User:bob", prefixed(TRANSACTIONAL_ID, "billing-")));

        assertEquals("""
                AlterConfigs on the cluster:

                User:carol  allowed
                    allowed by binding ClusterAdmin to User:carol on the cluster

                """ + NOT_COUNTED, run("who-can", "--operation", "ALTER_CONFIGS", "--cluster").out());
        assertEquals("""
                Write on transactional ID billing-1:

                User:bob  allowed
                    allowed by binding DeveloperWrite to User:bob on transactional IDs with prefix billing-

                """ + NOT_COUNTED,
                run("who-can", "--operation", "Write", "--transactional-id", "billing-1").out());
    }

    /**
     * The question is the operation on the resource, as an ordinary request
     * asks it. That a resource owner is let through to the ACL API, where
     * what it may bind is decided, does not make it one who can alter or
     * describe the cluster.
     */
    @Test
    void whoCanOnTheClusterIsNotAboutTheRequestsOfTheAclApi() {
        cluster.records.add(binding("ResourceOwner:User:alice", BILLING));
        cluster.records.add(binding("ClusterAdmin:User:carol", THE_CLUSTER));
        cluster.records.add(binding("SystemAdmin:User:dave", THE_CLUSTER));

        assertEquals("""
                Alter on the cluster:

                User:carol  allowed
                    allowed by binding ClusterAdmin to User:carol on the cluster
                User:dave  allowed
                    allowed by binding SystemAdmin to User:dave on the cluster

                """ + NOT_COUNTED, run("who-can", "--operation", "Alter", "--cluster").out());
        assertEquals("""
                Describe on the cluster:

                User:carol  allowed
                    allowed by binding ClusterAdmin to User:carol on the cluster
                User:dave  allowed
                    allowed by binding SystemAdmin to User:dave on the cluster

                """ + NOT_COUNTED, run("who-can", "--operation", "Describe", "--cluster").out());
        assertFalse(authorizerAllows("alice", AclOperation.ALTER, THE_CLUSTER));
        assertTrue(authorizerAllows("carol", AclOperation.ALTER, THE_CLUSTER));
    }

    @Test
    void whoCanPutsWhoeverHasOnlyWhatEveryoneHasInTheLineOfEveryone() {
        cluster.records.add(allow("User:*", prefixed(TOPIC, "public."), READ));
        cluster.records.add(binding("DeveloperWrite:User:alice", prefixed(TOPIC, "public.")));
        cluster.records.add(binding("DeveloperRead:User:bob", prefixed(TOPIC, "public.")));
        cluster.records.add(acl("User:carol", literal(TOPIC, "public.news"), READ, "*", DENY).toBinding());

        assertEquals("""
                Read on topic public.news:

                User:*  allowed
                    allowed by ACL allow User:* Read on topics with prefix public.
                User:bob  allowed
                    allowed by binding DeveloperRead to User:bob on prefix public.
                    allowed by ACL allow User:* Read on topics with prefix public.
                User:carol  denied
                    denied by ACL deny User:carol Read on topic public.news
                    without that, allowed by ACL allow User:* Read on topics with prefix public.

                """ + NOT_COUNTED, run("who-can", "--operation", "Read", "--topic", "public.news").out());
    }

    @Test
    void whatEveryoneMayIsAskedOfAUserNoRecordNamesAndABindingToUserStarIsNobodys() {
        cluster.records.add(allow("User:*", prefixed(TOPIC, "public."), READ));
        cluster.records.add(allow("User:anyone", literal(TOPIC, "billing.orders"), READ));
        cluster.records.add(binding("DeveloperWrite:User:*", prefixed(TOPIC, "public.")));

        assertEquals("""
                User:* may:
                  topics with prefix public.  Read, Describe  by ACL allow User:* Read on topics with prefix public.

                """ + NOT_COUNTED, run("what-can", "--principal", "User:*").out());
        assertEquals("""
                Read on topic billing.orders:

                User:anyone  allowed
                    allowed by ACL allow User:anyone Read on topic billing.orders

                """ + NOT_COUNTED, run("who-can", "--operation", "Read", "--topic", "billing.orders").out());
        assertEquals("""
                Write on topic public.news:

                Nobody, by the roles and the ACLs.

                """ + NOT_COUNTED, run("who-can", "--operation", "Write", "--topic", "public.news").out());
    }

    @Test
    void whoCanAsksFromAnAddressNoAclNamesUnlessToldWhere() {
        cluster.records.add(acl("User:alice", ORDERS, READ, "192.0.2.1", ALLOW).toBinding());
        cluster.records.add(allow("User:bob", ORDERS, READ));

        assertEquals("""
                Read on topic billing.orders:

                User:bob  allowed
                    allowed by ACL allow User:bob Read on topic billing.orders

                Some ACLs name a host: asked as from 192.0.2.2; --host asks as from another address.
                """ + NOT_COUNTED, run("who-can", "--operation", "Read", "--topic", "billing.orders").out());
        assertEquals("""
                Read on topic billing.orders:

                User:alice  allowed
                    allowed by ACL allow User:alice Read on topic billing.orders from host 192.0.2.1
                User:bob  allowed
                    allowed by ACL allow User:bob Read on topic billing.orders

                Some ACLs name a host: asked as from 192.0.2.1; --host asks as from another address.
                """ + NOT_COUNTED,
                run("who-can", "--operation", "Read", "--topic", "billing.orders", "--host", "192.0.2.1").out());
    }

    /**
     * The answer is the authorizer's: for every principal, operation and
     * resource asked, "who can" says allowed exactly when an authorizer
     * that was fed the same records does.
     */
    @Test
    void whoCanAnswersWhatTheAuthorizerAnswers() throws Exception {
        accessToBilling();
        cluster.records.add(binding("ResourceOwner:User:erin", literal(TOPIC, "*")));
        cluster.records.add(binding("ClusterAdmin:User:frank", THE_CLUSTER));
        cluster.records.add(binding("DeveloperWrite:User:bob", prefixed(GROUP, "billing.")));
        cluster.records.add(acl("User:erin", prefixed(TOPIC, "billing."), WRITE, "*", DENY).toBinding());
        cluster.records.add(acl("DeveloperRead:User:gina", BILLING, ALL, "*", DENY).toBinding());
        List<ResourcePattern> resources = List.of(ORDERS, literal(TOPIC, "billing.invoices"),
                literal(TOPIC, "payments.orders"), literal(GROUP, "billing.orders"), literal(GROUP, "other"),
                literal(TRANSACTIONAL_ID, "billing.orders"), THE_CLUSTER);
        int allowed = 0;
        int asked = 0;
        try (Access access = new Access(cluster.records, null)) {
            for (ResourcePattern resource : resources) {
                for (AclOperation operation : Role.everyAclOperation()) {
                    Map<String, Boolean> said = new HashMap<>();
                    for (Access.Answer answer : access.whoCan(operation, resource)) {
                        said.put(answer.principal().getName(), answer.allowed());
                        assertEquals(answer.allowed(), answer.deniedBy().isEmpty(),
                                () -> "a deny ACL is named exactly for who is denied: " + answer);
                        assertFalse(answer.allowedBy().isEmpty(), () -> "a reason is named: " + answer);
                    }
                    for (String user : List.of("alice", "bob", "carol", "dave", "erin", "frank", "gina", "zoe")) {
                        boolean authorizer = authorizerAllows(user, operation, resource);
                        assertEquals(authorizer, said.getOrDefault(user, false),
                                () -> user + " " + operation + " " + resource);
                        allowed += authorizer ? 1 : 0;
                        asked++;
                    }
                }
            }
        }
        assertEquals(7 * 13 * 8, asked);
        int yes = allowed;
        assertTrue(yes > 50 && yes < asked - 50, () -> "both answers are exercised: " + yes + " of " + 7 * 13 * 8);
    }

    // --- AC4: what-can --------------------------------------------------------------

    @Test
    void whatCanListsWhatEachBindingAndAclGivesAndWhatADenyAclTakesAway() {
        cluster.records.add(binding("DeveloperRead:User:alice", BILLING));
        cluster.records.add(binding("DeveloperRead:User:bob", prefixed(TOPIC, "payments.")));
        cluster.records.add(allow("User:alice", literal(TOPIC, "orders"), WRITE));
        cluster.records.add(allow("User:*", prefixed(TOPIC, "public."), DESCRIBE));
        cluster.records.add(allow("User:bob", literal(TOPIC, "invoices"), ALL));
        cluster.records.add(acl("User:alice", literal(TOPIC, "billing.secret"), READ, "*", DENY).toBinding());
        cluster.records.add(acl("User:bob", literal(TOPIC, "billing.orders"), ALL, "*", DENY).toBinding());

        assertEquals(new Ran(0, """
                User:alice may:
                  topics with prefix billing.             Read, Describe   by binding DeveloperRead to User:alice on prefix billing.
                  groups with prefix billing.             Read, Describe   by binding DeveloperRead to User:alice on prefix billing.
                  transactional IDs with prefix billing.  Describe         by binding DeveloperRead to User:alice on prefix billing.
                  topic orders                            Write, Describe  by ACL allow User:alice Write on topic orders
                  topics with prefix public.              Describe         by ACL allow User:* Describe on topics with prefix public.
                but not, whatever allows it:
                  topic billing.secret  Read  by ACL deny User:alice Read on topic billing.secret

                """ + NOT_COUNTED, ""), run("what-can", "--principal", "User:alice"));
    }

    @Test
    void whatCanOfAPrincipalNoRecordNamesIsNothing() {
        cluster.records.add(binding("DeveloperRead:User:alice", BILLING));
        cluster.records.add(acl("User:zoe", ORDERS, ALL, "*", DENY).toBinding());

        assertEquals("""
                User:bob may do nothing, by the roles and the ACLs.

                """ + NOT_COUNTED, run("what-can", "--principal", "User:bob").out());
        assertEquals("""
                User:zoe may do nothing, by the roles and the ACLs.
                It is denied:
                  topic billing.orders  all  by ACL deny User:zoe All on topic billing.orders

                """ + NOT_COUNTED, run("what-can", "--principal", "User:zoe").out());
    }

    @Test
    void whatCanShowsAClusterRoleAndBindingsForSubjectsAndConnectors() {
        cluster.records.add(binding("ClusterAdmin:User:carol", THE_CLUSTER));
        cluster.records.add(binding("DeveloperRead:User:carol", prefixed(TOPIC, "Subject:orders-")));
        cluster.records.add(binding("DeveloperManage:User:carol", literal(TOPIC, "Connector:billing-sink")));

        assertEquals("""
                User:carol may:
                  connector billing-sink        Create, ReadStatus, ReadActiveTopics, Pause, Resume, Scale, ResetActiveTopics                      by binding DeveloperManage to User:carol on connector billing-sink
                  subjects with prefix orders-  Read, ReadCompatibility                                                                            by binding DeveloperRead to User:carol on subjects with prefix orders-
                  every topic                   Create, Delete, Alter, Describe, DescribeConfigs, AlterConfigs                                     by binding ClusterAdmin to User:carol on the cluster
                  the cluster                   Create, Alter, Describe, DescribeConfigs, AlterConfigs                                             by binding ClusterAdmin to User:carol on the cluster
                  every subject                 all                                                                                                by binding ClusterAdmin to User:carol on the cluster
                  every connector               Create, Delete, ReadConfig, ReadStatus, ReadActiveTopics, Pause, Resume, Scale, ResetActiveTopics  by binding ClusterAdmin to User:carol on the cluster

                """ + NOT_COUNTED, run("what-can", "--principal", "User:carol").out());
    }

    /**
     * "What can" is true of the authorizer: on the very name of a line's
     * pattern the authorizer allows the operations the lines give there,
     * less the ones the lines of the deny ACLs take away there, and no
     * other.
     */
    @Test
    void whatCanSaysOfEachNameWhatTheAuthorizerAnswersThere() throws Exception {
        cluster.records.add(binding("DeveloperRead:User:alice", ORDERS));
        cluster.records.add(binding("DeveloperWrite:User:alice", ORDERS));
        cluster.records.add(binding("DeveloperRead:User:bob", ORDERS));
        cluster.records.add(allow("User:alice", literal(TOPIC, "billing.invoices"), READ));
        cluster.records.add(allow("User:bob", literal(TOPIC, "billing.invoices"), ALL));
        cluster.records.add(acl("User:alice", ORDERS, WRITE, "*", DENY).toBinding());
        cluster.records.add(acl("User:alice", literal(GROUP, "billing.orders"), ALL, "192.0.2.9", DENY).toBinding());
        cluster.records.add(acl("User:alice", literal(TRANSACTIONAL_ID, "billing.orders"), ALL, "192.0.2.8", DENY)
                .toBinding());
        cluster.records.add(acl("User:bob", literal(TRANSACTIONAL_ID, "billing.orders"), ALL, "*", DENY).toBinding());
        Set<String> all = new HashSet<>();
        Role.everyAclOperation().forEach(operation -> all.add(Wording.operation(operation)));
        Map<String, Set<String>> may = new HashMap<>();
        try (Access access = new Access(cluster.records, InetAddress.getByName("192.0.2.9"))) {
            KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
            for (Access.Line line : access.granted(alice)) {
                may.computeIfAbsent(line.on(), on -> new HashSet<>())
                        .addAll(line.operations().equals("all") ? all : List.of(line.operations().split(", ")));
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
                boolean authorizer = authorizerAllows("alice", operation, resource, "192.0.2.9");
                assertEquals(authorizer,
                        may.getOrDefault(Wording.resources(resource), Set.of()).contains(Wording.operation(operation)),
                        () -> operation + " on " + resource);
                allowed += authorizer ? 1 : 0;
            }
        }
        assertEquals(Set.of("Read", "Describe"), may.get("topic billing.orders"));
        assertEquals(Set.of(), may.get("group billing.orders"));
        assertEquals(Set.of("Write", "Describe"), may.get("transactional ID billing.orders"));
        assertEquals(2 + 2 + 2, allowed);
    }

    // --- AC5: refused ---------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("everyVerbUsedWell")
    void aCallTheClusterRefusesPrintsItsReasonAndExitsWithOne(List<String> command) {
        cluster.records.add(binding("DeveloperRead:User:alice", BILLING));
        cluster.refusal = AclRecords.Failure.of(new org.apache.kafka.common.errors.ClusterAuthorizationException(
                "User:bob may not bind DeveloperRead on billing.: it owns nothing there"));

        Ran ran = run(command.get(0), command.subList(1, command.size()));

        assertEquals(new Ran(1, "", "error: ClusterAuthorizationException: User:bob may not bind DeveloperRead on"
                + " billing.: it owns nothing there\n"), ran);
        assertEquals(Set.of(binding("DeveloperRead:User:alice", BILLING)), cluster.records);
        assertTrue(cluster.closed);
    }

    // --- AC6: usage -----------------------------------------------------------------

    static Stream<Arguments> wrongUsage() {
        return Stream.of(
                Arguments.of(List.of("bind", "--member", "User:alice", "--prefix", "billing."),
                        "--role is required"),
                Arguments.of(List.of("bind", "--role", "DeveloperRead", "--prefix", "billing."),
                        "--member is required"),
                Arguments.of(List.of("bind", "--role", "Developer", "--member", "User:alice", "--prefix", "billing."),
                        "There is no role Developer: the roles are DeveloperRead, DeveloperWrite, DeveloperManage,"
                                + " ResourceOwner, ResourceViewer, ClusterAdmin, SystemAdmin"),
                Arguments.of(List.of("bind", "--role", "developerread", "--member", "User:alice", "--prefix", "b."),
                        "There is no role developerread: the roles are DeveloperRead, DeveloperWrite,"
                                + " DeveloperManage, ResourceOwner, ResourceViewer, ClusterAdmin, SystemAdmin"),
                Arguments.of(List.of("bind", "--role", "DeveloperRead", "--member", "User:alice"),
                        "DeveloperRead is bound to a name pattern: give --prefix or --name"),
                Arguments.of(List.of("bind", "--role", "ClusterAdmin", "--member", "User:alice", "--prefix", "b."),
                        "Not a binding: ClusterAdmin is bound to the cluster, not to a name pattern"),
                Arguments.of(List.of("bind", "--role", "DeveloperRead", "--member", "alice", "--prefix", "billing."),
                        "Not a binding: the member is not <type>:<name>, as in DeveloperRead:User:alice"),
                Arguments.of(List.of("bind", "--role", "ClusterAdmin", "--member", "alice"),
                        "Not a binding: the member is not <type>:<name>, as in ClusterAdmin:User:alice"),
                Arguments.of(List.of("bind", "--role", "DeveloperRead", "--member", "User:alice", "--name", ""),
                        "Not a binding: the name is empty"),
                Arguments.of(List.of("bind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", "b.",
                        "--name", "b"), "Give --prefix or --name, not both"),
                Arguments.of(List.of("bind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", "b.",
                        "--group-only", "--subject"), "Give one of --group-only, --subject, not 2"),
                Arguments.of(List.of("bind", "--role", "ClusterAdmin", "--member", "User:alice", "--group-only"),
                        "--group-only narrows a pattern: give --prefix or --name"),
                Arguments.of(List.of("bind", "--role", "DeveloperRead", "--member", "User:alice", "--name",
                        "Subject:orders"),
                        "Not the binding asked for: --name Subject:orders would be read as subject orders: a name"
                                + " that starts with Subject: or Connector: is given without it, with --subject or"
                                + " --connector"),
                Arguments.of(List.of("bind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix",
                        "billing.", "--topic-only"), "Unknown option: --topic-only"),
                Arguments.of(List.of("bind", "--role", "DeveloperRead", "--role", "DeveloperWrite"),
                        "--role is given twice"),
                Arguments.of(List.of("bind", "--role"), "--role needs a value"),
                Arguments.of(List.of("bind", "--group-only=yes"), "--group-only takes no value"),
                Arguments.of(List.of("bind", "DeveloperRead"), "Unexpected argument: DeveloperRead"),
                Arguments.of(List.of("unbind", "--role", "DeveloperRead", "--member", "User:alice"),
                        "DeveloperRead is bound to a name pattern: give --prefix or --name"),
                Arguments.of(List.of("list", "--role", "Reader"),
                        "There is no role Reader: the roles are DeveloperRead, DeveloperWrite, DeveloperManage,"
                                + " ResourceOwner, ResourceViewer, ClusterAdmin, SystemAdmin"),
                Arguments.of(List.of("list", "--output", "yaml"), "--output is text or json, not yaml"),
                Arguments.of(List.of("list", "--prefix", "a", "--name", "b"), "Give --prefix or --name, not both"),
                Arguments.of(List.of("who-can", "--topic", "billing.orders"), "--operation is required"),
                Arguments.of(List.of("who-can", "--operation", "Peek", "--topic", "billing.orders"),
                        "There is no operation Peek: the operations are Read, Write, Create, Delete, Alter,"
                                + " Describe, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite,"
                                + " CreateTokens, DescribeTokens, TwoPhaseCommit"),
                Arguments.of(List.of("who-can", "--operation", "All", "--topic", "billing.orders"),
                        "There is no operation All: the operations are Read, Write, Create, Delete, Alter,"
                                + " Describe, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite,"
                                + " CreateTokens, DescribeTokens, TwoPhaseCommit"),
                Arguments.of(List.of("who-can", "--operation", "Read"),
                        "Give one of --topic, --group, --transactional-id, --cluster"),
                Arguments.of(List.of("who-can", "--operation", "Read", "--topic", "a", "--group", "b"),
                        "Give one of --topic, --group, --transactional-id, --cluster"),
                Arguments.of(List.of("who-can", "--operation", "Read", "--topic", ""),
                        "The name of the topic is empty"),
                Arguments.of(List.of("who-can", "--operation", "Read", "--topic", "a", "--host", "not an address"),
                        "--host is an address, not not an address"),
                Arguments.of(List.of("what-can"), "--principal is required"),
                Arguments.of(List.of("what-can", "--principal", "alice"),
                        "--principal is <type>:<name>, as in User:alice, not alice"));
    }

    @ParameterizedTest
    @MethodSource("wrongUsage")
    void wrongUsagePrintsWhatIsWrongAndTheUsageOfThatVerbAndExitsWithTwo(List<String> command, String wrong) {
        cluster.records.add(binding("DeveloperRead:User:alice", BILLING));
        RbacCommand.Verb verb = RbacCommand.Verb.of(command.get(0));

        Ran ran = run(command.get(0), command.subList(1, command.size()));

        assertEquals(new Ran(2, "", wrong + "\n\n" + verb.usage()), ran);
        assertNull(connectedWith, "the cluster is not reached");
        assertEquals(0, cluster.described + cluster.created + cluster.deleted);
    }

    @ParameterizedTest
    @EnumSource(RbacCommand.Verb.class)
    void aVerbWithoutTheClusterToConnectToIsWrongUsage(RbacCommand.Verb verb) {
        List<String> command = everyVerbUsedWell().filter(used -> used.get(0).equals(verb.word)).findFirst()
                .orElseThrow();

        Ran ran = ran(typed(command.get(0), command.subList(1, command.size())));

        assertEquals(new Ran(2, "", "--bootstrap-server is required\n\n" + verb.usage()), ran);
        assertNull(connectedWith);
    }

    @Test
    void noVerbAndAnUnknownVerbAreWrongUsageAndPrintEveryVerb() {
        assertEquals(new Ran(2, "", RbacCommand.help()), ran());
        assertEquals(new Ran(2, "", "Unknown verb: grant\n\n" + RbacCommand.help()),
                ran("grant", "--bootstrap-server", "localhost:9092"));
        assertNull(connectedWith);
    }

    @Test
    void helpDocumentsEveryVerbAndEveryOption() {
        Ran ran = ran("--help");

        assertEquals(new Ran(0, RbacCommand.help(), ""), ran);
        assertTrue(ran.out().contains("kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand"));
        assertTrue(ran.out().contains("Exit status: 0 done, 1 refused by the cluster or failed, 2 wrong usage or"));
        for (RbacCommand.Verb verb : RbacCommand.Verb.values()) {
            assertTrue(ran.out().contains("\n  " + verb.word + " "), verb.word);
            assertTrue(ran.out().contains(verb.usage()), verb.word);
            Set<String> options = new HashSet<>(verb.valued);
            options.addAll(verb.flags);
            options.addAll(List.of("--bootstrap-server", "--command-config", "--help"));
            for (String option : options) {
                assertTrue(verb.usage().contains("\n  " + option + " ") || verb.usage().contains(" " + option + ","),
                        () -> verb.word + " explains " + option);
                assertTrue(verb.usage().substring(0, verb.usage().indexOf("\n\n")).contains(option)
                        || option.equals("--help"), () -> verb.word + " shows " + option + " in its synopsis");
            }
        }
        for (String role : Role.NAMES) {
            assertTrue(RbacCommand.Verb.BIND.usage().contains(role), role);
        }
        assertNull(connectedWith);
    }

    @ParameterizedTest
    @EnumSource(RbacCommand.Verb.class)
    void helpOfAVerbPrintsItsUsageAndDoesNothing(RbacCommand.Verb verb) {
        assertEquals(new Ran(0, verb.usage(), ""), ran(typed(verb.word, List.of("--help"))));
        assertNull(connectedWith);
    }

    // --- AC7: reading changes nothing -------------------------------------------------

    static Stream<List<String>> everyVerbUsedWell() {
        return Stream.of(
                List.of("bind", "--role", "DeveloperRead", "--member", "User:bob", "--prefix", "billing."),
                List.of("unbind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", "billing."),
                List.of("list"),
                List.of("group add", "--group", "payments", "--member", "User:bob"),
                List.of("group remove", "--group", "payments", "--member", "User:alice"),
                List.of("group list"),
                List.of("who-can", "--operation", "Read", "--topic", "billing.orders"),
                List.of("what-can", "--principal", "User:alice"),
                List.of("apply", "--file", "docs/examples/apply.json"),
                List.of("import", "--file", "docs/examples/role-bindings.json", "--format", "role-bindings",
                        "--cluster-id", "cluster-1"));
    }

    static Stream<List<String>> readingVerbs() {
        return Stream.concat(everyVerbUsedWell().filter(command -> !command.get(0).endsWith("bind")
                && !command.get(0).equals("group add") && !command.get(0).equals("group remove")
                && !command.get(0).equals("apply") && !command.get(0).equals("import")), Stream.of(
                List.of("group list", "--group", "payments", "--output", "json"),
                List.of("list", "--output", "json"),
                List.of("list", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", "billing."),
                List.of("who-can", "--operation", "Describe", "--group", "billing.orders", "--host", "192.0.2.7"),
                List.of("who-can", "--operation", "Alter", "--cluster"),
                List.of("who-can", "--operation", "Write", "--transactional-id", "billing-1"),
                List.of("what-can", "--principal", "User:carol", "--host", "192.0.2.7")));
    }

    @ParameterizedTest
    @MethodSource("readingVerbs")
    void aVerbThatOnlyReadsCreatesAndDeletesNothing(List<String> command) {
        accessToBilling();
        everyKindOfBinding();
        cluster.records.add(acl("DeveloperRead:User:bob", BILLING, READ, "*", ALLOW).toBinding());
        Set<AclBinding> before = new HashSet<>(cluster.records);

        Ran ran = run(command.get(0), command.subList(1, command.size()));

        assertEquals(0, ran.status(), ran.err());
        assertFalse(ran.out().isEmpty());
        assertEquals(1, cluster.described);
        assertEquals(0, cluster.created);
        assertEquals(0, cluster.deleted);
        assertEquals(before, cluster.records);
        assertTrue(cluster.closed);
    }

    // --- the connection ---------------------------------------------------------------

    @Test
    void connectsWithTheBootstrapServerAndThePropertiesOfTheCommandConfig(@TempDir Path directory)
            throws IOException {
        Path config = directory.resolve("admin.properties");
        Files.writeString(config, "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\n"
                + "bootstrap.servers=ignored:1\n");

        assertEquals(0, ran("list", "--bootstrap-server", "broker-1:9092,broker-2:9092", "--command-config",
                config.toString()).status());

        Properties expected = new Properties();
        expected.setProperty("security.protocol", "SASL_PLAINTEXT");
        expected.setProperty("sasl.mechanism", "PLAIN");
        expected.setProperty("bootstrap.servers", "broker-1:9092,broker-2:9092");
        assertEquals(expected, connectedWith);
        assertTrue(cluster.closed);
    }

    @Test
    void aCommandConfigThatCannotBeReadIsWrongUsage(@TempDir Path directory) {
        Path missing = directory.resolve("missing.properties");

        Ran ran = ran("list", "--bootstrap-server", "localhost:9092", "--command-config", missing.toString());

        assertEquals(2, ran.status());
        assertTrue(ran.err().startsWith("Cannot read --command-config " + missing + ": "), ran.err());
        assertTrue(ran.err().endsWith(RbacCommand.Verb.LIST.usage()), ran.err());
        assertNull(connectedWith);
    }

    @Test
    void aClusterThatCannotBeReachedAtAllIsAFailure() {
        Ran ran = capture((out, err) -> RbacCommand.run(new String[] {"list", "--bootstrap-server", "localhost:9092"},
                out, err, properties -> {
                    throw new org.apache.kafka.common.KafkaException("Failed to construct kafka admin client",
                            new org.apache.kafka.common.config.ConfigException(
                                    "No resolvable bootstrap urls given in bootstrap.servers"));
                }));

        assertEquals(new Ran(1, "", "error: Failed to construct kafka admin client: No resolvable bootstrap urls"
                + " given in bootstrap.servers\n"), ran);
    }

    // --- helpers ----------------------------------------------------------------------

    /** Alice and carol hold a role on the prefix, an ACL allows bob, a deny ACL stops carol, dave is elsewhere. */
    private void accessToBilling() {
        cluster.records.add(binding("DeveloperRead:User:alice", BILLING));
        cluster.records.add(binding("DeveloperRead:User:carol", BILLING));
        cluster.records.add(allow("User:bob", ORDERS, READ));
        cluster.records.add(acl("User:carol", ORDERS, READ, "*", DENY).toBinding());
        cluster.records.add(binding("DeveloperRead:User:dave", prefixed(TOPIC, "payments.")));
    }

    private void everyKindOfBinding() {
        cluster.records.add(binding("DeveloperRead:User:alice", BILLING));
        cluster.records.add(binding("DeveloperRead:User:alice", prefixed(GROUP, "billing.")));
        cluster.records.add(binding("DeveloperWrite:User:bob", literal(TOPIC, "orders")));
        cluster.records.add(binding("DeveloperWrite:User:bob", literal(TRANSACTIONAL_ID, "billing-1")));
        cluster.records.add(binding("ClusterAdmin:User:carol", THE_CLUSTER));
        cluster.records.add(binding("ResourceOwner:User:dave", literal(TOPIC, "*")));
        cluster.records.add(binding("DeveloperRead:User:erin", prefixed(TOPIC, "Subject:orders-")));
        cluster.records.add(binding("DeveloperManage:User:alice", literal(TOPIC, "Connector:billing-sink")));
    }

    private static AclBinding binding(String roleAndMember, ResourcePattern pattern) {
        return acl(roleAndMember, pattern, ALL, "*", ALLOW).toBinding();
    }

    private static AclBinding allow(String principal, ResourcePattern pattern, AclOperation operation) {
        return acl(principal, pattern, operation, "*", ALLOW).toBinding();
    }

    /** Runs a verb against the cluster of this test. */
    private Ran run(String verb, String... options) {
        return run(verb, List.of(options));
    }

    private Ran run(String verb, List<String> options) {
        List<String> args = new ArrayList<>();
        args.add("--bootstrap-server");
        args.add("localhost:9092");
        args.addAll(options);
        return ran(typed(verb, args));
    }

    /** A verb, which may be two words, and its options, as they are typed. */
    private static String[] typed(String verb, List<String> options) {
        List<String> args = new ArrayList<>(List.of(verb.split(" ")));
        args.addAll(options);
        return args.toArray(new String[0]);
    }

    /** Runs the command exactly as typed. */
    private Ran ran(String... args) {
        cluster.described = 0;
        cluster.closed = false;
        connectedWith = null;
        return capture((out, err) -> RbacCommand.run(args, out, err, properties -> {
            connectedWith = properties;
            return cluster;
        }));
    }

    private interface Command {
        int run(PrintStream out, PrintStream err);
    }

    private static Ran capture(Command command) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int status = command.run(new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
        return new Ran(status, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    private boolean authorizerAllows(String user, AclOperation operation, ResourcePattern resource) {
        return authorizerAllows(user, operation, resource, "127.0.0.1");
    }

    /** What an authorizer answers that a broker fed the records of the cluster of this test. */
    private boolean authorizerAllows(String user, AclOperation operation, ResourcePattern resource, String from) {
        Metrics metrics = new Metrics();
        try (RbacAuthorizer authorizer = new RbacAuthorizer()) {
            authorizer.configure(Map.of());
            authorizer.withPluginMetrics(new PluginMetricsImpl(metrics, Map.of()));
            Map<Uuid, StandardAcl> snapshot = new HashMap<>();
            for (AclBinding record : cluster.records) {
                snapshot.put(Uuid.randomUuid(), StandardAcl.fromAclBinding(record));
            }
            authorizer.loadSnapshot(snapshot);
            authorizer.completeInitialLoad();
            RequestContext context = new RequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user),
                    "billing-1", InetAddress.getByName(from), ApiKeys.FETCH.id);
            return authorizer.authorize(context, List.of(new Action(operation, resource, 1, false, false)))
                    .get(0) == AuthorizationResult.ALLOWED;
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        } finally {
            metrics.close();
        }
    }
}
