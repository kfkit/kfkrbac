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
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.literal;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.prefixed;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * {@code import}, against ACL records held in memory. The example files are
 * the ones of {@code docs/examples/}, and what the command prints for them
 * is what {@code docs/import.md} shows; what it writes is read back by a
 * real {@link RbacAuthorizer}.
 */
class RbacCommandImportTest {

    private static final String BINDINGS = "docs/examples/role-bindings.json";
    private static final String ACLS = "docs/examples/role-acls.json";

    private static final Set<AclBinding> BINDINGS_OF_CLUSTER_1 = Set.of(
            record("DeveloperRead:User:alice", prefixed(TOPIC, "billing.")),
            record("DeveloperWrite:Group:payments", literal(TOPIC, "orders")),
            record("DeveloperWrite:Group:payments", literal(TRANSACTIONAL_ID, "billing-1")),
            record("ResourceOwner:User:bob", prefixed(TOPIC, "billing.")),
            record("ResourceOwner:User:bob", prefixed(TOPIC, "Subject:billing.")),
            record("ClusterAdmin:User:carol", literal(CLUSTER, "kafka-cluster")));

    private final FakeAclRecords cluster = new FakeAclRecords();

    @TempDir
    Path directory;

    // --- AC4: role-bindings ---------------------------------------------------------------

    @Test
    void importOfTheExampleOfRoleBindingsWritesTheBindingsTheDocumentationSaysItWrites() throws IOException {
        String documented = documented("rbac import --file docs/examples/role-bindings.json --format role-bindings"
                + " --cluster-id cluster-1");

        Commands.Ran imported = run(cluster, "import", "--file", BINDINGS, "--format", "role-bindings",
                "--cluster-id", "cluster-1");

        assertEquals(new Commands.Ran(0, documented, ""), imported);
        assertEquals(BINDINGS_OF_CLUSTER_1, cluster.records);
        assertEquals(1, cluster.created);
        assertEquals(0, cluster.described + cluster.deleted);
        assertEquals(documented("rbac list"), run(cluster, "list").out(), "what docs/import.md shows afterwards");
        assertTrue(imported.out().contains("""
                + binding DeveloperRead to User:alice on prefix billing.
                + binding DeveloperWrite to Group:payments on name orders
                + binding DeveloperWrite to Group:payments on transactional ID billing-1
                + binding ResourceOwner to User:bob on prefix billing.
                + binding ResourceOwner to User:bob on subjects with prefix billing.
                + binding ClusterAdmin to User:carol on the cluster
                """), imported.out());
    }

    @Test
    void anUnknownRoleAndAResourceTypeNothingAuthorizesAreInTheReportAndNotInTheCluster() {
        Commands.Ran imported = run(cluster, "import", "--file", BINDINGS, "--format", "role-bindings",
                "--cluster-id", "cluster-1");

        assertTrue(imported.out().contains("skipped [4] (line 35) Auditor to User:dave: there is no role Auditor"
                + " here: the roles are DeveloperRead, DeveloperWrite, DeveloperManage, ResourceOwner,"
                + " ResourceViewer, ClusterAdmin, SystemAdmin\n"), imported.out());
        assertTrue(imported.out().contains("skipped [2].resourcePatterns[2] (line 26) ResourceOwner to User:bob on"
                + " Dashboard billing. PREFIXED: nothing here authorizes the resource type Dashboard\n"),
                imported.out());
        assertTrue(imported.out().contains("; skipped 2;"), imported.out());
        for (AclBinding record : cluster.records) {
            assertFalse(record.entry().principal().contains("dave") || record.entry().principal().contains("Auditor")
                    || record.pattern().name().contains("Dashboard"), record.toString());
        }
        assertFalse(authorizerAllows(cluster.records, "dave", READ, literal(TOPIC, "billing.orders")));
        assertTrue(authorizerAllows(cluster.records, "alice", READ, literal(TOPIC, "billing.orders")));
    }

    @Test
    void clusterIdSelectsTheBindingsOfThatCluster() {
        Commands.Ran other = run(cluster, "import", "--file", BINDINGS, "--format", "role-bindings", "--cluster-id",
                "cluster-2");

        assertEquals(0, other.status(), other.err());
        assertEquals(Set.of(record("DeveloperRead:User:erin", literal(TOPIC, "orders"))), cluster.records);
        assertTrue(other.out().contains("left out [0] (line 2) DeveloperRead to User:alice: it is for the cluster"
                + " cluster-1\n"), other.out());
        assertTrue(other.out().contains("Wrote 1 binding; left out 5 for another cluster.\n"), other.out());
    }

    @Test
    void aFileForSeveralClustersIsNotImportedWithoutSayingWhich() {
        Commands.Ran ran = run(cluster, "import", "--file", BINDINGS, "--format", "role-bindings");

        assertEquals(new Commands.Ran(2, "", "The file has bindings for 2 clusters, cluster-1, cluster-2: say which"
                + " with --cluster-id\n\n" + RbacCommand.Verb.IMPORT.usage()), ran);
        assertEquals(Set.of(), cluster.records);
        assertFalse(cluster.closed, "the cluster is not reached");
    }

    @Test
    void aFileForOneClusterOrForNoneIsImportedWithoutClusterId() throws IOException {
        Path file = file("bindings.json", """
                [
                  {"principal": "User:alice", "role": "DeveloperRead",
                   "resourcePatterns": [{"resourceType": "Topic", "name": "orders", "patternType": "LITERAL"}],
                   "scope": {"clusters": {"kafka-cluster": "cluster-1"}}},
                  {"principal": "User:bob", "role": "SystemAdmin", "resourcePatterns": []},
                  {"principal": "User:carol", "role": "ClusterAdmin",
                   "resourcePatterns": [{"resourceType": "Cluster", "name": "kafka-cluster", "patternType": "LITERAL"}]}
                ]
                """);

        Commands.Ran imported = run(cluster, "import", "--file", file.toString(), "--format=role-bindings");

        assertEquals(new Commands.Ran(0, """
                + binding DeveloperRead to User:alice on name orders
                + binding SystemAdmin to User:bob on the cluster
                + binding ClusterAdmin to User:carol on the cluster
                Wrote 3 bindings.
                A binding on a name or a prefix is for the topics, the groups and the transactional IDs of that name: a pattern for topics is written as one.
                """, ""), imported);
        assertEquals(Set.of(record("DeveloperRead:User:alice", literal(TOPIC, "orders")),
                record("SystemAdmin:User:bob", literal(CLUSTER, "kafka-cluster")),
                record("ClusterAdmin:User:carol", literal(CLUSTER, "kafka-cluster"))), cluster.records);
    }

    @Test
    void patternsForATopicAndAGroupOfTheSameNameAreWrittenAsOneBinding() throws IOException {
        Path file = file("bindings.json", """
                [
                  {"principal": "User:alice", "role": "DeveloperRead", "resourcePatterns": [
                    {"resourceType": "Group", "name": "billing.", "patternType": "PREFIXED"},
                    {"resourceType": "Topic", "name": "billing.", "patternType": "PREFIXED"},
                    {"resourceType": "TransactionalId", "name": "billing.", "patternType": "PREFIXED"},
                    {"resourceType": "Group", "name": "billing.", "patternType": "LITERAL"},
                    {"resourceType": "Group", "name": "payments.", "patternType": "PREFIXED"}]},
                  {"principal": "User:bob", "role": "DeveloperRead", "resourcePatterns": [
                    {"resourceType": "Group", "name": "billing.", "patternType": "PREFIXED"}]}
                ]
                """);

        Commands.Ran imported = run(cluster, "import", "--file", file.toString(), "--format", "role-bindings");

        assertEquals(new Commands.Ran(0, """
                + binding DeveloperRead to User:alice on prefix billing.
                + binding DeveloperRead to User:alice on group billing.
                + binding DeveloperRead to User:alice on groups with prefix payments.
                + binding DeveloperRead to User:bob on groups with prefix billing.
                covered [0].resourcePatterns[0] (line 3) DeveloperRead to User:alice on Group billing. PREFIXED: within binding DeveloperRead to User:alice on prefix billing.
                covered [0].resourcePatterns[2] (line 5) DeveloperRead to User:alice on TransactionalId billing. PREFIXED: within binding DeveloperRead to User:alice on prefix billing.
                Wrote 4 bindings; 2 covered by another.
                A binding on a name or a prefix is for the topics, the groups and the transactional IDs of that name: a pattern for topics is written as one.
                """, ""), imported);
        assertEquals(Set.of(record("DeveloperRead:User:alice", prefixed(TOPIC, "billing.")),
                record("DeveloperRead:User:alice", literal(GROUP, "billing.")),
                record("DeveloperRead:User:alice", prefixed(GROUP, "payments.")),
                record("DeveloperRead:User:bob", prefixed(GROUP, "billing."))), cluster.records);
    }

    static Stream<Arguments> bindingsThatCannotBeMapped() {
        String topic = "[{\"resourceType\": \"Topic\", \"name\": \"orders\", \"patternType\": \"LITERAL\"}]";
        return Stream.of(
                Arguments.of("{\"principal\": \"alice\", \"role\": \"DeveloperRead\", \"resourcePatterns\": " + topic
                        + "}", "[0] (line 1) DeveloperRead to alice: the principal is neither User:<name> nor"
                                + " Group:<name>"),
                Arguments.of("{\"principal\": \"Team:billing\", \"role\": \"DeveloperRead\", \"resourcePatterns\": "
                        + topic + "}", "[0] (line 1) DeveloperRead to Team:billing: the principal is neither"
                                + " User:<name> nor Group:<name>"),
                Arguments.of("{\"principal\": \"User:alice\", \"role\": \"developerread\", \"resourcePatterns\": "
                        + topic + "}", "[0] (line 1) developerread to User:alice: there is no role developerread"
                                + " here: the roles are DeveloperRead, DeveloperWrite, DeveloperManage,"
                                + " ResourceOwner, ResourceViewer, ClusterAdmin, SystemAdmin"),
                Arguments.of("{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\"}",
                        "[0] (line 1) DeveloperRead to User:alice: DeveloperRead is bound to a name pattern, and"
                                + " this binding has no resource pattern"),
                Arguments.of("{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\", \"resourcePatterns\": "
                        + topic + ", \"scope\": {\"clusters\": {\"kafka-cluster\": \"cluster-1\","
                        + " \"another-cluster\": \"another-1\"}}}",
                        "[0] (line 1) DeveloperRead to User:alice: its scope has more than the Kafka cluster"
                                + " (another-cluster), and a binding here cannot be narrowed to that: it is for"
                                + " everything that reads the bindings of the Kafka cluster. Take that out of the"
                                + " file to import it so"),
                Arguments.of("{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\", \"resourcePatterns\": "
                        + topic + ", \"scope\": {\"path\": [\"org-1\"]}}",
                        "[0] (line 1) DeveloperRead to User:alice: its scope has more than the Kafka cluster"
                                + " (path), and a binding here cannot be narrowed to that: it is for everything"
                                + " that reads the bindings of the Kafka cluster. Take that out of the file to"
                                + " import it so"),
                Arguments.of("{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\", \"resourcePatterns\": "
                        + topic.replace("LITERAL", "MATCH") + "}",
                        "[0].resourcePatterns[0] (line 1) DeveloperRead to User:alice on Topic orders MATCH: the"
                                + " pattern type is neither LITERAL nor PREFIXED"),
                Arguments.of("{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\", \"resourcePatterns\": "
                        + topic.replace("Topic", "TOPIC") + "}",
                        "[0].resourcePatterns[0] (line 1) DeveloperRead to User:alice on TOPIC orders LITERAL:"
                                + " nothing here authorizes the resource type TOPIC"),
                Arguments.of("{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\", \"resourcePatterns\": "
                        + topic.replace("Topic", "Cluster") + "}",
                        "[0].resourcePatterns[0] (line 1) DeveloperRead to User:alice on Cluster orders LITERAL:"
                                + " the cluster is the literal name kafka-cluster"),
                Arguments.of("{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\", \"resourcePatterns\": "
                        + topic.replace("Topic", "Cluster").replace("orders", "kafka-cluster") + "}",
                        "[0].resourcePatterns[0] (line 1) DeveloperRead to User:alice on Cluster kafka-cluster"
                                + " LITERAL: DeveloperRead is bound to a name pattern, not to the cluster"),
                Arguments.of("{\"principal\": \"User:alice\", \"role\": \"ClusterAdmin\", \"resourcePatterns\": "
                        + topic + "}",
                        "[0].resourcePatterns[0] (line 1) ClusterAdmin to User:alice on Topic orders LITERAL:"
                                + " ClusterAdmin is bound to the cluster, not to a name pattern"),
                Arguments.of("{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\", \"resourcePatterns\": "
                        + topic.replace("orders", "Group:payments") + "}",
                        "[0].resourcePatterns[0] (line 1) DeveloperRead to User:alice on Topic Group:payments"
                                + " LITERAL: the name starts with Group:, which is reserved for the groups of"
                                + " membership records"),
                Arguments.of("{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\", \"resourcePatterns\": "
                        + topic.replace("orders", "Subject:orders") + "}",
                        "[0].resourcePatterns[0] (line 1) DeveloperRead to User:alice on Topic Subject:orders"
                                + " LITERAL: as a record it would be read as subject orders"),
                Arguments.of("{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\", \"resourcePatterns\": "
                        + topic.replace("orders", "") + "}",
                        "[0].resourcePatterns[0] (line 1) DeveloperRead to User:alice on Topic  LITERAL: the name"
                                + " is empty"));
    }

    @ParameterizedTest
    @MethodSource("bindingsThatCannotBeMapped")
    void aBindingThatCannotBeMappedIsSkippedAndReportedAndNeverGuessed(String binding, String skipped)
            throws IOException {
        Path file = file("bindings.json", "[" + binding + "]");

        Commands.Ran imported = run(cluster, "import", "--file", file.toString(), "--format", "role-bindings");

        assertEquals(new Commands.Ran(0, "skipped " + skipped + "\nWrote 0 bindings; skipped 1.\n", ""), imported);
        assertEquals(Set.of(), cluster.records);
        assertEquals(0, cluster.created, "nothing to write is not written");
    }

    // --- AC5: role-acls -------------------------------------------------------------------

    @Test
    void importOfTheExampleOfRoleAclsMakesAGroupForEachRoleWithItsMembersAndAclsThatNameIt() throws IOException {
        String documented = documented("rbac import --file docs/examples/role-acls.json --format role-acls");
        assertFalse(authorizerAllows(cluster.records, "alice", READ, literal(TOPIC, "orders")));

        Commands.Ran imported = run(cluster, "import", "--file", ACLS, "--format", "role-acls");

        assertEquals(new Commands.Ran(0, documented, ""), imported);
        assertEquals(Set.of(record("Member:User:alice", literal(TOPIC, "Group:orders-readers")),
                record("Member:User:bob", literal(TOPIC, "Group:orders-readers")),
                allow("Group:orders-readers", literal(TOPIC, "orders"), READ),
                allow("Group:orders-readers", prefixed(GROUP, "orders-"), READ)), cluster.records);
        assertEquals("""
                GROUP           MEMBER
                orders-readers  User:alice
                orders-readers  User:bob
                """, run(cluster, "group", "list").out());
        assertEquals(documented("rbac group list"), run(cluster, "group", "list").out(),
                "what docs/import.md shows afterwards");
        assertEquals(documented("rbac who-can --operation Read --topic orders"),
                run(cluster, "who-can", "--operation", "Read", "--topic", "orders").out(),
                "what docs/import.md shows afterwards");
        for (String member : List.of("alice", "bob")) {
            assertTrue(authorizerAllows(cluster.records, member, READ, literal(TOPIC, "orders")), member);
            assertTrue(authorizerAllows(cluster.records, member, READ, literal(GROUP, "orders-app")), member);
            assertFalse(authorizerAllows(cluster.records, member, WRITE, literal(TOPIC, "orders")), member);
            assertFalse(authorizerAllows(cluster.records, member, READ, literal(TOPIC, "billing.orders")), member);
        }
        assertFalse(authorizerAllows(cluster.records, "dave", READ, literal(TOPIC, "orders")), "not a member");
    }

    @Test
    void aRoleWithADenyAclIsReportedAndNotWritten() {
        Commands.Ran imported = run(cluster, "import", "--file", ACLS, "--format", "role-acls");

        assertTrue(imported.out().contains("skipped [1] (line 10) role billing-writers: it has a DENY ACL"
                + " ([1].acls[1] (line 15)), and a group cannot be denied: a DENY ACL names the users; nothing of the"
                + " role is written\n"), imported.out());
        for (AclBinding record : cluster.records) {
            assertFalse(record.entry().principal().contains("billing-writers")
                    || record.pattern().name().contains("billing-writers")
                    || record.entry().principal().contains("carol"), record.toString());
        }
        assertFalse(authorizerAllows(cluster.records, "carol", WRITE, literal(TOPIC, "billing.orders")));
    }

    static Stream<Arguments> partsOfARoleThatCannotBeMapped() {
        String acl = "{\"resourceType\": \"TOPIC\", \"resourceName\": \"orders\", \"patternType\": \"LITERAL\","
                + " \"operation\": \"READ\", \"permission\": \"ALLOW\", \"host\": \"*\"}";
        return Stream.of(
                Arguments.of("\"\"", "[\"User:alice\"]", acl,
                        "[0] (line 1) role : its name is empty, and it would be the name of the group; nothing of the"
                                + " role is written", 0),
                Arguments.of("\"readers\"", "[\"User:alice\"]", acl.replace("ALLOW", "Maybe"),
                        "[0] (line 1) role readers: the permission Maybe of an ACL of it ([0].acls[0] (line 1)) is"
                                + " neither ALLOW nor DENY; nothing of the role is written", 0),
                Arguments.of("\"readers\"", "[\"User:alice\", \"Group:billing\"]", acl,
                        "[0].members[1] (line 1) member Group:billing of the role readers: the member is a group,"
                                + " and a group is not a member of a group", 2),
                Arguments.of("\"readers\"", "[\"alice\"]", acl,
                        "[0].members[0] (line 1) member alice of the role readers: the member is not <type>:<name>,"
                                + " as in Member:User:alice", 1),
                Arguments.of("\"readers\"", "[\"User:alice\"]", acl.replace("TOPIC", "Dashboard"),
                        "[0].acls[0] (line 1) ACL ALLOW READ on Dashboard orders LITERAL of the role readers:"
                                + " Dashboard is not a resource type of an ACL", 1),
                Arguments.of("\"readers\"", "[\"User:alice\"]", acl.replace("READ", "Peek"),
                        "[0].acls[0] (line 1) ACL ALLOW Peek on TOPIC orders LITERAL of the role readers: Peek is"
                                + " not an operation of an ACL", 1),
                Arguments.of("\"readers\"", "[\"User:alice\"]", acl.replace("READ", "ANY"),
                        "[0].acls[0] (line 1) ACL ALLOW ANY on TOPIC orders LITERAL of the role readers: ANY is not"
                                + " an operation of an ACL", 1),
                Arguments.of("\"readers\"", "[\"User:alice\"]", acl.replace("LITERAL", "MATCH"),
                        "[0].acls[0] (line 1) ACL ALLOW READ on TOPIC orders MATCH of the role readers: the pattern"
                                + " type is neither LITERAL nor PREFIXED", 1),
                Arguments.of("\"readers\"", "[\"User:alice\"]", acl.replace("\"orders\"", "\"\""),
                        "[0].acls[0] (line 1) ACL ALLOW READ on TOPIC  LITERAL of the role readers: the resource"
                                + " name is empty", 1));
    }

    @ParameterizedTest
    @MethodSource("partsOfARoleThatCannotBeMapped")
    void whatCannotBeMappedOfARoleIsSkippedAndReported(String name, String members, String acl, String skipped,
            int written) throws IOException {
        Path file = file("roles.json", "[{\"name\": " + name + ", \"members\": " + members + ", \"acls\": [" + acl
                + "]}]");

        Commands.Ran imported = run(cluster, "import", "--file", file.toString(), "--format", "role-acls");

        assertEquals(0, imported.status(), imported.err());
        assertTrue(imported.out().contains("skipped " + skipped + "\n"), imported.out());
        assertTrue(imported.out().endsWith("Wrote " + written + " record" + (written == 1 ? "" : "s")
                + "; skipped 1.\n"), imported.out());
        assertEquals(written, cluster.records.size(), cluster.records.toString());
    }

    @Test
    void anAclOfARoleIsWrittenAsKafkaReadsItsNamesWithItsHost() throws IOException {
        Path file = file("roles.json", """
                [{"name": "config-readers", "members": [], "acls": [
                  {"resourceType": "topic", "resourceName": "*", "patternType": "literal",
                   "operation": "describe_configs", "permission": "allow", "host": "192.0.2.7"}]}]
                """);

        Commands.Ran imported = run(cluster, "import", "--file", file.toString(), "--format", "role-acls");

        assertEquals(new Commands.Ran(0, """
                + ACL allow Group:config-readers DescribeConfigs on every topic from host 192.0.2.7
                Wrote 1 record.
                """, ""), imported);
        assertEquals(Set.of(RbacAuthorizerTest.acl("Group:config-readers", literal(TOPIC, "*"), DESCRIBE_CONFIGS,
                "192.0.2.7", org.apache.kafka.common.acl.AclPermissionType.ALLOW).toBinding()), cluster.records);
    }

    // --- AC6: --dry-run -------------------------------------------------------------------

    static Stream<List<String>> dryRuns() {
        return Stream.of(
                List.of("import", "--file", BINDINGS, "--format", "role-bindings", "--cluster-id", "cluster-1",
                        "--dry-run"),
                List.of("import", "--file", ACLS, "--format", "role-acls", "--dry-run"));
    }

    @ParameterizedTest
    @MethodSource("dryRuns")
    void aDryRunPrintsWhatWouldBeWrittenAndSkippedAndWritesNothing(List<String> command) throws IOException {
        cluster.records.add(record("DeveloperRead:User:zoe", literal(TOPIC, "orders")));
        List<String> forReal = command.subList(0, command.size() - 1);
        FakeAclRecords another = new FakeAclRecords();
        String written = run(another, forReal.toArray(new String[0])).out();

        Commands.Ran dry = run(cluster, command.toArray(new String[0]));

        assertEquals(0, dry.status(), dry.err());
        assertEquals(Set.of(record("DeveloperRead:User:zoe", literal(TOPIC, "orders"))), cluster.records);
        assertEquals(0, cluster.created + cluster.deleted + cluster.described, "a dry run issues no call at all");
        assertTrue(dry.out().contains("\nskipped "), dry.out());
        assertTrue(dry.out().contains("Nothing was changed."), dry.out());
        assertEquals(written.replace("Wrote ", "Would write ").replaceFirst("(?m)^(Would write .*)\\.$",
                "$1. Nothing was changed."), dry.out(), "the same report as the run that writes");
        assertFalse(another.records.isEmpty());
        if (command.contains("role-acls")) {
            assertEquals(Commands.documented(Path.of("docs/command.md"),
                    "rbac import --file docs/examples/role-acls.json --format role-acls --dry-run"), dry.out(),
                    "docs/command.md shows this run");
        }
    }

    // --- AC7: files that cannot be read -----------------------------------------------------

    static Stream<Arguments> filesThatCannotBeImported() {
        String acl = "{\"resourceType\": \"TOPIC\", \"resourceName\": \"orders\", \"patternType\": \"LITERAL\","
                + " \"operation\": \"READ\", \"permission\": \"ALLOW\", \"host\": \"*\"}";
        return Stream.of(
                Arguments.of("role-bindings", "[{\"principal\": \"User:alice\", ]",
                        "line 1, column 30: not valid JSON: Unexpected character (']' (code 93))..."),
                Arguments.of("role-bindings", "{}", "line 1, column 1: is an object, and must be a list"),
                Arguments.of("role-bindings", "[{\"role\": \"DeveloperRead\"}]",
                        "line 1, column 2: [0]: \"principal\" is required"),
                Arguments.of("role-bindings", "[\n  {\"principal\": \"User:alice\", \"role\": \"DeveloperRead\"},\n"
                        + "  {\"principal\": \"User:bob\"}\n]", "line 3, column 3: [1]: \"role\" is required"),
                Arguments.of("role-bindings", "[{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\","
                        + " \"resourcePatterns\": [{\"resourceType\": \"Topic\", \"patternType\": \"LITERAL\"}]}]",
                        "line 1, column 76: [0].resourcePatterns[0]: \"name\" is required"),
                Arguments.of("role-bindings", "[{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\","
                        + " \"resourcePatterns\": {}}]",
                        "line 1, column 75: [0].resourcePatterns: is an object, and must be a list"),
                Arguments.of("role-bindings", "[{\"principal\": \"User:alice\", \"role\": \"DeveloperRead\","
                        + " \"scope\": {\"clusters\": {\"kafka-cluster\": 7}}}]",
                        "line 1, column 95: [0].scope.clusters.kafka-cluster: is a number, and must be a string"),
                Arguments.of("role-acls", "[{\"members\": [], \"acls\": []}]",
                        "line 1, column 2: [0]: \"name\" is required"),
                Arguments.of("role-acls", "[{\"name\": \"readers\", \"acls\": []}]",
                        "line 1, column 2: [0]: \"members\" is required"),
                Arguments.of("role-acls", "[{\"name\": \"readers\", \"members\": []}]",
                        "line 1, column 2: [0]: \"acls\" is required"),
                Arguments.of("role-acls", "[{\"name\": \"readers\", \"members\": [7], \"acls\": []}]",
                        "line 1, column 34: [0].members[0]: is a number, and must be a string"),
                Arguments.of("role-acls", "[{\"name\": \"readers\", \"members\": [], \"acls\": ["
                        + acl.replace(", \"host\": \"*\"", "") + "]}]",
                        "line 1, column 46: [0].acls[0]: \"host\" is required"),
                Arguments.of("role-acls", "[{\"name\": \"readers\", \"members\": [], \"acls\": ["
                        + acl.replace("\"operation\": \"READ\", ", "") + "]}]",
                        "line 1, column 46: [0].acls[0]: \"operation\" is required"),
                Arguments.of("role-acls", "[{\"name\": \"readers\", \"members\": \"User:alice\", \"acls\": []}]",
                        "line 1, column 33: [0].members: is a string, and must be a list"));
    }

    @ParameterizedTest
    @MethodSource("filesThatCannotBeImported")
    void aFileThatIsNotValidFailsWithThePlaceAndTheFieldAndStatusTwoAndWritesNothing(String format, String content,
            String wrong) throws IOException {
        Path file = file("import.json", content);

        Commands.Ran ran = run(cluster, "import", "--file", file.toString(), "--format", format);

        if (wrong.endsWith("...")) {
            // What the parser says after the place is the parser's own wording.
            assertEquals(2, ran.status());
            assertEquals("", ran.out());
            String start = "error: " + file + ": " + wrong.substring(0, wrong.length() - 3);
            assertTrue(ran.err().startsWith(start) && ran.err().endsWith("\n") && ran.err().lines().count() == 1,
                    ran.err());
        } else {
            assertEquals(new Commands.Ran(2, "", "error: " + file + ": " + wrong + "\n"), ran);
        }
        assertEquals(0, cluster.described + cluster.created + cluster.deleted);
        assertFalse(cluster.closed, "the cluster is not reached");
        assertEquals(Set.of(), cluster.records);
    }

    @Test
    void aFileWithOneThingMissingWritesNothingOfTheRest() throws IOException {
        Path file = file("roles.json", """
                [
                  {"name": "readers", "members": ["User:alice"], "acls": []},
                  {"name": "writers", "members": ["User:bob"]}
                ]
                """);

        Commands.Ran ran = run(cluster, "import", "--file", file.toString(), "--format", "role-acls");

        assertEquals(new Commands.Ran(2, "", "error: " + file + ": line 3, column 3: [1]: \"acls\" is required\n"),
                ran);
        assertEquals(Set.of(), cluster.records);
        assertEquals(0, cluster.created);
    }

    // --- usage, refusal -------------------------------------------------------------------

    static Stream<Arguments> wrongUsage() {
        return Stream.of(
                Arguments.of(List.of("import", "--format", "role-bindings"), "--file is required"),
                Arguments.of(List.of("import", "--file", BINDINGS), "--format is required"),
                Arguments.of(List.of("import", "--file", BINDINGS, "--format", "yaml"),
                        "--format is role-bindings or role-acls, not yaml"),
                Arguments.of(List.of("import", "--file", ACLS, "--format", "role-acls", "--cluster-id", "cluster-1"),
                        "--cluster-id selects among the bindings of --format role-bindings"),
                Arguments.of(List.of("import", "--file", BINDINGS, "--format", "role-bindings", "--no-delete"),
                        "Unknown option: --no-delete"));
    }

    @ParameterizedTest
    @MethodSource("wrongUsage")
    void wrongUsagePrintsWhatIsWrongAndTheUsageOfImportAndExitsWithTwo(List<String> command, String wrong) {
        Commands.Ran ran = run(cluster, command.toArray(new String[0]));

        assertEquals(new Commands.Ran(2, "", wrong + "\n\n" + RbacCommand.Verb.IMPORT.usage()), ran);
        assertEquals(0, cluster.described + cluster.created + cluster.deleted);
        assertFalse(cluster.closed, "the cluster is not reached");
    }

    @Test
    void anImportTheClusterRefusesPrintsItsReasonAndExitsWithOne() {
        cluster.refusal = AclRecords.Failure.of(new org.apache.kafka.common.errors.ClusterAuthorizationException(
                "User:bob may not create the ACL"));

        Commands.Ran ran = run(cluster, "import", "--file", ACLS, "--format", "role-acls");

        assertEquals(new Commands.Ran(1, "", "error: ClusterAuthorizationException: User:bob may not create the ACL\n"),
                ran);
        assertEquals(Set.of(), cluster.records);
    }

    /** What {@code docs/import.md} shows as the output of a command. */
    private static String documented(String command) throws IOException {
        return Commands.documented(Path.of("docs/import.md"), command);
    }

    private Path file(String name, String content) throws IOException {
        Path file = directory.resolve(name);
        Files.writeString(file, content);
        return file;
    }
}
