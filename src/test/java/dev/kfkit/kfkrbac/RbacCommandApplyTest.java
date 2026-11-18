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
import static dev.kfkit.kfkrbac.Commands.record;
import static dev.kfkit.kfkrbac.Commands.run;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.acl;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.literal;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.prefixed;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.ResourcePattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * {@code apply}, against ACL records held in memory: the file it reads is
 * what {@code list --output json} and {@code group list --output json}
 * print, and what it creates and deletes is counted by the fake.
 */
class RbacCommandApplyTest {

    private static final ResourcePattern BILLING = prefixed(TOPIC, "billing.");
    private static final ResourcePattern ORDERS = literal(TOPIC, "billing.orders");

    private static final AclBinding ALICE_READS = record("DeveloperRead:User:alice", BILLING);
    private static final AclBinding PAYMENTS_READ = record("DeveloperRead:Group:payments", BILLING);
    private static final AclBinding BOB_WRITES = record("DeveloperWrite:User:bob", literal(TOPIC, "orders"));
    private static final AclBinding CAROL_ADMIN = record("ClusterAdmin:User:carol", literal(CLUSTER, "kafka-cluster"));
    private static final AclBinding ALICE_IN_PAYMENTS = record("Member:User:alice", literal(TOPIC, "Group:payments"));
    private static final AclBinding DAVE_IN_AUDIT = record("Member:User:dave", literal(TOPIC, "Group:audit"));
    private static final AclBinding AN_ACL = allow("User:erin", ORDERS, READ);
    private static final AclBinding A_GROUP_ACL = allow("Group:audit", literal(TOPIC, "reports"), READ);
    private static final AclBinding A_DENY = acl("User:erin", ORDERS, READ, "*", DENY).toBinding();
    private static final AclBinding NOT_A_BINDING =
            acl("DeveloperRead:User:frank", BILLING, READ, "*", ALLOW).toBinding();

    private final FakeAclRecords cluster = new FakeAclRecords();

    @TempDir
    Path directory;

    // --- AC1 ----------------------------------------------------------------------------

    @Test
    void theListOfAClusterAppliedToAnEmptyClusterMakesListThereTheSame() throws IOException {
        cluster.records.addAll(List.of(ALICE_READS, PAYMENTS_READ, BOB_WRITES, CAROL_ADMIN, ALICE_IN_PAYMENTS,
                DAVE_IN_AUDIT, AN_ACL, A_GROUP_ACL,
                record("DeveloperRead:User:alice", prefixed(GROUP, "billing.")),
                record("DeveloperWrite:User:bob", literal(TRANSACTIONAL_ID, "billing-1")),
                record("ResourceOwner:User:dave", literal(TOPIC, "*")),
                record("DeveloperRead:User:erin", prefixed(TOPIC, "Subject:orders-")),
                record("DeveloperManage:User:alice", literal(TOPIC, "Connector:billing-sink")),
                record("DeveloperRead:User:CN=\"x\\y\"", literal(TOPIC, "a\"b"))));
        String list = run(cluster, "list").out();
        String listed = run(cluster, "list", "--output", "json").out();
        String groups = run(cluster, "group", "list", "--output", "json").out();
        Path bindingsOnly = file("bindings.json", listed);
        Path both = file("access.json", "{\"bindings\": " + listed + ", \"groups\": " + groups + "}");

        FakeAclRecords empty = new FakeAclRecords();
        Commands.Ran applied = run(empty, "apply", "--file", bindingsOnly.toString());

        assertEquals(0, applied.status(), applied.err());
        assertTrue(applied.out().endsWith("Created 10, deleted 0.\n"), applied.out());
        assertEquals(list, run(empty, "list").out());
        assertEquals(listed, run(empty, "list", "--output", "json").out());
        assertEquals("No groups.\n", run(empty, "group", "list").out());

        FakeAclRecords another = new FakeAclRecords();
        assertEquals(0, run(another, "apply", "--file", both.toString()).status());

        assertEquals(list, run(another, "list").out());
        assertEquals(run(cluster, "group", "list").out(), run(another, "group", "list").out());
        assertEquals(groups, run(another, "group", "list", "--output", "json").out());
        Set<AclBinding> expected = new HashSet<>(cluster.records);
        expected.removeAll(List.of(AN_ACL, A_GROUP_ACL));
        assertEquals(expected, another.records, "the bindings and the memberships, and no ordinary ACL");
    }

    // --- AC2 ----------------------------------------------------------------------------

    @Test
    void dryRunPrintsTheDifferenceAndChangesNothingThenApplyMakesExactlyItAndASecondApplyChangesNothing()
            throws IOException {
        cluster.records.addAll(List.of(ALICE_READS, BOB_WRITES, DAVE_IN_AUDIT, AN_ACL));
        Set<AclBinding> before = new HashSet<>(cluster.records);
        Path file = file("access.json", """
                {
                  "bindings": [
                    {"role":"DeveloperRead","member":"User:alice","resourceType":"TOPIC","name":"billing.","patternType":"PREFIXED"},
                    {"role":"DeveloperRead","member":"Group:payments","resourceType":"TOPIC","name":"billing.","patternType":"PREFIXED"},
                    {"role":"ClusterAdmin","member":"User:carol","resourceType":"CLUSTER","name":"kafka-cluster","patternType":"LITERAL"}
                  ],
                  "groups": [
                    {"group":"payments","member":"User:alice"}
                  ]
                }
                """);
        String difference = """
                + membership of User:alice in Group:payments
                + binding DeveloperRead to Group:payments on prefix billing.
                + binding ClusterAdmin to User:carol on the cluster
                - membership of User:dave in Group:audit
                - binding DeveloperWrite to User:bob on name orders
                """;

        Commands.Ran dry = run(cluster, "apply", "--file", file.toString(), "--dry-run");

        assertEquals(new Commands.Ran(0, difference + "Would create 3 and delete 2. Nothing was changed.\n", ""), dry);
        assertEquals(before, cluster.records);
        assertEquals(1, cluster.described);
        assertEquals(0, cluster.created + cluster.deleted, "a dry run issues no create and no delete");

        Commands.Ran applied = run(cluster, "apply", "--file", file.toString());

        assertEquals(new Commands.Ran(0, difference + "Created 3, deleted 2.\n", ""), applied);
        assertEquals(Set.of(ALICE_READS, PAYMENTS_READ, CAROL_ADMIN, ALICE_IN_PAYMENTS, AN_ACL), cluster.records);
        assertEquals(1, cluster.created);
        assertEquals(1, cluster.deleted);
        Set<AclBinding> after = new HashSet<>(cluster.records);

        Commands.Ran again = run(cluster, "apply", "--file", file.toString());

        assertEquals(new Commands.Ran(0, "Nothing to change: the cluster has what the file has.\n", ""), again);
        assertEquals(after, cluster.records);
        assertEquals(0, cluster.created + cluster.deleted);
        assertEquals(new Commands.Ran(0, "Nothing to change: the cluster has what the file has.\n", ""),
                run(cluster, "apply", "--file", file.toString(), "--dry-run"));
    }

    // --- AC3 ----------------------------------------------------------------------------

    @Test
    void applyLeavesOrdinaryAclsAndWhatIsNotABindingAlone() throws IOException {
        cluster.records.addAll(List.of(ALICE_READS, ALICE_IN_PAYMENTS, AN_ACL, A_GROUP_ACL, A_DENY, NOT_A_BINDING));
        Path nothing = file("nothing.json", "{\"bindings\": [], \"groups\": []}");

        Commands.Ran applied = run(cluster, "apply", "--file", nothing.toString());

        assertEquals("""
                - membership of User:alice in Group:payments
                - binding DeveloperRead to User:alice on prefix billing.
                Created 0, deleted 2.
                """, applied.out());
        assertEquals(0, applied.status());
        assertEquals(Set.of(AN_ACL, A_GROUP_ACL, A_DENY, NOT_A_BINDING), cluster.records);
        assertEquals(0, cluster.created);
    }

    @Test
    void noDeleteDeletesNothing() throws IOException {
        cluster.records.addAll(List.of(ALICE_READS, BOB_WRITES, DAVE_IN_AUDIT, AN_ACL));
        Set<AclBinding> before = new HashSet<>(cluster.records);
        Path file = file("access.json", """
                {"bindings": [
                   {"role":"DeveloperRead","member":"Group:payments","resourceType":"TOPIC","name":"billing.","patternType":"PREFIXED"},
                   {"role":"DeveloperRead","member":"User:alice","resourceType":"TOPIC","name":"billing.","patternType":"PREFIXED"}],
                 "groups": []}
                """);
        String difference = """
                + binding DeveloperRead to Group:payments on prefix billing.
                  not in the file, and left: membership of User:dave in Group:audit
                  not in the file, and left: binding DeveloperWrite to User:bob on name orders
                """;

        Commands.Ran dry = run(cluster, "apply", "--file", file.toString(), "--no-delete", "--dry-run");

        assertEquals(new Commands.Ran(0, difference + "Would create 1 and delete 0; 2 not in the file left as they"
                + " are, as --no-delete says. Nothing was changed.\n", ""), dry);
        assertEquals(before, cluster.records);

        Commands.Ran applied = run(cluster, "apply", "--file", file.toString(), "--no-delete");

        assertEquals(new Commands.Ran(0, difference + "Created 1, deleted 0; 2 not in the file left as they are, as"
                + " --no-delete says.\n", ""), applied);
        Set<AclBinding> withIt = new HashSet<>(before);
        withIt.add(PAYMENTS_READ);
        assertEquals(withIt, cluster.records);
        assertEquals(0, cluster.deleted, "--no-delete issues no delete");

        Commands.Ran again = run(cluster, "apply", "--file", file.toString(), "--no-delete");

        assertEquals("""
                  not in the file, and left: membership of User:dave in Group:audit
                  not in the file, and left: binding DeveloperWrite to User:bob on name orders
                Nothing to change; 2 not in the file left as they are, as --no-delete says.
                """, again.out());
        assertEquals(0, cluster.created + cluster.deleted);
    }

    @Test
    void whatTheFileDoesNotSpeakOfIsLeftAlone() throws IOException {
        cluster.records.addAll(List.of(ALICE_READS, ALICE_IN_PAYMENTS, DAVE_IN_AUDIT));
        Path bindingsOnly = file("bindings.json", "{\"bindings\": []}");
        Path groupsOnly = file("groups.json", "{\"groups\": [{\"group\":\"audit\",\"member\":\"User:dave\"}]}");
        Path anArray = file("array.json", "[]");

        assertEquals("""
                - membership of User:alice in Group:payments
                Created 0, deleted 1.
                """, run(cluster, "apply", "--file", groupsOnly.toString()).out());
        assertEquals(Set.of(ALICE_READS, DAVE_IN_AUDIT), cluster.records, "no binding is touched");

        assertEquals("""
                - binding DeveloperRead to User:alice on prefix billing.
                Created 0, deleted 1.
                """, run(cluster, "apply", "--file", bindingsOnly.toString()).out());
        assertEquals(Set.of(DAVE_IN_AUDIT), cluster.records, "no membership is touched");

        cluster.records.add(ALICE_READS);
        assertEquals(0, run(cluster, "apply", "--file", anArray.toString()).status());
        assertEquals(Set.of(DAVE_IN_AUDIT), cluster.records, "an array is the bindings, and no membership is touched");
    }

    @Test
    void applyOfTheExampleFileWritesWhatItHolds() throws IOException {
        assertTrue(Files.readString(Path.of("docs/command.md")).contains("```json\n"
                + Files.readString(Path.of("docs/examples/apply.json")) + "```"),
                "docs/command.md shows the example file as it is");

        Commands.Ran applied = run(cluster, "apply", "--file", "docs/examples/apply.json");

        assertEquals(new Commands.Ran(0, """
                + membership of User:alice in Group:payments
                + binding DeveloperRead to Group:payments on prefix billing.
                + binding DeveloperWrite to User:bob on name orders
                + binding ClusterAdmin to User:carol on the cluster
                Created 4, deleted 0.
                """, ""), applied);
        assertEquals(Set.of(ALICE_IN_PAYMENTS, PAYMENTS_READ, BOB_WRITES, CAROL_ADMIN), cluster.records);
    }

    @Test
    void aFileWithAByteOrderMarkOrInUtf16IsRead() throws IOException {
        String content = "{\"groups\": [{\"group\":\"payments\",\"member\":\"User:alice\"}]}";
        Path withMark = directory.resolve("mark.json");
        byte[] utf8 = content.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] marked = new byte[utf8.length + 3];
        marked[0] = (byte) 0xEF;
        marked[1] = (byte) 0xBB;
        marked[2] = (byte) 0xBF;
        System.arraycopy(utf8, 0, marked, 3, utf8.length);
        Files.write(withMark, marked);
        Path utf16 = directory.resolve("utf16.json");
        Files.write(utf16, content.getBytes(java.nio.charset.StandardCharsets.UTF_16));

        assertEquals(0, run(cluster, "apply", "--file", withMark.toString()).status());
        assertEquals(Set.of(ALICE_IN_PAYMENTS), cluster.records);
        assertEquals(new Commands.Ran(0, "Nothing to change: the cluster has what the file has.\n", ""),
                run(cluster, "apply", "--file", utf16.toString()));
    }

    // --- AC7 ----------------------------------------------------------------------------

    static Stream<Arguments> filesThatCannotBeApplied() {
        String binding = "{\"role\":\"DeveloperRead\",\"member\":\"User:alice\",\"resourceType\":\"TOPIC\","
                + "\"name\":\"billing.\",\"patternType\":\"PREFIXED\"}";
        return Stream.of(
                Arguments.of("", "line 1, column 1: not valid JSON: the file is empty"),
                Arguments.of("{\"bindings\": [\n  " + binding + ",\n  {\"role\": }\n]}",
                        "line 3, column 12: not valid JSON: Unexpected character ('}' (code 125))..."),
                Arguments.of("[" + binding + "] []", "line 1, column 116: not valid JSON: something follows the end"
                        + " of the value"),
                Arguments.of("{\"bindings\": [\n  " + binding + ",\n  {\"role\":\"DeveloperRead\",\"member\":"
                        + "\"User:bob\",\"resourceType\":\"TOPIC\",\"patternType\":\"PREFIXED\"}\n]}",
                        "line 3, column 3: bindings[1]: \"name\" is required"),
                Arguments.of("[{\"member\":\"User:alice\",\"resourceType\":\"TOPIC\",\"name\":\"a\","
                        + "\"patternType\":\"LITERAL\"}]", "line 1, column 2: [0]: \"role\" is required"),
                Arguments.of("[{\"group\":\"payments\",\"member\":\"User:alice\"}]",
                        "line 1, column 2: [0]: \"role\" is required"),
                Arguments.of("{\"groups\": [{\"group\":\"payments\"}]}",
                        "line 1, column 13: groups[0]: \"member\" is required"),
                Arguments.of("{\"groups\": [{\"member\":\"User:alice\"}]}",
                        "line 1, column 13: groups[0]: \"group\" is required"),
                Arguments.of("{\"bindings\": {}}", "line 1, column 14: bindings: is an object, and must be a list"),
                Arguments.of("{\"bindings\": [7]}",
                        "line 1, column 15: bindings[0]: is a number, and must be an object"),
                Arguments.of("\"bindings\"", "line 1, column 1: is a string, and must be an object"),
                Arguments.of("{}", "line 1, column 1: has neither \"bindings\" nor \"groups\""),
                Arguments.of("{\"binding\": []}", "line 1, column 13: binding: is not a field of this file, which has"
                        + " \"bindings\" and \"groups\""),
                Arguments.of(binding.replace("\"DeveloperRead\"", "null").replace("{", "[{") + "]",
                        "line 1, column 10: [0].role: is null, and must be a string"),
                Arguments.of("[" + binding.replace("DeveloperRead", "Reader") + "]",
                        "line 1, column 10: [0].role: is Reader, and there is no such role: the roles are"
                                + " DeveloperRead, DeveloperWrite, DeveloperManage, ResourceOwner, ResourceViewer,"
                                + " ClusterAdmin, SystemAdmin"),
                Arguments.of("[" + binding.replace("TOPIC", "Topic") + "]",
                        "line 1, column 63: [0].resourceType: is Topic, and must be TOPIC, GROUP, TRANSACTIONAL_ID"
                                + " or CLUSTER"),
                Arguments.of("[" + binding.replace("PREFIXED", "MATCH") + "]",
                        "line 1, column 103: [0].patternType: is MATCH, and must be LITERAL or PREFIXED"),
                Arguments.of("[" + binding.replace("TOPIC", "CLUSTER") + "]",
                        "line 1, column 2: [0]: is not a binding: DeveloperRead is bound to a name pattern, not to"
                                + " the cluster"),
                Arguments.of("[" + binding.replace("User:alice", "alice") + "]",
                        "line 1, column 2: [0]: is not a binding: the member is not <type>:<name>, as in"
                                + " DeveloperRead:User:alice"),
                Arguments.of("{\"groups\": [{\"group\":\"payments\",\"member\":\"Group:billing\"}]}",
                        "line 1, column 13: groups[0]: is not a membership: the member is a group, and a group is"
                                + " not a member of a group"),
                Arguments.of("{\"groups\": [{\"group\":\"\",\"member\":\"User:alice\"}]}",
                        "line 1, column 22: groups[0].group: is empty, and must be the name of a group"),
                Arguments.of("{\"bindings\": [], \"bindings\": []}",
                        "line 1, column 28: not valid JSON: Duplicate field 'bindings'..."));
    }

    @ParameterizedTest
    @MethodSource("filesThatCannotBeApplied")
    void aFileThatIsNotValidFailsWithThePlaceAndTheFieldAndStatusTwoAndWritesNothing(String content, String wrong)
            throws IOException {
        cluster.records.addAll(List.of(ALICE_READS, ALICE_IN_PAYMENTS));
        Path file = file("access.json", content);

        Commands.Ran ran = run(cluster, "apply", "--file", file.toString());

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
        assertEquals(Set.of(ALICE_READS, ALICE_IN_PAYMENTS), cluster.records);
    }

    @Test
    void aFileThatIsNotThereFailsWithStatusTwo() {
        Path missing = directory.resolve("missing.json");

        assertEquals(new Commands.Ran(2, "", "error: " + missing + ": there is no such file\n"),
                run(cluster, "apply", "--file", missing.toString()));
        assertFalse(cluster.closed, "the cluster is not reached");
    }

    // --- usage, refusal -------------------------------------------------------------------

    @Test
    void applyWithoutAFileOrWithAnOptionItDoesNotHaveIsWrongUsage() {
        String usage = RbacCommand.Verb.APPLY.usage();

        assertEquals(new Commands.Ran(2, "", "--file is required\n\n" + usage), run(cluster, "apply"));
        assertEquals(new Commands.Ran(2, "", "Unknown option: --force\n\n" + usage),
                run(cluster, "apply", "--file", "docs/examples/apply.json", "--force"));
        assertEquals(new Commands.Ran(2, "", "--bootstrap-server is required\n\n" + usage),
                Commands.ran(cluster, "apply", "--file", directory.resolve("missing.json").toString()));
        assertFalse(cluster.closed, "the cluster is not reached");
    }

    @Test
    void anApplyTheClusterRefusesPrintsItsReasonAndExitsWithOne() {
        cluster.refusal = AclRecords.Failure.of(new org.apache.kafka.common.errors.ClusterAuthorizationException(
                "User:bob may not list the ACLs"));

        Commands.Ran ran = run(cluster, "apply", "--file", "docs/examples/apply.json");

        assertEquals(new Commands.Ran(1, "", "error: ClusterAuthorizationException: User:bob may not list the ACLs\n"),
                ran);
        assertEquals(Set.of(), cluster.records);
    }

    private Path file(String name, String content) throws IOException {
        Path file = directory.resolve(name);
        Files.writeString(file, content);
        return file;
    }
}
