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
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientUnitTestEnv;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The command's way to a cluster, with Kafka's real admin client on a
 * network that is played to it: what the three calls send, what they make
 * of the answers, and what the whole command sends for a verb.
 */
class AdminAclRecordsTest {

    private static final ResourcePattern BILLING = prefixed(TOPIC, "billing.");
    private static final AclBinding ALICE_READS = acl("DeveloperRead:User:alice", BILLING, ALL, "*", ALLOW).toBinding();
    private static final AclBinding AN_ACL =
            acl("User:bob", literal(TOPIC, "billing.orders"), READ, "*", ALLOW).toBinding();

    private AdminClientUnitTestEnv env;

    /** Every request the admin client sent, in order. */
    private final List<AbstractRequest> sent = new ArrayList<>();

    @BeforeEach
    void start() {
        Map<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));
        env = new AdminClientUnitTestEnv(new MockTime(),
                new Cluster("the-cluster", nodes.values(), Set.of(), Set.of(), Set.of(), nodes.get(0)));
        env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
    }

    @AfterEach
    void stop() {
        env.close();
    }

    @Test
    void describeAsksWithTheFilterAndReturnsTheRecords() {
        theClusterHas(ALICE_READS, AN_ACL);

        assertEquals(Set.of(ALICE_READS, AN_ACL),
                Set.copyOf(new AdminAclRecords(env.adminClient()).describe(AclBindingFilter.ANY)));

        assertEquals(1, sent.size());
        assertEquals(AclBindingFilter.ANY, ((DescribeAclsRequest) sent.get(0)).filter());
    }

    @Test
    void createSendsTheRecords() {
        theClusterCreates();

        new AdminAclRecords(env.adminClient()).create(List.of(ALICE_READS));

        assertEquals(1, sent.size());
        assertEquals(List.of(ALICE_READS), created((CreateAclsRequest) sent.get(0)));
    }

    @Test
    void deleteSendsTheFiltersAndReturnsWhatWasDeleted() {
        theClusterDeletes(ALICE_READS);

        assertEquals(List.of(ALICE_READS),
                List.copyOf(new AdminAclRecords(env.adminClient()).delete(List.of(ALICE_READS.toFilter()))));

        assertEquals(1, sent.size());
        assertEquals(List.of(ALICE_READS.toFilter()), ((DeleteAclsRequest) sent.get(0)).filters());
    }

    @Test
    void aRefusalOfEachCallIsAFailureWithTheReasonTheClusterGave() {
        AdminAclRecords records = new AdminAclRecords(env.adminClient());

        theClusterRefusesToCreate("User:bob may not bind DeveloperRead on billing.: it owns nothing there");
        assertEquals("InvalidRequestException: User:bob may not bind DeveloperRead on billing.: it owns nothing there",
                assertThrows(AclRecords.Failure.class, () -> records.create(List.of(ALICE_READS))).getMessage());

        respond(new DescribeAclsResponse(new DescribeAclsResponseData()
                .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                .setErrorMessage("User:bob may not list ACLs"), ApiKeys.DESCRIBE_ACLS.latestVersion()));
        assertEquals("ClusterAuthorizationException: User:bob may not list ACLs",
                assertThrows(AclRecords.Failure.class, () -> records.describe(AclBindingFilter.ANY)).getMessage());

        respond(new DeleteAclsResponse(new DeleteAclsResponseData().setFilterResults(List.of(
                new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setErrorCode(Errors.SECURITY_DISABLED.code())
                        .setErrorMessage("No Authorizer is configured."))), ApiKeys.DELETE_ACLS.latestVersion()));
        assertEquals("SecurityDisabledException: No Authorizer is configured.",
                assertThrows(AclRecords.Failure.class, () -> records.delete(List.of(ALICE_READS.toFilter())))
                        .getMessage());
    }

    // --- the whole command over the admin client ------------------------------------

    @Test
    void bindSendsOneCreateOfTheRecordAndNothingElse() {
        theClusterCreates();

        Ran ran = run("bind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", "billing.");

        assertEquals(new Ran(0, "Bound DeveloperRead to User:alice on prefix billing.\n", ""), ran);
        assertEquals(1, sent.size());
        assertEquals(List.of(ALICE_READS), created((CreateAclsRequest) sent.get(0)));
    }

    @Test
    void unbindSendsOneDeleteOfExactlyThatRecordAndNothingElse() {
        theClusterDeletes(ALICE_READS);

        Ran ran = run("unbind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", "billing.");

        assertEquals(new Ran(0, "Unbound DeveloperRead to User:alice on prefix billing.\n", ""), ran);
        assertEquals(1, sent.size());
        assertEquals(List.of(ALICE_READS.toFilter()), ((DeleteAclsRequest) sent.get(0)).filters());
    }

    static Stream<List<String>> verbsThatRead() {
        return Stream.of(
                List.of("list"),
                List.of("list", "--output", "json"),
                List.of("who-can", "--operation", "Read", "--topic", "billing.orders"),
                List.of("what-can", "--principal", "User:alice"));
    }

    @ParameterizedTest
    @MethodSource("verbsThatRead")
    void aVerbThatReadsSendsOneDescribeAndNothingElse(List<String> command) {
        theClusterHas(ALICE_READS, AN_ACL);

        Ran ran = run(command.toArray(new String[0]));

        assertEquals(0, ran.status(), ran.err());
        assertTrue(ran.out().contains("User:alice"), ran.out());
        assertEquals(1, sent.size());
        assertEquals(AclBindingFilter.ANY, ((DescribeAclsRequest) sent.get(0)).filter());
    }

    @Test
    void aBindTheClusterRefusesPrintsTheReasonItGaveAndExitsWithOne() {
        theClusterRefusesToCreate("User:bob may not bind DeveloperRead on billing.: it owns nothing there");

        Ran ran = run("bind", "--role", "DeveloperRead", "--member", "User:alice", "--prefix", "billing.");

        assertEquals(new Ran(1, "", "error: InvalidRequestException: User:bob may not bind DeveloperRead on"
                + " billing.: it owns nothing there\n"), ran);
    }

    // --- helpers ----------------------------------------------------------------------

    private record Ran(int status, String out, String err) {
    }

    /** Runs the command against the admin client of this test; the command closes it. */
    private Ran run(String... verbAndOptions) {
        List<String> args = new ArrayList<>(List.of(verbAndOptions));
        args.add("--bootstrap-server");
        args.add("localhost:8121");
        Admin admin = env.adminClient();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int status = RbacCommand.run(args.toArray(new String[0]), new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8), properties -> new AdminAclRecords(admin));
        return new Ran(status, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    private void theClusterHas(AclBinding... records) {
        respond(new DescribeAclsResponse(new DescribeAclsResponseData()
                .setResources(DescribeAclsResponse.aclsResources(List.of(records))),
                ApiKeys.DESCRIBE_ACLS.latestVersion()));
    }

    private void theClusterCreates() {
        respond(new CreateAclsResponse(new CreateAclsResponseData()
                .setResults(List.of(new CreateAclsResponseData.AclCreationResult()))));
    }

    private void theClusterRefusesToCreate(String reason) {
        respond(new CreateAclsResponse(new CreateAclsResponseData().setResults(List.of(
                new CreateAclsResponseData.AclCreationResult()
                        .setErrorCode(Errors.INVALID_REQUEST.code())
                        .setErrorMessage(reason)))));
    }

    private void theClusterDeletes(AclBinding... records) {
        List<DeleteAclsResponseData.DeleteAclsMatchingAcl> matching = new ArrayList<>();
        for (AclBinding record : records) {
            matching.add(DeleteAclsResponse.matchingAcl(record, ApiError.NONE));
        }
        respond(new DeleteAclsResponse(new DeleteAclsResponseData().setFilterResults(List.of(
                new DeleteAclsResponseData.DeleteAclsFilterResult().setMatchingAcls(matching))),
                ApiKeys.DELETE_ACLS.latestVersion()));
    }

    /** The next request, whatever it is, is recorded and gets this answer. */
    private void respond(org.apache.kafka.common.requests.AbstractResponse response) {
        env.kafkaClient().prepareResponse(request -> {
            sent.add(request);
            return true;
        }, response);
    }

    private static List<AclBinding> created(CreateAclsRequest request) {
        List<AclBinding> records = new ArrayList<>();
        request.aclCreations().forEach(creation -> records.add(CreateAclsRequest.aclBinding(creation)));
        return records;
    }
}
