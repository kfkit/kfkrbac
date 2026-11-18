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
package dev.kfkit.kfkrbac.connect;

import static dev.kfkit.kfkrbac.TestRestAccess.onConnectors;
import static dev.kfkit.kfkrbac.TestRestAccess.onTheCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import dev.kfkit.kfkrbac.ConnectorOperation;
import dev.kfkit.kfkrbac.RestAccess;
import dev.kfkit.kfkrbac.TestRestAccess;

/** Every request of the table in docs/connect.md through the filter that ships, with and without the role. */
class ConnectorFilterTest {

    private static final String CREATE_BILLING = "{\"name\":\"billing-sink\",\"config\":{\"connector.class\":\"x\"}}";

    /** AC2: the rows of the table that name an operation on a connector. */
    static Stream<Arguments> rows() {
        return Stream.of(
                row("POST", "/connectors", CREATE_BILLING, ConnectorOperation.CREATE, false),
                row("PUT", "/connectors/billing-sink/config", "{}", ConnectorOperation.CREATE, false),
                row("PUT", "/connectors/billing-sink/config", "{}", ConnectorOperation.CONFIGURE, true),
                row("PATCH", "/connectors/billing-sink/config", "{}", ConnectorOperation.CONFIGURE, true),
                row("GET", "/connectors/billing-sink", "", ConnectorOperation.READ_CONFIG, true),
                row("GET", "/connectors/billing-sink/config", "", ConnectorOperation.READ_CONFIG, true),
                row("GET", "/connectors/billing-sink/tasks", "", ConnectorOperation.READ_CONFIG, true),
                row("GET", "/connectors/billing-sink/status", "", ConnectorOperation.READ_STATUS, true),
                row("GET", "/connectors/billing-sink/tasks/0/status", "", ConnectorOperation.READ_STATUS, true),
                row("GET", "/connectors/billing-sink/topics", "", ConnectorOperation.READ_ACTIVE_TOPICS, true),
                row("PUT", "/connectors/billing-sink/topics/reset", "", ConnectorOperation.RESET_ACTIVE_TOPICS, true),
                row("PUT", "/connectors/billing-sink/pause", "", ConnectorOperation.PAUSE, true),
                row("PUT", "/connectors/billing-sink/stop", "", ConnectorOperation.PAUSE, true),
                row("PUT", "/connectors/billing-sink/resume", "", ConnectorOperation.RESUME, true),
                row("POST", "/connectors/billing-sink/restart", "", ConnectorOperation.RESUME, true),
                row("POST", "/connectors/billing-sink/tasks/0/restart", "", ConnectorOperation.RESUME, true),
                row("PATCH", "/connectors/billing-sink/offsets", "{}", ConnectorOperation.CONFIGURE, true),
                row("DELETE", "/connectors/billing-sink/offsets", "", ConnectorOperation.CONFIGURE, true),
                row("GET", "/connectors/billing-sink/offsets", "", ConnectorOperation.READ_STATUS, true),
                row("DELETE", "/connectors/billing-sink", "", ConnectorOperation.DELETE, true),
                row("HEAD", "/connectors/billing-sink/config", "", ConnectorOperation.READ_CONFIG, true),
                row("GET", "/connectors/billing-sink/status/", "", ConnectorOperation.READ_STATUS, true));
    }

    private static Arguments row(String method, String path, String body, ConnectorOperation operation,
            boolean exists) {
        return Arguments.of(method, path, body, operation, exists);
    }

    @ParameterizedTest(name = "{0} {1} is {3}")
    @MethodSource("rows")
    void allowedWithARoleThatGrantsTheOperation(String method, String path, String body,
            ConnectorOperation operation, boolean exists) throws IOException {
        String role = Arrays.stream(new String[] {"DeveloperRead", "DeveloperWrite", "DeveloperManage",
            "ResourceViewer", "ResourceOwner"})
                .filter(name -> grants(name, operation))
                .findFirst().orElseThrow();
        FakeRequest request = FakeRequest.of(method, path).as("alice").withBody(body);
        filter(exists, request, onConnectors(role, "User:alice", "billing-"));
        assertEquals(0, request.status(), role + " grants " + operation + ": " + request.message());
        assertEquals(body, request.body(), "the worker still reads the whole body");
    }

    @ParameterizedTest(name = "{0} {1} is {3}")
    @MethodSource("rows")
    void refusedWithoutIt(String method, String path, String body, ConnectorOperation operation, boolean exists)
            throws IOException {
        FakeRequest request = FakeRequest.of(method, path).as("alice").withBody(body);
        filter(exists, request, onConnectors("ResourceOwner", "User:alice", "payments-"),
                onConnectors("ResourceOwner", "User:bob", "billing-"));
        assertEquals(403, request.status());
        String name = RestAccess.name(operation);
        assertTrue(request.message().contains("User:alice may not " + name + " connector billing-sink"),
                request.message());
        assertTrue(request.message().startsWith("{\"error_code\":403,\"message\":\"kfkrbac: "), request.message());
    }

    @ParameterizedTest(name = "{0} {1} is {3}")
    @MethodSource("rows")
    void refusedWithARoleThatDoesNotGrantTheOperation(String method, String path, String body,
            ConnectorOperation operation, boolean exists) throws IOException {
        String[] others = Arrays.stream(new String[] {"DeveloperRead", "DeveloperWrite", "DeveloperManage",
            "ResourceViewer"})
                .filter(name -> !grants(name, operation))
                .toArray(String[]::new);
        for (String role : others) {
            FakeRequest request = FakeRequest.of(method, path).as("alice").withBody(body);
            filter(exists, request, onConnectors(role, "User:alice", "billing-"));
            assertEquals(403, request.status(), role + " does not grant " + operation);
        }
    }

    @Test
    void theNameOfTheConnectorToCreateIsTrimmedAsTheWorkerTrimsIt() throws IOException {
        FakeRequest request = FakeRequest.of("POST", "/connectors").as("alice")
                .withBody("{\"name\":\" payments-sink \",\"config\":{\"name\":\"billing-sink\"}}");
        filter(false, request, onConnectors("DeveloperManage", "User:alice", "billing-"));
        assertEquals(403, request.status());
        assertTrue(request.message().contains("Create connector payments-sink"), request.message());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "not json", "{}", "{\"name\":7}", "{\"name\":\"  \"}", "[\"billing-sink\"]"})
    void aCreateThatNamesNoConnectorIsRefused(String body) throws IOException {
        FakeRequest request = FakeRequest.of("POST", "/connectors").as("alice").withBody(body);
        filter(false, request, onConnectors("ResourceOwner", "User:alice", "billing-"));
        assertEquals(400, request.status());
        assertEquals(body, request.body());
    }

    @ParameterizedTest
    @ValueSource(strings = {"/", "/connector-plugins", "/health", "/connectors"})
    void whatAnyAuthenticatedCallerMayAsk(String path) throws IOException {
        FakeRequest request = FakeRequest.of("GET", path).as("alice");
        filter(false, request);
        assertEquals(0, request.status(), request.message());
    }

    /** The rows that take ClusterAdmin or SystemAdmin, and what the table does not name. */
    static Stream<Arguments> administration() {
        return Stream.of(
                Arguments.of("GET", "/admin/loggers"),
                Arguments.of("PUT", "/admin/loggers/org.apache.kafka.connect"),
                Arguments.of("PUT", "/connector-plugins/FileStreamSink/config/validate"),
                Arguments.of("GET", "/connector-plugins/FileStreamSink/config"),
                Arguments.of("OPTIONS", "/connectors/billing-sink"),
                Arguments.of("POST", "/connectors/billing-sink"),
                Arguments.of("GET", "/connectors/billing-sink/something-new"),
                Arguments.of("DELETE", "/health"),
                Arguments.of("POST", "/"));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("administration")
    void administrationTakesClusterAdminOrSystemAdmin(String method, String path) throws IOException {
        AclBinding[] bindings = {onTheCluster("ClusterAdmin", "User:alice"), onTheCluster("SystemAdmin", "User:carol"),
            onConnectors("ResourceOwner", "User:bob", "billing-")};
        for (String user : List.of("alice", "carol")) {
            FakeRequest request = FakeRequest.of(method, path).as(user);
            filter(true, request, bindings);
            assertEquals(0, request.status(), user + ": " + request.message());
        }
        FakeRequest request = FakeRequest.of(method, path).as("bob");
        filter(true, request, bindings);
        assertEquals(403, request.status());
        assertTrue(request.message().contains("User:bob may not " + method + " " + (path.length() > 1 ? path.replaceAll("/$", "") : path)
                + ": that takes ClusterAdmin or SystemAdmin"), request.message());
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("administration")
    void aSuperUserPassesEverything(String method, String path) throws IOException {
        try (TestRestAccess test = TestRestAccess.of("User:admin")) {
            test.refresh();
            FakeRequest request = FakeRequest.of(method, path).as("admin");
            new ConnectorFilter(test.access(), FakeRequest.connectors()).filter(request.context());
            assertEquals(0, request.status(), request.message());
        }
    }

    /** AC4: an anonymous request gets 401, whatever it asks. */
    @ParameterizedTest
    @ValueSource(strings = {"/", "/health", "/connectors", "/connectors/billing-sink/status", "/admin/loggers"})
    void anAnonymousRequestIsRefused(String path) throws IOException {
        FakeRequest request = FakeRequest.of("GET", path);
        filter(true, request, onConnectors("ResourceOwner", "User:alice", "billing-"));
        assertEquals(401, request.status());
        FakeRequest withoutAChain = FakeRequest.of("GET", path).withCertificates(new X509Certificate[0]);
        filter(true, withoutAChain);
        assertEquals(401, withoutAChain.status());
    }

    /** AC4: over mutual TLS the caller is the subject of the client's certificate. */
    @Test
    void theCallerOfAMutualTlsRequestIsTheSubjectOfItsCertificate() throws IOException {
        X509Certificate[] chain = {TestRestAccess.alice()};
        FakeRequest allowed = FakeRequest.of("GET", "/connectors/billing-sink/status").withCertificates(chain);
        filter(true, allowed, onConnectors("ResourceViewer", "User:CN=alice,O=example", "billing-"));
        assertEquals(0, allowed.status(), allowed.message());

        FakeRequest refused = FakeRequest.of("GET", "/connectors/billing-sink/status").withCertificates(chain);
        filter(true, refused, onConnectors("ResourceViewer", "User:alice", "billing-"));
        assertEquals(403, refused.status());
        assertTrue(refused.message().contains("User:CN=alice,O=example may not ReadStatus"), refused.message());
    }

    /**
     * AC4: a request a follower forwards to the leader carries the user's
     * Authorization header, which the leader's worker authenticates again:
     * the leader decides it as the user, and the certificate of the worker
     * that forwarded it changes nothing.
     */
    @Test
    void aForwardedRequestWithBasicAuthenticationIsDecidedAsTheUser() throws IOException {
        X509Certificate[] worker = {TestRestAccess.alice()};
        FakeRequest refused = FakeRequest.of("DELETE", "/connectors/billing-sink").as("bob").withCertificates(worker);
        try (TestRestAccess test = TestRestAccess.of("User:CN=alice,O=example",
                onConnectors("DeveloperRead", "User:bob", "billing-"))) {
            test.refresh();
            new ConnectorFilter(test.access(), FakeRequest.connectors("billing-sink")).filter(refused.context());
        }
        assertEquals(403, refused.status());
        assertTrue(refused.message().contains("User:bob may not Delete connector billing-sink"), refused.message());
    }

    /**
     * AC4: with mutual TLS the leader sees the certificate of the worker that
     * forwarded the request, which passes when that worker's principal is a
     * super user, and only then.
     */
    @Test
    void aForwardedRequestWithMutualTlsPassesAsTheWorkersPrincipalWhenItIsASuperUser() throws IOException {
        X509Certificate[] worker = {TestRestAccess.alice()};
        FakeRequest passes = FakeRequest.of("DELETE", "/connectors/billing-sink").withCertificates(worker);
        try (TestRestAccess test = TestRestAccess.of("User:CN=alice,O=example")) {
            test.refresh();
            new ConnectorFilter(test.access(), FakeRequest.connectors("billing-sink")).filter(passes.context());
        }
        assertEquals(0, passes.status(), passes.message());

        FakeRequest refused = FakeRequest.of("DELETE", "/connectors/billing-sink").withCertificates(worker);
        filter(true, refused);
        assertEquals(403, refused.status());
    }

    /** The two requests workers send each other pass, as they do the worker's own filter; it verifies them. */
    @ParameterizedTest
    @ValueSource(strings = {"POST /connectors/billing-sink/tasks", "PUT /connectors/billing-sink/fence"})
    void requestsBetweenWorkersPass(String line) throws IOException {
        String[] parts = line.split(" ");
        try (TestRestAccess test = TestRestAccess.of("")) {
            FakeRequest request = FakeRequest.of(parts[0], parts[1]);
            new ConnectorFilter(test.access(), FakeRequest.connectors()).filter(request.context());
            assertEquals(0, request.status(), "before the bindings are read too");
        }
    }

    /** AC6: before the first read everything but the health check is refused with 503. */
    @ParameterizedTest
    @ValueSource(strings = {"GET /", "GET /connectors", "GET /connectors/billing-sink/status",
        "DELETE /connectors/billing-sink", "GET /admin/loggers", "GET /connector-plugins"})
    void beforeTheFirstReadEverythingButTheHealthCheckIsRefused(String line) throws IOException {
        String[] parts = line.split(" ");
        try (TestRestAccess test = TestRestAccess.of("User:admin",
                onConnectors("ResourceOwner", "User:alice", "billing-"))) {
            ConnectorFilter filter = new ConnectorFilter(test.access(), FakeRequest.connectors("billing-sink"));
            for (String user : List.of("alice", "admin")) {
                FakeRequest request = FakeRequest.of(parts[0], parts[1]).as(user);
                filter.filter(request.context());
                assertEquals(503, request.status(), user);
                assertTrue(request.message().contains("have not been read"), request.message());
            }
            FakeRequest health = FakeRequest.of("GET", "/health").as("alice");
            filter.filter(health.context());
            assertEquals(0, health.status());

            test.refresh();
            FakeRequest after = FakeRequest.of(parts[0], parts[1]).as("admin");
            filter.filter(after.context());
            assertEquals(0, after.status(), after.message());
        }
    }

    /** AC6: after a failed read the last bindings stay in force. */
    @Test
    void afterAFailedReadTheLastBindingsStayInForce() throws IOException {
        try (TestRestAccess test = TestRestAccess.of("", onConnectors("DeveloperManage", "User:alice", "billing-"))) {
            test.refresh();
            test.failReads();
            test.sleep(60_000);
            assertEquals(false, test.refresh());
            ConnectorFilter filter = new ConnectorFilter(test.access(), FakeRequest.connectors("billing-sink"));
            FakeRequest allowed = FakeRequest.of("PUT", "/connectors/billing-sink/pause").as("alice");
            filter.filter(allowed.context());
            assertEquals(0, allowed.status());
            FakeRequest refused = FakeRequest.of("PUT", "/connectors/payments-sink/pause").as("alice");
            filter.filter(refused.context());
            assertEquals(403, refused.status());
            assertEquals(60_000, test.access().ageMs());
        }
    }

    /** AC7: a binding created on the cluster is in force after the next read. */
    @Test
    void aBindingCreatedOnTheClusterIsInForceAfterTheNextRead() throws IOException {
        try (TestRestAccess test = TestRestAccess.of("")) {
            test.refresh();
            ConnectorFilter filter = new ConnectorFilter(test.access(), FakeRequest.connectors());
            FakeRequest before = FakeRequest.of("POST", "/connectors").as("alice").withBody(CREATE_BILLING);
            filter.filter(before.context());
            assertEquals(403, before.status());
            test.add(onConnectors("DeveloperManage", "User:alice", "billing-"));
            test.refresh();
            FakeRequest after = FakeRequest.of("POST", "/connectors").as("alice").withBody(CREATE_BILLING);
            filter.filter(after.context());
            assertEquals(0, after.status(), after.message());
        }
    }

    /** AC3: the list of connectors shows only those the caller may ReadStatus. */
    @Test
    void theListShowsOnlyTheConnectorsTheCallerMayReadTheStatusOf() throws IOException {
        Object shown = list("alice", List.of("billing-sink", "payments-sink", "billing-source"),
                onConnectors("ResourceViewer", "User:alice", "billing-"));
        assertEquals(List.of("billing-sink", "billing-source"), shown);
        assertEquals(List.of(), list("alice", List.of("billing-sink"),
                onConnectors("ResourceViewer", "User:alice", "payments-")));
    }

    /** AC3: with {@code ?expand=}, the same; and a connector's configuration only where it may ReadConfig. */
    @Test
    void theExpandedListShowsOnlyWhatTheCallerMaySee() throws IOException {
        Map<String, Object> listing = new LinkedHashMap<>();
        for (String name : List.of("billing-sink", "billing-source", "payments-sink")) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("status", "status of " + name);
            details.put("info", "config of " + name);
            listing.put(name, details);
        }
        Object shown = list("alice", listing, onConnectors("ResourceViewer", "User:alice", "billing-"),
                onConnectors("DeveloperRead", "User:alice", "billing-source"));
        assertEquals(Map.of(
                "billing-sink", Map.of("status", "status of billing-sink"),
                "billing-source", Map.of("status", "status of billing-source", "info", "config of billing-source")),
                shown);
    }

    @Test
    void aSuperUserSeesTheWholeList() throws IOException {
        try (TestRestAccess test = TestRestAccess.of("User:admin")) {
            test.refresh();
            assertEquals(List.of("billing-sink"), list(test, "admin", List.of("billing-sink")));
        }
    }

    @Test
    void aListOfAShapeTheFilterDoesNotKnowShowsNothing() throws IOException {
        assertEquals(Map.of(), list("alice", "billing-sink", onConnectors("ResourceOwner", "User:alice", "billing-")));
    }

    @Test
    void onlyTheListIsNarrowed() throws IOException {
        try (TestRestAccess test = TestRestAccess.of("", onConnectors("ResourceViewer", "User:alice", "billing-"))) {
            test.refresh();
            ConnectorFilter filter = new ConnectorFilter(test.access(), FakeRequest.connectors());
            FakeRequest request = FakeRequest.of("GET", "/connector-plugins").as("alice");
            filter.filter(request.context());
            FakeRequest.FakeResponse response = new FakeRequest.FakeResponse(List.of("payments-sink"));
            filter.filter(request.context(), response.context());
            assertEquals(List.of("payments-sink"), response.entity);
        }
    }

    private static Object list(String user, Object listing, AclBinding... bindings) throws IOException {
        try (TestRestAccess test = TestRestAccess.of("", bindings)) {
            test.refresh();
            return list(test, user, listing);
        }
    }

    private static Object list(TestRestAccess test, String user, Object listing) throws IOException {
        ConnectorFilter filter = new ConnectorFilter(test.access(), FakeRequest.connectors());
        FakeRequest request = FakeRequest.of("GET", "/connectors").as(user);
        filter.filter(request.context());
        assertEquals(0, request.status(), request.message());
        FakeRequest.FakeResponse response = new FakeRequest.FakeResponse(listing);
        filter.filter(request.context(), response.context());
        return response.entity;
    }

    private static void filter(boolean exists, FakeRequest request, AclBinding... bindings) throws IOException {
        try (TestRestAccess test = TestRestAccess.of("", bindings)) {
            test.refresh();
            ConnectClusterState cluster = exists ? FakeRequest.connectors("billing-sink") : FakeRequest.connectors();
            new ConnectorFilter(test.access(), cluster).filter(request.context());
        }
    }

    /** Whether the role grants the operation, by asking the class that ships with a binding. */
    private static boolean grants(String role, ConnectorOperation operation) {
        try (TestRestAccess test = TestRestAccess.of("", onConnectors(role, "User:x", "billing-"))) {
            test.refresh();
            return test.access().grants(new org.apache.kafka.common.security.auth.KafkaPrincipal("User", "x"),
                    operation, "billing-sink");
        }
    }
}
