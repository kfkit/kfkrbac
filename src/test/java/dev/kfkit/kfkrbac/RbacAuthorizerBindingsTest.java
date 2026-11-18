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
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.allow;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.deny;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.literal;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.prefixed;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.stderrOf;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Role bindings read from ACL records: the authorizer as the broker creates
 * it, fed the way the broker feeds it, with the real
 * {@link StandardAuthorizer} underneath and no broker.
 *
 * <p>The deny ACLs of {@link RbacAuthorizerTest} are run again here against
 * bindings that are records.
 */
class RbacAuthorizerBindingsTest {

    private static final String ORDERS = "billing.orders";
    private static final String INVOICES = "billing.invoices";
    private static final ResourcePattern BILLING = prefixed(TOPIC, "billing.");
    private static final ResourcePattern THE_CLUSTER = literal(CLUSTER, "kafka-cluster");

    private static final String NOT_A_BINDING = "is not a role binding";

    /** Where the authorizer registers its plugin metrics, as the broker would have it. */
    private Metrics metrics;
    private RbacAuthorizer authorizer;

    @AfterEach
    void close() {
        if (authorizer == null) {
            return;
        }
        try {
            authorizer.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            authorizer = null;
            metrics.close();
        }
    }

    // --- AC1: records arrive and leave ------------------------------------------

    @Test
    void aBindingAddedAllowsWhatItsRoleGrantsOnItsPatternUntilItIsRemoved() {
        start();
        assertEquals(DENIED, can("alice", WRITE, TOPIC, ORDERS));

        Uuid id = Uuid.randomUuid();
        authorizer.addAcl(id, binding("DeveloperWrite:User:alice", BILLING));
        assertEquals(ALLOWED, can("alice", WRITE, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", WRITE, TOPIC, "payments.orders"));
        assertEquals(DENIED, can("bob", WRITE, TOPIC, ORDERS));

        authorizer.removeAcl(id);
        assertEquals(DENIED, can("alice", WRITE, TOPIC, ORDERS));
    }

    @Test
    void aBindingInASnapshotAllowsTheSame() {
        start(binding("DeveloperWrite:User:alice", BILLING));

        assertEquals(ALLOWED, can("alice", WRITE, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", WRITE, TOPIC, "payments.orders"));
        assertEquals(DENIED, can("bob", WRITE, TOPIC, ORDERS));
    }

    @Test
    void aSecondSnapshotReplacesTheFirst() {
        start(binding("DeveloperWrite:User:alice", BILLING));

        authorizer.loadSnapshot(snapshot(binding("DeveloperWrite:User:alice", prefixed(TOPIC, "payments."))));
        assertEquals(DENIED, can("alice", WRITE, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("alice", WRITE, TOPIC, "payments.orders"));

        authorizer.loadSnapshot(Map.of());
        assertEquals(DENIED, can("alice", WRITE, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", WRITE, TOPIC, "payments.orders"));
    }

    @Test
    void aBindingFromASnapshotIsRemovedByItsId() {
        Uuid id = Uuid.randomUuid();
        start();
        authorizer.loadSnapshot(Map.of(id, binding("DeveloperWrite:User:alice", BILLING)));
        assertEquals(ALLOWED, can("alice", WRITE, TOPIC, ORDERS));

        authorizer.removeAcl(id);
        assertEquals(DENIED, can("alice", WRITE, TOPIC, ORDERS));
    }

    @Test
    void removingOneBindingLeavesTheOthersOfTheMember() {
        start();
        Uuid billing = Uuid.randomUuid();
        authorizer.addAcl(billing, binding("DeveloperWrite:User:alice", BILLING));
        authorizer.addAcl(Uuid.randomUuid(), binding("DeveloperWrite:User:alice", prefixed(TOPIC, "payments.")));
        authorizer.addAcl(Uuid.randomUuid(), binding("DeveloperRead:User:alice", BILLING));

        authorizer.removeAcl(billing);

        assertEquals(DENIED, can("alice", WRITE, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("alice", WRITE, TOPIC, "payments.orders"));
        assertEquals(ALLOWED, can("alice", READ, TOPIC, ORDERS));
    }

    // --- AC2: what a binding's resource type covers -------------------------------

    @Test
    void aTopicBindingCoversEveryTypeItsRoleGrants() {
        start(binding("DeveloperRead:User:bob", BILLING));

        assertEquals(ALLOWED, can("bob", READ, TOPIC, INVOICES));
        assertEquals(ALLOWED, can("bob", READ, GROUP, "billing.invoicing"));
        assertEquals(ALLOWED, can("bob", DESCRIBE, TRANSACTIONAL_ID, "billing.tx"));

        assertEquals(DENIED, can("bob", READ, TOPIC, "orders"));
        assertEquals(DENIED, can("bob", READ, GROUP, "payments.invoicing"));
        assertEquals(DENIED, can("bob", DESCRIBE, TRANSACTIONAL_ID, "payments.tx"));
        assertEquals(DENIED, can("bob", WRITE, TRANSACTIONAL_ID, "billing.tx"));
        assertEquals(DENIED, can("alice", READ, GROUP, "billing.invoicing"));
    }

    @Test
    void aGroupBindingCoversOnlyTheGroups() {
        start(binding("DeveloperRead:User:bob", prefixed(GROUP, "billing.")));

        assertEquals(ALLOWED, can("bob", READ, GROUP, "billing.invoicing"));
        assertEquals(DENIED, can("bob", READ, GROUP, "payments.invoicing"));
        assertEquals(DENIED, can("bob", READ, TOPIC, INVOICES));
        assertEquals(DENIED, can("bob", DESCRIBE, TRANSACTIONAL_ID, "billing.tx"));
    }

    @Test
    void aTransactionalIdBindingCoversOnlyTheTransactionalIds() {
        start(binding("DeveloperRead:User:bob", prefixed(TRANSACTIONAL_ID, "billing.")));

        assertEquals(ALLOWED, can("bob", DESCRIBE, TRANSACTIONAL_ID, "billing.tx"));
        assertEquals(DENIED, can("bob", DESCRIBE, TRANSACTIONAL_ID, "payments.tx"));
        assertEquals(DENIED, can("bob", READ, TOPIC, INVOICES));
        assertEquals(DENIED, can("bob", READ, GROUP, "billing.invoicing"));
    }

    @Test
    void aTransactionalProducerNeedsOneBinding() {
        start(binding("DeveloperWrite:User:alice", BILLING));

        assertEquals(ALLOWED, can("alice", WRITE, TRANSACTIONAL_ID, "billing.tx"));
        assertEquals(ALLOWED, can("alice", WRITE, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", READ, GROUP, "billing.invoicing"));
    }

    // --- AC3: a pattern matches what an ACL with that pattern would ---------------

    static Stream<Arguments> patterns() {
        return Stream.of(
                Arguments.of(literal(TOPIC, "orders"), List.of("orders")),
                Arguments.of(prefixed(TOPIC, "billing."), List.of("billing.", INVOICES)),
                Arguments.of(literal(TOPIC, "*"), List.of("orders", "orders-2", "billing", "billing.", INVOICES, "*")),
                Arguments.of(prefixed(TOPIC, "*"), List.of("*")));
    }

    @ParameterizedTest
    @MethodSource("patterns")
    void aPatternMatchesWhatAnAclWithThatPatternWould(ResourcePattern pattern, List<String> matched)
            throws IOException {
        List<String> names = List.of("orders", "orders-2", "billing", "billing.", INVOICES, "*");
        start(binding("DeveloperRead:User:alice", pattern));

        try (StandardAuthorizer acls = new StandardAuthorizer(); Metrics aclMetrics = new Metrics()) {
            started(acls, aclMetrics, allow("User:alice", pattern, READ));
            for (String name : names) {
                AuthorizationResult byAcl = acls.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH),
                        List.of(new Action(READ, literal(TOPIC, name), 1, true, true))).get(0);
                assertEquals(matched.contains(name) ? ALLOWED : DENIED, can("alice", READ, TOPIC, name), name);
                assertEquals(byAcl, can("alice", READ, TOPIC, name), name + ", as the ACL");
            }
        }
    }

    // --- AC4: a cluster role ------------------------------------------------------

    @Test
    void aClusterAdminCreatesAnyTopicAndReadsNone() {
        start(binding("ClusterAdmin:User:carol", THE_CLUSTER));

        assertEquals(ALLOWED, can("carol", CREATE, TOPIC, "orders"));
        assertEquals(ALLOWED, can("carol", CREATE, TOPIC, INVOICES));
        assertEquals(ALLOWED, can("carol", CREATE, CLUSTER, "kafka-cluster"));
        assertEquals(DENIED, can("carol", READ, TOPIC, "orders"));
        assertEquals(DENIED, can("carol", READ, TOPIC, INVOICES));
        assertEquals(DENIED, can("carol", READ, GROUP, "billing.invoicing"));
        assertEquals(DENIED, can("carol", CLUSTER_ACTION, CLUSTER, "kafka-cluster"));
    }

    @Test
    void withoutTheClusterRoleNoTopicIsCreated() {
        start(binding("ClusterAdmin:User:alice", THE_CLUSTER));

        assertEquals(DENIED, can("carol", CREATE, TOPIC, "orders"));
        assertEquals(DENIED, can("carol", CREATE, CLUSTER, "kafka-cluster"));
    }

    @Test
    void aSystemAdminMayDoEverythingEverywhere() {
        start(binding("SystemAdmin:User:carol", THE_CLUSTER));

        assertEquals(ALLOWED, can("carol", READ, TOPIC, "orders"));
        assertEquals(ALLOWED, can("carol", READ, GROUP, "billing.invoicing"));
        assertEquals(ALLOWED, can("carol", WRITE, TRANSACTIONAL_ID, "billing.tx"));
        assertEquals(ALLOWED, can("carol", CLUSTER_ACTION, CLUSTER, "kafka-cluster"));
        assertEquals(DENIED, can("alice", READ, TOPIC, "orders"));
    }

    // --- AC5: a record of a role that is not a binding ------------------------------

    static Stream<Arguments> malformed() {
        StandardAcl aliceReadsBilling = binding("DeveloperRead:User:alice", BILLING);
        return Stream.of(
                Arguments.of("operation not ALL",
                        acl("DeveloperRead:User:alice", BILLING, READ, "*", ALLOW), aliceReadsBilling, READ),
                Arguments.of("permission DENY",
                        acl("DeveloperRead:User:alice", BILLING, ALL, "*", DENY), aliceReadsBilling, READ),
                Arguments.of("host not *",
                        acl("DeveloperRead:User:alice", BILLING, ALL, "127.0.0.1", ALLOW), aliceReadsBilling, READ),
                Arguments.of("a cluster role on a name pattern",
                        binding("ClusterAdmin:User:alice", BILLING),
                        binding("ClusterAdmin:User:alice", THE_CLUSTER), CREATE),
                Arguments.of("a pattern role on the cluster",
                        binding("DeveloperRead:User:alice", THE_CLUSTER), aliceReadsBilling, READ),
                Arguments.of("a member that is not Type:name",
                        binding("DeveloperRead:alice", BILLING), aliceReadsBilling, READ));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformed")
    void aMalformedRecordGrantsNothingIsLoggedOnceAndCounted(String shape, StandardAcl record,
            StandardAcl wellFormed, AclOperation operation) {
        start();
        Uuid id = Uuid.randomUuid();

        String log = stderrOf(() -> authorizer.addAcl(id, record));

        for (ResourceType type : List.of(TOPIC, GROUP, TRANSACTIONAL_ID)) {
            for (AclOperation any : Role.everyAclOperation()) {
                assertEquals(DENIED, can("alice", any, type, INVOICES), any + " " + type);
            }
        }
        for (AclOperation any : Role.everyAclOperation()) {
            assertEquals(DENIED, can("alice", any, CLUSTER, "kafka-cluster"), any + " CLUSTER");
        }
        assertEquals(DENIED, byType("alice", operation, TOPIC));
        assertEquals(1, occurrences(log, NOT_A_BINDING), log);
        assertTrue(log.contains(id.toString()), log);
        assertEquals(1, malformedBindings());

        // The twin: the same record, well-formed, grants.
        authorizer.removeAcl(id);
        assertEquals(0, malformedBindings());
        String quiet = stderrOf(() -> authorizer.addAcl(id, wellFormed));
        assertEquals(ALLOWED, can("alice", operation, TOPIC, INVOICES));
        assertEquals(0, occurrences(quiet, NOT_A_BINDING), quiet);
        assertEquals(0, malformedBindings());
    }

    @Test
    void aMalformedRecordInSnapshotsIsLoggedOnceAndCountedWhileItIsThere() {
        start();
        Map<Uuid, StandardAcl> records = Map.of(
                Uuid.randomUuid(), acl("DeveloperRead:User:alice", BILLING, READ, "*", ALLOW),
                Uuid.randomUuid(), binding("ClusterAdmin:User:alice", BILLING),
                Uuid.randomUuid(), binding("DeveloperRead:User:bob", BILLING));

        String log = stderrOf(() -> {
            authorizer.loadSnapshot(records);
            authorizer.loadSnapshot(new HashMap<>(records));
        });

        assertEquals(2, occurrences(log, NOT_A_BINDING), log);
        assertEquals(2, malformedBindings());
        assertEquals(DENIED, can("alice", READ, TOPIC, INVOICES));
        assertEquals(ALLOWED, can("bob", READ, TOPIC, INVOICES));

        authorizer.loadSnapshot(Map.of());
        assertEquals(0, malformedBindings());
    }

    @Test
    void recordsThatBindNothingToThePrincipalGrantNothingAndAreNotMalformed() {
        start();

        // A role that does not exist, a group membership, a binding of a group, and a
        // member that is a wildcard to an ACL and one more name to a binding.
        String log = stderrOf(() -> {
            authorizer.addAcl(Uuid.randomUuid(), binding("DeveloperDelete:User:alice", BILLING));
            authorizer.addAcl(Uuid.randomUuid(), binding("Member:User:alice", literal(TOPIC, "Group:payments")));
            authorizer.addAcl(Uuid.randomUuid(), binding("DeveloperRead:Group:payments", BILLING));
            authorizer.addAcl(Uuid.randomUuid(), binding("DeveloperRead:User:*", BILLING));
        });

        assertEquals(DENIED, can("alice", READ, TOPIC, INVOICES));
        assertEquals(DENIED, byType("alice", READ, TOPIC));
        assertEquals(0, occurrences(log, NOT_A_BINDING), log);
        assertEquals(0, malformedBindings());
        assertEquals(4, authorizer.aclCount());
    }

    // --- AC6: the parent makes nothing of a binding record --------------------------

    @Test
    void aBindingRecordAllowsNothingWithoutTheRoles() throws IOException {
        StandardAcl record = binding("DeveloperWrite:User:alice", BILLING);

        start(new RbacAuthorizer(Roles.NONE), record);
        assertEquals(DENIED, can("alice", WRITE, TOPIC, ORDERS));
        assertEquals(DENIED, byType("alice", WRITE, TOPIC));
        assertEquals(List.of(record.toBinding()), listed(authorizer.acls(AclBindingFilter.ANY)));

        try (StandardAuthorizer parent = new StandardAuthorizer(); Metrics parentMetrics = new Metrics()) {
            started(parent, parentMetrics, record);
            assertEquals(List.of(DENIED), parent.authorize(RequestContext.of("alice", "billing-1", ApiKeys.PRODUCE),
                    List.of(new Action(WRITE, literal(TOPIC, ORDERS), 1, true, true))));
        }
    }

    @Test
    void withTheRolesTheSameRecordAllowsAndIsStillListed() {
        StandardAcl record = binding("DeveloperWrite:User:alice", BILLING);
        start(record);

        assertEquals(ALLOWED, can("alice", WRITE, TOPIC, ORDERS));
        assertEquals(ALLOWED, byType("alice", WRITE, TOPIC));
        assertEquals(List.of(record.toBinding()), listed(authorizer.acls(AclBindingFilter.ANY)));
        assertEquals(1, authorizer.aclCount());
    }

    // --- AC7: the second question ---------------------------------------------------

    @Test
    void byResourceTypeAWriterOfSomeTopicsMayWrite() {
        start(binding("DeveloperWrite:User:bob", BILLING));

        assertEquals(ALLOWED, byType("bob", WRITE, TOPIC));
    }

    static Stream<Arguments> bindingsThatDoNotLetBobWriteATopic() {
        return Stream.of(
                Arguments.of("of another member", binding("DeveloperWrite:User:alice", BILLING)),
                Arguments.of("of a role that does not write", binding("DeveloperRead:User:bob", BILLING)),
                Arguments.of("narrowed to groups", binding("DeveloperWrite:User:bob", prefixed(GROUP, "billing."))),
                Arguments.of("narrowed to transactional IDs",
                        binding("DeveloperWrite:User:bob", prefixed(TRANSACTIONAL_ID, "billing."))),
                Arguments.of("of a cluster role that does not write", binding("ClusterAdmin:User:bob", THE_CLUSTER)),
                Arguments.of("malformed", acl("DeveloperWrite:User:bob", BILLING, WRITE, "*", ALLOW)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("bindingsThatDoNotLetBobWriteATopic")
    void byResourceTypeWithoutSuchABindingItIsDenied(String kind, StandardAcl record) {
        start(record);

        assertEquals(DENIED, byType("bob", WRITE, TOPIC));
    }

    @Test
    void byResourceTypeANarrowedBindingAnswersForItsOwnType() {
        start(binding("DeveloperWrite:User:bob", prefixed(TRANSACTIONAL_ID, "billing.")));

        assertEquals(ALLOWED, byType("bob", WRITE, TRANSACTIONAL_ID));
        assertEquals(DENIED, byType("alice", WRITE, TRANSACTIONAL_ID));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dev.kfkit.kfkrbac.RbacAuthorizerTest#deniesThatCoverTheBinding")
    void byResourceTypeADenyCoveringTheBindingDenies(String kind, StandardAcl deny) {
        start(binding("DeveloperWrite:User:bob", BILLING), deny);

        assertEquals(DENIED, byType("bob", WRITE, TOPIC));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dev.kfkit.kfkrbac.RbacAuthorizerTest#deniesThatLeaveSomeOfTheBinding")
    void byResourceTypeADenyLeavingSomeOfTheBindingAllows(String kind, StandardAcl deny) {
        start(binding("DeveloperWrite:User:bob", BILLING), deny);

        assertEquals(ALLOWED, byType("bob", WRITE, TOPIC));
    }

    @Test
    void byResourceTypeALiteralBindingIsCoveredByADenyOfItsName() {
        ResourcePattern orders = literal(TOPIC, "orders");
        start(binding("DeveloperWrite:User:bob", orders));
        assertEquals(ALLOWED, byType("bob", WRITE, TOPIC));
        close();

        start(binding("DeveloperWrite:User:bob", orders), deny("User:bob", orders, WRITE, "*"));
        assertEquals(DENIED, byType("bob", WRITE, TOPIC));
    }

    @Test
    void byResourceTypeAWildcardBindingIsCoveredOnlyByAWildcardDeny() {
        ResourcePattern any = literal(TOPIC, "*");
        start(binding("DeveloperWrite:User:bob", any), deny("User:bob", BILLING, WRITE, "*"));
        assertEquals(ALLOWED, byType("bob", WRITE, TOPIC));
        close();

        start(binding("DeveloperWrite:User:bob", any), deny("User:bob", any, WRITE, "*"));
        assertEquals(DENIED, byType("bob", WRITE, TOPIC));
    }

    @Test
    void byResourceTypeOneUncoveredBindingIsEnough() {
        start(binding("DeveloperWrite:User:bob", BILLING), binding("DeveloperWrite:User:bob", literal(TOPIC,
                "orders")), deny("User:bob", BILLING, WRITE, "*"));

        assertEquals(ALLOWED, byType("bob", WRITE, TOPIC));
    }

    @Test
    void byResourceTypeAClusterRoleIsCoveredOnlyByAWildcardDeny() {
        StandardAcl admin = binding("SystemAdmin:User:bob", THE_CLUSTER);
        start(admin);
        assertEquals(ALLOWED, byType("bob", WRITE, TOPIC));
        assertEquals(ALLOWED, byType("bob", CLUSTER_ACTION, CLUSTER));
        assertEquals(DENIED, byType("alice", WRITE, TOPIC));
        close();

        start(admin, deny("User:bob", BILLING, WRITE, "*"));
        assertEquals(ALLOWED, byType("bob", WRITE, TOPIC));
        close();

        start(admin, deny("User:bob", literal(TOPIC, "*"), WRITE, "*"),
                deny("User:bob", THE_CLUSTER, CLUSTER_ACTION, "*"));
        assertEquals(DENIED, byType("bob", WRITE, TOPIC));
        assertEquals(DENIED, byType("bob", CLUSTER_ACTION, CLUSTER));
    }

    // --- AC8: a deny ACL beats a binding ----------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("dev.kfkit.kfkrbac.RbacAuthorizerTest#denies")
    void aDenyAclBeatsTheBinding(String kind, StandardAcl deny) {
        start(binding("DeveloperRead:User:bob", BILLING), deny);

        assertEquals(DENIED, can("bob", READ, TOPIC, INVOICES));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dev.kfkit.kfkrbac.RbacAuthorizerTest#deniesThatDoNotMatch")
    void aDenyAclThatDoesNotMatchLeavesTheBinding(String kind, StandardAcl deny) {
        start(binding("DeveloperRead:User:bob", BILLING), deny);

        assertEquals(ALLOWED, can("bob", READ, TOPIC, INVOICES));
    }

    @Test
    void aDenyAclAddedLaterBeatsTheBindingUntilItIsRemoved() {
        start(binding("DeveloperRead:User:bob", BILLING));
        Uuid id = Uuid.randomUuid();

        authorizer.addAcl(id, deny("User:bob", BILLING, READ, "*"));
        assertEquals(DENIED, can("bob", READ, TOPIC, INVOICES));

        authorizer.removeAcl(id);
        assertEquals(ALLOWED, can("bob", READ, TOPIC, INVOICES));
    }

    @Test
    void aDenyAclBeatsAClusterRole() {
        start(binding("SystemAdmin:User:bob", THE_CLUSTER), deny("User:bob", BILLING, READ, "*"));

        assertEquals(DENIED, can("bob", READ, TOPIC, INVOICES));
        assertEquals(ALLOWED, can("bob", READ, TOPIC, "orders"));
    }

    // --- AC9: bindings for what is not a broker resource --------------------------------

    @ParameterizedTest
    @ValueSource(strings = {"Subject:orders-", "Connector:billing-"})
    void aBindingForSubjectsOrConnectorsMatchesNoBrokerResource(String name) {
        String log = stderrOf(() -> start(binding("ResourceOwner:User:alice", prefixed(TOPIC, name)),
                binding("ResourceOwner:User:alice", literal(TOPIC, name + "1"))));
        String bare = name.substring(name.indexOf(':') + 1);

        for (ResourceType type : List.of(TOPIC, GROUP, TRANSACTIONAL_ID)) {
            for (String resource : List.of(name, name + "1", name + "2", bare, bare + "1")) {
                assertEquals(DENIED, can("alice", DESCRIBE, type, resource), type + " " + resource);
            }
            assertEquals(DENIED, byType("alice", DESCRIBE, type), type.toString());
        }
        assertEquals(0, occurrences(log, NOT_A_BINDING), log);
        assertEquals(0, malformedBindings());
    }

    @Test
    void aBindingOnAPrefixWithoutTheColonMatchesSuchNames() {
        start(binding("ResourceOwner:User:alice", prefixed(TOPIC, "Subject")));

        assertEquals(ALLOWED, can("alice", DESCRIBE, GROUP, "Subject:orders-1"));
        assertEquals(ALLOWED, can("alice", DESCRIBE, TRANSACTIONAL_ID, "Subject:orders-1"));
    }

    // --- AC10: records change while requests are answered --------------------------------

    @Test
    void bindingsChangeWhileOtherThreadsAuthorize() throws Exception {
        StandardAcl bobs = binding("DeveloperWrite:User:bob", BILLING);
        StandardAcl first = binding("DeveloperWrite:User:alice", BILLING);
        StandardAcl second = binding("ResourceOwner:User:alice", BILLING);
        StandardAcl erins = binding("DeveloperWrite:User:erin", BILLING);
        start(bobs);
        Uuid firstId = Uuid.randomUuid();
        authorizer.addAcl(firstId, first);

        // alice holds the write through one record or the other in every state the
        // writer leaves behind, bob always and dave never; erin comes and goes.
        int readers = 4;
        AtomicBoolean done = new AtomicBoolean();
        AtomicLong asked = new AtomicLong();
        Queue<String> wrong = new ConcurrentLinkedQueue<>();
        CountDownLatch reading = new CountDownLatch(readers);
        ExecutorService pool = Executors.newFixedThreadPool(readers);
        try {
            for (int i = 0; i < readers; i++) {
                pool.execute(() -> {
                    boolean counted = false;
                    try {
                        while (!done.get() && wrong.isEmpty()) {
                            expect(ALLOWED, can("alice", WRITE, TOPIC, ORDERS), "alice writes", wrong);
                            expect(ALLOWED, byType("alice", WRITE, TOPIC), "alice writes some topic", wrong);
                            expect(ALLOWED, can("bob", WRITE, TOPIC, ORDERS), "bob writes", wrong);
                            expect(DENIED, can("dave", WRITE, TOPIC, ORDERS), "dave writes", wrong);
                            expect(DENIED, byType("dave", WRITE, TOPIC), "dave writes some topic", wrong);
                            can("erin", WRITE, TOPIC, ORDERS);
                            byType("erin", WRITE, TOPIC);
                            asked.incrementAndGet();
                            if (!counted) {
                                counted = true;
                                reading.countDown();
                            }
                        }
                    } catch (Throwable e) {
                        wrong.add(e.toString());
                    } finally {
                        if (!counted) {
                            reading.countDown();
                        }
                    }
                });
            }
            assertTrue(reading.await(30, TimeUnit.SECONDS), "the readers started");

            for (int i = 0; i < 2000 && wrong.isEmpty(); i++) {
                Uuid secondId = Uuid.randomUuid();
                Uuid erinsId = Uuid.randomUuid();
                authorizer.addAcl(secondId, second);
                authorizer.removeAcl(firstId);
                authorizer.addAcl(erinsId, erins);
                firstId = Uuid.randomUuid();
                authorizer.addAcl(firstId, first);
                authorizer.removeAcl(secondId);
                authorizer.removeAcl(erinsId);
            }
            for (int i = 0; i < 500 && wrong.isEmpty(); i++) {
                authorizer.loadSnapshot(snapshot(bobs, i % 2 == 0 ? second : first, erins));
                authorizer.loadSnapshot(snapshot(bobs, i % 2 == 0 ? first : second));
            }
        } finally {
            done.set(true);
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "the readers stopped");
        }

        assertEquals(List.of(), new ArrayList<>(wrong));
        assertTrue(asked.get() >= readers, "every reader asked");
    }

    // --- helpers ---------------------------------------------------------------------

    /** The authorizer as the broker creates it. */
    private void start(StandardAcl... records) {
        start(new RbacAuthorizer(), records);
    }

    private void start(RbacAuthorizer created, StandardAcl... records) {
        metrics = new Metrics();
        authorizer = started(created, metrics, records);
    }

    private static <A extends StandardAuthorizer> A started(A authorizer, Metrics metrics, StandardAcl... records) {
        authorizer.configure(Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, "User:admin"));
        authorizer.withPluginMetrics(new PluginMetricsImpl(metrics, Map.of()));
        authorizer.loadSnapshot(snapshot(records));
        authorizer.completeInitialLoad();
        return authorizer;
    }

    private static Map<Uuid, StandardAcl> snapshot(StandardAcl... records) {
        Map<Uuid, StandardAcl> snapshot = new HashMap<>();
        for (StandardAcl record : records) {
            snapshot.put(Uuid.randomUuid(), record);
        }
        return snapshot;
    }

    /** A role binding as it is created: operation {@code ALL}, permission {@code ALLOW}, host {@code *}. */
    private static StandardAcl binding(String roleAndMember, ResourcePattern pattern) {
        return acl(roleAndMember, pattern, ALL, "*", ALLOW);
    }

    private AuthorizationResult can(String user, AclOperation operation, ResourceType type, String name) {
        return authorizer.authorize(RequestContext.of(user, "billing-1", ApiKeys.FETCH),
                List.of(new Action(operation, literal(type, name), 1, true, true))).get(0);
    }

    private AuthorizationResult byType(String user, AclOperation operation, ResourceType type) {
        return authorizer.authorizeByResourceType(RequestContext.of(user, "billing-1", ApiKeys.INIT_PRODUCER_ID),
                operation, type);
    }

    /** The gauge the authorizer registered with the broker's plugin metrics. */
    private int malformedBindings() {
        for (KafkaMetric metric : metrics.metrics().values()) {
            if (metric.metricName().name().equals(RbacAuthorizer.MALFORMED_BINDINGS_METRIC)) {
                return (Integer) metric.metricValue();
            }
        }
        throw new AssertionError("no metric " + RbacAuthorizer.MALFORMED_BINDINGS_METRIC);
    }

    private static void expect(AuthorizationResult expected, AuthorizationResult actual, String what,
            Queue<String> wrong) {
        if (actual != expected) {
            wrong.add(what + ": " + actual);
        }
    }

    private static List<AclBinding> listed(Iterable<AclBinding> acls) {
        List<AclBinding> list = new ArrayList<>();
        acls.forEach(list::add);
        return list;
    }

    private static int occurrences(String text, String part) {
        int count = 0;
        for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + part.length())) {
            count++;
        }
        return count;
    }
}
