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

import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The parent authorizer can be driven without a broker: configure, hand it
 * plugin metrics, load the ACLs, finish the initial load, authorize. That is
 * how Kafka's own tests do it and how every test here reaches the code that
 * ships.
 *
 * <p>Since Kafka 4.1 the broker always calls {@code withPluginMetrics} on the
 * authorizer and {@code authorize} dereferences what it set; skip the call
 * and the parent throws a {@code NullPointerException}.
 *
 * <p>In most tests {@code User:bob} holds a role that lets him read the
 * topics starting with {@code billing.}, and no ACL allows him anything.
 */
class RbacAuthorizerTest {

    private static final String INVOICES = "billing.invoices";
    private static final ResourcePattern BILLING = prefixed(ResourceType.TOPIC, "billing.");
    private static final ResourcePattern ORDERS = literal(ResourceType.TOPIC, "orders");

    private RbacAuthorizer authorizer;

    @AfterEach
    void close() {
        if (authorizer != null) {
            try {
                authorizer.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            authorizer = null;
        }
    }

    // --- the parent alone ---------------------------------------------------

    @Test
    void withoutRolesTheVerdictIsTheParents() {
        start(Roles.NONE);
        List<Action> read = List.of(new Action(READ, ORDERS, 1, true, true));

        assertEquals(List.of(ALLOWED),
                authorizer.authorize(RequestContext.of("admin", "billing-1", ApiKeys.FETCH), read));
        assertEquals(List.of(DENIED),
                authorizer.authorize(RequestContext.of("bob", "billing-2", ApiKeys.FETCH), read));
    }

    // --- AC1: a role allows what no ACL allows ------------------------------

    @Test
    void aRoleAllowsWhatNoAclAllows() {
        start(bobReadsBilling());

        assertEquals(ALLOWED, read("bob", INVOICES));
    }

    @Test
    void withoutTheRoleTheSameRequestIsDenied() {
        start(new BoundRoles().grant("alice", READ, BILLING));

        assertEquals(DENIED, read("bob", INVOICES));
    }

    @Test
    void aRoleCoversOnlyItsPatternAndOperation() {
        start(bobReadsBilling());

        assertEquals(DENIED, read("bob", "orders"));
        assertEquals(DENIED, authorize("bob", WRITE, literal(ResourceType.TOPIC, INVOICES)));
    }

    @Test
    void eachActionOfARequestIsAnsweredOnItsOwn() {
        start(bobReadsBilling());
        List<Action> actions = List.of(
                new Action(READ, literal(ResourceType.TOPIC, INVOICES), 1, true, true),
                new Action(READ, ORDERS, 1, true, true));

        assertEquals(List.of(ALLOWED, DENIED),
                authorizer.authorize(RequestContext.of("bob", "billing-1", ApiKeys.FETCH), actions));
    }

    // --- AC2: a deny ACL beats every role ------------------------------------

    static Stream<Arguments> denies() {
        ResourcePattern invoices = literal(ResourceType.TOPIC, INVOICES);
        return Stream.of(
                Arguments.of("literal", deny("User:bob", invoices, READ, "*")),
                Arguments.of("prefixed", deny("User:bob", BILLING, READ, "*")),
                Arguments.of("shorter prefix", deny("User:bob", prefixed(ResourceType.TOPIC, "bill"), READ, "*")),
                Arguments.of("wildcard", deny("User:bob", literal(ResourceType.TOPIC, "*"), READ, "*")),
                Arguments.of("User:*", deny("User:*", invoices, READ, "*")),
                Arguments.of("this host", deny("User:bob", invoices, READ, "127.0.0.1")),
                Arguments.of("ALL operations", deny("User:bob", invoices, ALL, "*")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("denies")
    void aDenyAclBeatsTheRole(String kind, StandardAcl deny) {
        start(bobReadsBilling(), deny);

        assertEquals(DENIED, read("bob", INVOICES));
    }

    static Stream<Arguments> deniesThatDoNotMatch() {
        ResourcePattern invoices = literal(ResourceType.TOPIC, INVOICES);
        return Stream.of(
                Arguments.of("another host", deny("User:bob", invoices, READ, "10.0.0.9")),
                Arguments.of("another principal", deny("User:alice", invoices, READ, "*")),
                Arguments.of("another operation", deny("User:bob", invoices, WRITE, "*")),
                Arguments.of("another topic", deny("User:bob", ORDERS, READ, "*")),
                Arguments.of("a longer prefix", deny("User:bob", prefixed(ResourceType.TOPIC, "billing.x"), READ, "*")),
                Arguments.of("another resource type", deny("User:bob", literal(ResourceType.GROUP, INVOICES), READ,
                        "*")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deniesThatDoNotMatch")
    void aDenyAclThatDoesNotMatchLeavesTheRole(String kind, StandardAcl deny) {
        start(bobReadsBilling(), deny);

        assertEquals(ALLOWED, read("bob", INVOICES));
    }

    @Test
    void aDenyAclStillBeatsAnAllowAcl() {
        ResourcePattern invoices = literal(ResourceType.TOPIC, INVOICES);
        start(bobReadsBilling(), allow("User:bob", invoices, READ), deny("User:bob", BILLING, READ, "*"));

        assertEquals(DENIED, read("bob", INVOICES));
    }

    // --- AC3: deny ACLs follow the metadata log ------------------------------

    @Test
    void aDenyAclAddedAfterStartupTakesEffect() {
        start(bobReadsBilling());
        Uuid id = Uuid.randomUuid();

        authorizer.addAcl(id, deny("User:bob", BILLING, READ, "*"));
        assertEquals(DENIED, read("bob", INVOICES));

        authorizer.removeAcl(id);
        assertEquals(ALLOWED, read("bob", INVOICES));
    }

    @Test
    void removingAnAllowAclLeavesTheDenyAclsAlone() {
        start(bobReadsBilling());
        Uuid allowId = Uuid.randomUuid();
        authorizer.addAcl(allowId, allow("User:alice", ORDERS, READ));
        authorizer.addAcl(Uuid.randomUuid(), deny("User:bob", BILLING, READ, "*"));

        authorizer.removeAcl(allowId);

        assertEquals(DENIED, read("bob", INVOICES));
        assertEquals(DENIED, read("alice", "orders"));
    }

    @Test
    void aSnapshotReplacesTheDenyAcls() {
        start(bobReadsBilling(), deny("User:bob", BILLING, READ, "*"));
        assertEquals(DENIED, read("bob", INVOICES));

        authorizer.loadSnapshot(Map.of(Uuid.randomUuid(), allow("User:alice", ORDERS, READ)));
        assertEquals(ALLOWED, read("bob", INVOICES));
        assertEquals(ALLOWED, read("alice", "orders"));

        authorizer.loadSnapshot(Map.of(Uuid.randomUuid(), deny("User:bob", BILLING, READ, "*")));
        assertEquals(DENIED, read("bob", INVOICES));
        assertEquals(DENIED, read("alice", "orders"));
    }

    @Test
    void aSnapshotMayKeepADenyAclUnderANewId() {
        StandardAcl deny = deny("User:bob", BILLING, READ, "*");
        start(bobReadsBilling(), deny);

        authorizer.loadSnapshot(Map.of(Uuid.randomUuid(), deny));
        assertEquals(DENIED, read("bob", INVOICES));

        authorizer.loadSnapshot(Map.of());
        assertEquals(ALLOWED, read("bob", INVOICES));
    }

    // --- AC4: what the parent allows never reaches the roles -----------------

    @Test
    void aRequestTheParentAllowsIsNotPassedToTheRoles() {
        start(new NeverAsked(), allow("User:bob", literal(ResourceType.TOPIC, INVOICES), READ));

        assertEquals(ALLOWED, read("bob", INVOICES));
        assertEquals(ALLOWED, authorizer.authorizeByResourceType(
                RequestContext.of("bob", "billing-1", ApiKeys.INIT_PRODUCER_ID), READ, ResourceType.TOPIC));
    }

    @Test
    void aSuperUserIsNotPassedToTheRoles() {
        start(new NeverAsked(), deny("User:*", BILLING, READ, "*"));

        assertEquals(ALLOWED, read("admin", INVOICES));
    }

    // --- AC5: a failing lookup denies ---------------------------------------

    @Test
    void aFailingRoleLookupIsDeniedAndLogged() {
        start(new Failing());

        String log = stderrOf(() -> assertEquals(DENIED, read("bob", INVOICES)));

        assertTrue(log.contains("Role lookup failed for User:bob READ"), log);
        assertTrue(log.contains("bindings not loaded"), log);
    }

    @Test
    void aFailingRoleLookupByResourceTypeIsDeniedAndLogged() {
        start(new Failing());

        String log = stderrOf(() -> assertEquals(DENIED, authorizer.authorizeByResourceType(
                RequestContext.of("bob", "billing-1", ApiKeys.INIT_PRODUCER_ID), WRITE, ResourceType.TOPIC)));

        assertTrue(log.contains("Role lookup failed for User:bob WRITE on any TOPIC"), log);
        assertTrue(log.contains("bindings not loaded"), log);
    }

    @Test
    void aRoleLookupThatKeepsFailingIsLoggedOnceAndEveryRequestIsDenied() {
        start(new Failing());

        String log = stderrOf(() -> {
            for (int i = 0; i < 5; i++) {
                assertEquals(DENIED, read("bob", INVOICES));
                assertEquals(DENIED, writeAnyTopic("bob"));
            }
        });

        assertEquals(1, occurrences(log, "ERROR dev.kfkit.kfkrbac.RbacAuthorizer - Role lookup failed"), log);
        assertEquals(1, occurrences(log, "IllegalStateException: bindings not loaded"), log);
    }

    @Test
    void withTheIntervalAtZeroEveryFailingRoleLookupIsLogged() {
        start(new Failing(), Map.of(DeniedLog.INTERVAL_CONFIG, "0"));

        String log = stderrOf(() -> {
            for (int i = 0; i < 5; i++) {
                assertEquals(DENIED, read("bob", INVOICES));
                assertEquals(DENIED, writeAnyTopic("bob"));
            }
        });

        assertEquals(10, occurrences(log, "ERROR dev.kfkit.kfkrbac.RbacAuthorizer - Role lookup failed"), log);
    }

    // --- AC6: still a ClusterMetadataAuthorizer -------------------------------

    @Test
    void staysAClusterMetadataAuthorizer() {
        // The broker's AclPublisher feeds ACLs only to this type; losing it
        // would mean an authorizer that denies everything, silently.
        assertTrue(ClusterMetadataAuthorizer.class.isInstance(new RbacAuthorizer()));
    }

    @Test
    void aclChangesReachTheParent() {
        start(Roles.NONE);
        Uuid allowId = Uuid.randomUuid();
        Uuid denyId = Uuid.randomUuid();

        authorizer.addAcl(allowId, allow("User:alice", ORDERS, READ));
        assertEquals(ALLOWED, read("alice", "orders"));
        authorizer.addAcl(denyId, deny("User:alice", ORDERS, READ, "*"));
        assertEquals(DENIED, read("alice", "orders"));
        assertEquals(2, authorizer.aclCount());
        assertEquals(2, count(authorizer.acls(AclBindingFilter.ANY)));

        authorizer.removeAcl(denyId);
        assertEquals(ALLOWED, read("alice", "orders"));
        authorizer.removeAcl(allowId);
        assertEquals(DENIED, read("alice", "orders"));
        assertEquals(0, authorizer.aclCount());
    }

    // --- AC7: the second question --------------------------------------------

    @Test
    void byResourceTypeARoleOnSomeTopicAllows() {
        start(new BoundRoles().grant("bob", WRITE, BILLING));

        assertEquals(ALLOWED, writeAnyTopic("bob"));
    }

    @Test
    void byResourceTypeWithoutTheRoleItIsTheParentsAnswer() {
        start(new BoundRoles().grant("alice", WRITE, BILLING));
        assertEquals(DENIED, writeAnyTopic("bob"));
        close();

        start(new BoundRoles().grant("bob", READ, BILLING));
        assertEquals(DENIED, writeAnyTopic("bob"));
        close();

        start(new BoundRoles().grant("bob", WRITE, prefixed(ResourceType.GROUP, "billing.")));
        assertEquals(DENIED, writeAnyTopic("bob"));
        close();

        start(Roles.NONE, allow("User:bob", ORDERS, WRITE));
        assertEquals(ALLOWED, writeAnyTopic("bob"));
    }

    static Stream<Arguments> deniesThatCoverTheBinding() {
        return Stream.of(
                Arguments.of("the same prefix", deny("User:bob", BILLING, WRITE, "*")),
                Arguments.of("a shorter prefix", deny("User:bob", prefixed(ResourceType.TOPIC, "bill"), WRITE, "*")),
                Arguments.of("the wildcard", deny("User:bob", literal(ResourceType.TOPIC, "*"), WRITE, "*")),
                Arguments.of("User:*", deny("User:*", BILLING, WRITE, "*")),
                Arguments.of("this host", deny("User:bob", BILLING, WRITE, "127.0.0.1")),
                Arguments.of("ALL operations", deny("User:bob", BILLING, ALL, "*")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deniesThatCoverTheBinding")
    void byResourceTypeADenyCoveringTheBindingDenies(String kind, StandardAcl deny) {
        start(new BoundRoles().grant("bob", WRITE, BILLING), deny);

        assertEquals(DENIED, writeAnyTopic("bob"));
    }

    static Stream<Arguments> deniesThatLeaveSomeOfTheBinding() {
        return Stream.of(
                Arguments.of("a literal of the prefix's name", deny("User:bob", literal(ResourceType.TOPIC,
                        "billing."), WRITE, "*")),
                Arguments.of("one topic under the prefix", deny("User:bob", literal(ResourceType.TOPIC, INVOICES),
                        WRITE, "*")),
                Arguments.of("a longer prefix", deny("User:bob", prefixed(ResourceType.TOPIC, "billing.x"), WRITE,
                        "*")),
                Arguments.of("another host", deny("User:bob", BILLING, WRITE, "10.0.0.9")),
                Arguments.of("another operation", deny("User:bob", BILLING, READ, "*")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deniesThatLeaveSomeOfTheBinding")
    void byResourceTypeADenyLeavingSomeOfTheBindingAllows(String kind, StandardAcl deny) {
        start(new BoundRoles().grant("bob", WRITE, BILLING), deny);

        assertEquals(ALLOWED, writeAnyTopic("bob"));
    }

    @Test
    void byResourceTypeALiteralBindingIsCoveredByADenyOfItsName() {
        ResourcePattern orders = literal(ResourceType.TOPIC, "orders");
        start(new BoundRoles().grant("bob", WRITE, orders));
        assertEquals(ALLOWED, writeAnyTopic("bob"));
        close();

        start(new BoundRoles().grant("bob", WRITE, orders), deny("User:bob", orders, WRITE, "*"));
        assertEquals(DENIED, writeAnyTopic("bob"));
    }

    @Test
    void byResourceTypeAWildcardBindingIsCoveredOnlyByAWildcardDeny() {
        ResourcePattern any = literal(ResourceType.TOPIC, "*");
        start(new BoundRoles().grant("bob", WRITE, any), deny("User:bob", BILLING, WRITE, "*"));
        assertEquals(ALLOWED, writeAnyTopic("bob"));
        close();

        start(new BoundRoles().grant("bob", WRITE, any), deny("User:bob", any, WRITE, "*"));
        assertEquals(DENIED, writeAnyTopic("bob"));
    }

    @Test
    void byResourceTypeOneUncoveredBindingIsEnough() {
        start(new BoundRoles().grant("bob", WRITE, BILLING).grant("bob", WRITE, ORDERS),
                deny("User:bob", BILLING, WRITE, "*"));

        assertEquals(ALLOWED, writeAnyTopic("bob"));
    }

    // --- helpers -------------------------------------------------------------

    private void start(Roles roles, StandardAcl... acls) {
        start(roles, Map.of(), acls);
    }

    private void start(Roles roles, Map<String, String> configs, StandardAcl... acls) {
        authorizer = new RbacAuthorizer(roles);
        Map<String, String> all = new HashMap<>(configs);
        all.put(StandardAuthorizer.SUPER_USERS_CONFIG, "User:admin");
        authorizer.configure(all);
        authorizer.withPluginMetrics(new PluginMetricsImpl(new Metrics(), Map.of()));
        Map<Uuid, StandardAcl> snapshot = new HashMap<>();
        for (StandardAcl acl : acls) {
            snapshot.put(Uuid.randomUuid(), acl);
        }
        authorizer.loadSnapshot(snapshot);
        authorizer.completeInitialLoad();
    }

    private static BoundRoles bobReadsBilling() {
        return new BoundRoles().grant("bob", READ, BILLING);
    }

    private AuthorizationResult read(String user, String topic) {
        return authorize(user, READ, literal(ResourceType.TOPIC, topic));
    }

    private AuthorizationResult authorize(String user, AclOperation operation, ResourcePattern resource) {
        return authorizer.authorize(RequestContext.of(user, "billing-1", ApiKeys.FETCH),
                List.of(new Action(operation, resource, 1, true, true))).get(0);
    }

    private AuthorizationResult writeAnyTopic(String user) {
        return authorizer.authorizeByResourceType(RequestContext.of(user, "billing-1", ApiKeys.INIT_PRODUCER_ID),
                WRITE, ResourceType.TOPIC);
    }

    static ResourcePattern literal(ResourceType type, String name) {
        return new ResourcePattern(type, name, PatternType.LITERAL);
    }

    static ResourcePattern prefixed(ResourceType type, String name) {
        return new ResourcePattern(type, name, PatternType.PREFIXED);
    }

    static StandardAcl allow(String principal, ResourcePattern resource, AclOperation operation) {
        return acl(principal, resource, operation, "*", AclPermissionType.ALLOW);
    }

    static StandardAcl deny(String principal, ResourcePattern resource, AclOperation operation, String host) {
        return acl(principal, resource, operation, host, AclPermissionType.DENY);
    }

    static StandardAcl acl(String principal, ResourcePattern resource, AclOperation operation, String host,
            AclPermissionType permission) {
        return new StandardAcl(resource.resourceType(), resource.name(), resource.patternType(), principal, host,
                operation, permission);
    }

    private static int occurrences(String text, String part) {
        int n = 0;
        for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + part.length())) {
            n++;
        }
        return n;
    }

    private static int count(Iterable<?> items) {
        int n = 0;
        for (Object ignored : items) {
            n++;
        }
        return n;
    }

    /** What the code writes to standard error while it runs: where the test logger writes. */
    static String stderrOf(Runnable code) {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            code.run();
        } finally {
            System.setErr(original);
        }
        return captured.toString(StandardCharsets.UTF_8);
    }

    /** A test double that fails the test when it is asked anything. */
    private static final class NeverAsked implements Roles {
        @Override
        public boolean grants(KafkaPrincipal principal, Action action) {
            throw new AssertionError("asked for " + principal + " " + action);
        }

        @Override
        public Collection<ResourcePattern> grantedPatterns(KafkaPrincipal principal, AclOperation operation,
                ResourceType resourceType) {
            throw new AssertionError("asked for " + principal + " " + operation + " " + resourceType);
        }
    }

    /** Roles whose store cannot be read. */
    private static final class Failing implements Roles {
        @Override
        public boolean grants(KafkaPrincipal principal, Action action) {
            throw new IllegalStateException("bindings not loaded");
        }

        @Override
        public Collection<ResourcePattern> grantedPatterns(KafkaPrincipal principal, AclOperation operation,
                ResourceType resourceType) {
            throw new IllegalStateException("bindings not loaded");
        }
    }
}
