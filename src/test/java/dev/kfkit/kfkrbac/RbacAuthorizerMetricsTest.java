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
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.deny;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.literal;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.prefixed;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigException;
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

/**
 * kfkwho's access metrics on request: the authorizer as the broker creates
 * it, with bindings that are records, and the MBeans kfkwho's recorder
 * registers read from the platform MBean server, where an exporter finds
 * them.
 *
 * <p>The recorder creates a series on its own thread, so what was recorded
 * is waited for. {@code User:bob} sends everything as client
 * {@code billing-1}.
 */
class RbacAuthorizerMetricsTest {

    private static final String INVOICES = "billing.invoices";
    private static final String PAYROLL = "billing.payroll.2026";
    private static final ResourcePattern BILLING = prefixed(TOPIC, "billing.");
    private static final Map<String, Object> ON = Map.of(RbacAuthorizer.METRICS_ENABLED_CONFIG, "true");

    private static final String RECORDER_THREAD = "kfkwho-series";
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final MBeanServer MBEANS = ManagementFactory.getPlatformMBeanServer();

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

    // --- AC1: the verdict recorded is the one returned -------------------------

    @Test
    void whatARoleAllowsAndNoAclAllowsIsRecordedAsAllowed() {
        start(ON, binding("DeveloperRead:User:bob", BILLING));

        assertEquals(ALLOWED, can("bob", READ, INVOICES));

        assertEquals(Map.of("ALLOWED", 1.0), recorded(INVOICES, Map.of("ALLOWED", 1.0)));
    }

    @Test
    void withoutTheRoleTheSameRequestIsRecordedAsDenied() {
        start(ON, binding("DeveloperRead:User:alice", BILLING));

        assertEquals(DENIED, can("bob", READ, INVOICES));

        assertEquals(Map.of("DENIED", 1.0), recorded(INVOICES, Map.of("DENIED", 1.0)));
    }

    @Test
    void whatTheRoleOfAGroupAllowsItsMemberIsRecordedAsAllowed() {
        start(ON, binding("DeveloperRead:Group:payments", BILLING),
                acl("Member:User:bob", literal(TOPIC, "Group:payments"), ALL, "*", ALLOW));

        assertEquals(ALLOWED, can("bob", READ, INVOICES));
        assertEquals(DENIED, can("alice", READ, INVOICES));

        assertEquals(Map.of("ALLOWED", 1.0, "DENIED", 1.0), recorded(INVOICES, Map.of("ALLOWED", 1.0, "DENIED", 1.0)));
        assertEquals(Set.of("User:alice DENIED", "User:bob ALLOWED"), names("kfkwho:type=access,*").stream()
                .map(name -> tag(name, "principal") + " " + tag(name, "result")).collect(Collectors.toSet()));
    }

    @Test
    void whatADenyAclStopsIsRecordedAsDenied() {
        start(ON, binding("DeveloperRead:User:bob", BILLING),
                deny("User:bob", prefixed(TOPIC, "billing.payroll."), READ, "*"));

        assertEquals(DENIED, can("bob", READ, PAYROLL));

        assertEquals(Map.of("DENIED", 1.0), recorded(PAYROLL, Map.of("DENIED", 1.0)));
    }

    @Test
    void eachActionOfARequestIsRecordedWithItsOwnVerdict() {
        start(ON, binding("DeveloperRead:User:bob", BILLING),
                deny("User:bob", prefixed(TOPIC, "billing.payroll."), READ, "*"));
        List<Action> actions = List.of(new Action(READ, literal(TOPIC, INVOICES), 1, true, true),
                new Action(READ, literal(TOPIC, PAYROLL), 1, true, true));

        for (int i = 0; i < 3; i++) {
            assertEquals(List.of(ALLOWED, DENIED), authorizer.authorize(bob(ApiKeys.FETCH), actions));
        }

        assertEquals(Map.of("ALLOWED", 3.0), recorded(INVOICES, Map.of("ALLOWED", 3.0)));
        assertEquals(Map.of("DENIED", 3.0), recorded(PAYROLL, Map.of("DENIED", 3.0)));
    }

    @Test
    void theSeriesAreKfkwhosUnderItsOwnNames() {
        start(ON, binding("DeveloperRead:User:bob", BILLING));

        assertEquals(ALLOWED, can("bob", READ, INVOICES));
        recorded(INVOICES, Map.of("ALLOWED", 1.0));

        // The name kfkwho's exporter rules match: its domain, its tags in its order, its client id rules.
        assertEquals(Set.of("kfkwho:type=access,principal=\"User:bob\",client-id=billing,resource-type=TOPIC,"
                + "resource=billing.invoices,operation=READ,api=FETCH,result=ALLOWED"), names("kfkwho:type=access,*"));
    }

    /**
     * To answer {@code authorizeByResourceType} the parent asks
     * {@code authorize} about a resource that does not exist, and is told
     * what the ACLs alone say. That is not what the caller is told, which a
     * role may have changed: it is not recorded.
     */
    @Test
    void theParentsOwnQuestionIsNotRecorded() {
        start(ON, binding("DeveloperWrite:User:bob", BILLING));

        assertEquals(ALLOWED, authorizer.authorizeByResourceType(bob(ApiKeys.INIT_PRODUCER_ID), WRITE, TOPIC));
        // Series are created in the order they were first seen: this one after any of the question above.
        assertEquals(ALLOWED, can("bob", WRITE, INVOICES));
        recorded(INVOICES, Map.of("ALLOWED", 1.0));

        assertEquals(Set.of(INVOICES), names("kfkwho:type=access,*").stream()
                .map(name -> tag(name, "resource")).collect(Collectors.toSet()));
    }

    // --- AC2: off means off ------------------------------------------------------

    static Stream<Arguments> off() {
        return Stream.of(
                Arguments.of("absent", Map.of()),
                Arguments.of("false", Map.of(RbacAuthorizer.METRICS_ENABLED_CONFIG, "false")),
                Arguments.of("Boolean.FALSE", Map.of(RbacAuthorizer.METRICS_ENABLED_CONFIG, Boolean.FALSE)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("off")
    void withTheSettingOffNothingOfKfkwhoExists(String how, Map<String, Object> settings) {
        start(settings, binding("DeveloperRead:User:bob", BILLING));

        assertEquals(ALLOWED, can("bob", READ, INVOICES));
        assertEquals(DENIED, can("bob", WRITE, INVOICES));

        assertEquals(Set.of(), names("kfkwho:*"));
        assertEquals(List.of(), recorderThreads());
    }

    @Test
    void withTheSettingOnTheRecorderExistsUntilTheAuthorizerIsClosed() {
        start(ON);

        assertTrue(names("kfkwho:*").contains("kfkwho:type=series"), names("kfkwho:*").toString());
        assertEquals(List.of(RECORDER_THREAD), recorderThreads());

        close();

        assertEquals(Set.of(), names("kfkwho:*"));
        assertEquals(List.of(), recorderThreads());
    }

    /** kfkwho records on brokers only, and is handed the whole configuration to see what the node is. */
    @Test
    void aNodeThatIsOnlyAControllerRecordsNothing() {
        start(Map.of(RbacAuthorizer.METRICS_ENABLED_CONFIG, "true", "process.roles", "controller"),
                binding("DeveloperRead:User:bob", BILLING));

        assertEquals(ALLOWED, can("bob", READ, INVOICES));

        assertEquals(Set.of(), names("kfkwho:*"));
        assertEquals(List.of(), recorderThreads());
    }

    // --- AC3: a wrong value fails configure --------------------------------------

    @Test
    void aWrongKfkwhoValueFailsConfigureAndNamesTheKey() throws IOException {
        try (RbacAuthorizer created = new RbacAuthorizer()) {
            ConfigException failure = assertThrows(ConfigException.class, () -> created.configure(Map.of(
                    RbacAuthorizer.METRICS_ENABLED_CONFIG, "true", "kfkwho.max.series", "0")));

            assertTrue(failure.getMessage().contains("kfkwho.max.series"), failure.getMessage());
        }
        assertEquals(Set.of(), names("kfkwho:*"));
        assertEquals(List.of(), recorderThreads());
    }

    @Test
    void withTheSettingOffKfkwhosSettingsAreNotRead() {
        start(Map.of("kfkwho.max.series", "0"), binding("DeveloperRead:User:bob", BILLING));

        assertEquals(ALLOWED, can("bob", READ, INVOICES));
    }

    @Test
    void aWrongValueOfTheSettingItselfFailsConfigureAndNamesTheKey() throws IOException {
        try (RbacAuthorizer created = new RbacAuthorizer()) {
            ConfigException failure = assertThrows(ConfigException.class,
                    () -> created.configure(Map.of(RbacAuthorizer.METRICS_ENABLED_CONFIG, "yes")));

            assertTrue(failure.getMessage().contains(RbacAuthorizer.METRICS_ENABLED_CONFIG), failure.getMessage());
        }
        assertEquals(Set.of(), names("kfkwho:*"));
    }

    // --- helpers -----------------------------------------------------------------

    private void start(Map<String, ?> settings, StandardAcl... records) {
        Map<String, Object> configs = new HashMap<>(settings);
        configs.put(StandardAuthorizer.SUPER_USERS_CONFIG, "User:admin");
        Map<Uuid, StandardAcl> snapshot = new HashMap<>();
        for (StandardAcl record : records) {
            snapshot.put(Uuid.randomUuid(), record);
        }
        metrics = new Metrics();
        authorizer = new RbacAuthorizer();
        authorizer.configure(configs);
        authorizer.withPluginMetrics(new PluginMetricsImpl(metrics, Map.of()));
        authorizer.loadSnapshot(snapshot);
        authorizer.completeInitialLoad();
    }

    private static StandardAcl binding(String roleAndMember, ResourcePattern pattern) {
        return acl(roleAndMember, pattern, ALL, "*", ALLOW);
    }

    private static RequestContext bob(ApiKeys api) {
        return RequestContext.of("bob", "billing-1", api);
    }

    private AuthorizationResult can(String user, AclOperation operation, String topic) {
        return authorizer.authorize(RequestContext.of(user, "billing-1", ApiKeys.FETCH),
                List.of(new Action(operation, literal(ResourceType.TOPIC, topic), 1, true, true))).get(0);
    }

    /**
     * The requests recorded for the topic, by verdict: read until they are
     * what is expected, and returned as they are when they never become it.
     */
    private static Map<String, Double> recorded(String topic, Map<String, Double> expected) {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (true) {
            Map<String, Double> byVerdict = new TreeMap<>();
            for (String name : names("kfkwho:type=access,*")) {
                if (topic.equals(tag(name, "resource"))) {
                    byVerdict.merge(tag(name, "result"), total(name), Double::sum);
                }
            }
            if (byVerdict.equals(expected) || System.nanoTime() > deadline) {
                return byVerdict;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return byVerdict;
            }
        }
    }

    /** The MBeans registered now whose names match the pattern. */
    private static Set<String> names(String pattern) {
        try {
            Set<String> names = new TreeSet<>();
            for (ObjectName name : MBEANS.queryNames(new ObjectName(pattern), null)) {
                names.add(name.toString());
            }
            return names;
        } catch (JMException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String tag(String name, String tag) {
        try {
            String value = new ObjectName(name).getKeyProperty(tag);
            return value != null && value.startsWith("\"") ? ObjectName.unquote(value) : value;
        } catch (JMException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The series' {@code request-total}; zero for a series that expired between the listing and the read. */
    private static double total(String name) {
        try {
            return (Double) MBEANS.getAttribute(new ObjectName(name), "request-total");
        } catch (JMException e) {
            return 0;
        }
    }

    private static List<String> recorderThreads() {
        return Thread.getAllStackTraces().keySet().stream().map(Thread::getName)
                .filter(RECORDER_THREAD::equals).toList();
    }
}
