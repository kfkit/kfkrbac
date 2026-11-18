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

import static dev.kfkit.kfkrbac.TestRestAccess.membership;
import static dev.kfkit.kfkrbac.TestRestAccess.onConnectors;
import static dev.kfkit.kfkrbac.TestRestAccess.onTheCluster;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

/** What an extension outside the broker reads, and how it answers. */
class RestAccessTest {

    private static final KafkaPrincipal ALICE = new KafkaPrincipal("User", "alice");

    @Test
    void theCallerIsTheBasicAuthenticationUserFirstAndTheCertificatesSubjectOtherwise() {
        X509Certificate[] chain = {TestRestAccess.alice()};
        assertEquals(ALICE, RestAccess.caller("alice", chain));
        assertEquals(new KafkaPrincipal("User", "CN=alice,O=example"), RestAccess.caller(null, chain));
        assertEquals(new KafkaPrincipal("User", "CN=alice,O=example"), RestAccess.caller("", chain));
    }

    @Test
    void aRequestWithNeitherIsAnonymous() {
        assertNull(RestAccess.caller(null, null));
        assertNull(RestAccess.caller("", new X509Certificate[0]));
        assertNull(RestAccess.caller(null, "not a chain"));
    }

    @Test
    void nothingIsReadyOrGrantedBeforeTheFirstRead() {
        try (TestRestAccess test = TestRestAccess.of("", onConnectors("ResourceOwner", "User:alice", "billing-"))) {
            assertFalse(test.access().ready());
            assertFalse(test.access().grants(ALICE, ConnectorOperation.CREATE, "billing-sink"));
            assertTrue(test.refresh());
            assertTrue(test.access().ready());
            assertTrue(test.access().grants(ALICE, ConnectorOperation.CREATE, "billing-sink"));
        }
    }

    @Test
    void aBindingGrantsWhatItsRoleGrantsOnTheConnectorsItsPatternCovers() {
        try (TestRestAccess test = TestRestAccess.of("", onConnectors("DeveloperRead", "User:alice", "billing-"))) {
            test.refresh();
            assertTrue(test.access().grants(ALICE, ConnectorOperation.READ_CONFIG, "billing-sink"));
            assertFalse(test.access().grants(ALICE, ConnectorOperation.READ_CONFIG, "payments-sink"));
            assertFalse(test.access().grants(ALICE, ConnectorOperation.CREATE, "billing-sink"));
            assertFalse(test.access().grants(new KafkaPrincipal("User", "bob"), ConnectorOperation.READ_CONFIG,
                    "billing-sink"));
        }
    }

    @Test
    void aBindingOnTopicsGrantsNothingOnConnectors() {
        try (TestRestAccess test = TestRestAccess.of("", BindingRecords.record("ResourceOwner", "User:alice",
                RoleBinding.Target.NAMES, "billing-", org.apache.kafka.common.resource.PatternType.PREFIXED))) {
            test.refresh();
            assertFalse(test.access().grants(ALICE, ConnectorOperation.READ_STATUS, "billing-sink"));
        }
    }

    @Test
    void aClusterRoleGrantsOnEveryConnectorAndRunsTheService() {
        try (TestRestAccess test = TestRestAccess.of("", onTheCluster("ClusterAdmin", "User:alice"),
                onTheCluster("SystemAdmin", "User:carol"), onConnectors("ResourceOwner", "User:bob", "billing-"))) {
            test.refresh();
            assertTrue(test.access().grants(ALICE, ConnectorOperation.DELETE, "anything"));
            assertFalse(test.access().grants(ALICE, ConnectorOperation.CONFIGURE, "anything"),
                    "ClusterAdmin does not configure connectors (docs/roles.md)");
            assertTrue(test.access().administers(ALICE));
            assertTrue(test.access().administers(new KafkaPrincipal("User", "carol")));
            assertFalse(test.access().administers(new KafkaPrincipal("User", "bob")));
        }
    }

    /** AC5: a role bound to a group applies to its members, by the membership records of the same read. */
    @Test
    void aRoleBoundToAGroupAppliesToItsMembers() {
        AclBinding aliceInPayments = membership("User:alice", "Group:payments");
        try (TestRestAccess test = TestRestAccess.of("",
                onConnectors("DeveloperManage", "Group:payments", "billing-"), aliceInPayments)) {
            test.refresh();
            assertTrue(test.access().grants(ALICE, ConnectorOperation.CREATE, "billing-sink"),
                    "she is a member of Group:payments, which holds DeveloperManage on Connector:billing-");
            assertFalse(test.access().grants(ALICE, ConnectorOperation.CREATE, "payments-sink"));
            assertFalse(test.access().grants(new KafkaPrincipal("User", "bob"), ConnectorOperation.CREATE,
                    "billing-sink"), "he is in no group");

            test.remove(aliceInPayments);
            test.refresh();
            assertFalse(test.access().grants(ALICE, ConnectorOperation.CREATE, "billing-sink"),
                    "she left the group at the next read");
        }
    }

    /** AC5, the twin: a group that holds nothing gives its members nothing, and a membership alone grants nothing. */
    @Test
    void aMembershipAloneGrantsNothing() {
        try (TestRestAccess test = TestRestAccess.of("", membership("User:alice", "Group:payments"),
                onConnectors("DeveloperManage", "Group:billing", "billing-"))) {
            test.refresh();
            assertFalse(test.access().grants(ALICE, ConnectorOperation.CREATE, "billing-sink"),
                    "the binding is Group:billing's, and she is in Group:payments");
            assertEquals(1.0, (Double) test.access().metrics().metrics().entrySet().stream()
                    .filter(entry -> entry.getKey().name().equals("bindings")).findFirst().orElseThrow()
                    .getValue().metricValue(), "a membership is not a binding");
        }
    }

    /** AC5: a cluster role held through a group runs the service. */
    @Test
    void aClusterRoleHeldThroughAGroupRunsTheService() {
        try (TestRestAccess test = TestRestAccess.of("", onTheCluster("ClusterAdmin", "Group:ops"),
                membership("User:alice", "Group:ops"))) {
            test.refresh();
            assertTrue(test.access().administers(ALICE));
            assertFalse(test.access().administers(new KafkaPrincipal("User", "bob")));
        }
    }

    @Test
    void aSuperUserIsNamedInTheSettings() {
        try (TestRestAccess test = TestRestAccess.of(" User:connect ; User:admin;")) {
            assertTrue(test.access().superUser(new KafkaPrincipal("User", "connect")));
            assertTrue(test.access().superUser(new KafkaPrincipal("User", "admin")));
            assertTrue(test.access().administers(new KafkaPrincipal("User", "admin")));
            assertFalse(test.access().superUser(ALICE));
        }
    }

    @Test
    void settingsThatCannotBeReadAreRefused() {
        assertThrows(ConfigException.class, () -> RestAccess.validate(Map.of(RestAccess.SUPER_USERS, "alice")));
        assertThrows(ConfigException.class,
                () -> RestAccess.validate(Map.of(RestAccess.REFRESH_INTERVAL_MS, "soon")));
        assertThrows(ConfigException.class, () -> RestAccess.validate(Map.of(RestAccess.REFRESH_INTERVAL_MS, "0")));
        RestAccess.validate(Map.of(RestAccess.SUPER_USERS, "User:a;User:b", RestAccess.REFRESH_INTERVAL_MS, "250"));
        assertEquals(RestAccess.DEFAULT_REFRESH_INTERVAL_MS, RestAccess.interval(null));
        assertEquals(250, RestAccess.interval(250));
    }

    /** AC6: a failed read keeps the last bindings in force, and their age grows in the gauge. */
    @Test
    void aFailedReadKeepsTheLastBindingsAndTheirAgeGrows() {
        MockTime time = new MockTime();
        FakeAclRecords records = new FakeAclRecords(onConnectors("DeveloperManage", "User:alice", "billing-"));
        Metrics metrics = new Metrics(time);
        try (RestAccess access = new RestAccess(records, Set.of(), time, metrics)) {
            time.sleep(300);
            assertEquals(300, access.ageMs(), "since the start while nothing has been read");
            assertTrue(access.refresh());
            assertEquals(0.0, age(metrics));

            records.refusal = new AclRecords.Failure("TimeoutException: the cluster did not answer", null);
            time.sleep(7000);
            assertFalse(access.refresh());
            assertTrue(access.ready());
            assertTrue(access.grants(ALICE, ConnectorOperation.CREATE, "billing-sink"));
            assertEquals(7000.0, age(metrics));
            time.sleep(5000);
            assertFalse(access.refresh());
            assertEquals(12000.0, age(metrics));

            records.refusal = null;
            assertTrue(access.refresh());
            assertEquals(0.0, age(metrics));
        }
        assertTrue(records.closed);
    }

    /** AC7: a binding created on the cluster is in force within the refresh interval. */
    @Test
    void aBindingCreatedOnTheClusterIsInForceWithinTheInterval() throws InterruptedException {
        List<AclBinding> cluster = new CopyOnWriteArrayList<>();
        AclRecords records = new AclRecords() {
            @Override
            public Collection<AclBinding> describe(AclBindingFilter filter) {
                return cluster.stream().filter(filter::matches).toList();
            }

            @Override
            public void create(Collection<AclBinding> created) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Collection<AclBinding> delete(Collection<AclBindingFilter> filters) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
            }
        };
        try (RestAccess access = new RestAccess(records, Set.of(), Time.SYSTEM,
                new Metrics())) {
            access.refreshEvery(100, "test");
            awaitReady(access);
            assertFalse(access.grants(ALICE, ConnectorOperation.CREATE, "billing-sink"));
            cluster.add(onConnectors("DeveloperManage", "User:alice", "billing-"));
            long created = System.nanoTime();
            while (!access.grants(ALICE, ConnectorOperation.CREATE, "billing-sink")) {
                assertTrue(System.nanoTime() - created < 5_000_000_000L, "not in force after 5 s");
                Thread.sleep(10);
            }
        }
    }

    private static void awaitReady(RestAccess access) throws InterruptedException {
        long start = System.nanoTime();
        while (!access.ready()) {
            assertTrue(System.nanoTime() - start < 5_000_000_000L, "not read after 5 s");
            Thread.sleep(10);
        }
    }

    private static Object age(Metrics metrics) {
        return metrics.metric(metrics.metricName(RestAccess.AGE_METRIC, RestAccess.METRIC_GROUP)).metricValue();
    }
}
