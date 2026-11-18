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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.Test;

/**
 * {@link AclSet} matches an ACL as the broker's {@link StandardAuthorizer}
 * does: every ACL of a set that covers what can vary, asked every question
 * of a set that covers the same, answered by both, and the answers equal.
 *
 * <p>The broker's authorizer is asked with one ACL at a time, and then with
 * many that overlap. An
 * {@code ALLOW} ACL matches when it answers {@code ALLOWED} and allows
 * nothing for want of an ACL; a {@code DENY} ACL matches when, next to an
 * ACL that allows everyone everything, it answers {@code DENIED}.
 */
class AclSetTest {

    private static final List<ResourceType> TYPES = List.of(ResourceType.TOPIC, ResourceType.GROUP,
            ResourceType.CLUSTER);

    private static final List<String> PRINCIPALS = List.of("User:alice", "User:*", "User:bob", "Group:payments",
            "Group:*");

    private static final List<String> HOSTS = List.of("*", "10.0.0.1", "10.0.0.9", "10.0.0.0/24", "10.0.1.0/24",
            "192.168.1.1");

    private static final List<String> NAMES = List.of("orders", "orders.archive", "ord", "billing.invoices", "*",
            "kafka-cluster");

    @Test
    void everyAclIsMatchedAsTheBrokersAuthorizerMatchesIt() throws IOException {
        List<StandardAcl> acls = new ArrayList<>();
        for (ResourceType type : TYPES) {
            for (ResourcePattern pattern : List.of(literal(type, "orders"), literal(type, "*"),
                    new ResourcePattern(type, "ord", PatternType.PREFIXED),
                    new ResourcePattern(type, "billing.", PatternType.PREFIXED),
                    literal(type, "kafka-cluster"))) {
                for (String principal : PRINCIPALS) {
                    for (String host : HOSTS) {
                        for (AclOperation operation : operations()) {
                            for (AclPermissionType permission : List.of(AclPermissionType.ALLOW,
                                    AclPermissionType.DENY)) {
                                acls.add(new StandardAcl(type, pattern.name(), pattern.patternType(), principal,
                                        host, operation, permission));
                            }
                        }
                    }
                }
            }
        }
        List<KafkaPrincipal> askedAs = List.of(new KafkaPrincipal("User", "alice"),
                new KafkaPrincipal("Group", "payments"));
        List<InetAddress> askedFrom = List.of(InetAddress.getByName("10.0.0.1"), InetAddress.getByName("10.0.0.9"),
                InetAddress.getByName("10.0.1.7"));

        int matched = 0;
        int asked = 0;
        for (StandardAcl acl : acls) {
            try (Broker broker = new Broker(List.of(acl))) {
                AclSet set = new AclSet();
                set.add(Uuid.randomUuid(), acl);
                for (KafkaPrincipal principal : askedAs) {
                    for (InetAddress from : askedFrom) {
                        for (AclOperation operation : operations()) {
                            for (ResourceType type : List.of(acl.resourceType(), otherThan(acl.resourceType()))) {
                                for (String name : NAMES) {
                                    ResourcePattern resource = literal(type, name);
                                    boolean expected = broker.matches(principal, from, operation, resource);
                                    boolean actual = set.matches(principal, from.getHostAddress(), operation,
                                            resource);
                                    assertEquals(expected, actual, () -> acl + " asked as " + principal + " from "
                                            + from + " for " + operation + " on " + resource);
                                    asked++;
                                    matched += actual ? 1 : 0;
                                }
                            }
                        }
                    }
                }
            }
        }
        assertTrue(matched > 1000 && matched < asked / 2, matched + " of " + asked + " questions matched");
    }

    @Test
    void manyAclsTogetherAreMatchedAsTheBrokersAuthorizerMatchesThem() throws IOException {
        // Prefixes of several lengths, on the same name and on others, literal names and the wildcard.
        List<StandardAcl> acls = new ArrayList<>();
        for (String prefix : List.of("o", "or", "ord", "orders.", "billing.", "billing.payroll.", "x")) {
            acls.add(new StandardAcl(ResourceType.TOPIC, prefix, PatternType.PREFIXED, "User:alice", "*",
                    prefix.length() % 2 == 0 ? AclOperation.READ : AclOperation.WRITE, AclPermissionType.DENY));
        }
        for (String name : List.of("orders", "billing.invoices", "*")) {
            acls.add(new StandardAcl(ResourceType.TOPIC, name, PatternType.LITERAL, "User:alice", "10.0.0.1",
                    AclOperation.DESCRIBE, AclPermissionType.DENY));
        }
        acls.add(new StandardAcl(ResourceType.GROUP, "ord", PatternType.PREFIXED, "User:*", "*", AclOperation.ALL,
                AclPermissionType.DENY));
        AclSet set = new AclSet();
        for (StandardAcl acl : acls) {
            set.add(Uuid.randomUuid(), acl);
        }
        KafkaPrincipal alice = new KafkaPrincipal("User", "alice");
        int matched = 0;
        try (Broker broker = new Broker(acls)) {
            for (InetAddress from : List.of(InetAddress.getByName("10.0.0.1"), InetAddress.getByName("10.0.0.2"))) {
                for (AclOperation operation : operations()) {
                    for (ResourceType type : TYPES) {
                        for (String name : List.of("o", "orders", "orders.archive", "ord", "billing.", "billing",
                                "billing.invoices", "billing.payroll.march", "*", "x", "", "kafka-cluster")) {
                            ResourcePattern resource = literal(type, name);
                            boolean actual = set.matches(alice, from.getHostAddress(), operation, resource);
                            assertEquals(broker.matches(alice, from, operation, resource), actual,
                                    () -> "asked from " + from + " for " + operation + " on " + resource);
                            matched += actual ? 1 : 0;
                        }
                    }
                }
            }
        }
        assertTrue(matched > 50, matched + " matched");
    }

    @Test
    void anAclMatchesOnlyWhileItIsInTheSet() {
        AclSet set = new AclSet();
        Uuid id = Uuid.randomUuid();
        StandardAcl acl = new StandardAcl(ResourceType.TOPIC, "orders", PatternType.LITERAL, "User:alice", "*",
                AclOperation.READ, AclPermissionType.DENY);
        KafkaPrincipal alice = new KafkaPrincipal("User", "alice");
        ResourcePattern orders = literal(ResourceType.TOPIC, "orders");

        assertFalse(set.matches(alice, "10.0.0.1", AclOperation.READ, orders));
        set.add(id, acl);
        assertTrue(set.matches(alice, "10.0.0.1", AclOperation.READ, orders));
        assertTrue(set.remove(id));
        assertFalse(set.matches(alice, "10.0.0.1", AclOperation.READ, orders));
        assertFalse(set.remove(id));
        set.load(Map.of(id, acl));
        assertTrue(set.matches(alice, "10.0.0.1", AclOperation.READ, orders));
        assertEquals(Map.of(id, acl), set.all());
        set.load(Map.of());
        assertFalse(set.matches(alice, "10.0.0.1", AclOperation.READ, orders));
    }

    private static List<AclOperation> operations() {
        List<AclOperation> operations = new ArrayList<>();
        for (AclOperation operation : AclOperation.values()) {
            if (operation != AclOperation.ANY && operation != AclOperation.UNKNOWN) {
                operations.add(operation);
            }
        }
        return operations;
    }

    private static ResourceType otherThan(ResourceType type) {
        return type == ResourceType.TOPIC ? ResourceType.GROUP : ResourceType.TOPIC;
    }

    private static ResourcePattern literal(ResourceType type, String name) {
        return new ResourcePattern(type, name, PatternType.LITERAL);
    }

    /** The broker's authorizer with ACLs of one permission, and with ACLs that allow everything next to DENY ones. */
    private static final class Broker implements Closeable {
        private final Metrics metrics = new Metrics();
        private final StandardAuthorizer authorizer = new StandardAuthorizer();
        private final boolean deny;

        Broker(List<StandardAcl> held) {
            deny = held.get(0).permissionType() == AclPermissionType.DENY;
            authorizer.configure(Map.of(StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "false"));
            authorizer.withPluginMetrics(new PluginMetricsImpl(metrics, Map.of()));
            Map<Uuid, StandardAcl> acls = new HashMap<>();
            for (StandardAcl acl : held) {
                acls.put(Uuid.randomUuid(), acl);
            }
            if (deny) {
                for (ResourceType type : TYPES) {
                    acls.put(Uuid.randomUuid(), new StandardAcl(type, "*", PatternType.LITERAL, "User:*", "*",
                            AclOperation.ALL, AclPermissionType.ALLOW));
                }
            }
            authorizer.loadSnapshot(acls);
            authorizer.completeInitialLoad();
        }

        boolean matches(KafkaPrincipal principal, InetAddress from, AclOperation operation,
                ResourcePattern resource) {
            RequestContext context = new RequestContext(principal, "billing-1", from, ApiKeys.FETCH.id);
            AuthorizationResult result = authorizer.authorize(context,
                    List.of(new Action(operation, resource, 1, false, false))).get(0);
            return deny ? result == AuthorizationResult.DENIED : result == AuthorizationResult.ALLOWED;
        }

        @Override
        public void close() throws IOException {
            try {
                authorizer.close();
            } finally {
                metrics.close();
            }
        }
    }
}
