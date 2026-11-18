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

import java.io.IOException;
import java.net.InetAddress;
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
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * The cost of one {@code authorize} call for one Fetch of one topic, among
 * 1000 allow ACLs and 200 deny ACLs of other principals, and as many role
 * bindings in the cluster as {@link #bindings} says.
 *
 * <p>{@code alice} is allowed by an ACL. {@code bob} holds
 * {@code DeveloperRead} on the prefix {@code billing.}: he is allowed
 * {@code billing.invoices} by that role and denied what starts with
 * {@code billing.payroll.} by an ACL, which is found, as the denies of a
 * hundred others on each of the two prefixes are passed over. {@code dave}
 * has nothing and is denied by default. The other bindings are those of a
 * thousand applications, each on prefixes of its own.
 * {@code parentAllowedByAcl} is the broker's own {@link StandardAuthorizer}
 * holding the same records and answering {@code alice}: what an ACL-allowed
 * request costs without roles.
 *
 * <p>The benchmarks named {@code WithMetrics} ask an authorizer that holds
 * the same records and has {@code kfkrbac.metrics.enabled=true}, once the
 * series of the request exists, which is every call but the first. That
 * authorizer is a state of its own, {@link Metered}, so that the JVM of a
 * benchmark without metrics never has a recorder in it, as a broker with
 * the setting off does not: a recorder that exists anywhere in the JVM
 * changes what the JIT compiler makes of the parent's code.
 */
@State(Scope.Benchmark)
public class AuthorizeBenchmark {

    private static final ResourcePattern INVOICES =
            new ResourcePattern(ResourceType.TOPIC, "billing.invoices", PatternType.LITERAL);
    private static final List<Action> READ_INVOICES =
            List.of(new Action(AclOperation.READ, INVOICES, 1, true, true));
    private static final List<Action> READ_PAYROLL = List.of(new Action(AclOperation.READ,
            new ResourcePattern(ResourceType.TOPIC, "billing.payroll.2026", PatternType.LITERAL), 1, true, true));

    /** The role bindings in the cluster, the one of {@code bob} among them. */
    @Param({"1", "100", "10000"})
    public int bindings;

    private StandardAuthorizer parent;
    private RbacAuthorizer rbac;
    private Map<Uuid, StandardAcl> acls;
    private final AuthorizableRequestContext alice = context("alice");
    private final AuthorizableRequestContext bob = context("bob");
    private final AuthorizableRequestContext dave = context("dave");

    @Setup(Level.Trial)
    public void setUp() {
        acls = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            acls.put(Uuid.randomUuid(), acl("User:app-" + i, "topic-" + i, PatternType.LITERAL,
                    AclPermissionType.ALLOW));
        }
        for (int i = 0; i < 100; i++) {
            acls.put(Uuid.randomUuid(), acl("User:app-" + i, "billing.", PatternType.PREFIXED,
                    AclPermissionType.DENY));
            acls.put(Uuid.randomUuid(), acl("User:app-" + i, "billing.payroll.", PatternType.PREFIXED,
                    AclPermissionType.DENY));
        }
        acls.put(Uuid.randomUuid(), acl("User:alice", "billing.invoices", PatternType.LITERAL,
                AclPermissionType.ALLOW));
        acls.put(Uuid.randomUuid(), acl("User:bob", "billing.payroll.", PatternType.PREFIXED,
                AclPermissionType.DENY));

        acls.put(Uuid.randomUuid(), binding("DeveloperRead:User:bob", "billing."));
        for (int i = 1; i < bindings; i++) {
            acls.put(Uuid.randomUuid(), binding("DeveloperRead:User:app-" + i % 1000, "team-" + i + "."));
        }

        parent = start(new StandardAuthorizer(), Map.of(), acls);
        rbac = start(new RbacAuthorizer(), Map.of(), acls);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        parent.close();
        rbac.close();
    }

    @Benchmark
    public AuthorizationResult parentAllowedByAcl() {
        return parent.authorize(alice, READ_INVOICES).get(0);
    }

    @Benchmark
    public AuthorizationResult allowedByAcl() {
        return rbac.authorize(alice, READ_INVOICES).get(0);
    }

    @Benchmark
    public AuthorizationResult allowedByRole() {
        return rbac.authorize(bob, READ_INVOICES).get(0);
    }

    @Benchmark
    public AuthorizationResult allowedByAclWithMetrics(Metered metered) {
        return metered.authorizer.authorize(alice, READ_INVOICES).get(0);
    }

    @Benchmark
    public AuthorizationResult allowedByRoleWithMetrics(Metered metered) {
        return metered.authorizer.authorize(bob, READ_INVOICES).get(0);
    }

    @Benchmark
    public AuthorizationResult deniedByAcl() {
        return rbac.authorize(bob, READ_PAYROLL).get(0);
    }

    @Benchmark
    public AuthorizationResult deniedByDefault() {
        return rbac.authorize(dave, READ_INVOICES).get(0);
    }

    /** An authorizer with the records of the benchmark and kfkwho's metrics on. */
    @State(Scope.Benchmark)
    public static class Metered {

        private RbacAuthorizer authorizer;

        @Setup(Level.Trial)
        public void setUp(AuthorizeBenchmark records) {
            authorizer = start(new RbacAuthorizer(), Map.of("kfkrbac.metrics.enabled", "true"), records.acls);
        }

        @TearDown(Level.Trial)
        public void tearDown() throws IOException {
            authorizer.close();
        }
    }

    private static <A extends StandardAuthorizer> A start(A authorizer, Map<String, ?> configs,
            Map<Uuid, StandardAcl> acls) {
        authorizer.configure(configs);
        authorizer.withPluginMetrics(new PluginMetricsImpl(new Metrics(), Map.of()));
        authorizer.loadSnapshot(acls);
        authorizer.completeInitialLoad();
        return authorizer;
    }

    private static StandardAcl acl(String principal, String name, PatternType patternType,
            AclPermissionType permission) {
        return new StandardAcl(ResourceType.TOPIC, name, patternType, principal, "*", AclOperation.READ, permission);
    }

    private static StandardAcl binding(String roleAndMember, String prefix) {
        return new StandardAcl(ResourceType.TOPIC, prefix, PatternType.PREFIXED, roleAndMember, "*", AclOperation.ALL,
                AclPermissionType.ALLOW);
    }

    private static AuthorizableRequestContext context(String user) {
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user);
        InetAddress address = InetAddress.getLoopbackAddress();
        return new AuthorizableRequestContext() {
            @Override
            public String listenerName() {
                return "PLAINTEXT";
            }

            @Override
            public SecurityProtocol securityProtocol() {
                return SecurityProtocol.PLAINTEXT;
            }

            @Override
            public KafkaPrincipal principal() {
                return principal;
            }

            @Override
            public InetAddress clientAddress() {
                return address;
            }

            @Override
            public int requestType() {
                return ApiKeys.FETCH.id;
            }

            @Override
            public int requestVersion() {
                return ApiKeys.FETCH.latestVersion();
            }

            @Override
            public String clientId() {
                return "billing-1";
            }

            @Override
            public int correlationId() {
                return 0;
            }
        };
    }
}
