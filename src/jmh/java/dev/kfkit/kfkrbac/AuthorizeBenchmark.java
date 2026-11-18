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
import java.util.Collection;
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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * The cost of one {@code authorize} call for one Fetch of one topic, among
 * 1000 allow ACLs and 100 deny ACLs of other principals.
 *
 * <p>{@code alice} is allowed by an ACL, {@code bob} by a role, {@code carol}
 * holds the same role and is denied by an ACL, {@code dave} has nothing and
 * is denied by default. {@code parentAllowedByAcl} is the broker's own
 * {@link StandardAuthorizer} answering {@code alice}: what an ACL-allowed
 * request costs without roles.
 */
@State(Scope.Benchmark)
public class AuthorizeBenchmark {

    private static final ResourcePattern INVOICES =
            new ResourcePattern(ResourceType.TOPIC, "billing.invoices", PatternType.LITERAL);
    private static final List<Action> READ_INVOICES =
            List.of(new Action(AclOperation.READ, INVOICES, 1, true, true));

    private StandardAuthorizer parent;
    private RbacAuthorizer rbac;
    private final AuthorizableRequestContext alice = context("alice");
    private final AuthorizableRequestContext bob = context("bob");
    private final AuthorizableRequestContext carol = context("carol");
    private final AuthorizableRequestContext dave = context("dave");

    @Setup(Level.Trial)
    public void setUp() {
        Map<Uuid, StandardAcl> acls = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            acls.put(Uuid.randomUuid(), acl("User:app-" + i, "topic-" + i, PatternType.LITERAL,
                    AclPermissionType.ALLOW));
        }
        for (int i = 0; i < 100; i++) {
            acls.put(Uuid.randomUuid(), acl("User:app-" + i, "billing.", PatternType.PREFIXED,
                    AclPermissionType.DENY));
        }
        acls.put(Uuid.randomUuid(), acl("User:alice", "billing.invoices", PatternType.LITERAL,
                AclPermissionType.ALLOW));
        acls.put(Uuid.randomUuid(), acl("User:carol", "billing.", PatternType.PREFIXED, AclPermissionType.DENY));

        parent = start(new StandardAuthorizer(), acls);
        rbac = start(new RbacAuthorizer(new BillingReaders()), acls);
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
    public AuthorizationResult deniedByAcl() {
        return rbac.authorize(carol, READ_INVOICES).get(0);
    }

    @Benchmark
    public AuthorizationResult deniedByDefault() {
        return rbac.authorize(dave, READ_INVOICES).get(0);
    }

    private static <A extends StandardAuthorizer> A start(A authorizer, Map<Uuid, StandardAcl> acls) {
        authorizer.configure(Map.of());
        authorizer.withPluginMetrics(new PluginMetricsImpl(new Metrics(), Map.of()));
        authorizer.loadSnapshot(acls);
        authorizer.completeInitialLoad();
        return authorizer;
    }

    private static StandardAcl acl(String principal, String name, PatternType patternType,
            AclPermissionType permission) {
        return new StandardAcl(ResourceType.TOPIC, name, patternType, principal, "*", AclOperation.READ, permission);
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

    /** {@code bob} and {@code carol} may read the topics starting with {@code billing.}. */
    private static final class BillingReaders implements Roles {
        private static final KafkaPrincipal BOB = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob");
        private static final KafkaPrincipal CAROL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "carol");
        private static final ResourcePattern BILLING =
                new ResourcePattern(ResourceType.TOPIC, "billing.", PatternType.PREFIXED);

        @Override
        public boolean grants(KafkaPrincipal principal, Action action) {
            return (principal.equals(BOB) || principal.equals(CAROL))
                    && action.operation() == AclOperation.READ
                    && action.resourcePattern().resourceType() == ResourceType.TOPIC
                    && action.resourcePattern().name().startsWith(BILLING.name());
        }

        @Override
        public Collection<ResourcePattern> grantedPatterns(KafkaPrincipal principal, AclOperation operation,
                ResourceType resourceType) {
            return (principal.equals(BOB) || principal.equals(CAROL)) && operation == AclOperation.READ
                    && resourceType == ResourceType.TOPIC ? List.of(BILLING) : List.of();
        }
    }
}
