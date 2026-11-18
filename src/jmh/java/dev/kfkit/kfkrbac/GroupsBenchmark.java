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
 * The cost of one {@code authorize} call for one Fetch of one topic by a
 * principal that is in as many groups as {@link #groups} says, among the
 * ACLs of {@link AuthorizeBenchmark}, a hundred role bindings of others and
 * a thousand other users in two of fifty groups each.
 *
 * <p>{@code bob} holds {@code DeveloperRead} on the prefix {@code billing.}:
 * by name when he is in no group, and otherwise through one of his groups,
 * while the others hold a role elsewhere and are looked up in the order the
 * snapshot left them in. Each of his groups also has an {@code ALLOW} ACL of
 * its own, which is the worst case for what no role grants: the ACLs of
 * every group are asked. {@code allowedByAclOfGroup} is the read that the
 * ACL of one of his groups allows, and with no group it is a denial like
 * {@code deniedByDefault}, where nothing allows the read.
 *
 * <p>{@code groupsOf} is the one thing a request on its way to the roles
 * does for groups whatever it asks: the lookup of the principal's groups,
 * alone. {@code noGroupsToLookUp} is what stood in its place before there
 * were groups.
 */
@State(Scope.Benchmark)
public class GroupsBenchmark {

    private static final List<Action> READ_INVOICES = read("billing.invoices");
    private static final List<Action> READ_REPORTS = read("reports");
    private static final List<Action> READ_PAYROLL = read("payroll.2026");

    /** The groups {@code bob} is in. */
    @Param({"0", "1", "20"})
    public int groups;

    private RbacAuthorizer rbac;
    private Memberships memberships;
    private final AuthorizableRequestContext bob = context("bob");

    @Setup(Level.Trial)
    public void setUp() {
        Map<Uuid, StandardAcl> acls = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            put(acls, acl("User:app-" + i, "topic-" + i, PatternType.LITERAL, AclPermissionType.ALLOW));
            put(acls, member("User:app-" + i, "Group:team-" + i % 50));
            put(acls, member("User:app-" + i, "Group:team-" + (i + 7) % 50));
        }
        for (int i = 0; i < 100; i++) {
            put(acls, acl("User:app-" + i, "billing.", PatternType.PREFIXED, AclPermissionType.DENY));
            put(acls, acl("User:app-" + i, "billing.payroll.", PatternType.PREFIXED, AclPermissionType.DENY));
            put(acls, binding("DeveloperRead:Group:team-" + i % 50, "team-" + i + "."));
        }
        put(acls, acl("User:bob", "billing.payroll.", PatternType.PREFIXED, AclPermissionType.DENY));

        String holder = groups == 0 ? "User:bob" : "Group:squad-" + (groups - 1);
        put(acls, binding("DeveloperRead:" + holder, "billing."));
        for (int i = 0; i < groups; i++) {
            put(acls, member("User:bob", "Group:squad-" + i));
            put(acls, binding("DeveloperRead:Group:squad-" + i, "squad-" + i + "."));
            put(acls, acl("Group:squad-" + i, i == groups - 1 ? "reports" : "squad-" + i + ".reports",
                    PatternType.LITERAL, AclPermissionType.ALLOW));
        }

        Map<Uuid, BindingCodec.Membership> members = new HashMap<>();
        acls.forEach((id, acl) -> {
            if (BindingCodec.decode(acl) instanceof BindingCodec.Membership membership) {
                members.put(id, membership);
            }
        });
        memberships = new Memberships();
        memberships.load(members);

        rbac = new RbacAuthorizer();
        rbac.configure(Map.of());
        rbac.withPluginMetrics(new PluginMetricsImpl(new Metrics(), Map.of()));
        rbac.loadSnapshot(acls);
        rbac.completeInitialLoad();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        rbac.close();
    }

    @Benchmark
    public AuthorizationResult allowedByRole() {
        return rbac.authorize(bob, READ_INVOICES).get(0);
    }

    @Benchmark
    public AuthorizationResult allowedByAclOfGroup() {
        return rbac.authorize(bob, READ_REPORTS).get(0);
    }

    @Benchmark
    public AuthorizationResult deniedByDefault() {
        return rbac.authorize(bob, READ_PAYROLL).get(0);
    }

    @Benchmark
    public List<KafkaPrincipal> groupsOf() {
        return memberships.of(bob.principal());
    }

    @Benchmark
    public List<KafkaPrincipal> noGroupsToLookUp() {
        return Identities.SELF.of(bob.principal());
    }

    private static void put(Map<Uuid, StandardAcl> acls, StandardAcl acl) {
        acls.put(Uuid.randomUuid(), acl);
    }

    private static List<Action> read(String topic) {
        return List.of(new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL), 1, true, true));
    }

    private static StandardAcl acl(String principal, String name, PatternType patternType,
            AclPermissionType permission) {
        return new StandardAcl(ResourceType.TOPIC, name, patternType, principal, "*", AclOperation.READ, permission);
    }

    private static StandardAcl binding(String roleAndMember, String prefix) {
        return new StandardAcl(ResourceType.TOPIC, prefix, PatternType.PREFIXED, roleAndMember, "*", AclOperation.ALL,
                AclPermissionType.ALLOW);
    }

    private static StandardAcl member(String member, String group) {
        return new StandardAcl(ResourceType.TOPIC, group, PatternType.LITERAL, "Member:" + member, "*",
                AclOperation.ALL, AclPermissionType.ALLOW);
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
