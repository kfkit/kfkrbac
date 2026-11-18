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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.authorizer.Action;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * The two things {@code authorize} does before it asks the ACLs, so that
 * the broker's authorizer log says what was decided, each on its own: they
 * are too small to show next to an ACL lookup in {@link AuthorizeBenchmark}.
 *
 * <p>{@code holdsNoBinding} is what every request pays: the lookup that
 * says a principal holds no binding, among the bindings of a thousand
 * others. {@code holdsABinding} and {@code oneActionWithoutDeniedLine} are
 * what a request of a principal that holds one pays on top: the same lookup,
 * and the copy of its action that the ACLs are asked with.
 */
@State(Scope.Benchmark)
public class RoleAuditBenchmark {

    private static final List<Action> READ_INVOICES = List.of(new Action(AclOperation.READ,
            new ResourcePattern(ResourceType.TOPIC, "billing.invoices", PatternType.LITERAL), 1, true, true));

    private final KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
    private final KafkaPrincipal bob = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob");
    private BindingIndex bindings;

    @Setup(Level.Trial)
    public void setUp() {
        Map<Uuid, StandardAcl> records = new HashMap<>();
        records.put(Uuid.randomUuid(), binding("DeveloperRead:User:bob", "billing."));
        for (int i = 1; i < 10_000; i++) {
            records.put(Uuid.randomUuid(), binding("DeveloperRead:User:app-" + i % 1000, "team-" + i + "."));
        }
        bindings = new BindingIndex();
        bindings.load(records);
    }

    @Benchmark
    public boolean holdsNoBinding() {
        return bindings.holdsAny(alice);
    }

    @Benchmark
    public boolean holdsABinding() {
        return bindings.holdsAny(bob);
    }

    @Benchmark
    public List<Action> oneActionWithoutDeniedLine() {
        return RoleAudit.withoutDeniedLine(READ_INVOICES);
    }

    private static StandardAcl binding(String roleAndMember, String prefix) {
        return new StandardAcl(ResourceType.TOPIC, prefix, PatternType.PREFIXED, roleAndMember, "*", AclOperation.ALL,
                AclPermissionType.ALLOW);
    }
}
