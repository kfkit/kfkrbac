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

import java.net.InetAddress;
import java.util.List;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * What the line for a denied request adds to a denial, with the logger on:
 * the benchmarks of {@code authorize} run without a logger, where it adds
 * one check. The lines go to a {@link Blackhole}, so writing them out is
 * not in the numbers.
 *
 * <p>{@code repeated} is a client that retries one denied request: the line
 * is not written again, and this is what each retry costs.
 * {@code neverSeenBefore} is the worst there is: every denial is of a
 * resource never denied before, so each writes its line, and what is
 * remembered is full and makes room over and over.
 */
@State(Scope.Benchmark)
public class DeniedLogBenchmark {

    private static final Action READ_ORDERS = read("orders");

    private final AuthorizableRequestContext bob = context("bob");
    private DeniedLog log;
    private int next;

    @Setup(Level.Trial)
    public void setUp(Blackhole lines) {
        List<RoleBinding> held = List.of((RoleBinding) BindingCodec.decode(new StandardAcl(ResourceType.TOPIC,
                "billing.", PatternType.PREFIXED, "DeveloperRead:User:bob", "*", AclOperation.ALL,
                AclPermissionType.ALLOW)));
        log = new DeniedLog(context -> held, (context, action) -> false, new DeniedLog.Sink() {
            @Override
            public boolean enabled() {
                return true;
            }

            @Override
            public void write(String line) {
                lines.consume(line);
            }
        }, System::nanoTime, DeniedLog.MAX_REMEMBERED);
        log.denied(bob, READ_ORDERS, true);
    }

    @Benchmark
    public void repeated() {
        log.denied(bob, READ_ORDERS, true);
    }

    @Benchmark
    public int neverSeenBefore() {
        log.denied(bob, read("orders-" + next++), true);
        return log.remembered();
    }

    private static Action read(String topic) {
        return new Action(AclOperation.READ, new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL), 1,
                true, true);
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
