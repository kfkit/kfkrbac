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
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The line for a denied request, on its own: what it says, how often it is
 * written and how much is remembered. The time is the test's, and the lines
 * are collected instead of logged; {@link RbacAuthorizerDeniedLogTest} shows
 * the authorizer writing them to the logger.
 */
class DeniedLogTest {

    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final AuthorizableRequestContext BOB = RequestContext.of("bob", "billing-1", ApiKeys.FETCH);

    private final List<String> lines = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong now = new AtomicLong(-5 * SECOND); // the clock may be negative, and may wrap
    private final AtomicInteger heldAsked = new AtomicInteger();
    private final AtomicInteger denyAclsAsked = new AtomicInteger();
    private volatile boolean enabled = true;
    private volatile boolean denyAclMatches;
    private volatile List<RoleBinding> held = List.of();

    // --- AC1: who, what, with which request, and why ---------------------------

    @Test
    void aDenyAclIsNamedAsTheCause() {
        denyAclMatches = true;
        held = List.of(binding("DeveloperRead:User:bob", TOPIC, "billing.", PatternType.PREFIXED));

        log(100).denied(BOB, read(TOPIC, "billing.payroll"), true);

        assertEquals(List.of("Denied User:bob READ on TOPIC 'billing.payroll' (request Fetch): a deny ACL matches"),
                lines);
    }

    @Test
    void noAclAndNoRoleIsNamedAsTheCause() {
        log(100).denied(BOB, read(TOPIC, "orders"), true);

        assertEquals(List.of("Denied User:bob READ on TOPIC 'orders' (request Fetch): "
                + "no ACL and no role allows it; roles held for TOPIC: none"), lines);
    }

    @Test
    void rolesThatCouldNotBeConsultedAreNamedAsTheCause() {
        log(100).denied(BOB, read(TOPIC, "orders"), false);

        assertEquals(List.of("Denied User:bob READ on TOPIC 'orders' (request Fetch): "
                + "the roles could not be consulted, see \"Role lookup failed\" in the broker's log"), lines);
        assertEquals(0, heldAsked.get(), "what could not be consulted is not asked again");
        assertEquals(0, denyAclsAsked.get());
    }

    @Test
    void theRequestTypeIsTheNameOfTheApi() {
        DeniedLog log = log(100);

        log.denied(RequestContext.of("bob", "billing-1", ApiKeys.JOIN_GROUP), read(GROUP, "invoicing"), true);
        log.denied(new RequestContext(new KafkaPrincipal("User", "bob"), "billing-1",
                InetAddress.getLoopbackAddress(), 32000), read(GROUP, "shipping"), true);

        assertTrue(lines.get(0).startsWith("Denied User:bob READ on GROUP 'invoicing' (request JoinGroup): "),
                lines.get(0));
        assertTrue(lines.get(1).startsWith("Denied User:bob READ on GROUP 'shipping' (request unknown 32000): "),
                lines.get(1));
    }

    @Test
    void forTheRequestOnlySystemAdminAnswersTheLineSaysSo() {
        held = List.of(binding("ClusterAdmin:User:bob", CLUSTER, "kafka-cluster", PatternType.LITERAL));

        log(100).denied(RequestContext.of("bob", "billing-1", ApiKeys.ALTER_USER_SCRAM_CREDENTIALS),
                new Action(ALTER, new ResourcePattern(CLUSTER, "kafka-cluster", PatternType.LITERAL), 1, true, true),
                true);

        assertEquals(List.of("Denied User:bob ALTER on CLUSTER 'kafka-cluster' (request AlterUserScramCredentials): "
                + "no ACL allows it, and for this request no role but SystemAdmin does; "
                + "roles held for CLUSTER: ClusterAdmin on the cluster"), lines);
    }

    @Test
    void aNameCannotStartALineOfItsOwn() {
        log(100).denied(BOB, read(GROUP, "invoicing\nDenied User:admin\r \u0007"), true);

        assertEquals(1, lines.size());
        assertTrue(lines.get(0).startsWith(
                "Denied User:bob READ on GROUP 'invoicing\\u000aDenied User:admin\\u000d\\u2028\\u0007' (request"),
                lines.get(0));
    }

    @Test
    void aLongNameIsCut() {
        String name = "g".repeat(DeniedLog.MAX_NAME_LENGTH) + "-and-then-some";

        log(100).denied(BOB, read(GROUP, name), true);

        assertTrue(lines.get(0).startsWith("Denied User:bob READ on GROUP '" + "g".repeat(DeniedLog.MAX_NAME_LENGTH)
                + "... (14 more characters)' (request Fetch): "), lines.get(0));
    }

    // --- AC2: the roles the principal holds for that resource type -----------------

    @Test
    void theRolesHeldOnOtherPatternsAreNamed() {
        held = List.of(
                binding("DeveloperRead:User:bob", TOPIC, "billing.", PatternType.PREFIXED),
                binding("DeveloperRead:User:bob", TOPIC, "orders-eu", PatternType.LITERAL),
                binding("ResourceOwner:User:bob", TOPIC, "*", PatternType.LITERAL),
                binding("SystemAdmin:User:bob", CLUSTER, "kafka-cluster", PatternType.LITERAL));

        log(100).denied(BOB, read(TOPIC, "orders"), true);

        assertEquals(List.of("Denied User:bob READ on TOPIC 'orders' (request Fetch): no ACL and no role allows it; "
                + "roles held for TOPIC: DeveloperRead on prefix billing., DeveloperRead on name orders-eu, "
                + "ResourceOwner on every name, SystemAdmin on the cluster"), lines);
    }

    @Test
    void aRoleThatDoesNotGrantTheOperationSaysSo() {
        held = List.of(
                binding("DeveloperWrite:User:bob", TOPIC, "orders", PatternType.LITERAL),
                binding("ClusterAdmin:User:bob", CLUSTER, "kafka-cluster", PatternType.LITERAL));

        log(100).denied(BOB, read(TOPIC, "orders"), true);

        assertEquals(List.of("Denied User:bob READ on TOPIC 'orders' (request Fetch): no ACL and no role allows it; "
                + "roles held for TOPIC: DeveloperWrite on name orders (does not grant READ on TOPIC), "
                + "ClusterAdmin on the cluster (does not grant READ on TOPIC)"), lines);
    }

    @Test
    void onlyTheRolesThatNameTheResourceTypeAreListed() {
        held = List.of(
                binding("DeveloperRead:User:bob", GROUP, "billing.", PatternType.PREFIXED),
                binding("DeveloperRead:User:bob", TOPIC, "Connector:billing-", PatternType.PREFIXED));
        DeniedLog log = log(100);

        log.denied(BOB, read(TOPIC, "orders"), true);
        log.denied(BOB, read(GROUP, "orders"), true);

        assertTrue(lines.get(0).endsWith("; roles held for TOPIC: none"), lines.get(0));
        assertTrue(lines.get(1).endsWith("; roles held for GROUP: DeveloperRead on groups with prefix billing."), lines.get(1));
    }

    @Test
    void aRoleHeldAsAnotherMemberNamesTheMember() {
        held = List.of(binding("DeveloperRead:Group:payments", TOPIC, "billing.", PatternType.PREFIXED));

        log(100).denied(BOB, read(TOPIC, "orders"), true);

        assertTrue(lines.get(0).endsWith(
                "; roles held for TOPIC: DeveloperRead on prefix billing. as Group:payments"), lines.get(0));
    }

    @Test
    void aLongListOfRolesIsCutAndCounted() {
        List<RoleBinding> many = new ArrayList<>();
        for (int i = 0; i < DeniedLog.MAX_ROLES_NAMED + 5; i++) {
            many.add(binding("DeveloperRead:User:bob", TOPIC, "team-" + i + ".", PatternType.PREFIXED));
        }
        held = many;

        log(100).denied(BOB, read(TOPIC, "orders"), true);

        String line = lines.get(0);
        assertTrue(line.contains("DeveloperRead on prefix team-" + (DeniedLog.MAX_ROLES_NAMED - 1) + "."), line);
        assertFalse(line.contains("prefix team-" + DeniedLog.MAX_ROLES_NAMED + "."), line);
        assertTrue(line.endsWith(", and 5 more"), line);
    }

    // --- AC3: once per interval, and then how many were not logged ------------------

    @Test
    void theSameRequestIsLoggedOncePerIntervalAndThenSaysHowManyWereNot() {
        DeniedLog log = log(100);
        Action action = read(TOPIC, "orders");

        log.denied(BOB, action, true);
        for (int i = 0; i < 41; i++) {
            now.addAndGet(SECOND);
            log.denied(BOB, action, true);
        }
        assertEquals(1, lines.size(), "41 seconds in");

        now.addAndGet(19 * SECOND - 1);
        log.denied(BOB, action, true);
        assertEquals(1, lines.size(), "a nanosecond short of the minute");

        now.addAndGet(1);
        log.denied(BOB, action, true);
        assertEquals(2, lines.size(), "the minute is over");
        assertEquals(lines.get(0) + "; denied 42 more times since this was last logged", lines.get(1));

        now.addAndGet(60 * SECOND);
        log.denied(BOB, action, true);
        assertEquals(lines.get(0), lines.get(2), "nothing was suppressed in the minute before");
    }

    @Test
    void oneSuppressedDenialIsOneMoreTime() {
        DeniedLog log = log(100);
        Action action = read(TOPIC, "orders");

        log.denied(BOB, action, true);
        log.denied(BOB, action, true);
        now.addAndGet(60 * SECOND);
        log.denied(BOB, action, true);

        assertTrue(lines.get(1).endsWith("; denied 1 more time since this was last logged"), lines.get(1));
    }

    @Test
    void anotherPrincipalOperationResourceTypeOrNameIsLoggedOnItsOwn() {
        DeniedLog log = log(100);

        log.denied(BOB, read(TOPIC, "orders"), true);
        log.denied(RequestContext.of("alice", "billing-1", ApiKeys.FETCH), read(TOPIC, "orders"), true);
        log.denied(BOB, new Action(WRITE, literal(TOPIC, "orders"), 1, true, true), true);
        log.denied(BOB, read(GROUP, "orders"), true);
        log.denied(BOB, read(TOPIC, "orders-eu"), true);
        log.deniedOnAnyResource(BOB, READ, TOPIC, DeniedLog.Cause.NOT_GRANTED);
        assertEquals(6, lines.size());

        log.denied(RequestContext.of("bob", "billing-2", ApiKeys.OFFSET_FETCH), read(TOPIC, "orders"), true);
        assertEquals(6, lines.size(), "the client and the request do not tell two denials apart");
    }

    @Test
    void theIntervalIsTheSetting() {
        DeniedLog log = log(100);
        log.configure(Map.of(DeniedLog.INTERVAL_CONFIG, "5"));
        Action action = read(TOPIC, "orders");

        log.denied(BOB, action, true);
        now.addAndGet(5 * SECOND - 1);
        log.denied(BOB, action, true);
        assertEquals(1, lines.size());

        now.addAndGet(1);
        log.denied(BOB, action, true);
        assertEquals(2, lines.size());
    }

    @Test
    void anIntervalOfZeroLogsEveryDenialAndRemembersNothing() {
        DeniedLog log = log(100);
        log.configure(Map.of(DeniedLog.INTERVAL_CONFIG, 0));
        Action action = read(TOPIC, "orders");

        for (int i = 0; i < 3; i++) {
            log.denied(BOB, action, true);
            log.deniedOnAnyResource(BOB, WRITE, TOPIC, DeniedLog.Cause.NOT_GRANTED);
        }

        assertEquals(6, lines.size());
        assertEquals(lines.get(0), lines.get(4));
        assertEquals(0, log.remembered());
    }

    @Test
    void withoutTheSettingTheIntervalIsAMinute() {
        DeniedLog log = log(100);
        log.configure(Map.of());
        Action action = read(TOPIC, "orders");

        log.denied(BOB, action, true);
        now.addAndGet(DeniedLog.DEFAULT_INTERVAL_SECONDS * SECOND - 1);
        log.denied(BOB, action, true);
        assertEquals(1, lines.size());
        assertEquals(60, DeniedLog.DEFAULT_INTERVAL_SECONDS);
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "a minute", "1.5", "", "60s", "2147483648"})
    void aWrongIntervalIsRefusedAndNamesTheSetting(String value) {
        ConfigException refused = assertThrows(ConfigException.class,
                () -> log(100).configure(Map.of(DeniedLog.INTERVAL_CONFIG, value)));

        assertEquals("Invalid value " + value + " for configuration kfkrbac.denied.log.interval.seconds: "
                + "must be a whole number of seconds, 0 or more; 0 logs every denied request", refused.getMessage());
    }

    // --- AC5: a probe is not a failure ------------------------------------------------

    @Test
    void aDenialTheBrokerDoesNotWantLoggedWritesAndRemembersNothing() {
        DeniedLog log = log(100);

        log.denied(BOB, new Action(READ, literal(TOPIC, "orders"), 1, true, false), true);

        assertEquals(List.of(), lines);
        assertEquals(0, log.remembered());
        assertEquals(0, heldAsked.get());
        assertEquals(0, denyAclsAsked.get());

        log.denied(BOB, read(TOPIC, "orders"), true);
        assertEquals(1, lines.size(), "the same denial, wanted logged, is logged");
    }

    @Test
    void aSilencedLoggerCostsNoMemoryAndNoLookup() {
        enabled = false;
        DeniedLog log = log(100);

        log.denied(BOB, read(TOPIC, "orders"), true);
        log.deniedOnAnyResource(BOB, WRITE, TOPIC, DeniedLog.Cause.NOT_GRANTED);

        assertEquals(List.of(), lines);
        assertEquals(0, log.remembered());
        assertEquals(0, heldAsked.get());
        assertEquals(0, denyAclsAsked.get());
    }

    // --- AC6: what is remembered is bounded -------------------------------------------

    @Test
    void neverRemembersMoreThanItsCapacity() {
        DeniedLog log = log(100);

        for (int i = 0; i < 100_000; i++) {
            log.denied(BOB, read(TOPIC, "orders-" + i), true);
            assertTrue(log.remembered() <= 100, () -> String.valueOf(log.remembered()));
            if (i % 7 == 0) {
                now.addAndGet(SECOND);
            }
        }
        assertEquals(100_000, lines.stream().filter(line -> line.startsWith("Denied ")).count(),
                "a request never seen before is always logged");
    }

    @Test
    void neverRemembersMoreThanItsCapacityWhenManyThreadsAreDenied() throws InterruptedException {
        int threads = 8;
        DeniedLog log = new DeniedLog(context -> held, (context, action) -> false, new DeniedLog.Sink() {
            @Override
            public boolean enabled() {
                return true;
            }

            @Override
            public void write(String line) {
                // the lines are not what this test is about
            }
        }, now::get, 64);
        AtomicInteger most = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                int thread = t;
                pool.execute(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int i = 0; i < 50_000; i++) {
                        // every fourth is another thread's too, so that threads meet on a request
                        String topic = i % 4 == 0 ? "orders-" + i : "orders-" + thread + "-" + i;
                        log.denied(BOB, read(TOPIC, topic), true);
                        most.accumulateAndGet(log.remembered(), Math::max);
                    }
                });
            }
            start.countDown();
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "the threads finished");
        }

        assertTrue(most.get() <= 64, () -> "remembered " + most.get());
        assertTrue(most.get() > 32, () -> "remembered " + most.get() + ": the bound was not reached");
    }

    @Test
    void whenFullTheRequestsWhoseIntervalIsOverAreForgottenAndTheOthersKept() {
        DeniedLog log = log(16);
        for (int i = 0; i < 8; i++) {
            log.denied(BOB, read(TOPIC, "old-" + i), true);
        }
        log.denied(BOB, read(TOPIC, "old-0"), true); // not logged, and never will be counted
        now.addAndGet(60 * SECOND);
        for (int i = 0; i < 8; i++) {
            log.denied(BOB, read(TOPIC, "new-" + i), true);
        }
        assertEquals(16, log.remembered());
        lines.clear();

        log.denied(BOB, read(TOPIC, "orders"), true);

        assertEquals(9, log.remembered());
        assertEquals(List.of("Forgot 8 of the denied requests remembered so as not to log them again, to stay "
                + "within 16; 1 denials that were not logged will not be counted in a later line",
                "Denied User:bob READ on TOPIC 'orders' (request Fetch): no ACL and no role allows it; "
                        + "roles held for TOPIC: none"), lines);

        lines.clear();
        for (int i = 0; i < 8; i++) {
            log.denied(BOB, read(TOPIC, "new-" + i), true);
        }
        assertEquals(List.of(), lines, "what was kept is still not logged again");
    }

    @Test
    void whenFullOfRequestsWithinTheirIntervalAllAreForgottenAndLoggedAgain() {
        DeniedLog log = log(16);
        for (int i = 0; i < 16; i++) {
            log.denied(BOB, read(TOPIC, "orders-" + i), true);
        }
        for (int i = 0; i < 3; i++) {
            log.denied(BOB, read(TOPIC, "orders-5"), true);
        }
        assertEquals(16, lines.size());
        lines.clear();

        log.denied(BOB, read(TOPIC, "orders-16"), true);
        assertEquals(1, log.remembered());
        assertEquals("Forgot 16 of the denied requests remembered so as not to log them again, to stay within 16; "
                + "3 denials that were not logged will not be counted in a later line", lines.get(0));

        log.denied(BOB, read(TOPIC, "orders-5"), true);
        assertEquals(3, lines.size(), "forgotten, so logged again");
        assertFalse(lines.get(2).contains("more time"), lines.get(2));
    }

    @Test
    void namesAreToldApartByTheirFirstCharacters() {
        DeniedLog log = log(100);
        String head = "g".repeat(DeniedLog.MAX_NAME_LENGTH);

        log.denied(BOB, read(GROUP, head + "-1"), true);
        log.denied(BOB, read(GROUP, head + "-2"), true);
        log.denied(BOB, read(GROUP, head.substring(1)), true);

        assertEquals(2, lines.size(), "the first two are one request to remember");
        assertEquals(2, log.remembered());
    }

    // --- the second question: any resource of the type --------------------------------

    @Test
    void theSecondQuestionSaysAnyResourceOfTheTypeAndWhatIsHeldForIt() {
        held = List.of(
                binding("DeveloperRead:User:bob", TOPIC, "billing.", PatternType.PREFIXED),
                binding("DeveloperWrite:User:bob", GROUP, "billing.", PatternType.PREFIXED));
        AuthorizableRequestContext producer = RequestContext.of("bob", "billing-1", ApiKeys.INIT_PRODUCER_ID);
        DeniedLog log = log(100);

        log.deniedOnAnyResource(producer, WRITE, TOPIC, DeniedLog.Cause.NOT_GRANTED);
        log.deniedOnAnyResource(producer, WRITE, TOPIC, DeniedLog.Cause.NOT_GRANTED);

        assertEquals(List.of("Denied User:bob WRITE on any TOPIC (request InitProducerId): "
                + "no ACL and no role allows it; roles held for TOPIC: "
                + "DeveloperRead on prefix billing. (does not grant WRITE on TOPIC)"), lines);
    }

    @Test
    void theSecondQuestionNamesDenyAclsThatCoverWhatARoleGrants() {
        held = List.of(binding("DeveloperWrite:User:bob", TOPIC, "billing.", PatternType.PREFIXED));

        log(100).deniedOnAnyResource(RequestContext.of("bob", "billing-1", ApiKeys.INIT_PRODUCER_ID), WRITE, TOPIC,
                DeniedLog.Cause.DENY_ACL);

        assertEquals(List.of("Denied User:bob WRITE on any TOPIC (request InitProducerId): "
                + "deny ACLs cover everything a role or a group allows it on; roles held for TOPIC: "
                + "DeveloperWrite on prefix billing."), lines);
    }

    @Test
    void theSecondQuestionSaysWhenTheRolesCouldNotBeConsulted() {
        log(100).deniedOnAnyResource(RequestContext.of("bob", "billing-1", ApiKeys.INIT_PRODUCER_ID), WRITE, TOPIC,
                DeniedLog.Cause.ROLES_FAILED);

        assertEquals(List.of("Denied User:bob WRITE on any TOPIC (request InitProducerId): "
                + "the roles could not be consulted, see \"Role lookup failed\" in the broker's log"), lines);
        assertEquals(0, heldAsked.get());
    }

    // --- never in the way ----------------------------------------------------------------

    @Test
    void aFailureToDescribeADenialIsNotThrownIntoTheRequest() {
        DeniedLog log = new DeniedLog(context -> {
            throw new IllegalStateException("bindings not loaded");
        }, (context, action) -> {
            throw new IllegalStateException("deny ACLs not loaded");
        }, sink(), now::get, 100);

        log.denied(BOB, read(TOPIC, "orders"), true);
        log.deniedOnAnyResource(BOB, WRITE, TOPIC, DeniedLog.Cause.NOT_GRANTED);

        assertEquals(List.of(), lines);
    }

    // --- helpers -----------------------------------------------------------------------

    private DeniedLog log(int capacity) {
        return new DeniedLog(context -> {
            heldAsked.incrementAndGet();
            return held;
        }, (context, action) -> {
            denyAclsAsked.incrementAndGet();
            return denyAclMatches;
        }, sink(), now::get, capacity);
    }

    private DeniedLog.Sink sink() {
        return new DeniedLog.Sink() {
            @Override
            public boolean enabled() {
                return enabled;
            }

            @Override
            public void write(String line) {
                lines.add(line);
            }
        };
    }

    private static Action read(ResourceType type, String name) {
        return new Action(READ, literal(type, name), 1, true, true);
    }

    private static ResourcePattern literal(ResourceType type, String name) {
        return new ResourcePattern(type, name, PatternType.LITERAL);
    }

    /** A binding as the authorizer reads it from its ACL record. */
    static RoleBinding binding(String roleAndMember, ResourceType type, String name, PatternType patternType) {
        BindingCodec.Decoded decoded = BindingCodec.decode(new StandardAcl(type, name, patternType, roleAndMember,
                "*", ALL, AclPermissionType.ALLOW));
        if (decoded instanceof RoleBinding binding) {
            return binding;
        }
        throw new AssertionError(roleAndMember + " on " + name + " is not a binding: " + decoded);
    }
}
