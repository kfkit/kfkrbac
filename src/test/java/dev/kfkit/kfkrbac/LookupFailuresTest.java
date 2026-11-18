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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Test;

/** The line {@code Role lookup failed}: once per interval, with how many were not logged. */
class LookupFailuresTest {

    private static final KafkaPrincipal BOB = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "bob");
    private static final ResourcePattern ORDERS = new ResourcePattern(ResourceType.TOPIC, "orders",
            PatternType.LITERAL);
    private static final IllegalStateException BROKEN = new IllegalStateException("bindings not loaded");

    private final AtomicLong now = new AtomicLong(1_000);
    private final List<String> lines = new ArrayList<>();
    private final List<Throwable> causes = new ArrayList<>();
    private final LookupFailures failures = new LookupFailures((line, cause) -> {
        lines.add(line);
        causes.add(cause);
    }, now::get);

    @Test
    void theFirstFailureIsLoggedWithItsException() {
        failures.failed(BOB, AclOperation.READ, ORDERS, BROKEN);

        assertEquals(List.of("Role lookup failed for User:bob READ on " + ORDERS + "; denied"), lines);
        assertSame(BROKEN, causes.get(0));
    }

    @Test
    void aLookupThatKeepsFailingIsLoggedOncePerIntervalWithHowManyWereLeftOut() {
        for (int i = 0; i < 5; i++) {
            failures.failed(BOB, AclOperation.READ, ORDERS, BROKEN);
            now.addAndGet(TimeUnit.SECONDS.toNanos(10));
        }
        assertEquals(1, lines.size(), lines.toString());

        now.addAndGet(TimeUnit.SECONDS.toNanos(10));
        failures.failed(BOB, AclOperation.WRITE, "any TOPIC", BROKEN);

        assertEquals(List.of("Role lookup failed for User:bob READ on " + ORDERS + "; denied",
                "Role lookup failed for User:bob WRITE on any TOPIC; denied; 4 more lookups failed and were denied "
                        + "since this was last logged"), lines);
        assertEquals(List.of(BROKEN, BROKEN), causes);
    }

    @Test
    void oneLeftOutIsSaidInTheSingular() {
        failures.failed(BOB, AclOperation.READ, ORDERS, BROKEN);
        failures.failed(BOB, AclOperation.READ, ORDERS, BROKEN);
        now.addAndGet(TimeUnit.SECONDS.toNanos(DeniedLog.DEFAULT_INTERVAL_SECONDS));
        failures.failed(BOB, AclOperation.READ, ORDERS, BROKEN);

        assertTrue(lines.get(1).endsWith("; denied; 1 more lookup failed and was denied since this was last logged"),
                lines.get(1));
    }

    @Test
    void theIntervalIsTheDeniedLogsAndZeroLogsEveryFailure() {
        failures.configure(Map.of(DeniedLog.INTERVAL_CONFIG, "0"));

        for (int i = 0; i < 3; i++) {
            failures.failed(BOB, AclOperation.READ, ORDERS, BROKEN);
        }

        assertEquals(3, lines.size());
        assertTrue(lines.stream().allMatch(line -> line.endsWith("; denied")), lines.toString());
    }

    @Test
    void aShorterIntervalIsHonoured() {
        failures.configure(Map.of(DeniedLog.INTERVAL_CONFIG, "5"));

        failures.failed(BOB, AclOperation.READ, ORDERS, BROKEN);
        now.addAndGet(TimeUnit.SECONDS.toNanos(5));
        failures.failed(BOB, AclOperation.READ, ORDERS, BROKEN);

        assertEquals(2, lines.size());
    }

    @Test
    void aWrongIntervalIsRefusedWithItsKey() {
        ConfigException refused = assertThrows(ConfigException.class,
                () -> failures.configure(Map.of(DeniedLog.INTERVAL_CONFIG, "-1")));

        assertTrue(refused.getMessage().contains(DeniedLog.INTERVAL_CONFIG), refused.getMessage());
    }
}
