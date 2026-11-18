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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code ERROR} line {@code Role lookup failed}, with the exception, at
 * most once per interval.
 *
 * <p>A lookup that fails once fails for every request until what broke is
 * mended, and a broker answers thousands of requests a second: the first
 * failure is logged with its stack trace, the ones that follow within the
 * interval are only counted, and the next line says how many there were.
 * Every one of them is denied all the same; this only decides what is
 * written. The interval is that of the denied log,
 * {@value DeniedLog#INTERVAL_CONFIG}, and {@code 0} logs every failure.
 *
 * <p>The lines go to the logger of {@link RbacAuthorizer}. A failure that is
 * not logged costs a clock read and two atomic operations, and takes no
 * lock. A failure that arrives while a line is written may be missing from
 * the count of the next one.
 */
final class LookupFailures {

    /** Where the lines go. */
    interface Sink {
        void write(String line, Throwable cause);
    }

    private static final Logger LOG = LoggerFactory.getLogger(RbacAuthorizer.class);

    private static final Sink LOGGED = LOG::error;

    /** {@link #loggedAt} before the first line. */
    private static final long NEVER = Long.MIN_VALUE;

    private final Sink sink;
    private final LongSupplier nanoTime;

    /** When the last line was written, or {@link #NEVER}. */
    private final AtomicLong loggedAt = new AtomicLong(NEVER);

    /** How many failures were not logged since the last line. */
    private final AtomicLong suppressed = new AtomicLong();

    private volatile long intervalNanos = TimeUnit.SECONDS.toNanos(DeniedLog.DEFAULT_INTERVAL_SECONDS);

    LookupFailures() {
        this(LOGGED, System::nanoTime);
    }

    /** For tests: where the lines go and what the time is. */
    LookupFailures(Sink sink, LongSupplier nanoTime) {
        this.sink = Objects.requireNonNull(sink, "sink");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    /**
     * Reads {@value DeniedLog#INTERVAL_CONFIG}.
     *
     * @throws org.apache.kafka.common.config.ConfigException when it is not a whole number of seconds, 0 or more
     */
    void configure(Map<String, ?> configs) {
        intervalNanos = TimeUnit.SECONDS.toNanos(DeniedLog.intervalSeconds(configs.get(DeniedLog.INTERVAL_CONFIG)));
    }

    /**
     * The roles could not be consulted for a request, which is denied.
     *
     * @param resource what was asked about, as it is to be written
     */
    void failed(KafkaPrincipal principal, AclOperation operation, Object resource, RuntimeException cause) {
        long notLogged = due();
        if (notLogged < 0) {
            return;
        }
        StringBuilder line = new StringBuilder(160).append("Role lookup failed for ").append(principal).append(' ')
                .append(operation).append(" on ").append(resource).append("; denied");
        if (notLogged > 0) {
            line.append("; ").append(notLogged).append(notLogged == 1 ? " more lookup" : " more lookups")
                    .append(" failed and ").append(notLogged == 1 ? "was" : "were")
                    .append(" denied since this was last logged");
        }
        sink.write(line.toString(), cause);
    }

    /** The number of failures not logged since the last line when a line is due now, or -1 when none is. */
    private long due() {
        long interval = intervalNanos;
        if (interval == 0) {
            return suppressed.getAndSet(0);
        }
        long now = nanoTime.getAsLong();
        long at = loggedAt.get();
        if ((at == NEVER || now - at >= interval) && loggedAt.compareAndSet(at, now)) {
            return suppressed.getAndSet(0);
        }
        suppressed.incrementAndGet();
        return -1;
    }
}
