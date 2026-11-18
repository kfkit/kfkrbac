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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The line that says what was missing when a request is denied: who asked,
 * for which operation, on which resource, with which request, and why it was
 * denied; and, when no ACL and no role allowed it, the roles the principal
 * does hold for that resource type, so that a binding on the wrong prefix or
 * of the wrong role shows at once.
 *
 * <p>The lines go to a logger of their own, {@value #LOGGER}, at
 * {@code INFO}, so that an operator can route or silence them apart from
 * everything else. They say nothing the broker's own authorizer log would
 * not, but the cause and the roles and patterns of the principal that asked:
 * no host, no client ID, nothing about any other principal.
 *
 * <p>A client retries a denied request many times a second, so a line is
 * written at most once per interval for the same principal, operation and
 * resource, and the line that follows says how many denials were not logged
 * in between. What was logged is remembered for at most {@code capacity}
 * requests, {@value #MAX_REMEMBERED} on a broker, each by a principal and a
 * resource name of at most {@value #MAX_NAME_LENGTH} characters: longer
 * names are told apart by their first {@value #MAX_NAME_LENGTH}. When that
 * many are remembered, the ones whose interval is over are forgotten; when
 * that frees less than an eighth, all are, and a line says so. A request
 * that was forgotten is logged again when it is next denied. A denial that
 * arrives while its request is being forgotten may be missing from a count.
 *
 * <p>Only ever called for a denial that is final. A denial the broker does
 * not want logged ({@link Action#logIfDenied()} false), and any denial when
 * the logger is silenced, costs that check and nothing else. A repeated
 * denial is one lookup and takes no lock; the first denial of a request
 * takes a lock for the time it takes to remember it. Nothing here throws
 * into {@code authorize}.
 */
final class DeniedLog {

    /** The logger the lines are written to. */
    static final String LOGGER = "dev.kfkit.kfkrbac.denied";

    /** The setting: how many seconds pass before the same denial is logged again; 0 logs every denial. */
    static final String INTERVAL_CONFIG = "kfkrbac.denied.log.interval.seconds";

    static final int DEFAULT_INTERVAL_SECONDS = 60;

    /** How many different denied requests are remembered at most. */
    static final int MAX_REMEMBERED = 10_000;

    /** How much of a name is remembered, and shown. */
    static final int MAX_NAME_LENGTH = 256;

    /** How many of the roles a principal holds are named in one line. */
    static final int MAX_ROLES_NAMED = 20;

    /** Why a request was denied. */
    enum Cause {
        /** A deny ACL matched: no role is ever asked past it. */
        DENY_ACL,
        /** No ACL allowed it and no role bound to the principal grants it. */
        NOT_GRANTED,
        /** The roles could not be consulted, and nothing is granted on failure. */
        ROLES_FAILED
    }

    /** Whether a deny ACL matches an action: asked only when a line is written. */
    interface DenyAcls {
        boolean match(AuthorizableRequestContext requestContext, Action action);
    }

    /** Where the lines go. */
    interface Sink {
        /** Whether a line would be written at all. */
        boolean enabled();

        void write(String line);
    }

    private static final Logger LOG = LoggerFactory.getLogger(DeniedLog.class);

    private static final Sink LOGGED = new Sink() {
        private final Logger denied = LoggerFactory.getLogger(LOGGER);

        @Override
        public boolean enabled() {
            return denied.isInfoEnabled();
        }

        @Override
        public void write(String line) {
            denied.info(line);
        }
    };

    /** A request as it is told apart; the name is {@code null} for "any resource of the type". */
    private record Key(String principalType, String principalName, AclOperation operation, ResourceType resourceType,
            String resourceName) {
    }

    /** When the line of a request was last written, and how many of its denials were not logged since. */
    private static final class Remembered {
        private final AtomicLong loggedAt;
        private final AtomicLong suppressed = new AtomicLong();

        Remembered(long now) {
            loggedAt = new AtomicLong(now);
        }

        /** The number of denials not logged since the last line when a line is due now, or -1 when none is. */
        long due(long now, long interval) {
            long at = loggedAt.get();
            if (now - at >= interval && loggedAt.compareAndSet(at, now)) {
                return suppressed.getAndSet(0);
            }
            suppressed.incrementAndGet();
            return -1;
        }
    }

    private final Function<AuthorizableRequestContext, List<RoleBinding>> held;
    private final DenyAcls denyAcls;
    private final Sink sink;
    private final LongSupplier nanoTime;
    private final int capacity;

    /** Read on the request path; added to under the lock of {@code this}, so never more than {@code capacity}. */
    private final ConcurrentHashMap<Key, Remembered> remembered = new ConcurrentHashMap<>();

    private volatile long intervalNanos = TimeUnit.SECONDS.toNanos(DEFAULT_INTERVAL_SECONDS);

    /**
     * @param held every binding that applies to the principal of a request
     * @param denyAcls whether a deny ACL matches an action
     */
    DeniedLog(Function<AuthorizableRequestContext, List<RoleBinding>> held, DenyAcls denyAcls) {
        this(held, denyAcls, LOGGED, System::nanoTime, MAX_REMEMBERED);
    }

    /** For tests and benchmarks: where the lines go, what the time is and how much is remembered. */
    DeniedLog(Function<AuthorizableRequestContext, List<RoleBinding>> held, DenyAcls denyAcls, Sink sink,
            LongSupplier nanoTime, int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity " + capacity);
        }
        this.held = Objects.requireNonNull(held, "held");
        this.denyAcls = Objects.requireNonNull(denyAcls, "denyAcls");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.capacity = capacity;
    }

    /**
     * Reads {@value #INTERVAL_CONFIG}.
     *
     * @throws ConfigException when it is not a whole number of seconds, 0 or more
     */
    void configure(Map<String, ?> configs) {
        intervalNanos = TimeUnit.SECONDS.toNanos(intervalSeconds(configs.get(INTERVAL_CONFIG)));
    }

    private static int intervalSeconds(Object value) {
        if (value == null) {
            return DEFAULT_INTERVAL_SECONDS;
        }
        try {
            long seconds = Long.parseLong(value.toString().trim());
            if (seconds >= 0 && seconds <= Integer.MAX_VALUE) {
                return (int) seconds;
            }
        } catch (NumberFormatException e) {
            // not a number: refused below, with the key
        }
        throw new ConfigException(INTERVAL_CONFIG, value,
                "must be a whole number of seconds, 0 or more; 0 logs every denied request");
    }

    /**
     * An action was denied, and that is final.
     *
     * @param rolesConsulted false when the roles could not be consulted, which is why it was denied
     */
    void denied(AuthorizableRequestContext requestContext, Action action, boolean rolesConsulted) {
        if (!action.logIfDenied() || !sink.enabled()) {
            return;
        }
        ResourcePattern resource = action.resourcePattern();
        try {
            long suppressed = due(requestContext.principal(), action.operation(), resource.resourceType(),
                    resource.name());
            if (suppressed < 0) {
                return;
            }
            Cause cause = !rolesConsulted ? Cause.ROLES_FAILED
                    : denyAcls.match(requestContext, action) ? Cause.DENY_ACL : Cause.NOT_GRANTED;
            sink.write(line(requestContext, action.operation(), resource.resourceType(), resource.name(), cause,
                    suppressed));
        } catch (RuntimeException e) {
            LOG.warn("Could not say why {} was denied {} on {}", requestContext.principal(), action.operation(),
                    resource, e);
        }
    }

    /**
     * The broker's second question, "may this principal do this on any
     * resource of the type", was answered {@code DENIED}, and that is final.
     * The question carries no word on whether to log it; its denial fails
     * the request, so it is logged.
     */
    void deniedOnAnyResource(AuthorizableRequestContext requestContext, AclOperation operation,
            ResourceType resourceType, Cause cause) {
        if (!sink.enabled()) {
            return;
        }
        try {
            long suppressed = due(requestContext.principal(), operation, resourceType, null);
            if (suppressed >= 0) {
                sink.write(line(requestContext, operation, resourceType, null, cause, suppressed));
            }
        } catch (RuntimeException e) {
            LOG.warn("Could not say why {} was denied {} on any {}", requestContext.principal(), operation,
                    resourceType, e);
        }
    }

    /** For tests: how many denied requests are remembered. */
    int remembered() {
        return remembered.size();
    }

    // --- at most one line per interval -----------------------------------------

    /** The number of denials not logged since the last line when a line is due now, or -1 when none is. */
    private long due(KafkaPrincipal principal, AclOperation operation, ResourceType resourceType,
            String resourceName) {
        long interval = intervalNanos;
        if (interval == 0) {
            return 0;
        }
        Key key = new Key(head(principal.getPrincipalType()), head(principal.getName()), operation, resourceType,
                resourceName == null ? null : head(resourceName));
        long now = nanoTime.getAsLong();
        Remembered last = remembered.get(key);
        if (last == null) {
            last = remember(key, now);
            if (last == null) {
                return 0;
            }
        }
        return last.due(now, interval);
    }

    /** Remembers a request that is logged now; returns what is remembered of it when another thread was first. */
    private Remembered remember(Key key, long now) {
        long[] forgotten = null;
        synchronized (this) {
            Remembered raced = remembered.get(key);
            if (raced != null) {
                return raced;
            }
            if (remembered.size() >= capacity) {
                forgotten = makeRoom(now);
            }
            remembered.put(key, new Remembered(now));
        }
        if (forgotten != null) {
            sink.write("Forgot " + forgotten[0] + " of the denied requests remembered so as not to log them again, "
                    + "to stay within " + capacity + "; " + forgotten[1]
                    + " denials that were not logged will not be counted in a later line");
        }
        return null;
    }

    /**
     * Forgets the requests whose interval is over, and all of them when that
     * frees less than an eighth: a pass over everything then pays for that
     * many new requests at least. Returns how many were forgotten and how
     * many denials that were not logged went with them.
     */
    private long[] makeRoom(long now) {
        long interval = intervalNanos;
        long requests = 0;
        long denials = 0;
        for (Iterator<Remembered> all = remembered.values().iterator(); all.hasNext();) {
            Remembered one = all.next();
            if (now - one.loggedAt.get() >= interval) {
                all.remove();
                requests++;
                denials += one.suppressed.get();
            }
        }
        if (remembered.size() >= capacity - capacity / 8) {
            for (Remembered one : remembered.values()) {
                requests++;
                denials += one.suppressed.get();
            }
            remembered.clear();
        }
        return new long[] {requests, denials};
    }

    private static String head(String name) {
        return name.length() <= MAX_NAME_LENGTH ? name : name.substring(0, MAX_NAME_LENGTH);
    }

    // --- the line ----------------------------------------------------------------

    private String line(AuthorizableRequestContext requestContext, AclOperation operation, ResourceType resourceType,
            String resourceName, Cause cause, long suppressed) {
        KafkaPrincipal principal = requestContext.principal();
        StringBuilder line = new StringBuilder(200).append("Denied ");
        show(line, principal.getPrincipalType());
        line.append(':');
        show(line, principal.getName());
        line.append(' ').append(operation).append(" on ");
        if (resourceName == null) {
            line.append("any ").append(resourceType);
        } else {
            line.append(resourceType).append(" '");
            show(line, resourceName);
            line.append('\'');
        }
        int request = requestContext.requestType();
        line.append(" (request ").append(ApiKeys.hasId(request) ? ApiKeys.forId(request).name : "unknown " + request)
                .append("): ");
        switch (cause) {
            case DENY_ACL:
                line.append(resourceName == null
                        ? "deny ACLs cover everything a role or a group allows it on"
                        : "a deny ACL matches");
                break;
            case ROLES_FAILED:
                line.append("the roles could not be consulted, see \"Role lookup failed\" in the broker's log");
                break;
            default:
                line.append(RbacAuthorizer.keptForSystemAdmin(requestContext, operation, resourceType)
                        ? "no ACL allows it, and for this request no role but SystemAdmin does"
                        : "no ACL and no role allows it");
        }
        if (cause == Cause.NOT_GRANTED || cause == Cause.DENY_ACL && resourceName == null) {
            rolesHeld(line, principal, held.apply(requestContext), operation, resourceType);
        }
        if (suppressed > 0) {
            line.append("; denied ").append(suppressed).append(suppressed == 1 ? " more time" : " more times")
                    .append(" since this was last logged");
        }
        return line.toString();
    }

    /**
     * The roles the principal holds that name resources of the type, each
     * with what it is bound to, in the words of the command. One whose role
     * does not grant the operation on the type says so; the others grant it,
     * on a pattern that does not cover what was asked for.
     */
    private static void rolesHeld(StringBuilder line, KafkaPrincipal principal, List<RoleBinding> bindings,
            AclOperation operation, ResourceType resourceType) {
        line.append("; roles held for ").append(resourceType).append(": ");
        int named = 0;
        int more = 0;
        for (RoleBinding binding : bindings) {
            if (!binding.reaches(resourceType)) {
                continue;
            }
            if (named == MAX_ROLES_NAMED) {
                more++;
                continue;
            }
            if (named++ > 0) {
                line.append(", ");
            }
            line.append(binding.role().roleName()).append(" on ").append(Wording.on(binding));
            KafkaPrincipal member = binding.member();
            if (!member.getPrincipalType().equals(principal.getPrincipalType())
                    || !member.getName().equals(principal.getName())) {
                line.append(" as ");
                show(line, member.getPrincipalType());
                line.append(':');
                show(line, member.getName());
            }
            if (!binding.role().grants(operation, resourceType)) {
                line.append(" (does not grant ").append(operation).append(" on ").append(resourceType).append(')');
            }
        }
        if (named == 0) {
            line.append("none");
        } else if (more > 0) {
            line.append(", and ").append(more).append(" more");
        }
    }

    /**
     * A name a client chose, as it is safe to show: cut to
     * {@value #MAX_NAME_LENGTH} characters, and with no character that would
     * start a line of its own.
     */
    private static void show(StringBuilder line, String name) {
        int length = Math.min(name.length(), MAX_NAME_LENGTH);
        for (int i = 0; i < length; i++) {
            char c = name.charAt(i);
            if (Character.isISOControl(c) || c == ' ' || c == ' ') {
                line.append("\\u").append(Character.forDigit(c >> 12 & 0xf, 16))
                        .append(Character.forDigit(c >> 8 & 0xf, 16)).append(Character.forDigit(c >> 4 & 0xf, 16))
                        .append(Character.forDigit(c & 0xf, 16));
            } else {
                line.append(c);
            }
        }
        if (name.length() > length) {
            line.append("... (").append(name.length() - length).append(" more characters)");
        }
    }
}
