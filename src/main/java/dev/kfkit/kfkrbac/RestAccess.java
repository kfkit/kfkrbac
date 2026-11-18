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

import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The role bindings of a cluster as an extension outside the broker reads
 * them, and the questions it asks of them: who is calling, whether the
 * bindings are loaded, and whether a role bound to the caller allows what
 * it asks.
 *
 * <p>The bindings are read with the admin client ({@code describeAcls}),
 * with the connection settings of the process the extension runs in, and
 * kept in memory; they are read again every
 * {@value #DEFAULT_REFRESH_INTERVAL_MS} ms, or every
 * {@code kfkrbac.refresh.interval.ms}. Until the first read succeeds
 * nothing is {@link #ready()}; when a later read fails, the last bindings
 * stay in force and their age, the gauge {@code bindings-age-ms} in the
 * MBean {@code kfkrbac:type=rest-bindings}, grows.
 *
 * <p>Nothing here depends on Kafka Connect: an extension for another REST
 * service asks the same questions. Public only for the extensions in this
 * jar; it is not an API to build on.
 */
public final class RestAccess implements AutoCloseable {

    /** Principals that pass whatever they ask, as the broker's {@code super.users}: {@code User:a;User:b}. */
    public static final String SUPER_USERS = "kfkrbac.super.users";

    /** How often the bindings are read again, in milliseconds. */
    public static final String REFRESH_INTERVAL_MS = "kfkrbac.refresh.interval.ms";

    static final long DEFAULT_REFRESH_INTERVAL_MS = 5000;

    /** The group of the metrics, and so the {@code type} of their MBean. */
    static final String METRIC_GROUP = "rest-bindings";

    /** The gauge of how old the bindings in force are. */
    static final String AGE_METRIC = "bindings-age-ms";

    /** The request attribute a servlet container puts a client's certificate chain in. */
    public static final String CERTIFICATE_ATTRIBUTE = "jakarta.servlet.request.X509Certificate";

    private static final Logger LOG = LoggerFactory.getLogger(RestAccess.class);

    private final AclRecords records;
    private final Set<KafkaPrincipal> superUsers;
    private final Identities identities;
    private final Time time;
    private final Metrics metrics;
    private final long started;
    private ScheduledExecutorService refresher;

    /** {@code null} until the first read succeeds. */
    private volatile BindingSnapshot bindings;
    private volatile long readAt = -1;

    RestAccess(AclRecords records, Set<KafkaPrincipal> superUsers, Identities identities, Time time,
            Metrics metrics) {
        this.records = Objects.requireNonNull(records, "records");
        this.superUsers = Set.copyOf(superUsers);
        this.identities = Objects.requireNonNull(identities, "identities");
        this.time = Objects.requireNonNull(time, "time");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.started = time.milliseconds();
        metrics.addMetric(metrics.metricName(AGE_METRIC, METRIC_GROUP,
                "How old the role bindings in force are, in milliseconds: since the last read that succeeded, "
                        + "or since the start while none has."),
                (Measurable) (config, now) -> ageMs());
        metrics.addMetric(metrics.metricName("bindings", METRIC_GROUP, "How many role bindings are in force."),
                (Measurable) (config, now) -> {
                    BindingSnapshot held = bindings;
                    return held == null ? 0 : held.bindings();
                });
    }

    /**
     * Reads the settings, connects to the cluster with the admin client and
     * starts reading the bindings in the background.
     *
     * @param settings the process's own settings: the admin client's
     *        connection settings among them, and the {@code kfkrbac.*} ones
     * @param name what the reader's thread and the admin client are called after
     * @return the bindings, not yet read
     * @throws ConfigException when a {@code kfkrbac.*} setting cannot be read
     */
    public static RestAccess start(Map<String, ?> settings, String name) {
        Set<KafkaPrincipal> superUsers = superUsers(settings.get(SUPER_USERS));
        long interval = interval(settings.get(REFRESH_INTERVAL_MS));
        Map<String, Object> client = new HashMap<>();
        Set<String> known = AdminClientConfig.configNames();
        settings.forEach((key, value) -> {
            if (known.contains(key) && value != null) {
                client.put(key, value);
            }
        });
        client.put(AdminClientConfig.CLIENT_ID_CONFIG, "kfkrbac-" + name);
        Metrics metrics = new Metrics(new MetricConfig(), List.of(new JmxReporter()), Time.SYSTEM,
                new KafkaMetricsContext("kfkrbac"));
        Admin admin;
        try {
            admin = Admin.create(client);
        } catch (RuntimeException e) {
            metrics.close();
            throw e;
        }
        RestAccess access = new RestAccess(new AdminAclRecords(admin), superUsers, Identities.SELF, Time.SYSTEM,
                metrics);
        access.refreshEvery(interval, name);
        return access;
    }

    /** Reads the bindings now, and then again after every interval, on a thread of its own. */
    void refreshEvery(long intervalMs, String name) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "kfkrbac-" + name + "-bindings");
            thread.setDaemon(true);
            return thread;
        });
        refresher = executor;
        executor.scheduleWithFixedDelay(this::refresh, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Reads the bindings once. When the read fails, what was read before
     * stays in force.
     *
     * @return whether the read succeeded
     */
    boolean refresh() {
        try {
            BindingSnapshot read = BindingSnapshot.of(records.describe(AclBindingFilter.ANY), identities);
            boolean first = bindings == null;
            bindings = read;
            readAt = time.milliseconds();
            if (first) {
                LOG.info("Read {} role bindings; requests are decided from now on", read.bindings());
            }
            if (read.malformed() > 0) {
                LOG.debug("{} ACL records have a role as their principal type and are not role bindings; "
                        + "they grant nothing", read.malformed());
            }
            return true;
        } catch (RuntimeException e) {
            LOG.warn("Could not read the role bindings; {}: {}",
                    bindings == null ? "every request but the health check is refused until they are read"
                            : "the ones read " + ageMs() + " ms ago stay in force",
                    e.toString());
            return false;
        }
    }

    /**
     * Who is calling, as a principal of type {@code User}: the user the
     * process authenticated with HTTP basic authentication or, when there
     * is none, the subject of the client's certificate, as the broker writes
     * it by default ({@code User:CN=alice,O=example}).
     *
     * @param authenticated the authenticated user's name, or {@code null}
     * @param certificates the client's certificate chain, or {@code null}
     * @return the caller, or {@code null} when the request is anonymous
     */
    public static KafkaPrincipal caller(String authenticated, Object certificates) {
        if (authenticated != null && !authenticated.isEmpty()) {
            return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, authenticated);
        }
        if (certificates instanceof X509Certificate[] chain && chain.length > 0 && chain[0] != null) {
            return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, chain[0].getSubjectX500Principal().getName());
        }
        return null;
    }

    /** Whether the bindings have been read at least once. */
    public boolean ready() {
        return bindings != null;
    }

    /** Whether the principal is one of {@code kfkrbac.super.users}. */
    public boolean superUser(KafkaPrincipal principal) {
        return superUsers.contains(principal);
    }

    /**
     * Whether a role bound to the principal grants the operation on the
     * connector. No, while the bindings have not been read.
     */
    public boolean grants(KafkaPrincipal principal, ConnectorOperation operation, String connector) {
        BindingSnapshot held = bindings;
        return held != null && held.grants(principal, operation, connector);
    }

    /**
     * Whether the principal runs the service: a super user, or one that
     * holds {@code ClusterAdmin} or {@code SystemAdmin}.
     */
    public boolean administers(KafkaPrincipal principal) {
        if (superUser(principal)) {
            return true;
        }
        BindingSnapshot held = bindings;
        return held != null
                && (held.holds(principal, Role.CLUSTER_ADMIN) || held.holds(principal, Role.SYSTEM_ADMIN));
    }

    /** An operation as the table of roles writes it: {@code ReadStatus}. */
    public static String name(ConnectorOperation operation) {
        return Wording.operation(operation);
    }

    /** How old the bindings in force are, in milliseconds; since the start while none has been read. */
    public long ageMs() {
        long read = readAt;
        return time.milliseconds() - (read < 0 ? started : read);
    }

    Metrics metrics() {
        return metrics;
    }

    @Override
    public void close() {
        ScheduledExecutorService executor = refresher;
        if (executor != null) {
            executor.shutdownNow();
        }
        records.close();
        metrics.close();
    }

    /**
     * Checks the {@code kfkrbac.*} settings without connecting to anything.
     *
     * @throws ConfigException when one cannot be read
     */
    public static void validate(Map<String, ?> settings) {
        superUsers(settings.get(SUPER_USERS));
        interval(settings.get(REFRESH_INTERVAL_MS));
    }

    static Set<KafkaPrincipal> superUsers(Object value) {
        if (value == null) {
            return Set.of();
        }
        Set<KafkaPrincipal> principals = new LinkedHashSet<>();
        for (String text : value.toString().split(";")) {
            String trimmed = text.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            KafkaPrincipal principal = Wording.principal(trimmed);
            if (principal == null) {
                throw new ConfigException(SUPER_USERS, value, "each principal is <type>:<name>, as in User:alice");
            }
            principals.add(principal);
        }
        return Collections.unmodifiableSet(principals);
    }

    static long interval(Object value) {
        if (value == null) {
            return DEFAULT_REFRESH_INTERVAL_MS;
        }
        long interval;
        try {
            interval = value instanceof Number number ? number.longValue() : Long.parseLong(value.toString().trim());
        } catch (NumberFormatException e) {
            throw new ConfigException(REFRESH_INTERVAL_MS, value, "a number of milliseconds");
        }
        if (interval <= 0) {
            throw new ConfigException(REFRESH_INTERVAL_MS, value, "more than 0 milliseconds");
        }
        return interval;
    }
}
