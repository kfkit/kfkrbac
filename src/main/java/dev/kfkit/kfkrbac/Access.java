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
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
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

/**
 * Who may do what, computed from the ACL records of a cluster the way its
 * brokers decide it, and why.
 *
 * <p>No rule of matching is written here. Whether a request is allowed is
 * asked of a {@link RbacAuthorizer} that was given every record, as a broker
 * gives them to its own. The reason behind an answer is found by asking
 * about one record at a time: a binding is asked through
 * {@link RoleBinding}, as {@link BindingIndex} asks it, for the members
 * {@link Identities} gives; an ordinary ACL is asked of a
 * {@link StandardAuthorizer} that holds that ACL alone; a deny ACL is asked
 * of one that holds it and an ACL allowing everyone, so that it denies
 * exactly when the deny matches.
 *
 * <p>The answer is the one for an ordinary request. The requests that
 * create, delete and list ACLs and the one that replaces a user's
 * credentials are decided by rules of their own, which are not asked here.
 * And it cannot know what the brokers' own configuration allows:
 * {@code super.users} and {@code allow.everyone.if.no.acl.found}.
 */
final class Access implements AutoCloseable {

    /**
     * One principal's answer to "who can".
     *
     * @param principal who
     * @param allowed what the authorizer answers
     * @param allowedBy the bindings and ACLs that allow it, each on its own
     * @param deniedBy the deny ACLs that take it away
     */
    record Answer(KafkaPrincipal principal, boolean allowed, List<AclBinding> allowedBy, List<AclBinding> deniedBy) {
    }

    /**
     * One line of "what can": what one record gives the principal, or takes away.
     *
     * @param on the resources, in words
     * @param operations the operations, in words
     * @param by the record behind the line
     */
    record Line(String on, String operations, AclBinding by) {
    }

    private record Bound(AclBinding record, RoleBinding binding) {
    }

    /**
     * A question as a broker would describe it to its authorizer, for a
     * request the authorizer treats like any other. It decides a few
     * requests by rules of their own: the ones that create, delete and list
     * ACLs, and the one that replaces a user's credentials. Those are not
     * what "who can do this operation" asks.
     */
    private record Asked(KafkaPrincipal principal, InetAddress clientAddress) implements AuthorizableRequestContext {

        @Override
        public String listenerName() {
            return "kfkrbac";
        }

        @Override
        public SecurityProtocol securityProtocol() {
            return SecurityProtocol.PLAINTEXT;
        }

        @Override
        public int requestType() {
            return ApiKeys.METADATA.id;
        }

        @Override
        public int requestVersion() {
            return ApiKeys.METADATA.latestVersion();
        }

        @Override
        public String clientId() {
            return "kfkrbac";
        }

        @Override
        public int correlationId() {
            return 0;
        }
    }

    /** {@code User:*}: the principal an ACL names to mean everyone. */
    private static final KafkaPrincipal EVERYONE = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*");

    private static final Comparator<KafkaPrincipal> BY_NAME = Comparator.comparing(KafkaPrincipal::toString);

    private final Identities identities;
    private final InetAddress host;
    private final List<Bound> bindings = new ArrayList<>();
    private final List<AclBinding> allows = new ArrayList<>();
    private final List<AclBinding> denies = new ArrayList<>();
    private final Set<String> hosts = new HashSet<>();

    /** Every principal a binding or an ACL names, {@code User:*} among them when an ACL does. */
    private final Set<KafkaPrincipal> named = new TreeSet<>(BY_NAME);

    /**
     * A user no record names, asked about in place of {@code User:*}: what
     * it may do is what everyone may. The authorizer cannot be asked about
     * a principal that is itself called {@code User:*}.
     */
    private final KafkaPrincipal anyone;

    private final Metrics authorizerMetrics = new Metrics();
    private final Metrics aloneMetrics = new Metrics();

    /** Holds every record: its answer is the answer. */
    private final RbacAuthorizer authorizer = new RbacAuthorizer();

    /** Holds one ACL at a time: says whether that ACL matches. */
    private final StandardAuthorizer alone = new StandardAuthorizer();

    /**
     * @param records every ACL record of the cluster
     * @param identities the members a binding may name for it to apply to a principal
     * @param host where the request is asked from, or {@code null} for an address no ACL names
     */
    Access(Collection<AclBinding> records, Identities identities, InetAddress host) {
        this.identities = Objects.requireNonNull(identities, "identities");
        List<AclBinding> ordered = new ArrayList<>(records);
        ordered.sort(BindingRecords.ORDER);
        for (AclBinding record : ordered) {
            BindingCodec.Decoded decoded = BindingCodec.decode(record);
            if (decoded instanceof RoleBinding binding) {
                bindings.add(new Bound(record, binding));
                named.add(binding.member());
            } else if (decoded instanceof BindingCodec.Ordinary) {
                (record.entry().permissionType() == AclPermissionType.DENY ? denies : allows).add(record);
                if (!"*".equals(record.entry().host())) {
                    hosts.add(record.entry().host());
                }
                KafkaPrincipal principal = Wording.principal(record.entry().principal());
                if (principal != null) {
                    named.add(principal);
                }
            }
        }
        this.host = host == null ? anAddressNoAclNames(hosts) : host;
        KafkaPrincipal unnamed = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "anyone");
        while (named.contains(unnamed)) {
            unnamed = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, unnamed.getName() + "-");
        }
        this.anyone = unnamed;
        start(authorizer, authorizerMetrics);
        start(alone, aloneMetrics);
        load(authorizer, ordered);
    }

    /** The address the questions are asked from. */
    InetAddress host() {
        return host;
    }

    /** Whether some ACL applies to one host only: the answers then depend on {@link #host()}. */
    boolean dependsOnHost() {
        return !hosts.isEmpty();
    }

    /**
     * Who may do the operation on the resource: every principal a record
     * names that the authorizer allows, and every one it would allow were it
     * not for a deny ACL. {@code User:*} is everyone, as in an ACL: its
     * answer is the one of a user no record names, and a principal with
     * that answer, for the same reasons, is left out.
     *
     * @param resource a resource by its literal name, as a broker asks about it
     */
    List<Answer> whoCan(AclOperation operation, ResourcePattern resource) {
        Action action = action(operation, resource);
        Map<KafkaPrincipal, List<AclBinding>> allowedBy = new HashMap<>();
        Map<KafkaPrincipal, List<AclBinding>> deniedBy = new HashMap<>();
        for (Bound bound : bindings) {
            if (bound.binding().grants(operation, resource.resourceType(), resource.name())) {
                for (KafkaPrincipal principal : named) {
                    if (holds(asked(principal), bound.binding())) {
                        allowedBy.computeIfAbsent(principal, allowed -> new ArrayList<>()).add(bound.record());
                    }
                }
            }
        }
        for (AclBinding acl : allows) {
            load(alone, List.of(acl));
            for (KafkaPrincipal principal : named) {
                if (allowed(alone, asked(principal), action)) {
                    allowedBy.computeIfAbsent(principal, allowed -> new ArrayList<>()).add(acl);
                }
            }
        }
        for (AclBinding acl : denies) {
            load(alone, List.of(acl, everyoneMay(resource.resourceType())));
            for (KafkaPrincipal principal : named) {
                if (!allowed(alone, asked(principal), action)) {
                    deniedBy.computeIfAbsent(principal, denied -> new ArrayList<>()).add(acl);
                }
            }
        }

        List<Answer> answers = new ArrayList<>();
        Answer everyone = null;
        for (KafkaPrincipal principal : named) {
            boolean allowed = allowed(authorizer, asked(principal), action);
            List<AclBinding> reasons = allowedBy.getOrDefault(principal, List.of());
            if (!allowed && reasons.isEmpty()) {
                continue;
            }
            Answer answer = new Answer(principal, allowed, reasons, deniedBy.getOrDefault(principal, List.of()));
            if (EVERYONE.equals(principal)) {
                everyone = answer;
            }
            answers.add(answer);
        }
        if (everyone != null) {
            Answer theirs = everyone;
            answers.removeIf(answer -> answer != theirs && answer.allowed() == theirs.allowed()
                    && answer.allowedBy().equals(theirs.allowedBy()) && answer.deniedBy().equals(theirs.deniedBy()));
        }
        return answers;
    }

    /**
     * What the bindings and the allow ACLs give the principal, one line for
     * each record and kind of resource. A deny ACL may take some of it away:
     * see {@link #denied}. For {@code User:*} it is what everyone is given.
     *
     * @param whom the principal
     */
    List<Line> granted(KafkaPrincipal whom) {
        KafkaPrincipal principal = asked(whom);
        List<Line> lines = new ArrayList<>();
        for (Bound bound : bindings) {
            RoleBinding binding = bound.binding();
            if (!holds(principal, binding)) {
                continue;
            }
            for (ResourceType type : ResourceType.values()) {
                List<AclOperation> operations = new ArrayList<>();
                ResourcePattern pattern = null;
                for (AclOperation operation : Role.everyAclOperation()) {
                    ResourcePattern granted = binding.grantedPattern(operation, type);
                    if (granted != null) {
                        operations.add(operation);
                        pattern = granted;
                    }
                }
                if (pattern != null) {
                    lines.add(new Line(Wording.resources(pattern),
                            Wording.operations(operations, Role.everyAclOperation()), bound.record()));
                }
            }
            elsewhere(binding, bound.record(), lines);
        }
        for (AclBinding acl : allows) {
            load(alone, List.of(acl));
            List<AclOperation> operations = matching(principal, acl.pattern(), true);
            if (!operations.isEmpty()) {
                lines.add(new Line(Wording.resources(acl.pattern()),
                        Wording.operations(operations, Role.everyAclOperation()), acl));
            }
        }
        return lines;
    }

    /**
     * What the deny ACLs take away from the principal, whatever gives it: a
     * deny beats every role and every allow.
     *
     * @param whom the principal
     */
    List<Line> denied(KafkaPrincipal whom) {
        KafkaPrincipal principal = asked(whom);
        List<Line> lines = new ArrayList<>();
        for (AclBinding acl : denies) {
            load(alone, List.of(acl, everyoneMay(acl.pattern().resourceType())));
            List<AclOperation> operations = matching(principal, acl.pattern(), false);
            if (!operations.isEmpty()) {
                lines.add(new Line(Wording.resources(acl.pattern()),
                        Wording.operations(operations, Role.everyAclOperation()), acl));
            }
        }
        return lines;
    }

    @Override
    public void close() {
        try {
            try {
                authorizer.close();
            } finally {
                alone.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            authorizerMetrics.close();
            aloneMetrics.close();
        }
    }

    /**
     * What a binding gives on subjects and connectors, which no broker
     * decides: the operations of its role, as {@link Role} has them, on the
     * names the binding is for, or on every name when it is on the cluster.
     */
    private static void elsewhere(RoleBinding binding, AclBinding record, List<Line> lines) {
        RoleBinding.Target target = binding.target();
        Role role = binding.role();
        if ((target == RoleBinding.Target.SUBJECTS || target == RoleBinding.Target.CLUSTER)
                && !role.subjectOperations().isEmpty()) {
            lines.add(new Line(target == RoleBinding.Target.CLUSTER ? "every subject" : Wording.on(binding),
                    Wording.operations(role.subjectOperations(), List.of(SubjectOperation.values())), record));
        }
        if ((target == RoleBinding.Target.CONNECTORS || target == RoleBinding.Target.CLUSTER)
                && !role.connectorOperations().isEmpty()) {
            lines.add(new Line(target == RoleBinding.Target.CLUSTER ? "every connector" : Wording.on(binding),
                    Wording.operations(role.connectorOperations(), List.of(ConnectorOperation.values())), record));
        }
    }

    /** The principal to ask about: itself, or for {@code User:*} a user no record names. */
    private KafkaPrincipal asked(KafkaPrincipal principal) {
        return EVERYONE.equals(principal) ? anyone : principal;
    }

    /** Whether the binding applies to the principal: it names one of the principal's identities. */
    private boolean holds(KafkaPrincipal principal, RoleBinding binding) {
        return identities.of(principal).contains(binding.member());
    }

    /**
     * The operations for which the one ACL that {@link #alone} holds matches
     * the principal, asked on the very name of the ACL's own pattern: a
     * name every pattern covers that has it.
     *
     * @param allowed the answer that means "it matches": allowed for an allow ACL, denied for a deny ACL
     */
    private List<AclOperation> matching(KafkaPrincipal principal, ResourcePattern pattern, boolean allowed) {
        ResourcePattern resource = new ResourcePattern(pattern.resourceType(), pattern.name(), PatternType.LITERAL);
        List<AclOperation> operations = new ArrayList<>();
        for (AclOperation operation : Role.everyAclOperation()) {
            if (allowed(alone, principal, action(operation, resource)) == allowed) {
                operations.add(operation);
            }
        }
        return operations;
    }

    private boolean allowed(StandardAuthorizer asked, KafkaPrincipal principal, Action action) {
        return asked.authorize(new Asked(principal, host), List.of(action)).get(0) == AuthorizationResult.ALLOWED;
    }

    private static Action action(AclOperation operation, ResourcePattern resource) {
        return new Action(operation, resource, 1, false, false);
    }

    /**
     * An ACL that allows everyone everything on resources of the type:
     * beside it, a deny ACL shows where it matches.
     */
    private static AclBinding everyoneMay(ResourceType type) {
        return new AclBinding(new ResourcePattern(type, ResourcePattern.WILDCARD_RESOURCE, PatternType.LITERAL),
                new AccessControlEntry(EVERYONE.toString(), "*", AclOperation.ALL, AclPermissionType.ALLOW));
    }

    private static void start(StandardAuthorizer authorizer, Metrics metrics) {
        authorizer.configure(Map.of());
        authorizer.withPluginMetrics(new PluginMetricsImpl(metrics, Map.of()));
        authorizer.loadSnapshot(Map.of());
        authorizer.completeInitialLoad();
    }

    private static void load(StandardAuthorizer authorizer, Collection<AclBinding> records) {
        Map<Uuid, StandardAcl> snapshot = new HashMap<>();
        for (AclBinding record : records) {
            snapshot.put(Uuid.randomUuid(), StandardAcl.fromAclBinding(record));
        }
        authorizer.loadSnapshot(snapshot);
    }

    /** An address of the range set aside for documentation that no ACL has as its host. */
    private static InetAddress anAddressNoAclNames(Set<String> hosts) {
        try {
            InetAddress address = null;
            for (int last = 1; last < 255; last++) {
                address = InetAddress.getByAddress(new byte[] {(byte) 192, 0, 2, (byte) last});
                if (!hosts.contains(address.getHostAddress())) {
                    break;
                }
            }
            return address;
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }
}
