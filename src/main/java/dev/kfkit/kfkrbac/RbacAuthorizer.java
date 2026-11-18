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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The broker's {@link StandardAuthorizer} with roles.
 *
 * <p>A role names a set of permissions once; a role binding hands it to a
 * user or a group. Roles only ever add access: what an ACL allows stays
 * allowed, what an ACL denies stays denied, and a request no ACL allows is
 * allowed when a role bound to its principal covers it.
 *
 * <p>A role binding is an ACL record whose principal is
 * {@code <Role>:<member>}. The broker delivers it like any ACL; the parent
 * stores it and never matches it, and {@link BindingIndex} keeps it as a
 * binding.
 *
 * <p>Extending rather than wrapping is deliberate. The broker feeds ACLs
 * only to authorizers that are a {@code ClusterMetadataAuthorizer}; a plain
 * wrapper would silently never receive them.
 *
 * <p>The parent answers {@code DENIED} both when a deny ACL matches and when
 * no ACL allows. To tell the two apart, the deny ACLs, and only those, are
 * also fed to a private second {@link StandardAuthorizer} that holds, besides
 * them, one ACL allowing everyone everything on every resource type. A deny
 * beats an allow, so it answers {@code DENIED} exactly when a deny ACL
 * matches, by Kafka's own matching rules. It is asked only after the parent
 * has denied and a role has granted, so a request an ACL allows costs what it
 * costs without roles.
 *
 * <p>Bindings are created and deleted through the ACL API, and
 * {@link #createAcls} and {@link #deleteAcls}, which the controller calls
 * with the caller's own principal, decide who may: the rules are
 * {@link Delegation}'s. A principal that holds {@code ResourceOwner} is let
 * through the broker's check before those calls, and before a listing of
 * the ACLs, and through nothing else that check guards. The same check
 * guards the request that replaces a user's password, and there no role but
 * {@code SystemAdmin} passes it.
 *
 * <p>Configure with {@code authorizer.class.name=dev.kfkit.kfkrbac.RbacAuthorizer}.
 */
public class RbacAuthorizer extends StandardAuthorizer {

    private static final Logger LOG = LoggerFactory.getLogger(RbacAuthorizer.class);

    /**
     * Appended to a prefix to name a resource that only a deny on the whole
     * prefix (or wider) can match: no deny ACL is ever written for a name
     * that continues with NUL.
     */
    private static final String PAST_THE_PREFIX = "\u0000";

    /**
     * Set while the parent answers {@code authorizeByResourceType}: it calls
     * {@link #authorize} itself, and that answer must be the ACLs' alone.
     */
    private static final ThreadLocal<Boolean> ACLS_ONLY = new ThreadLocal<>();

    /**
     * What makes the private authorizer allow whatever no deny matches. Its
     * {@code allow.everyone.if.no.acl.found} would not do: that applies only
     * to a resource no ACL at all names, and a deny for another principal,
     * host or operation names it.
     */
    private static final Map<Uuid, StandardAcl> ALLOW_EVERYTHING = allowEverything();

    /** The name of the gauge that counts the records which look like bindings and are not. */
    static final String MALFORMED_BINDINGS_METRIC = "malformed-bindings";

    /** What decides for the roles: {@link #bindings} on a broker. */
    private final Roles roles;

    /** The role bindings among the ACL records the broker delivers. */
    private final BindingIndex bindings;

    /** The deny ACLs of the parent and {@link #ALLOW_EVERYTHING}: denies exactly when a deny ACL matches. */
    private final StandardAuthorizer denies = new StandardAuthorizer();

    /** The deny ACLs {@link #denies} holds, by ID. Written by the metadata thread only. */
    private final Map<Uuid, StandardAcl> denyAcls = new ConcurrentHashMap<>();

    /** The private authorizer's own metrics; never reported. */
    private final Metrics denyMetrics = new Metrics();

    public RbacAuthorizer() {
        this(new BindingIndex());
    }

    private RbacAuthorizer(BindingIndex bindings) {
        this(bindings, bindings);
    }

    /** For tests: the roles decide as given, whatever the ACL records bind. */
    RbacAuthorizer(Roles roles) {
        this(roles, new BindingIndex());
    }

    private RbacAuthorizer(Roles roles, BindingIndex bindings) {
        this.roles = Objects.requireNonNull(roles, "roles");
        this.bindings = bindings;
        denies.withPluginMetrics(new PluginMetricsImpl(denyMetrics, Map.of()));
        denies.loadSnapshot(ALLOW_EVERYTHING);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        Map<String, Object> denyConfigs = new HashMap<>(configs);
        denyConfigs.remove(SUPER_USERS_CONFIG);
        denyConfigs.put(ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "false");
        denies.configure(denyConfigs);
    }

    @Override
    public void withPluginMetrics(PluginMetrics metrics) {
        super.withPluginMetrics(metrics);
        Gauge<Integer> malformed = (config, now) -> bindings.malformed();
        metrics.addMetric(metrics.metricName(MALFORMED_BINDINGS_METRIC,
                "ACL records with a role as their principal type that are not role bindings and grant nothing.",
                new LinkedHashMap<>()), malformed);
    }

    @Override
    public void completeInitialLoad() {
        denies.completeInitialLoad();
        super.completeInitialLoad();
    }

    // The order of the updates below keeps a deny ACL in the private authorizer
    // for as long as the parent has it: between the two updates a request may see
    // a deny that is not there yet or no longer there, never miss one that is.
    // The bindings follow the parent: a record it refuses is not read as a binding.

    @Override
    public void addAcl(Uuid id, StandardAcl acl) {
        if (acl.permissionType() == AclPermissionType.DENY) {
            denies.addAcl(id, acl);
            denyAcls.put(id, acl);
        }
        super.addAcl(id, acl);
        bindings.add(id, acl);
    }

    @Override
    public void removeAcl(Uuid id) {
        super.removeAcl(id);
        bindings.remove(id);
        if (denyAcls.remove(id) != null) {
            denies.removeAcl(id);
        }
    }

    @Override
    public void loadSnapshot(Map<Uuid, StandardAcl> acls) {
        Map<Uuid, StandardAcl> newDenies = new HashMap<>();
        acls.forEach((id, acl) -> {
            if (acl.permissionType() == AclPermissionType.DENY) {
                newDenies.put(id, acl);
            }
        });
        // Old and new denies together while the parent switches, then the new ones.
        Map<Uuid, StandardAcl> both = new HashMap<>(newDenies);
        Set<StandardAcl> present = new HashSet<>(newDenies.values());
        denyAcls.forEach((id, acl) -> {
            if (!both.containsKey(id) && present.add(acl)) {
                both.put(id, acl);
            }
        });
        denies.loadSnapshot(withAllowEverything(both));
        super.loadSnapshot(acls);
        bindings.load(acls);
        denies.loadSnapshot(withAllowEverything(newDenies));
        denyAcls.clear();
        denyAcls.putAll(newDenies);
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        List<AuthorizationResult> results = super.authorize(requestContext, actions);
        List<AuthorizationResult> granted = null;
        for (int i = 0; i < results.size(); i++) {
            if (results.get(i) == AuthorizationResult.DENIED && ACLS_ONLY.get() == null
                    && roleAllows(requestContext, actions.get(i))) {
                if (granted == null) {
                    granted = new ArrayList<>(results);
                }
                granted.set(i, AuthorizationResult.ALLOWED);
            }
        }
        return granted == null ? results : granted;
    }

    /**
     * The parent's answer, or {@code ALLOWED} when a role grants the
     * operation on some resource of the type that no deny ACL of the
     * principal covers entirely.
     *
     * <p>The parent's answer comes from the ACLs alone, its own call to
     * {@code authorize} included; the roles are asked only when it is
     * {@code DENIED}.
     */
    @Override
    public AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation op,
            ResourceType resourceType) {
        AuthorizationResult parent;
        ACLS_ONLY.set(Boolean.TRUE);
        try {
            parent = super.authorizeByResourceType(requestContext, op, resourceType);
        } finally {
            ACLS_ONLY.remove();
        }
        if (parent == AuthorizationResult.ALLOWED) {
            return parent;
        }
        try {
            KafkaPrincipal principal = principal(requestContext);
            if (keptForSystemAdmin(requestContext, op, resourceType)
                    && !bindings.holds(principal, Role.SYSTEM_ADMIN)) {
                return AuthorizationResult.DENIED;
            }
            for (ResourcePattern pattern : roles.grantedPatterns(principal, op, resourceType)) {
                if (!coveredByDeny(requestContext, op, pattern)) {
                    return AuthorizationResult.ALLOWED;
                }
            }
        } catch (RuntimeException e) {
            LOG.error("Role lookup failed for {} {} on any {}; denied", requestContext.principal(), op,
                    resourceType, e);
        }
        return AuthorizationResult.DENIED;
    }

    /**
     * Creates the records when the caller may create every one of them, and
     * none otherwise: each result then says why.
     *
     * <p>Called on the controller, after it has asked {@link #authorize} for
     * {@code ALTER} on the cluster, with the principal of whoever sent the
     * request.
     */
    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext,
            List<AclBinding> aclBindings) {
        Delegation.Decision decision;
        try {
            decision = Delegation.create(caller(requestContext), aclBindings);
        } catch (RuntimeException e) {
            LOG.error("Could not decide whether {} may create {}; refused", requestContext.principal(), aclBindings,
                    e);
            AclCreateResult undecided = new AclCreateResult(undecided(requestContext, "created"));
            return completed(aclBindings.size(), i -> undecided);
        }
        if (decision.allowed()) {
            return super.createAcls(requestContext, aclBindings);
        }
        LOG.info("Refused to create {} ACL record(s): {}", aclBindings.size(), reason(decision));
        return completed(aclBindings.size(), i -> new AclCreateResult(refusal(decision, i)));
    }

    /**
     * Deletes what the filters match when the caller may delete every such
     * record, and nothing otherwise: each result then says why.
     *
     * <p>A principal that the ACLs allow {@code ALTER} on the cluster, and a
     * {@code SystemAdmin}, delete what their filters match when the
     * controller applies them. Anyone else has the filters
     * resolved here, against the records this node has, and deletes exactly
     * the records that were checked: a record that appears between the check
     * and the delete is not deleted with them.
     */
    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext,
            List<AclBindingFilter> aclBindingFilters) {
        List<List<AclBinding>> matched; // null for a caller that may delete any record
        Delegation.Decision decision;
        try {
            Delegation.Caller caller = caller(requestContext);
            matched = caller.managesEverything() ? null : matched(aclBindingFilters);
            decision = matched == null ? null : Delegation.delete(caller, matched);
        } catch (RuntimeException e) {
            LOG.error("Could not decide whether {} may delete what {} match; refused", requestContext.principal(),
                    aclBindingFilters, e);
            AclDeleteResult undecided = new AclDeleteResult(undecided(requestContext, "deleted"));
            return completed(aclBindingFilters.size(), i -> undecided);
        }
        if (decision == null) {
            return super.deleteAcls(requestContext, aclBindingFilters);
        }
        if (!decision.allowed()) {
            LOG.info("Refused to delete what {} filter(s) match: {}", aclBindingFilters.size(), reason(decision));
            return completed(aclBindingFilters.size(), i -> new AclDeleteResult(refusal(decision, i)));
        }
        return deleteExactly(requestContext, matched);
    }

    /** For each filter, the records this node has that it matches. */
    private List<List<AclBinding>> matched(List<AclBindingFilter> filters) {
        List<List<AclBinding>> matched = new ArrayList<>(filters.size());
        for (AclBindingFilter filter : filters) {
            List<AclBinding> records = new ArrayList<>();
            acls(filter).forEach(records::add);
            matched.add(records);
        }
        return matched;
    }

    /**
     * Deletes these records and no other, each once, and answers for each of
     * the caller's filters with what became of the records it matched.
     */
    private List<? extends CompletionStage<AclDeleteResult>> deleteExactly(AuthorizableRequestContext requestContext,
            List<List<AclBinding>> matched) {
        Map<AclBinding, Integer> positions = new LinkedHashMap<>();
        for (List<AclBinding> records : matched) {
            for (AclBinding record : records) {
                positions.putIfAbsent(record, positions.size());
            }
        }
        if (positions.isEmpty()) {
            return completed(matched.size(), i -> new AclDeleteResult(List.of()));
        }
        List<AclBindingFilter> exactly = new ArrayList<>(positions.size());
        for (AclBinding record : positions.keySet()) {
            exactly.add(record.toFilter());
        }
        List<? extends CompletionStage<AclDeleteResult>> deleted = super.deleteAcls(requestContext, exactly);
        List<CompletionStage<AclDeleteResult>> results = new ArrayList<>(matched.size());
        for (List<AclBinding> records : matched) {
            CompletionStage<List<AclBindingDeleteResult>> ofTheFilter =
                    CompletableFuture.completedFuture(new ArrayList<>());
            for (AclBinding record : records) {
                ofTheFilter = ofTheFilter.thenCombine(deleted.get(positions.get(record)), (all, one) -> {
                    if (one.exception().isPresent()) {
                        all.add(new AclBindingDeleteResult(record, one.exception().get()));
                    } else {
                        all.addAll(one.aclBindingDeleteResults());
                    }
                    return all;
                });
            }
            results.add(ofTheFilter.thenApply(AclDeleteResult::new));
        }
        return results;
    }

    @Override
    public void close() throws IOException {
        try {
            denies.close();
            denyMetrics.close();
        } finally {
            super.close();
        }
    }

    /**
     * Whether a role grants an action the parent denied, or lets its holder
     * through to the ACL API, and no deny ACL matches it.
     */
    private boolean roleAllows(AuthorizableRequestContext requestContext, Action action) {
        try {
            KafkaPrincipal principal = principal(requestContext);
            boolean granted = roles.grants(principal, action);
            if (granted) {
                granted = !keptForSystemAdmin(requestContext, action.operation(),
                        action.resourcePattern().resourceType()) || bindings.holds(principal, Role.SYSTEM_ADMIN);
            } else {
                granted = opensTheAclApi(requestContext, action) && bindings.holds(principal, Role.RESOURCE_OWNER);
            }
            return granted
                    && !denied(requestContext, action.operation(), action.resourcePattern(),
                            action.resourceReferenceCount());
        } catch (RuntimeException e) {
            LOG.error("Role lookup failed for {} {} on {}; denied", requestContext.principal(),
                    action.operation(), action.resourcePattern(), e);
            return false;
        }
    }

    /**
     * Whether the action is the broker's check before it lets a request to
     * the ACL API through: {@code ALTER} on the cluster to create or delete
     * ACLs, {@code DESCRIBE} on the cluster to list them. A resource owner
     * passes it, so that it can reach {@link #createAcls} and
     * {@link #deleteAcls}, where what it may do is decided, and list what it
     * has bound. The same question with any other request type is not this
     * check: the role opens the ACL API, not the cluster.
     */
    private static boolean opensTheAclApi(AuthorizableRequestContext requestContext, Action action) {
        ResourcePattern resource = action.resourcePattern();
        if (resource.resourceType() != ResourceType.CLUSTER || resource.patternType() != PatternType.LITERAL
                || !Resource.CLUSTER_NAME.equals(resource.name())) {
            return false;
        }
        int request = requestContext.requestType();
        if (action.operation() == AclOperation.ALTER) {
            return request == ApiKeys.CREATE_ACLS.id || request == ApiKeys.DELETE_ACLS.id;
        }
        return action.operation() == AclOperation.DESCRIBE && request == ApiKeys.DESCRIBE_ACLS.id;
    }

    /**
     * Whether the question is one that, of all the roles, only
     * {@code SystemAdmin} answers: {@code ALTER} on the cluster for a request
     * that changes who can authenticate, so that a role is not a way to
     * become a super user. {@code AlterUserScramCredentials} replaces the
     * password of any user, a super user's included, and the broker guards
     * it with the same {@code ALTER} on the cluster that lets an operator
     * move partitions and elect leaders. What an ACL allows is not touched.
     *
     * <p>The requests that create and delete ACLs are guarded with it too,
     * and are not here: {@link #createAcls} and {@link #deleteAcls} decide
     * them, with a reason for the caller.
     */
    private static boolean keptForSystemAdmin(AuthorizableRequestContext requestContext, AclOperation operation,
            ResourceType resourceType) {
        return operation == AclOperation.ALTER && resourceType == ResourceType.CLUSTER
                && requestContext.requestType() == ApiKeys.ALTER_USER_SCRAM_CREDENTIALS.id;
    }

    /** Who asks, with everything the rules of {@link Delegation} need to know about it. */
    private Delegation.Caller caller(AuthorizableRequestContext requestContext) {
        KafkaPrincipal principal = principal(requestContext);
        return new Delegation.Caller(principal, aclsAllowAlteringTheCluster(requestContext),
                bindings.heldBy(principal));
    }

    /**
     * Whether the ACLs alone, the roles left out, allow the principal
     * {@code ALTER} on the cluster: the parent's own answer, which is
     * {@code ALLOWED} for a super user and for whoever an ACL allows and none
     * denies. Such a principal created and deleted any ACL before there were
     * roles, and still does.
     */
    private boolean aclsAllowAlteringTheCluster(AuthorizableRequestContext requestContext) {
        Action quiet = new Action(AclOperation.ALTER,
                new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL), 1, false,
                false);
        return super.authorize(requestContext, List.of(quiet)).get(0) == AuthorizationResult.ALLOWED;
    }

    /** The error a caller gets for the record, or the filter, at that position of a refused request. */
    private static ApiException refusal(Delegation.Decision decision, int position) {
        Delegation.Answer answer = decision.answers().get(position);
        Delegation.Outcome outcome = answer.outcome() == Delegation.Outcome.WITH_THE_REQUEST
                ? decision.refusal().map(Delegation.Answer::outcome).orElse(Delegation.Outcome.FORBIDDEN)
                : answer.outcome();
        return outcome == Delegation.Outcome.MALFORMED
                ? new InvalidRequestException(answer.reason())
                : new ClusterAuthorizationException(answer.reason());
    }

    private static String reason(Delegation.Decision decision) {
        return decision.refusal().map(Delegation.Answer::reason).orElse("refused");
    }

    /** Never granted on failure: what could not be decided is refused, and says so. */
    private static ApiException undecided(AuthorizableRequestContext requestContext, String done) {
        return new ClusterAuthorizationException("Nothing was " + done + ": whether " + requestContext.principal()
                + " may do this could not be decided, because of an error that is in the controller's log");
    }

    private static <T> List<CompletableFuture<T>> completed(int size, IntFunction<T> result) {
        List<CompletableFuture<T>> futures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            futures.add(CompletableFuture.completedFuture(result.apply(i)));
        }
        return futures;
    }

    /**
     * Whether a deny ACL matches every resource the pattern names. A literal
     * name is asked as it is; a prefix, or the wildcard as the empty prefix,
     * is asked as a name that only a deny of that prefix or wider matches.
     */
    private boolean coveredByDeny(AuthorizableRequestContext requestContext, AclOperation op,
            ResourcePattern pattern) {
        String name;
        if (pattern.patternType() == PatternType.LITERAL) {
            name = ResourcePattern.WILDCARD_RESOURCE.equals(pattern.name())
                    ? PAST_THE_PREFIX
                    : pattern.name();
        } else if (pattern.patternType() == PatternType.PREFIXED) {
            name = pattern.name() + PAST_THE_PREFIX;
        } else {
            return true; // not a pattern a binding can have: grants nothing
        }
        return denied(requestContext, op, new ResourcePattern(pattern.resourceType(), name, PatternType.LITERAL), 1);
    }

    private boolean denied(AuthorizableRequestContext requestContext, AclOperation op, ResourcePattern resource,
            int resourceReferenceCount) {
        Action quiet = new Action(op, resource, resourceReferenceCount, false, false);
        return denies.authorize(requestContext, List.of(quiet)).get(0) == AuthorizationResult.DENIED;
    }

    private static Map<Uuid, StandardAcl> allowEverything() {
        Map<Uuid, StandardAcl> acls = new HashMap<>();
        for (ResourceType type : ResourceType.values()) {
            if (type != ResourceType.ANY && type != ResourceType.UNKNOWN) {
                acls.put(Uuid.randomUuid(), new StandardAcl(type, ResourcePattern.WILDCARD_RESOURCE,
                        PatternType.LITERAL, "User:*", "*", AclOperation.ALL, AclPermissionType.ALLOW));
            }
        }
        return Map.copyOf(acls);
    }

    private static Map<Uuid, StandardAcl> withAllowEverything(Map<Uuid, StandardAcl> denyAcls) {
        Map<Uuid, StandardAcl> acls = new HashMap<>(denyAcls);
        acls.putAll(ALLOW_EVERYTHING);
        return acls;
    }

    /** The principal as the ACLs see it: a plain {@link KafkaPrincipal}, whatever the builder made. */
    private static KafkaPrincipal principal(AuthorizableRequestContext requestContext) {
        KafkaPrincipal principal = requestContext.principal();
        return principal.getClass() == KafkaPrincipal.class
                ? principal
                : new KafkaPrincipal(principal.getPrincipalType(), principal.getName());
    }
}
