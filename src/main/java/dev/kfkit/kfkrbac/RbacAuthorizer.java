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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
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

    private final Roles roles;

    /** The deny ACLs of the parent and {@link #ALLOW_EVERYTHING}: denies exactly when a deny ACL matches. */
    private final StandardAuthorizer denies = new StandardAuthorizer();

    /** The deny ACLs {@link #denies} holds, by ID. Written by the metadata thread only. */
    private final Map<Uuid, StandardAcl> denyAcls = new ConcurrentHashMap<>();

    /** The private authorizer's own metrics; never reported. */
    private final Metrics denyMetrics = new Metrics();

    public RbacAuthorizer() {
        this(Roles.NONE);
    }

    RbacAuthorizer(Roles roles) {
        this.roles = Objects.requireNonNull(roles, "roles");
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
    public void completeInitialLoad() {
        denies.completeInitialLoad();
        super.completeInitialLoad();
    }

    // The order of the updates below keeps a deny ACL in the private authorizer
    // for as long as the parent has it: between the two updates a request may see
    // a deny that is not there yet or no longer there, never miss one that is.

    @Override
    public void addAcl(Uuid id, StandardAcl acl) {
        if (acl.permissionType() == AclPermissionType.DENY) {
            denies.addAcl(id, acl);
            denyAcls.put(id, acl);
        }
        super.addAcl(id, acl);
    }

    @Override
    public void removeAcl(Uuid id) {
        super.removeAcl(id);
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
            for (ResourcePattern pattern : roles.grantedPatterns(principal(requestContext), op, resourceType)) {
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

    @Override
    public void close() throws IOException {
        try {
            denies.close();
            denyMetrics.close();
        } finally {
            super.close();
        }
    }

    /** Whether a role grants an action the parent denied, and no deny ACL matches it. */
    private boolean roleAllows(AuthorizableRequestContext requestContext, Action action) {
        try {
            return roles.grants(principal(requestContext), action)
                    && !denied(requestContext, action.operation(), action.resourcePattern(),
                            action.resourceReferenceCount());
        } catch (RuntimeException e) {
            LOG.error("Role lookup failed for {} {} on {}; denied", requestContext.principal(),
                    action.operation(), action.resourcePattern(), e);
            return false;
        }
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
