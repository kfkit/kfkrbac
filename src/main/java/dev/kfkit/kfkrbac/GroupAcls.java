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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

/**
 * The ordinary ACLs whose principal is a group, honoured for the group's
 * members.
 *
 * <p>The broker's authorizer stores an ACL of {@code Group:payments} and
 * never matches it, because no client is that principal. The ACLs of groups
 * are therefore also kept here, by group, and the request's question is
 * asked once for each group of the principal that has one, as that group:
 * the resource, the operation and the host are matched by the rules the
 * broker's authorizer matches an ACL with ({@link AclSet}), and nothing is
 * written to its log. No super user is known here, and nothing is allowed
 * for want of an ACL.
 *
 * <p>It only ever adds access, and only to what the broker's authorizer
 * denied: {@link RbacAuthorizer} asks it after the roles, and checks the
 * deny ACLs after it. A principal in no group, and one whose groups have no
 * ACL, costs one lookup that finds nothing.
 *
 * <p>A {@code DENY} ACL of a group cannot be created through this
 * authorizer. One that is in the metadata anyway is applied where this
 * authorizer decides, which is the reading that grants less:
 * {@link #denied} says whether one matches for a group of the principal, and
 * then neither a role nor a group allows the request. It cannot take away
 * what an ACL allows the member by name: that request never comes here.
 *
 * <p>Fed by {@link BindingIndex} from the broker's metadata thread; asked on
 * the request path, without a lock.
 */
final class GroupAcls implements Closeable {

    /** Appended to a prefix to name a resource that only an ACL on that prefix, or a wider one, matches. */
    private static final String PAST_THE_PREFIX = "\u0000";

    private static final String GROUP_PRINCIPAL = BindingCodec.GROUP_TYPE + ":";

    private final Identities identities;

    /** The ACLs of groups by the ID of their record, {@code DENY} ones included. Guarded by {@code this}. */
    private final Map<Uuid, StandardAcl> byId = new HashMap<>();

    /** The {@code ALLOW} ACLs of each group that has one. The arrays are never written to once they are here. */
    private volatile ConcurrentHashMap<KafkaPrincipal, StandardAcl[]> allowsOf = new ConcurrentHashMap<>();

    /** The {@code DENY} ACLs of each group that has one. The arrays are never written to once they are here. */
    private volatile ConcurrentHashMap<KafkaPrincipal, StandardAcl[]> deniesOf = new ConcurrentHashMap<>();

    /**
     * @param identities which groups a principal is in
     */
    GroupAcls(Identities identities) {
        this.identities = Objects.requireNonNull(identities, "identities");
    }

    // --- the metadata thread ---------------------------------------------------

    /** Whether the record is an ACL whose principal is a group: the only records kept here. */
    static boolean ofAGroup(StandardAcl acl) {
        return acl.principal() != null && acl.principal().startsWith(GROUP_PRINCIPAL)
                && acl.principal().length() > GROUP_PRINCIPAL.length();
    }

    /** An ACL of a group was created. */
    synchronized void add(Uuid id, StandardAcl acl) {
        remove(id); // an ID is not added twice; if it were, the first ACL must not outlive its record
        byId.put(id, acl);
        KafkaPrincipal group = group(acl);
        if (acl.permissionType() == AclPermissionType.ALLOW) {
            allowsOf.merge(group, new StandardAcl[] {acl}, GroupAcls::with);
        } else {
            deniesOf.merge(group, new StandardAcl[] {acl}, GroupAcls::with);
        }
    }

    /** An ACL record was deleted: nothing happens unless it was an ACL of a group. */
    synchronized void remove(Uuid id) {
        StandardAcl acl = byId.remove(id);
        if (acl == null) {
            return;
        }
        KafkaPrincipal group = group(acl);
        (acl.permissionType() == AclPermissionType.ALLOW ? allowsOf : deniesOf)
                .computeIfPresent(group, (principal, held) -> without(held, acl));
    }

    /** These are now all the ACLs of groups in the cluster. */
    synchronized void load(Map<Uuid, StandardAcl> acls) {
        Map<KafkaPrincipal, List<StandardAcl>> allowed = new HashMap<>();
        Map<KafkaPrincipal, List<StandardAcl>> denied = new HashMap<>();
        acls.forEach((id, acl) -> (acl.permissionType() == AclPermissionType.ALLOW ? allowed : denied)
                .computeIfAbsent(group(acl), group -> new ArrayList<>()).add(acl));
        allowsOf = byGroup(allowed);
        deniesOf = byGroup(denied);
        byId.clear();
        byId.putAll(acls);
    }

    private static ConcurrentHashMap<KafkaPrincipal, StandardAcl[]> byGroup(
            Map<KafkaPrincipal, List<StandardAcl>> lists) {
        ConcurrentHashMap<KafkaPrincipal, StandardAcl[]> arrays = new ConcurrentHashMap<>();
        lists.forEach((group, list) -> arrays.put(group, list.toArray(new StandardAcl[0])));
        return arrays;
    }

    /** Nothing is held but memory: kept so that whoever owns one closes it. */
    @Override
    public void close() throws IOException {
        // nothing to release
    }

    // --- the request path ------------------------------------------------------

    /**
     * Whether an {@code ALLOW} ACL of a group of the principal allows the
     * action, by the rules the broker's authorizer would match it with were
     * the client that group.
     *
     * @param principal the principal of the request, a plain {@link KafkaPrincipal}
     */
    boolean allows(AuthorizableRequestContext requestContext, KafkaPrincipal principal, Action action) {
        return allowing(requestContext, principal, action) != null;
    }

    /**
     * The first group of the principal that an {@code ALLOW} ACL allows the
     * action, or {@code null} when none of its groups is allowed it.
     *
     * @param principal the principal of the request, a plain {@link KafkaPrincipal}
     */
    KafkaPrincipal allowing(AuthorizableRequestContext requestContext, KafkaPrincipal principal, Action action) {
        Map<KafkaPrincipal, StandardAcl[]> withAcls = allowsOf;
        if (withAcls.isEmpty()) {
            return null;
        }
        List<KafkaPrincipal> groups = identities.of(principal);
        String host = null;
        for (int i = 0; i < groups.size(); i++) {
            KafkaPrincipal group = groups.get(i);
            StandardAcl[] held = withAcls.get(group);
            if (held == null) {
                continue;
            }
            if (host == null) {
                host = requestContext.clientAddress().getHostAddress();
            }
            if (AclSet.anyMatches(held, group, host, action.operation(), action.resourcePattern())) {
                return group;
            }
        }
        return null;
    }

    /**
     * The resources of a type on which an {@code ALLOW} ACL of a group of
     * the principal allows the operation from the request's host, as the
     * patterns of those ACLs: literal, prefixed, or the literal wildcard
     * {@code *}. Empty when none does. Not for every request: it walks the
     * ACLs of the principal's groups.
     *
     * @param principal the principal of the request, a plain {@link KafkaPrincipal}
     */
    Collection<ResourcePattern> allowedPatterns(AuthorizableRequestContext requestContext, KafkaPrincipal principal,
            AclOperation operation, ResourceType resourceType) {
        Map<KafkaPrincipal, StandardAcl[]> withAcls = allowsOf;
        if (withAcls.isEmpty()) {
            return List.of();
        }
        List<ResourcePattern> patterns = new ArrayList<>();
        String host = requestContext.clientAddress().getHostAddress();
        for (KafkaPrincipal group : identities.of(principal)) {
            StandardAcl[] held = withAcls.get(group);
            if (held == null) {
                continue;
            }
            for (StandardAcl acl : held) {
                String name = aNameOnlyItCovers(acl);
                if (acl.resourceType() != resourceType || name == null) {
                    continue;
                }
                // Asked about a name that this pattern, or a wider one, covers: the operation and the
                // host are then matched as for any request.
                if (AclSet.anyMatches(held, group, host, operation,
                        new ResourcePattern(resourceType, name, PatternType.LITERAL))) {
                    patterns.add(new ResourcePattern(resourceType, acl.resourceName(), acl.patternType()));
                }
            }
        }
        return patterns;
    }

    /**
     * Whether a {@code DENY} ACL of a group of the principal matches the
     * operation on a literal resource from the host. Such an ACL is refused
     * when it is created; one that is in the metadata anyway is asked here as
     * the group it names. A deny ACL of {@code User:*} is not a group's: the
     * caller asks for it as the principal itself.
     *
     * @param principal the principal of the request, a plain {@link KafkaPrincipal}
     */
    boolean denied(KafkaPrincipal principal, String host, AclOperation operation, ResourcePattern resource) {
        Map<KafkaPrincipal, StandardAcl[]> withDenies = deniesOf;
        if (withDenies.isEmpty()) {
            return false;
        }
        for (KafkaPrincipal group : identities.of(principal)) {
            StandardAcl[] held = withDenies.get(group);
            if (held != null && AclSet.anyMatches(held, group, host, operation, resource)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A resource name that the ACL's pattern covers and that no narrower
     * pattern does, or {@code null} for a pattern no record has.
     */
    private static String aNameOnlyItCovers(StandardAcl acl) {
        if (acl.patternType() == PatternType.LITERAL) {
            return ResourcePattern.WILDCARD_RESOURCE.equals(acl.resourceName()) ? PAST_THE_PREFIX : acl.resourceName();
        }
        return acl.patternType() == PatternType.PREFIXED ? acl.resourceName() + PAST_THE_PREFIX : null;
    }

    private static KafkaPrincipal group(StandardAcl acl) {
        return new KafkaPrincipal(BindingCodec.GROUP_TYPE, acl.principal().substring(GROUP_PRINCIPAL.length()));
    }

    private static StandardAcl[] with(StandardAcl[] held, StandardAcl[] added) {
        StandardAcl[] both = new StandardAcl[held.length + added.length];
        System.arraycopy(held, 0, both, 0, held.length);
        System.arraycopy(added, 0, both, held.length, added.length);
        return both;
    }

    /** The array without one mention of that ACL, or {@code null} when nothing is left. */
    private static StandardAcl[] without(StandardAcl[] held, StandardAcl acl) {
        int at = -1;
        for (int i = 0; i < held.length && at < 0; i++) {
            if (held[i].equals(acl)) {
                at = i;
            }
        }
        if (at < 0) {
            return held;
        }
        if (held.length == 1) {
            return null;
        }
        StandardAcl[] rest = new StandardAcl[held.length - 1];
        System.arraycopy(held, 0, rest, 0, at);
        System.arraycopy(held, at + 1, rest, at, rest.length - at);
        return rest;
    }
}
