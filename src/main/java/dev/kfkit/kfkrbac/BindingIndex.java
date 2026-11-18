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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.authorizer.Action;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The role bindings of the cluster, in memory, by member: the {@link Roles}
 * the authorizer decides with on a broker.
 *
 * <p>Fed with every ACL record the broker delivers ({@link #add},
 * {@link #remove}, {@link #load}); it keeps the ones {@link BindingCodec}
 * reads as bindings, hands the group memberships to {@link Memberships} and
 * the ACLs of groups to {@link GroupAcls}, and leaves the rest to the parent
 * authorizer. A record that looks like a binding or a membership and is not
 * one grants nothing; it is logged once and counted for as long as it is in
 * the metadata, and so is a {@code DENY} ACL of a group.
 *
 * <p>Reads are on the request path and take no lock: one lookup by member
 * for each identity of the principal, then a walk over that member's own
 * bindings, never over the cluster's. What a member holds is an array that
 * is replaced, never changed, so a reader sees a binding entirely or not at
 * all; a snapshot replaces the whole map at once. Writes come from the
 * broker's metadata thread, rarely, and are serialized.
 */
final class BindingIndex implements Roles {

    private static final Logger LOG = LoggerFactory.getLogger(BindingIndex.class);

    /** The groups the membership records make. */
    private final Memberships memberships = new Memberships();

    /** Which members a binding may name for it to apply to a principal: {@link #memberships} on a broker. */
    private final Identities identities;

    /** The ordinary ACLs whose principal is a group. */
    private final GroupAcls groupAcls;

    /** What each member holds. The arrays are never written to once they are here. */
    private volatile ConcurrentHashMap<KafkaPrincipal, RoleBinding[]> byMember = new ConcurrentHashMap<>();

    /** The bindings by the ID of their record, to remove them by. Guarded by {@code this}. */
    private final Map<Uuid, RoleBinding> byId = new HashMap<>();

    /** The records that are not what their principal says, by ID. Guarded by {@code this}. */
    private final Map<Uuid, StandardAcl> malformed = new HashMap<>();

    private volatile int malformedCount;

    /** A principal is in the groups the membership records put it in. */
    BindingIndex() {
        this.identities = memberships;
        this.groupAcls = new GroupAcls(memberships);
    }

    /** A principal is in the groups given, whatever the membership records say. */
    BindingIndex(Identities identities) {
        this.identities = Objects.requireNonNull(identities, "identities");
        this.groupAcls = new GroupAcls(identities);
    }

    /** The ordinary ACLs whose principal is a group, among the records this is fed. */
    GroupAcls groupAcls() {
        return groupAcls;
    }

    // --- the metadata thread ---------------------------------------------------

    /** An ACL record was created. */
    synchronized void add(Uuid id, StandardAcl acl) {
        remove(id); // an ID is not added twice; if it were, the first binding must not outlive its record
        BindingCodec.Decoded decoded = BindingCodec.decode(acl);
        if (decoded instanceof RoleBinding binding) {
            byId.put(id, binding);
            byMember.merge(binding.member(), new RoleBinding[] {binding}, BindingIndex::with);
            return;
        }
        if (decoded instanceof BindingCodec.Membership membership) {
            memberships.add(id, membership);
            return;
        }
        if (decoded instanceof BindingCodec.Malformed notWhatItSays) {
            malformed.put(id, acl);
            malformedCount = malformed.size();
            warn(id, acl, notWhatItSays);
        }
        if (GroupAcls.ofAGroup(acl)) {
            groupAcls.add(id, acl);
        }
    }

    /** An ACL record was deleted. */
    synchronized void remove(Uuid id) {
        RoleBinding binding = byId.remove(id);
        if (binding != null) {
            byMember.computeIfPresent(binding.member(), (member, held) -> without(held, binding));
        }
        if (malformed.remove(id) != null) {
            malformedCount = malformed.size();
        }
        memberships.remove(id);
        groupAcls.remove(id);
    }

    /**
     * These are now all the ACL records of the cluster.
     *
     * <p>The bindings are replaced at once. The groups are not in the same
     * place, so while the bindings and the ACLs of groups are replaced a
     * member is only in the groups it is in both before and after: what a
     * request is allowed at any moment, the records before or the records
     * after allow.
     */
    synchronized void load(Map<Uuid, StandardAcl> acls) {
        Map<Uuid, RoleBinding> bindings = new HashMap<>();
        Map<Uuid, StandardAcl> notBindings = new HashMap<>();
        Map<KafkaPrincipal, List<RoleBinding>> held = new HashMap<>();
        Map<Uuid, BindingCodec.Membership> members = new HashMap<>();
        Map<Uuid, StandardAcl> ofGroups = new HashMap<>();
        acls.forEach((id, acl) -> {
            BindingCodec.Decoded decoded = BindingCodec.decode(acl);
            if (decoded instanceof RoleBinding binding) {
                bindings.put(id, binding);
                held.computeIfAbsent(binding.member(), member -> new ArrayList<>()).add(binding);
                return;
            }
            if (decoded instanceof BindingCodec.Membership membership) {
                members.put(id, membership);
                return;
            }
            if (decoded instanceof BindingCodec.Malformed notWhatItSays) {
                notBindings.put(id, acl);
                if (!acl.equals(malformed.get(id))) {
                    warn(id, acl, notWhatItSays);
                }
            }
            if (GroupAcls.ofAGroup(acl)) {
                ofGroups.put(id, acl);
            }
        });
        ConcurrentHashMap<KafkaPrincipal, RoleBinding[]> loaded = new ConcurrentHashMap<>();
        held.forEach((member, list) -> loaded.put(member, list.toArray(new RoleBinding[0])));
        memberships.narrow(members.values());
        byMember = loaded;
        groupAcls.load(ofGroups);
        memberships.load(members);
        byId.clear();
        byId.putAll(bindings);
        malformed.clear();
        malformed.putAll(notBindings);
        malformedCount = notBindings.size();
    }

    /**
     * How many records in the metadata are not what their principal says: a
     * binding or a membership the format does not allow, which grants
     * nothing, or a {@code DENY} ACL of a group.
     */
    int malformed() {
        return malformedCount;
    }

    // --- the request path ------------------------------------------------------

    @Override
    public boolean grants(KafkaPrincipal principal, Action action) {
        ResourcePattern resource = action.resourcePattern();
        Map<KafkaPrincipal, RoleBinding[]> bindings = byMember;
        List<KafkaPrincipal> members = identities.of(principal);
        for (int i = 0; i < members.size(); i++) {
            RoleBinding[] held = bindings.get(members.get(i));
            if (held == null) {
                continue;
            }
            for (RoleBinding binding : held) {
                if (binding.grants(action.operation(), resource.resourceType(), resource.name())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Collection<ResourcePattern> grantedPatterns(KafkaPrincipal principal, AclOperation operation,
            ResourceType resourceType) {
        Map<KafkaPrincipal, RoleBinding[]> bindings = byMember;
        List<ResourcePattern> patterns = new ArrayList<>();
        for (KafkaPrincipal member : identities.of(principal)) {
            RoleBinding[] held = bindings.get(member);
            if (held == null) {
                continue;
            }
            for (RoleBinding binding : held) {
                ResourcePattern pattern = binding.grantedPattern(operation, resourceType);
                if (pattern != null) {
                    patterns.add(pattern);
                }
            }
        }
        return patterns;
    }

    /**
     * Whether a binding applies to the principal or it is in a group, whose
     * bindings and ACLs are its members': when neither, nothing but the
     * ACLs that name the principal allows it anything. On the request path,
     * before the ACLs are asked: one lookup for its groups and one for its
     * bindings, no lock.
     */
    boolean holdsAny(KafkaPrincipal principal) {
        List<KafkaPrincipal> members = identities.of(principal);
        if (members.size() > 1) {
            return true;
        }
        Map<KafkaPrincipal, RoleBinding[]> bindings = byMember;
        for (int i = 0; i < members.size(); i++) {
            if (bindings.containsKey(members.get(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a binding of the role applies to the principal, whatever it
     * names. On the request path: no lock, nothing allocated.
     */
    boolean holds(KafkaPrincipal principal, Role role) {
        Map<KafkaPrincipal, RoleBinding[]> bindings = byMember;
        List<KafkaPrincipal> members = identities.of(principal);
        for (int i = 0; i < members.size(); i++) {
            RoleBinding[] held = bindings.get(members.get(i));
            if (held == null) {
                continue;
            }
            for (RoleBinding binding : held) {
                if (binding.role() == role) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Every binding that applies to the principal: its own and those of its
     * other identities. For deciding what the principal may bind, which is
     * asked rarely; not for the request path.
     */
    List<RoleBinding> heldBy(KafkaPrincipal principal) {
        Map<KafkaPrincipal, RoleBinding[]> bindings = byMember;
        List<RoleBinding> all = new ArrayList<>();
        for (KafkaPrincipal member : identities.of(principal)) {
            RoleBinding[] held = bindings.get(member);
            if (held != null) {
                all.addAll(Arrays.asList(held));
            }
        }
        return all;
    }

    private static void warn(Uuid id, StandardAcl acl, BindingCodec.Malformed notWhatItSays) {
        LOG.warn("ACL {} {} and {}, {}: {}: {}", id, notWhatItSays.kind().looksLike(), notWhatItSays.kind().fault(),
                notWhatItSays.kind().consequence(), notWhatItSays.reason(), acl);
    }

    private static RoleBinding[] with(RoleBinding[] held, RoleBinding[] added) {
        RoleBinding[] both = Arrays.copyOf(held, held.length + added.length);
        System.arraycopy(added, 0, both, held.length, added.length);
        return both;
    }

    /** The array without that very binding, or {@code null} when nothing is left. */
    private static RoleBinding[] without(RoleBinding[] held, RoleBinding binding) {
        int at = -1;
        for (int i = 0; i < held.length && at < 0; i++) {
            if (held[i] == binding) {
                at = i;
            }
        }
        if (at < 0) {
            return held;
        }
        if (held.length == 1) {
            return null;
        }
        RoleBinding[] rest = new RoleBinding[held.length - 1];
        System.arraycopy(held, 0, rest, 0, at);
        System.arraycopy(held, at + 1, rest, at, rest.length - at);
        return rest;
    }
}
