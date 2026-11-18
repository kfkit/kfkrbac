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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * The groups of the cluster, in memory, by member: the {@link Identities} a
 * broker decides with.
 *
 * <p>Fed by {@link BindingIndex} with the membership records among the ACL
 * records the broker delivers. A group is nothing but its name: it exists
 * for as long as a record puts a member in it, and a binding or an ACL of a
 * group nobody is in grants nothing.
 *
 * <p>{@link #of} is on the request path and takes no lock: one lookup by
 * principal, which finds nothing for a principal in no group. What a member
 * is in is a list that is replaced, never changed. Writes come from the
 * broker's metadata thread, rarely, and are serialized.
 *
 * <p>A snapshot arrives in two steps, {@link #narrow} and then
 * {@link #load}, with everything else that depends on groups switched in
 * between: for that moment a member is only in the groups it is in both
 * before and after, so that no request is allowed by the old memberships
 * together with the new bindings, or the other way round.
 */
final class Memberships implements Identities {

    /**
     * For each member of some group, itself and then its groups. A group is
     * there once for every record that puts the member in it.
     */
    private volatile ConcurrentHashMap<KafkaPrincipal, List<KafkaPrincipal>> byMember = new ConcurrentHashMap<>();

    /** The memberships by the ID of their record, to remove them by. Guarded by {@code this}. */
    private final Map<Uuid, BindingCodec.Membership> byId = new HashMap<>();

    // --- the metadata thread ---------------------------------------------------

    /** A membership record was created. */
    synchronized void add(Uuid id, BindingCodec.Membership membership) {
        remove(id); // an ID is not added twice; if it were, the first membership must not outlive its record
        byId.put(id, membership);
        byMember.merge(membership.member(), List.of(membership.member(), membership.group()),
                (identities, added) -> with(identities, membership.group()));
    }

    /** An ACL record was deleted: nothing happens unless it was a membership. */
    synchronized void remove(Uuid id) {
        BindingCodec.Membership membership = byId.remove(id);
        if (membership != null) {
            byMember.computeIfPresent(membership.member(),
                    (member, identities) -> without(identities, membership.group()));
        }
    }

    /**
     * The first step of a snapshot: every member leaves the groups that the
     * snapshot does not put it in. Nobody joins a group yet.
     *
     * @param coming the memberships of the snapshot
     */
    synchronized void narrow(Collection<BindingCodec.Membership> coming) {
        Map<KafkaPrincipal, Set<KafkaPrincipal>> staying = new HashMap<>();
        for (BindingCodec.Membership membership : coming) {
            staying.computeIfAbsent(membership.member(), member -> new HashSet<>()).add(membership.group());
        }
        for (Map.Entry<KafkaPrincipal, List<KafkaPrincipal>> entry : byMember.entrySet()) {
            Set<KafkaPrincipal> groups = staying.getOrDefault(entry.getKey(), Set.of());
            List<KafkaPrincipal> identities = entry.getValue();
            List<KafkaPrincipal> kept = new ArrayList<>(identities.size());
            kept.add(identities.get(0));
            for (int i = 1; i < identities.size(); i++) {
                if (groups.contains(identities.get(i))) {
                    kept.add(identities.get(i));
                }
            }
            if (kept.size() == 1) {
                byMember.remove(entry.getKey());
            } else if (kept.size() < identities.size()) {
                byMember.put(entry.getKey(), List.copyOf(kept));
            }
        }
        byId.values().removeIf(membership -> !staying.getOrDefault(membership.member(), Set.of())
                .contains(membership.group()));
    }

    /** The second step of a snapshot: these are now all the memberships of the cluster. */
    synchronized void load(Map<Uuid, BindingCodec.Membership> memberships) {
        Map<KafkaPrincipal, List<KafkaPrincipal>> identities = new HashMap<>();
        for (BindingCodec.Membership membership : memberships.values()) {
            identities.computeIfAbsent(membership.member(), member -> {
                List<KafkaPrincipal> list = new ArrayList<>();
                list.add(member);
                return list;
            }).add(membership.group());
        }
        ConcurrentHashMap<KafkaPrincipal, List<KafkaPrincipal>> loaded = new ConcurrentHashMap<>();
        identities.forEach((member, list) -> loaded.put(member, List.copyOf(list)));
        byMember = loaded;
        byId.clear();
        byId.putAll(memberships);
    }

    // --- the request path ------------------------------------------------------

    @Override
    public List<KafkaPrincipal> of(KafkaPrincipal principal) {
        List<KafkaPrincipal> identities = byMember.get(principal);
        return identities == null ? List.of(principal) : identities;
    }

    private static List<KafkaPrincipal> with(List<KafkaPrincipal> identities, KafkaPrincipal group) {
        List<KafkaPrincipal> more = new ArrayList<>(identities.size() + 1);
        more.addAll(identities);
        more.add(group);
        return List.copyOf(more);
    }

    /** The list without one mention of the group, or {@code null} when the member is in no group any more. */
    private static List<KafkaPrincipal> without(List<KafkaPrincipal> identities, KafkaPrincipal group) {
        int at = identities.lastIndexOf(group);
        if (at < 1) {
            return identities;
        }
        if (identities.size() == 2) {
            return null;
        }
        List<KafkaPrincipal> fewer = new ArrayList<>(identities);
        fewer.remove(at);
        return List.copyOf(fewer);
    }
}
