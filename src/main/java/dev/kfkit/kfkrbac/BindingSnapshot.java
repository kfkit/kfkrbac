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
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * The role bindings among a cluster's ACL records as a client lists them,
 * by member, at one moment: what an extension outside the broker decides
 * with. Immutable; a newer listing is a new snapshot.
 *
 * <p>A principal holds the bindings that name it and those that name a
 * group it is a member of: the membership records of the same listing are
 * read by {@link Memberships}, the code the broker reads them with. A
 * record that looks like a binding or a membership and is not one grants
 * nothing, here as on the broker. Nothing here depends on Kafka Connect.
 */
final class BindingSnapshot {

    private final Map<KafkaPrincipal, RoleBinding[]> byMember;
    private final Identities identities;
    private final int bindings;
    private final int malformed;

    private BindingSnapshot(Map<KafkaPrincipal, RoleBinding[]> byMember, Identities identities, int bindings,
            int malformed) {
        this.byMember = byMember;
        this.identities = identities;
        this.bindings = bindings;
        this.malformed = malformed;
    }

    /**
     * The bindings among the records, each applying to its member and to the
     * members of the group it names; ordinary ACLs are left out.
     */
    static BindingSnapshot of(Collection<AclBinding> records) {
        Map<KafkaPrincipal, List<RoleBinding>> held = new HashMap<>();
        Map<Uuid, BindingCodec.Membership> memberships = new HashMap<>();
        int bindings = 0;
        int malformed = 0;
        for (AclBinding record : records) {
            BindingCodec.Decoded decoded = BindingCodec.decode(record);
            if (decoded instanceof RoleBinding binding) {
                held.computeIfAbsent(binding.member(), member -> new ArrayList<>()).add(binding);
                bindings++;
            } else if (decoded instanceof BindingCodec.Membership membership) {
                // A listing carries no record IDs; one of its own keeps each membership apart.
                memberships.put(new Uuid(memberships.size() + 1L, 0L), membership);
            } else if (decoded instanceof BindingCodec.Malformed) {
                malformed++;
            }
        }
        Map<KafkaPrincipal, RoleBinding[]> byMember = new HashMap<>();
        held.forEach((member, list) -> byMember.put(member, list.toArray(new RoleBinding[0])));
        Memberships groups = new Memberships();
        groups.load(memberships);
        return new BindingSnapshot(Map.copyOf(byMember), groups, bindings, malformed);
    }

    /** Whether a role bound to the principal grants the operation on the connector. */
    boolean grants(KafkaPrincipal principal, ConnectorOperation operation, String connector) {
        for (KafkaPrincipal member : identities.of(principal)) {
            RoleBinding[] held = byMember.get(member);
            if (held == null) {
                continue;
            }
            for (RoleBinding binding : held) {
                if (binding.grants(operation, connector)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Whether a binding of the role applies to the principal, whatever it names. */
    boolean holds(KafkaPrincipal principal, Role role) {
        for (KafkaPrincipal member : identities.of(principal)) {
            RoleBinding[] held = byMember.get(member);
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

    /** How many bindings there are. */
    int bindings() {
        return bindings;
    }

    /** How many records look like bindings and are not: they grant nothing. */
    int malformed() {
        return malformed;
    }
}
