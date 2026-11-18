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

import java.util.Collection;
import java.util.List;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;

/**
 * What the roles bound to a principal grant, as {@link RbacAuthorizer} asks
 * it.
 *
 * <p>Only ever consulted for what the ACLs did not allow, and its answer only
 * ever adds access: deny ACLs are checked by the authorizer, not here. Both
 * methods run on the broker's request path: in memory, no I/O, no blocking.
 *
 * <p>Which roles exist and how bindings are read from ACL records are
 * decided elsewhere; until then the authorizer runs with {@link #NONE}.
 */
interface Roles {

    /** No role is bound to anyone: the ACLs decide alone. */
    Roles NONE = new Roles() {
        @Override
        public boolean grants(KafkaPrincipal principal, Action action) {
            return false;
        }

        @Override
        public Collection<ResourcePattern> grantedPatterns(KafkaPrincipal principal, AclOperation operation,
                ResourceType resourceType) {
            return List.of();
        }
    };

    /**
     * Whether a role bound to the principal grants the action.
     *
     * @param principal the principal of the request, a plain {@link KafkaPrincipal}
     * @param action an action on a literal resource, as the broker asks it
     */
    boolean grants(KafkaPrincipal principal, Action action);

    /**
     * The resources of a type on which a role bound to the principal grants
     * the operation, as the patterns of the bindings: literal, prefixed, or
     * the literal wildcard {@code *}. Empty when no role grants it anywhere.
     *
     * @param principal the principal of the request, a plain {@link KafkaPrincipal}
     */
    Collection<ResourcePattern> grantedPatterns(KafkaPrincipal principal, AclOperation operation,
            ResourceType resourceType);
}
