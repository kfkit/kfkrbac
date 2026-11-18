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

import java.util.Objects;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * A role binding: the member holds the role on what the binding names.
 *
 * <p>Made by {@link BindingCodec} from an ACL record, and only from a
 * well-formed one, so every instance is a binding the record format allows.
 * Immutable; the lookups are safe on the request path.
 *
 * @param role the role the member holds
 * @param member who holds it: {@code User:alice}, or a group
 * @param target what the binding names
 * @param name the name pattern, without the {@code Subject:} or
 *        {@code Connector:} in front of it; {@code kafka-cluster} for the cluster
 * @param patternType {@code LITERAL} or {@code PREFIXED}
 */
record RoleBinding(Role role, KafkaPrincipal member, Target target, String name, PatternType patternType)
        implements BindingCodec.Decoded {

    /** What a binding names, which follows from the resource type and the name of its record. */
    enum Target {
        /** A {@code TOPIC} record: the topics, groups and transactional IDs the pattern covers. */
        NAMES,
        /** A {@code GROUP} record: the groups the pattern covers, and nothing else. */
        GROUPS,
        /** A {@code TRANSACTIONAL_ID} record: the transactional IDs the pattern covers, and nothing else. */
        TRANSACTIONAL_IDS,
        /** A {@code CLUSTER} record: every resource of every kind the role names, whatever its name. */
        CLUSTER,
        /** A {@code TOPIC} record named {@code Subject:...}: subjects of a schema registry, never a broker resource. */
        SUBJECTS,
        /** A {@code TOPIC} record named {@code Connector:...}: connectors, never a broker resource. */
        CONNECTORS
    }

    RoleBinding {
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(member, "member");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(patternType, "patternType");
    }

    /**
     * Whether the binding grants the operation on the broker resource of
     * that type and name.
     */
    boolean grants(AclOperation operation, ResourceType type, String resourceName) {
        return reaches(type) && role.grants(operation, type) && (target == Target.CLUSTER || covers(resourceName));
    }

    /**
     * The resources of a broker type on which the binding grants the
     * operation, as a pattern an ACL could have, or {@code null} when it
     * grants the operation on none.
     */
    ResourcePattern grantedPattern(AclOperation operation, ResourceType type) {
        if (!reaches(type) || !role.grants(operation, type)) {
            return null;
        }
        if (target != Target.CLUSTER) {
            return new ResourcePattern(type, name, patternType);
        }
        return type == ResourceType.CLUSTER
                ? new ResourcePattern(type, Resource.CLUSTER_NAME, PatternType.LITERAL)
                : new ResourcePattern(type, ResourcePattern.WILDCARD_RESOURCE, PatternType.LITERAL);
    }

    /** Whether the binding names resources of the broker type at all. */
    private boolean reaches(ResourceType type) {
        switch (target) {
            case NAMES:
                return type == ResourceType.TOPIC || type == ResourceType.GROUP
                        || type == ResourceType.TRANSACTIONAL_ID;
            case GROUPS:
                return type == ResourceType.GROUP;
            case TRANSACTIONAL_IDS:
                return type == ResourceType.TRANSACTIONAL_ID;
            case CLUSTER:
                return true;
            default:
                return false;
        }
    }

    /** Whether the pattern covers the name, by the rules of an ACL with that pattern. */
    private boolean covers(String resourceName) {
        if (patternType == PatternType.PREFIXED) {
            return resourceName.startsWith(name);
        }
        return name.equals(ResourcePattern.WILDCARD_RESOURCE) || name.equals(resourceName);
    }
}
