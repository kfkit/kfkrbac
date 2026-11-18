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

import java.util.Comparator;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

/**
 * Writes a role binding and a group membership as the ACL records that
 * {@link BindingCodec} reads back: the formats are in {@code AGENTS.md},
 * "The record" and "Groups".
 *
 * <p>Nothing here decides whether a record is a binding or a membership the
 * format allows: whoever writes one reads it back with the codec first, and
 * the cluster has the last word.
 */
final class BindingRecords {

    /** A fixed order for records, so that what is printed does not depend on how the cluster listed them. */
    static final Comparator<AclBinding> ORDER = Comparator
            .comparing((AclBinding record) -> record.pattern().resourceType())
            .thenComparing(record -> record.pattern().name())
            .thenComparing(record -> record.pattern().patternType())
            .thenComparing(record -> record.entry().principal())
            .thenComparing(record -> record.entry().operation())
            .thenComparing(record -> record.entry().permissionType())
            .thenComparing(record -> record.entry().host());

    private BindingRecords() {
    }

    /**
     * The record of a binding.
     *
     * @param role the role's name, as in {@link Role#roleName()}
     * @param member who holds it, as it is written: {@code User:alice}
     * @param target what the binding names
     * @param name the name pattern, without {@code Subject:} or {@code Connector:} in front; ignored for the cluster
     * @param patternType {@code LITERAL} or {@code PREFIXED}; ignored for the cluster
     */
    static AclBinding record(String role, String member, RoleBinding.Target target, String name,
            PatternType patternType) {
        ResourcePattern pattern;
        switch (target) {
            case CLUSTER:
                pattern = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL);
                break;
            case GROUPS:
                pattern = new ResourcePattern(ResourceType.GROUP, name, patternType);
                break;
            case TRANSACTIONAL_IDS:
                pattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, name, patternType);
                break;
            case SUBJECTS:
                pattern = new ResourcePattern(ResourceType.TOPIC, BindingCodec.SUBJECT_PREFIX + name, patternType);
                break;
            case CONNECTORS:
                pattern = new ResourcePattern(ResourceType.TOPIC, BindingCodec.CONNECTOR_PREFIX + name, patternType);
                break;
            default:
                pattern = new ResourcePattern(ResourceType.TOPIC, name, patternType);
                break;
        }
        return new AclBinding(pattern,
                new AccessControlEntry(role + ":" + member, "*", AclOperation.ALL, AclPermissionType.ALLOW));
    }

    /**
     * The record of a membership: the member is in the group.
     *
     * @param group the name of the group, without {@code Group:} in front: {@code payments}
     * @param member who is in it, as it is written: {@code User:alice}
     */
    static AclBinding membership(String group, String member) {
        return new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, BindingCodec.GROUP_PREFIX + group, PatternType.LITERAL),
                new AccessControlEntry(BindingCodec.MEMBER_TYPE + ":" + member, "*", AclOperation.ALL,
                        AclPermissionType.ALLOW));
    }
}
