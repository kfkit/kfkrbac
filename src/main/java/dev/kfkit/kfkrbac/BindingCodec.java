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

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;

/**
 * Reads an ACL record as what it stands for: a role binding, a group
 * membership, or an ordinary ACL.
 *
 * <p>A binding is an ACL whose principal is {@code <Role>:<member>}, a
 * membership one whose principal is {@code Member:<member>}; the record
 * formats are in {@code AGENTS.md}, "The record" and "Groups". The record
 * comes in two shapes, and both are read by the same rules: the broker hands
 * the authorizer a {@link StandardAcl}, a client reads an {@link AclBinding}.
 *
 * <p>A record whose principal type is a role or {@code Member} but which the
 * format does not allow is {@link Malformed}: it is never a binding and never
 * a membership, so it grants nothing. So is a {@code DENY} ACL whose
 * principal is a group. Nothing here throws on a record, whatever is in it.
 */
final class BindingCodec {

    /** The principal type of a group-membership record: {@code Member:User:alice}. */
    static final String MEMBER_TYPE = "Member";

    /** The principal type of a group: {@code Group:payments}. */
    static final String GROUP_TYPE = "Group";

    /** In front of the name of a binding for subjects of a schema registry. */
    static final String SUBJECT_PREFIX = "Subject:";

    /** In front of the name of a binding for connectors. */
    static final String CONNECTOR_PREFIX = "Connector:";

    /**
     * In front of the name of a group in a membership record. Reserved: no
     * binding's name starts with it, on any resource type.
     */
    static final String GROUP_PREFIX = "Group:";

    /** What an ACL record stands for. */
    sealed interface Decoded permits RoleBinding, Membership, Ordinary, Malformed {
    }

    /**
     * A group-membership record: the member is in the group. Made only from
     * a well-formed record, so the member is never a group.
     *
     * @param member who is in the group: {@code User:alice}
     * @param group the group, as a principal: {@code Group:payments}
     */
    record Membership(KafkaPrincipal member, KafkaPrincipal group) implements Decoded {

        Membership {
            Objects.requireNonNull(member, "member");
            Objects.requireNonNull(group, "group");
        }
    }

    /**
     * An ACL like any other: its principal type is neither a role nor
     * {@code Member}. An {@code ALLOW} ACL whose principal is a group is one
     * of them.
     */
    record Ordinary() implements Decoded {
    }

    /**
     * A record that is not what its principal says it is, and that nobody
     * may create. It grants nothing.
     *
     * @param kind what it fails to be
     * @param reason what is wrong with it, for the operator
     */
    record Malformed(Kind kind, String reason) implements Decoded {

        /** What a malformed record fails to be. */
        enum Kind {
            /** Its principal type is a role, and it is not a binding the format allows. */
            BINDING("has a role as its principal type", "is not a role binding", "so it grants nothing"),
            /** Its principal type is {@code Member}, and it is not a membership the format allows. */
            MEMBERSHIP("has " + MEMBER_TYPE + " as its principal type", "is not a group membership",
                    "so it puts nobody in a group"),
            /** It is a {@code DENY} ACL whose principal is a group. */
            GROUP_DENY("has a group as its principal", "is a DENY for a group",
                    "so it takes away what roles and groups give the members and nothing an ACL allows them");

            private final String looksLike;
            private final String fault;
            private final String consequence;

            Kind(String looksLike, String fault, String consequence) {
                this.looksLike = looksLike;
                this.fault = fault;
                this.consequence = consequence;
            }

            /** What makes the record look like what it is not: {@code has a role as its principal type}. */
            String looksLike() {
                return looksLike;
            }

            /** What is wrong with it, in a few words: {@code is not a role binding}. */
            String fault() {
                return fault;
            }

            /** What becomes of such a record that is in the metadata anyway: {@code so it grants nothing}. */
            String consequence() {
                return consequence;
            }
        }

        /** A record of a role that is not a binding. */
        Malformed(String reason) {
            this(Kind.BINDING, reason);
        }
    }

    private static final Ordinary ORDINARY = new Ordinary();

    /** Why a deny of a group is refused, for whoever tries to create one. */
    private static final Malformed GROUP_DENY = new Malformed(Malformed.Kind.GROUP_DENY,
            "a group only adds access, and a DENY for a group cannot take away what an ACL allows a member by"
            + " name; deny the users by name instead");

    private BindingCodec() {
    }

    /** Reads a record as the broker hands it to the authorizer. */
    static Decoded decode(StandardAcl acl) {
        return decode(acl.resourceType(), acl.resourceName(), acl.patternType(), acl.principal(), acl.host(),
                acl.operation(), acl.permissionType());
    }

    /** Reads a record as a client lists it. */
    static Decoded decode(AclBinding acl) {
        return decode(acl.pattern().resourceType(), acl.pattern().name(), acl.pattern().patternType(),
                acl.entry().principal(), acl.entry().host(), acl.entry().operation(), acl.entry().permissionType());
    }

    private static Decoded decode(ResourceType resourceType, String resourceName, PatternType patternType,
            String principal, String host, AclOperation operation, AclPermissionType permission) {
        int colon = principal == null ? -1 : principal.indexOf(':');
        if (colon < 0) {
            return ORDINARY;
        }
        String principalType = principal.substring(0, colon);
        if (MEMBER_TYPE.equals(principalType)) {
            return membership(principal.substring(colon + 1), resourceType, resourceName, patternType, host,
                    operation, permission);
        }
        Role role = Role.named(principalType).orElse(null);
        if (role == null) {
            return GROUP_TYPE.equals(principalType) && permission != AclPermissionType.ALLOW ? GROUP_DENY : ORDINARY;
        }
        if (operation != AclOperation.ALL) {
            return new Malformed("the operation is " + operation + ", a binding has ALL");
        }
        if (permission != AclPermissionType.ALLOW) {
            return new Malformed("the permission is " + permission + ", a binding has ALLOW");
        }
        if (!"*".equals(host)) {
            return new Malformed("the host is " + host + ", a binding has *");
        }
        KafkaPrincipal member = member(principal.substring(colon + 1));
        if (member == null) {
            return new Malformed("the member is not <type>:<name>, as in " + role + ":User:alice");
        }
        if (resourceType == ResourceType.CLUSTER) {
            return onTheCluster(role, member, resourceName, patternType);
        }
        return onAPattern(role, member, resourceType, resourceName, patternType);
    }

    private static Decoded membership(String memberText, ResourceType resourceType, String resourceName,
            PatternType patternType, String host, AclOperation operation, AclPermissionType permission) {
        if (operation != AclOperation.ALL) {
            return notAMembership("the operation is " + operation + ", a membership has ALL");
        }
        if (permission != AclPermissionType.ALLOW) {
            return notAMembership("the permission is " + permission + ", a membership has ALLOW");
        }
        if (!"*".equals(host)) {
            return notAMembership("the host is " + host + ", a membership has *");
        }
        KafkaPrincipal member = member(memberText);
        if (member == null) {
            return notAMembership("the member is not <type>:<name>, as in " + MEMBER_TYPE + ":User:alice");
        }
        if (GROUP_TYPE.equals(member.getPrincipalType())) {
            return notAMembership("the member is a group, and a group is not a member of a group");
        }
        if (resourceType != ResourceType.TOPIC) {
            return notAMembership("the resource type is " + resourceType + ", a membership has TOPIC");
        }
        if (patternType != PatternType.LITERAL) {
            return notAMembership("the pattern type is " + patternType + ", a membership has LITERAL");
        }
        if (resourceName == null || !resourceName.startsWith(GROUP_PREFIX)
                || resourceName.length() == GROUP_PREFIX.length()) {
            return notAMembership("the name is '" + resourceName + "', a membership is on the name " + GROUP_PREFIX
                    + "<name>, as in " + GROUP_PREFIX + "payments");
        }
        return new Membership(member,
                new KafkaPrincipal(GROUP_TYPE, resourceName.substring(GROUP_PREFIX.length())));
    }

    private static Malformed notAMembership(String reason) {
        return new Malformed(Malformed.Kind.MEMBERSHIP, reason);
    }

    private static Decoded onTheCluster(Role role, KafkaPrincipal member, String resourceName,
            PatternType patternType) {
        if (role.scope() != Role.Scope.CLUSTER) {
            return new Malformed(role + " is bound to a name pattern, not to the cluster");
        }
        if (patternType != PatternType.LITERAL || !Resource.CLUSTER_NAME.equals(resourceName)) {
            return new Malformed("the cluster is the literal name " + Resource.CLUSTER_NAME);
        }
        return new RoleBinding(role, member, RoleBinding.Target.CLUSTER, resourceName, patternType);
    }

    private static Decoded onAPattern(Role role, KafkaPrincipal member, ResourceType resourceType,
            String resourceName, PatternType patternType) {
        if (resourceType != ResourceType.TOPIC && resourceType != ResourceType.GROUP
                && resourceType != ResourceType.TRANSACTIONAL_ID) {
            return new Malformed("the resource type is " + resourceType
                    + ", a binding has TOPIC, GROUP, TRANSACTIONAL_ID or CLUSTER");
        }
        if (role.scope() != Role.Scope.PATTERN) {
            return new Malformed(role + " is bound to the cluster, not to a name pattern");
        }
        if (patternType != PatternType.LITERAL && patternType != PatternType.PREFIXED) {
            return new Malformed("the pattern type is " + patternType + ", a binding has LITERAL or PREFIXED");
        }
        if (resourceName == null || resourceName.isEmpty()) {
            return new Malformed("the name is empty");
        }
        if (resourceName.startsWith(GROUP_PREFIX)) {
            return new Malformed("the name starts with " + GROUP_PREFIX
                    + ", which is reserved for the groups of membership records");
        }
        RoleBinding.Target elsewhere = resourceName.startsWith(SUBJECT_PREFIX) ? RoleBinding.Target.SUBJECTS
                : resourceName.startsWith(CONNECTOR_PREFIX) ? RoleBinding.Target.CONNECTORS
                : null;
        if (elsewhere == null) {
            RoleBinding.Target target = resourceType == ResourceType.TOPIC ? RoleBinding.Target.NAMES
                    : resourceType == ResourceType.GROUP ? RoleBinding.Target.GROUPS
                    : RoleBinding.Target.TRANSACTIONAL_IDS;
            return new RoleBinding(role, member, target, resourceName, patternType);
        }
        if (resourceType != ResourceType.TOPIC) {
            return new Malformed("a binding for subjects or connectors has the resource type TOPIC, not "
                    + resourceType);
        }
        String name = resourceName.substring(resourceName.indexOf(':') + 1);
        if (name.isEmpty()) {
            return new Malformed("the name after " + resourceName + " is empty");
        }
        return new RoleBinding(role, member, elsewhere, name, patternType);
    }

    /**
     * The member of a binding or of a group, {@code User:alice}, or
     * {@code null} when it is not a type and a name.
     */
    private static KafkaPrincipal member(String text) {
        int colon = text.indexOf(':');
        if (colon <= 0 || colon == text.length() - 1) {
            return null;
        }
        return new KafkaPrincipal(text.substring(0, colon), text.substring(colon + 1));
    }
}
