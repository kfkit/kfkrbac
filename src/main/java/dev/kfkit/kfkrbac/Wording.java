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
import java.util.Locale;
import java.util.StringJoiner;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/** How the command says things: bindings, ACLs, patterns and operations, for people and as JSON. */
final class Wording {

    private Wording() {
    }

    /** An operation as the table of roles writes it: {@code DescribeConfigs}. */
    static String operation(Enum<?> operation) {
        StringBuilder text = new StringBuilder();
        for (String word : operation.name().split("_")) {
            text.append(word.charAt(0)).append(word.substring(1).toLowerCase(Locale.ROOT));
        }
        return text.toString();
    }

    /** The operations, or {@code all} when they are every one of their kind. */
    static String operations(Collection<? extends Enum<?>> granted, Collection<? extends Enum<?>> every) {
        return granted.size() == every.size() ? "all" : list(granted);
    }

    /** The operations, one after the other. */
    static String list(Collection<? extends Enum<?>> operations) {
        StringJoiner text = new StringJoiner(", ");
        for (Enum<?> operation : operations) {
            text.add(operation(operation));
        }
        return text.toString();
    }

    /** A resource type in words: {@code transactional ID}. */
    static String type(ResourceType type) {
        return type == ResourceType.TRANSACTIONAL_ID
                ? "transactional ID"
                : type.name().toLowerCase(Locale.ROOT).replace('_', ' ');
    }

    /** The resources of a kind that a name pattern covers: {@code topics with prefix billing.}. */
    static String resources(String kind, String name, PatternType patternType) {
        if (patternType == PatternType.PREFIXED) {
            return kind + "s with prefix " + name;
        }
        return ResourcePattern.WILDCARD_RESOURCE.equals(name) ? "every " + kind : kind + " " + name;
    }

    /** The resources a pattern of an ACL covers. */
    static String resources(ResourcePattern pattern) {
        return pattern.resourceType() == ResourceType.CLUSTER
                ? "the cluster"
                : resources(type(pattern.resourceType()), pattern.name(), pattern.patternType());
    }

    /** What a binding names: {@code prefix billing.}, {@code groups with prefix billing.}, {@code the cluster}. */
    static String on(RoleBinding binding) {
        switch (binding.target()) {
            case CLUSTER:
                return "the cluster";
            case GROUPS:
                return resources("group", binding.name(), binding.patternType());
            case TRANSACTIONAL_IDS:
                return resources("transactional ID", binding.name(), binding.patternType());
            case SUBJECTS:
                return resources("subject", binding.name(), binding.patternType());
            case CONNECTORS:
                return resources("connector", binding.name(), binding.patternType());
            default:
                if (binding.patternType() == PatternType.PREFIXED) {
                    return "prefix " + binding.name();
                }
                return ResourcePattern.WILDCARD_RESOURCE.equals(binding.name())
                        ? "every name"
                        : "name " + binding.name();
        }
    }

    /** A binding in one phrase: {@code DeveloperRead to User:alice on prefix billing.}. */
    static String binding(RoleBinding binding) {
        return binding.role() + " to " + binding.member() + " on " + on(binding);
    }

    /** A membership in one phrase: {@code User:alice in Group:payments}. */
    static String membership(BindingCodec.Membership membership) {
        return membership.member() + " in " + membership.group();
    }

    /** A record in one phrase, as what it stands for: a binding, a membership, or an ACL. */
    static String record(AclBinding record) {
        BindingCodec.Decoded decoded = BindingCodec.decode(record);
        if (decoded instanceof RoleBinding binding) {
            return "binding " + binding(binding);
        }
        if (decoded instanceof BindingCodec.Membership membership) {
            return "membership of " + membership(membership);
        }
        AccessControlEntry entry = record.entry();
        return "ACL " + (entry.permissionType() == AclPermissionType.DENY ? "deny" : "allow") + " "
                + entry.principal() + " " + operation(entry.operation()) + " on " + resources(record.pattern())
                + ("*".equals(entry.host()) ? "" : " from host " + entry.host());
    }

    /**
     * A principal as it is written in an ACL, {@code User:alice}: the type
     * up to the first colon, the name after it. {@code null} when it is not
     * written that way.
     */
    static KafkaPrincipal principal(String text) {
        int colon = text == null ? -1 : text.indexOf(':');
        if (colon <= 0 || colon == text.length() - 1) {
            return null;
        }
        return new KafkaPrincipal(text.substring(0, colon), text.substring(colon + 1));
    }

    /**
     * A binding as the JSON object of {@code list --output json}: the role,
     * the member, and the resource type, name and pattern type of its record.
     */
    static String json(AclBinding record, RoleBinding binding) {
        return "{\"role\":" + quoted(binding.role().roleName())
                + ",\"member\":" + quoted(binding.member().toString())
                + ",\"resourceType\":" + quoted(record.pattern().resourceType().name())
                + ",\"name\":" + quoted(record.pattern().name())
                + ",\"patternType\":" + quoted(record.pattern().patternType().name()) + "}";
    }

    /**
     * A membership as the JSON object of {@code group list --output json}:
     * the name of the group, without {@code Group:} in front, and the member.
     */
    static String json(BindingCodec.Membership membership) {
        return "{\"group\":" + quoted(membership.group().getName())
                + ",\"member\":" + quoted(membership.member().toString()) + "}";
    }

    private static String quoted(String text) {
        StringBuilder json = new StringBuilder("\"");
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"' || c == '\\') {
                json.append('\\').append(c);
            } else if (c < 0x20) {
                json.append(String.format("\\u%04x", (int) c));
            } else {
                json.append(c);
            }
        }
        return json.append('"').toString();
    }
}
