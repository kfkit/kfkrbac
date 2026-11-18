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
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What the broker's authorizer log says of a request a role, or an ACL of a
 * group, allows.
 *
 * <p>The parent authorizer writes a line to the logger {@value #LOGGER} as
 * it answers: "Denied", at {@code INFO}, for what no ACL allows. For a
 * request a role then allows that line would say the opposite of what
 * happened, once per request. So the parent is asked without it
 * ({@link #withoutDeniedLine}), and when a role allows the request the line
 * the parent would have written for an allowed one is written here, to the
 * same logger, in the same words and at the same level, with the binding
 * where the parent names its rule. A request an ACL of a group allows is
 * written the same way, with the group.
 */
final class RoleAudit {

    /** The broker's logger for what its authorizer decided; the parent writes its own lines to it. */
    static final String LOGGER = "kafka.authorizer.logger";

    private static final Logger AUDIT = LoggerFactory.getLogger(LOGGER);

    private final Function<AuthorizableRequestContext, List<RoleBinding>> held;
    private final BiFunction<AuthorizableRequestContext, Action, KafkaPrincipal> allowedGroup;

    /**
     * @param held every binding that applies to the principal of a request
     * @param allowedGroup the group of the principal that an ACL allows an action, or {@code null}
     */
    RoleAudit(Function<AuthorizableRequestContext, List<RoleBinding>> held,
            BiFunction<AuthorizableRequestContext, Action, KafkaPrincipal> allowedGroup) {
        this.held = Objects.requireNonNull(held, "held");
        this.allowedGroup = Objects.requireNonNull(allowedGroup, "allowedGroup");
    }

    /**
     * The same actions for the parent to answer without a "Denied" line at
     * {@code INFO}: copies that do not ask for it. The list itself, and
     * nothing allocated, when no action asks for it.
     */
    static List<Action> withoutDeniedLine(List<Action> actions) {
        int size = actions.size();
        for (int i = 0; i < size; i++) {
            if (actions.get(i).logIfDenied()) {
                List<Action> quiet = new ArrayList<>(size);
                for (int j = 0; j < size; j++) {
                    Action action = actions.get(j);
                    quiet.add(action.logIfDenied()
                            ? new Action(action.operation(), action.resourcePattern(),
                                    action.resourceReferenceCount(), action.logIfAllowed(), false)
                            : action);
                }
                return quiet;
            }
        }
        return actions;
    }

    /**
     * A role, or an ACL of a group, allowed an action the parent did not.
     * Written as the parent writes an allowed action: at {@code DEBUG} when
     * the broker wants it logged, at {@code TRACE} otherwise. Never throws.
     */
    void allowed(AuthorizableRequestContext requestContext, Action action) {
        if (action.logIfAllowed() && AUDIT.isDebugEnabled()) {
            AUDIT.debug(message(requestContext, action));
        } else if (AUDIT.isTraceEnabled()) {
            AUDIT.trace(message(requestContext, action));
        }
    }

    /** The parent's line for an allowed action, word for word, up to the rule. */
    private String message(AuthorizableRequestContext requestContext, Action action) {
        KafkaPrincipal principal = requestContext.principal();
        ResourcePattern resource = action.resourcePattern();
        int request = requestContext.requestType();
        StringBuilder line = new StringBuilder(240);
        line.append("Principal = ").append(principal.getPrincipalType()).append(':').append(principal.getName());
        line.append(" is Allowed operation = ").append(action.operation());
        line.append(" from host = ").append(requestContext.clientAddress().getHostAddress());
        line.append(" on resource = ");
        pascalCase(line, resource.resourceType().name());
        line.append(':').append(resource.patternType()).append(':').append(resource.name());
        line.append(" for request = ").append(ApiKeys.hasId(request) ? ApiKeys.forId(request).name : request);
        line.append(" with resourceRefCount = ").append(action.resourceReferenceCount());
        line.append(" based on rule ");
        RoleBinding binding = null;
        KafkaPrincipal group = null;
        try {
            binding = allowing(requestContext, action);
            group = binding == null ? allowedGroup.apply(requestContext, action) : null;
        } catch (RuntimeException e) {
            // the request was allowed; what allowed it stays unnamed
        }
        if (binding != null) {
            line.append("RoleBinding(role=").append(binding.role().roleName()).append(", member=")
                    .append(binding.member()).append(", resourceType=").append(recordType(binding))
                    .append(", resourceName=").append(binding.name()).append(", patternType=")
                    .append(binding.patternType()).append(')');
        } else if (group != null) {
            line.append("GroupAcl(group=").append(group).append(')');
        } else {
            line.append("RoleBinding");
        }
        return line.toString();
    }

    /**
     * The binding that allowed the action: the first one that grants it or,
     * for the broker's check before the ACL API, which no binding grants,
     * the first of {@code ResourceOwner}. {@code null} when no binding of
     * the cluster did: an ACL of a group may have.
     */
    private RoleBinding allowing(AuthorizableRequestContext requestContext, Action action) {
        ResourcePattern resource = action.resourcePattern();
        boolean systemAdminOnly = RbacAuthorizer.keptForSystemAdmin(requestContext, action.operation(),
                resource.resourceType());
        boolean beforeTheAclApi = RbacAuthorizer.opensTheAclApi(requestContext, action);
        RoleBinding owner = null;
        for (RoleBinding binding : held.apply(requestContext)) {
            if (binding.grants(action.operation(), resource.resourceType(), resource.name())
                    && (!systemAdminOnly || binding.role() == Role.SYSTEM_ADMIN)) {
                return binding;
            }
            if (owner == null && beforeTheAclApi && binding.role() == Role.RESOURCE_OWNER) {
                owner = binding;
            }
        }
        return owner;
    }

    /**
     * A resource type as the parent writes it: {@code TRANSACTIONAL_ID} as
     * {@code TransactionalId}. The parent's own helper for this is not at
     * the same place in every release this runs on.
     */
    private static void pascalCase(StringBuilder line, String upperCase) {
        boolean first = true;
        for (int i = 0; i < upperCase.length(); i++) {
            char c = upperCase.charAt(i);
            if (c == '_') {
                first = true;
            } else {
                line.append(first ? c : Character.toLowerCase(c));
                first = false;
            }
        }
    }

    /** The resource type of the ACL record the binding is, among those that name a broker's resources. */
    private static ResourceType recordType(RoleBinding binding) {
        switch (binding.target()) {
            case GROUPS:
                return ResourceType.GROUP;
            case TRANSACTIONAL_IDS:
                return ResourceType.TRANSACTIONAL_ID;
            case CLUSTER:
                return ResourceType.CLUSTER;
            default:
                return ResourceType.TOPIC;
        }
    }
}
