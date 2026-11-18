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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SPIKE, not for merging: role bindings stored as ACL records in the cluster
 * metadata log, with the role as the principal type.
 *
 * <p>A binding is an ACL whose principal is {@code <Role>:<member>}, for
 * example {@code DeveloperWrite:User:alice} on the prefixed pattern
 * {@code billing.}. No client ever authenticates as a principal of type
 * {@code DeveloperWrite}, so the parent never matches it; this class reads
 * such ACLs as "the member holds the role on that pattern".
 *
 * <p>What the spike is to show on a real broker: the records reach this
 * class through {@code addAcl}; a role then allows what no ACL allows;
 * {@code createAcls} runs with the caller's own principal, so a resource
 * owner can be let to bind roles inside the prefix it owns and nowhere else.
 */
public class RbacAuthorizer extends StandardAuthorizer {

    private static final Logger LOG = LoggerFactory.getLogger(RbacAuthorizer.class);

    static final String OWNER = "ResourceOwner";

    /** The roles of the spike: per resource type, the operations a role grants. */
    static final Map<String, Map<ResourceType, Set<AclOperation>>> ROLES = Map.of(
            "DeveloperRead", Map.of(
                    ResourceType.TOPIC, EnumSet.of(AclOperation.READ, AclOperation.DESCRIBE),
                    ResourceType.GROUP, EnumSet.of(AclOperation.READ, AclOperation.DESCRIBE)),
            "DeveloperWrite", Map.of(
                    ResourceType.TOPIC, EnumSet.of(AclOperation.WRITE, AclOperation.DESCRIBE),
                    ResourceType.TRANSACTIONAL_ID, EnumSet.of(AclOperation.WRITE, AclOperation.DESCRIBE)),
            OWNER, Map.of(
                    ResourceType.TOPIC, EnumSet.allOf(AclOperation.class),
                    ResourceType.GROUP, EnumSet.allOf(AclOperation.class),
                    ResourceType.TRANSACTIONAL_ID, EnumSet.allOf(AclOperation.class)));

    /** One binding: the member holds the role on the pattern. */
    record Binding(String role, String member, String name, PatternType patternType) {

        boolean covers(String resourceName) {
            if (patternType == PatternType.PREFIXED) {
                return resourceName.startsWith(name);
            }
            return name.equals("*") || name.equals(resourceName);
        }

        /** Whether everything the other pattern matches, this one matches too. */
        boolean covers(ResourcePattern other) {
            if (patternType == PatternType.PREFIXED) {
                return other.name().startsWith(name);
            }
            return other.patternType() == PatternType.LITERAL && name.equals(other.name());
        }
    }

    private final Map<Uuid, Binding> bindings = new ConcurrentHashMap<>();
    private Set<String> superUsers = Set.of();

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        Object configured = configs.get(SUPER_USERS_CONFIG);
        Set<String> users = new HashSet<>();
        if (configured != null) {
            for (String user : configured.toString().split(";")) {
                if (!user.isBlank()) {
                    users.add(user.trim());
                }
            }
        }
        superUsers = users;
    }

    @Override
    public void addAcl(Uuid id, StandardAcl acl) {
        super.addAcl(id, acl);
        Binding binding = binding(acl.principal(), acl.resourceName(), acl.patternType());
        if (binding != null) {
            bindings.put(id, binding);
            LOG.info("kfkrbac spike: binding added {} {}", id, binding);
        }
    }

    @Override
    public void removeAcl(Uuid id) {
        super.removeAcl(id);
        Binding removed = bindings.remove(id);
        if (removed != null) {
            LOG.info("kfkrbac spike: binding removed {} {}", id, removed);
        }
    }

    @Override
    public void loadSnapshot(Map<Uuid, StandardAcl> acls) {
        super.loadSnapshot(acls);
        bindings.clear();
        acls.forEach((id, acl) -> {
            Binding binding = binding(acl.principal(), acl.resourceName(), acl.patternType());
            if (binding != null) {
                bindings.put(id, binding);
            }
        });
        LOG.info("kfkrbac spike: snapshot loaded, {} binding(s) of {} acl(s)", bindings.size(), acls.size());
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        List<AuthorizationResult> results = new ArrayList<>(super.authorize(requestContext, actions));
        String principal = requestContext.principal().toString();
        for (int i = 0; i < results.size(); i++) {
            if (results.get(i) == AuthorizationResult.DENIED && granted(requestContext, principal, actions.get(i))) {
                results.set(i, AuthorizationResult.ALLOWED);
            }
        }
        return results;
    }

    /**
     * The broker's second question: may this principal do the operation on any
     * resource of the type at all. An idempotent producer is asked it for WRITE
     * on topics before it sends anything. The parent answers from ACLs only.
     */
    @Override
    public AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext, AclOperation op,
            ResourceType resourceType) {
        AuthorizationResult result = super.authorizeByResourceType(requestContext, op, resourceType);
        if (result == AuthorizationResult.ALLOWED) {
            return result;
        }
        String principal = requestContext.principal().toString();
        for (Binding binding : bindings.values()) {
            Set<AclOperation> operations = ROLES.get(binding.role).get(resourceType);
            if (binding.member.equals(principal) && operations != null && operations.contains(op)) {
                return AuthorizationResult.ALLOWED;
            }
        }
        return result;
    }

    private boolean granted(AuthorizableRequestContext context, String principal, Action action) {
        ResourcePattern resource = action.resourcePattern();
        // The broker asks for ALTER on the cluster before it hands CreateAcls to
        // createAcls. An owner passes this gate; createAcls decides what it may bind.
        // The ACL tool lists before it creates, and listing asks for DESCRIBE on the cluster.
        boolean createsAcls = action.operation() == AclOperation.ALTER
                && context.requestType() == ApiKeys.CREATE_ACLS.id;
        boolean listsAcls = action.operation() == AclOperation.DESCRIBE
                && context.requestType() == ApiKeys.DESCRIBE_ACLS.id;
        if (resource.resourceType() == ResourceType.CLUSTER && (createsAcls || listsAcls)) {
            return bindings.values().stream().anyMatch(b -> b.role.equals(OWNER) && b.member.equals(principal));
        }
        for (Binding binding : bindings.values()) {
            if (!binding.member.equals(principal)) {
                continue;
            }
            Set<AclOperation> operations = ROLES.get(binding.role).get(resource.resourceType());
            if (operations != null && operations.contains(action.operation()) && binding.covers(resource.name())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext,
            List<AclBinding> aclBindings) {
        String principal = requestContext.principal().toString();
        LOG.info("kfkrbac spike: createAcls by {} requestType={} listener={} acls={}", principal,
                requestContext.requestType(), requestContext.listenerName(), aclBindings);
        if (superUsers.contains(principal)) {
            return super.createAcls(requestContext, aclBindings);
        }
        for (AclBinding acl : aclBindings) {
            Binding wanted = binding(acl.entry().principal(), acl.pattern().name(), acl.pattern().patternType());
            String refusal = wanted == null ? "only role bindings may be created by a resource owner"
                    : owns(principal, acl.pattern()) ? null
                    : principal + " owns nothing that covers " + acl.pattern().name();
            if (refusal != null) {
                LOG.info("kfkrbac spike: createAcls refused: {}", refusal);
                AclCreateResult refused = new AclCreateResult(new ClusterAuthorizationException(refusal));
                List<CompletionStage<AclCreateResult>> all = new ArrayList<>();
                aclBindings.forEach(unused -> all.add(CompletableFuture.completedFuture(refused)));
                return all;
            }
        }
        return super.createAcls(requestContext, aclBindings);
    }

    private boolean owns(String principal, ResourcePattern pattern) {
        return bindings.values().stream()
                .anyMatch(b -> b.role.equals(OWNER) && b.member.equals(principal) && b.covers(pattern));
    }

    /** The binding an ACL stands for, or null when it is an ordinary ACL. */
    private static Binding binding(String aclPrincipal, String resourceName, PatternType patternType) {
        KafkaPrincipal parsed = principal(aclPrincipal);
        if (parsed == null || !ROLES.containsKey(parsed.getPrincipalType())) {
            return null;
        }
        return new Binding(parsed.getPrincipalType(), parsed.getName(), resourceName, patternType);
    }

    private static KafkaPrincipal principal(String text) {
        int colon = text.indexOf(':');
        return colon <= 0 ? null : new KafkaPrincipal(text.substring(0, colon), text.substring(colon + 1));
    }
}
