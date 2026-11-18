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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;

/**
 * Roles held in memory for tests: each grant is a principal, an operation
 * and a binding pattern. Counts how often it is asked.
 */
final class BoundRoles implements Roles {

    private record Grant(KafkaPrincipal principal, AclOperation operation, ResourcePattern pattern) {
    }

    private final List<Grant> grants = new ArrayList<>();
    final AtomicInteger asked = new AtomicInteger();

    BoundRoles grant(String user, AclOperation operation, ResourcePattern pattern) {
        grants.add(new Grant(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user), operation, pattern));
        return this;
    }

    @Override
    public boolean grants(KafkaPrincipal principal, Action action) {
        asked.incrementAndGet();
        ResourcePattern resource = action.resourcePattern();
        for (Grant grant : grants) {
            if (grant.principal().equals(principal) && grant.operation() == action.operation()
                    && grant.pattern().resourceType() == resource.resourceType()
                    && covers(grant.pattern(), resource.name())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Collection<ResourcePattern> grantedPatterns(KafkaPrincipal principal, AclOperation operation,
            ResourceType resourceType) {
        asked.incrementAndGet();
        List<ResourcePattern> patterns = new ArrayList<>();
        for (Grant grant : grants) {
            if (grant.principal().equals(principal) && grant.operation() == operation
                    && grant.pattern().resourceType() == resourceType) {
                patterns.add(grant.pattern());
            }
        }
        return patterns;
    }

    private static boolean covers(ResourcePattern pattern, String name) {
        if (pattern.patternType() == PatternType.PREFIXED) {
            return name.startsWith(pattern.name());
        }
        return pattern.name().equals(ResourcePattern.WILDCARD_RESOURCE) || pattern.name().equals(name);
    }
}
