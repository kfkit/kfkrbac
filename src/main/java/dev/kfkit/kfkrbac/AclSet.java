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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;

/**
 * ACLs that are asked whether one of them matches a question, by the rules
 * the broker's {@code StandardAuthorizer} matches an ACL with, and without a
 * line in the broker's authorizer log.
 *
 * <p>This is what the deny ACLs, and the ACLs of groups, are asked through.
 * A private {@code StandardAuthorizer} would match them the same way, but it
 * writes every answer to {@code kafka.authorizer.logger}, at {@code TRACE}
 * when it is not asked to log it, as if it had been a decision of the
 * broker's: that logger is one for every instance and cannot be silenced for
 * one. The rules are those of the broker's
 * {@code StandardAuthorizerData.findResult}, and a test holds them to it:
 * <ul>
 * <li>the resource type is the one asked about; a literal name matches the
 * same name, the literal {@code *} every name, a prefix every name that
 * starts with it;</li>
 * <li>the principal is the one asked as, or {@code User:*};</li>
 * <li>the host is {@code *}, the client's address, or, on a broker whose
 * ACLs take a range of addresses, a range that holds it;</li>
 * <li>the operation is {@code ALL} or the one asked about; and an
 * {@code ALLOW} ACL for {@code READ}, {@code WRITE}, {@code DELETE} or
 * {@code ALTER} also matches {@code DESCRIBE}, one for {@code ALTER_CONFIGS}
 * also {@code DESCRIBE_CONFIGS}.</li>
 * </ul>
 *
 * <p>Written by the broker's metadata thread, under the lock of the
 * instance; read on the request path without one, from an index that is
 * replaced as a whole on every change. A question looks up the ACLs of its
 * resource type on its own name, on the wildcard, and on each prefix of its
 * name that is as long as a prefix some ACL names, and walks only those.
 */
final class AclSet {

    private static final String WILDCARD = ResourcePattern.WILDCARD_RESOURCE;

    private static final String ANY_PRINCIPAL = "User:*";

    private static final Set<AclOperation> IMPLIES_DESCRIBE = EnumSet.of(AclOperation.DESCRIBE, AclOperation.READ,
            AclOperation.WRITE, AclOperation.DELETE, AclOperation.ALTER);

    private static final Set<AclOperation> IMPLIES_DESCRIBE_CONFIGS = EnumSet.of(AclOperation.DESCRIBE_CONFIGS,
            AclOperation.ALTER_CONFIGS);

    /**
     * Whether an address is in a range of addresses written as an ACL's
     * host, as the broker decides it; {@code null} on a broker whose ACLs
     * know no ranges, where the host is matched as it is written.
     */
    private static final MethodHandle IN_RANGE = inRange();

    /** The ACLs by the ID of their record. Guarded by {@code this}. */
    private final Map<Uuid, StandardAcl> byId = new HashMap<>();

    /** The ACLs of one resource type, by what their pattern names. */
    private record OfAType(Map<String, StandardAcl[]> literal, StandardAcl[] wildcard,
            Map<String, StandardAcl[]> prefixed, int[] prefixLengths) {
    }

    /** The ACLs of each resource type that has one. Replaced, never changed. */
    private volatile Map<ResourceType, OfAType> byType = Map.of();

    /** An ACL record was created; one with the same ID is replaced. */
    synchronized void add(Uuid id, StandardAcl acl) {
        byId.put(id, acl);
        index();
    }

    /** An ACL record was deleted; whether it was here. */
    synchronized boolean remove(Uuid id) {
        if (byId.remove(id) == null) {
            return false;
        }
        index();
        return true;
    }

    /** These are now all the ACLs. */
    synchronized void load(Map<Uuid, StandardAcl> acls) {
        byId.clear();
        byId.putAll(acls);
        index();
    }

    /** The ACLs, by the ID of their record. */
    synchronized Map<Uuid, StandardAcl> all() {
        return Map.copyOf(byId);
    }

    /**
     * Whether one of the ACLs matches the operation on the resource for the
     * principal from the host.
     *
     * @param resource a literal resource name, as the broker asks about one
     */
    boolean matches(KafkaPrincipal principal, String host, AclOperation operation, ResourcePattern resource) {
        OfAType ofTheType = byType.get(resource.resourceType());
        if (ofTheType == null) {
            return false;
        }
        String name = resource.name();
        StandardAcl[] named = ofTheType.literal().get(name);
        if (named != null && anyMatches(named, principal, host, operation, resource)) {
            return true;
        }
        if (anyMatches(ofTheType.wildcard(), principal, host, operation, resource)) {
            return true;
        }
        for (int length : ofTheType.prefixLengths()) {
            if (length > name.length()) {
                break;
            }
            StandardAcl[] prefixed = ofTheType.prefixed().get(name.substring(0, length));
            if (prefixed != null && anyMatches(prefixed, principal, host, operation, resource)) {
                return true;
            }
        }
        return false;
    }

    /** Whether one of these ACLs matches, by the rules above. */
    static boolean anyMatches(StandardAcl[] acls, KafkaPrincipal principal, String host, AclOperation operation,
            ResourcePattern resource) {
        String asked = null;
        for (StandardAcl acl : acls) {
            if (acl.resourceType() != resource.resourceType()
                    || !matchesName(acl, resource.name())
                    || !matchesOperation(acl, operation)
                    || !matchesHost(acl, host)) {
                continue;
            }
            if (asked == null) {
                asked = principal.getPrincipalType() + ":" + principal.getName();
            }
            if (asked.equals(acl.principal()) || ANY_PRINCIPAL.equals(acl.principal())) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesName(StandardAcl acl, String name) {
        if (acl.patternType() == PatternType.LITERAL) {
            return acl.resourceName().equals(name) || acl.resourceName().equals(WILDCARD);
        }
        return acl.patternType() == PatternType.PREFIXED && name.startsWith(acl.resourceName());
    }

    private static boolean matchesOperation(StandardAcl acl, AclOperation operation) {
        AclOperation granted = acl.operation();
        if (granted == AclOperation.ALL || granted == operation) {
            return true;
        }
        if (acl.permissionType() != AclPermissionType.ALLOW) {
            return false;
        }
        if (operation == AclOperation.DESCRIBE) {
            return IMPLIES_DESCRIBE.contains(granted);
        }
        return operation == AclOperation.DESCRIBE_CONFIGS && IMPLIES_DESCRIBE_CONFIGS.contains(granted);
    }

    private static boolean matchesHost(StandardAcl acl, String host) {
        String allowed = acl.host();
        if (allowed.equals(WILDCARD) || allowed.equals(host)) {
            return true;
        }
        if (IN_RANGE == null) {
            return false;
        }
        try {
            return (boolean) IN_RANGE.invokeExact(host, allowed);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable e) {
            throw new IllegalStateException("could not match the host " + allowed, e);
        }
    }

    private void index() {
        Map<ResourceType, List<StandardAcl>> lists = new EnumMap<>(ResourceType.class);
        for (StandardAcl acl : byId.values()) {
            lists.computeIfAbsent(acl.resourceType(), type -> new ArrayList<>()).add(acl);
        }
        Map<ResourceType, OfAType> index = new EnumMap<>(ResourceType.class);
        lists.forEach((type, list) -> index.put(type, ofAType(list)));
        byType = index;
    }

    private static OfAType ofAType(List<StandardAcl> acls) {
        Map<String, List<StandardAcl>> literal = new HashMap<>();
        List<StandardAcl> wildcard = new ArrayList<>();
        Map<String, List<StandardAcl>> prefixed = new HashMap<>();
        for (StandardAcl acl : acls) {
            if (acl.patternType() == PatternType.LITERAL) {
                if (acl.resourceName().equals(WILDCARD)) {
                    wildcard.add(acl);
                } else {
                    literal.computeIfAbsent(acl.resourceName(), name -> new ArrayList<>()).add(acl);
                }
            } else if (acl.patternType() == PatternType.PREFIXED) {
                prefixed.computeIfAbsent(acl.resourceName(), name -> new ArrayList<>()).add(acl);
            } // no other pattern is stored by the broker, and none would match
        }
        return new OfAType(arrays(literal), wildcard.toArray(new StandardAcl[0]), arrays(prefixed),
                prefixed.keySet().stream().mapToInt(String::length).distinct().sorted().toArray());
    }

    private static Map<String, StandardAcl[]> arrays(Map<String, List<StandardAcl>> lists) {
        Map<String, StandardAcl[]> arrays = new HashMap<>();
        lists.forEach((name, list) -> arrays.put(name, list.toArray(new StandardAcl[0])));
        return arrays;
    }

    private static MethodHandle inRange() {
        try {
            Class<?> cidr = Class.forName("org.apache.kafka.server.authorizer.internals.CidrUtils", true,
                    StandardAcl.class.getClassLoader());
            return MethodHandles.publicLookup().findStatic(cidr, "isInRange",
                    MethodType.methodType(boolean.class, String.class, String.class));
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            return null; // a broker whose ACLs know no ranges: what it matches by is what is matched here
        }
    }
}
