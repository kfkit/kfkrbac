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

import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.CREATE;
import static org.apache.kafka.common.acl.AclOperation.DELETE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;

/**
 * The seven roles and what each grants, on the broker's topics, groups,
 * transactional IDs and cluster, and on the subjects of a schema registry
 * and the connectors of Kafka Connect.
 *
 * <p>This is the one place that says what a role means; everything that
 * decides access asks it. {@code docs/roles.md} shows the same table, and a
 * test fails when the two disagree. A role's meaning never changes once
 * released: a different set of permissions gets a different name.
 *
 * <p>Only data and lookups: no I/O, safe to call from the request path.
 */
enum Role {

    DEVELOPER_READ("DeveloperRead", Scope.PATTERN,
            Grants.acl(READ, DESCRIBE),
            Grants.acl(READ, DESCRIBE),
            Grants.acl(DESCRIBE),
            Grants.acl(),
            Grants.subject(SubjectOperation.READ, SubjectOperation.READ_COMPATIBILITY),
            Grants.connector(ConnectorOperation.READ_CONFIG, ConnectorOperation.READ_STATUS,
                    ConnectorOperation.READ_ACTIVE_TOPICS)),

    DEVELOPER_WRITE("DeveloperWrite", Scope.PATTERN,
            Grants.acl(WRITE, DESCRIBE),
            Grants.acl(),
            Grants.acl(WRITE, DESCRIBE),
            Grants.acl(),
            Grants.subject(SubjectOperation.WRITE, SubjectOperation.READ_COMPATIBILITY),
            Grants.connector(ConnectorOperation.READ_STATUS, ConnectorOperation.READ_ACTIVE_TOPICS,
                    ConnectorOperation.CONFIGURE, ConnectorOperation.RESET_ACTIVE_TOPICS)),

    DEVELOPER_MANAGE("DeveloperManage", Scope.PATTERN,
            Grants.acl(CREATE, DELETE, DESCRIBE, DESCRIBE_CONFIGS),
            Grants.acl(DESCRIBE, DELETE),
            Grants.acl(DESCRIBE),
            Grants.acl(),
            Grants.subject(SubjectOperation.READ_COMPATIBILITY, SubjectOperation.WRITE_COMPATIBILITY),
            Grants.connector(ConnectorOperation.CREATE, ConnectorOperation.READ_STATUS,
                    ConnectorOperation.READ_ACTIVE_TOPICS, ConnectorOperation.PAUSE, ConnectorOperation.RESUME,
                    ConnectorOperation.SCALE, ConnectorOperation.RESET_ACTIVE_TOPICS)),

    RESOURCE_OWNER("ResourceOwner", Scope.PATTERN,
            Grants.everyAcl(),
            Grants.everyAcl(),
            Grants.everyAcl(),
            Grants.acl(),
            Grants.everySubject(),
            Grants.everyConnector()),

    RESOURCE_VIEWER("ResourceViewer", Scope.PATTERN,
            Grants.acl(DESCRIBE, DESCRIBE_CONFIGS),
            Grants.acl(DESCRIBE),
            Grants.acl(DESCRIBE),
            Grants.acl(),
            Grants.subject(SubjectOperation.READ, SubjectOperation.READ_COMPATIBILITY),
            Grants.connector(ConnectorOperation.READ_STATUS)),

    CLUSTER_ADMIN("ClusterAdmin", Scope.CLUSTER,
            Grants.acl(CREATE, DELETE, ALTER, ALTER_CONFIGS, DESCRIBE, DESCRIBE_CONFIGS),
            Grants.acl(),
            Grants.acl(),
            Grants.acl(CREATE, ALTER, ALTER_CONFIGS, DESCRIBE, DESCRIBE_CONFIGS),
            Grants.subject(SubjectOperation.READ, SubjectOperation.WRITE, SubjectOperation.DELETE,
                    SubjectOperation.READ_COMPATIBILITY, SubjectOperation.WRITE_COMPATIBILITY),
            Grants.connector(ConnectorOperation.CREATE, ConnectorOperation.READ_CONFIG,
                    ConnectorOperation.READ_STATUS, ConnectorOperation.READ_ACTIVE_TOPICS, ConnectorOperation.PAUSE,
                    ConnectorOperation.RESUME, ConnectorOperation.SCALE, ConnectorOperation.DELETE,
                    ConnectorOperation.RESET_ACTIVE_TOPICS)),

    SYSTEM_ADMIN("SystemAdmin", Scope.CLUSTER,
            Grants.everyAcl(),
            Grants.everyAcl(),
            Grants.everyAcl(),
            Grants.everyAcl(),
            Grants.everySubject(),
            Grants.everyConnector());

    /** What a binding of the role names. */
    enum Scope {
        /** A name pattern: a literal name, a prefix, or {@code *}. */
        PATTERN,
        /** The cluster: the role grants on every resource of every kind it names, whatever its name. */
        CLUSTER
    }

    private static final Map<String, Role> BY_NAME;

    /**
     * The names of the roles, which are reserved as principal types: a
     * binding is an ACL record whose principal is {@code <Role>:<member>}.
     */
    static final Set<String> NAMES;

    static {
        Map<String, Role> byName = new LinkedHashMap<>();
        for (Role role : values()) {
            byName.put(role.roleName, role);
        }
        BY_NAME = Collections.unmodifiableMap(byName);
        NAMES = Collections.unmodifiableSet(byName.keySet());
    }

    private final String roleName;
    private final Scope scope;
    private final Map<ResourceType, Set<AclOperation>> acl;
    private final Set<SubjectOperation> subject;
    private final Set<ConnectorOperation> connector;

    Role(String roleName, Scope scope, Set<AclOperation> topic, Set<AclOperation> group,
            Set<AclOperation> transactionalId, Set<AclOperation> cluster, Set<SubjectOperation> subject,
            Set<ConnectorOperation> connector) {
        this.roleName = roleName;
        this.scope = scope;
        Map<ResourceType, Set<AclOperation>> acl = new EnumMap<>(ResourceType.class);
        acl.put(ResourceType.TOPIC, topic);
        acl.put(ResourceType.GROUP, group);
        acl.put(ResourceType.TRANSACTIONAL_ID, transactionalId);
        acl.put(ResourceType.CLUSTER, cluster);
        this.acl = Collections.unmodifiableMap(acl);
        this.subject = subject;
        this.connector = connector;
    }

    /**
     * The role of that name, exactly as written, case included; empty when
     * there is none.
     */
    static Optional<Role> named(String name) {
        return Optional.ofNullable(name == null ? null : BY_NAME.get(name));
    }

    /** The role's name as it is written in a binding: {@code DeveloperRead}. */
    String roleName() {
        return roleName;
    }

    /** Whether the role is bound to a name pattern or to the cluster. */
    Scope scope() {
        return scope;
    }

    /**
     * The operations the role grants on resources of a broker type. Empty for
     * a type the role grants nothing on, and for every type but
     * {@code TOPIC}, {@code GROUP}, {@code TRANSACTIONAL_ID} and
     * {@code CLUSTER}.
     */
    Set<AclOperation> operations(ResourceType type) {
        return acl.getOrDefault(type, Set.of());
    }

    /** Whether the role grants the operation on resources of the broker type. */
    boolean grants(AclOperation operation, ResourceType type) {
        Set<AclOperation> granted = acl.get(type);
        return granted != null && granted.contains(operation);
    }

    /** The operations the role grants on subjects. */
    Set<SubjectOperation> subjectOperations() {
        return subject;
    }

    boolean grants(SubjectOperation operation) {
        return subject.contains(operation);
    }

    /** The operations the role grants on connectors. */
    Set<ConnectorOperation> connectorOperations() {
        return connector;
    }

    boolean grants(ConnectorOperation operation) {
        return connector.contains(operation);
    }

    /**
     * Every operation the broker can ask about: each {@link AclOperation}
     * but {@code UNKNOWN}, {@code ANY} and {@code ALL}, which only ever
     * appear in filters and records, never in a request. "all" in the table
     * of roles is this set.
     */
    static Set<AclOperation> everyAclOperation() {
        return Grants.EVERY_ACL;
    }

    @Override
    public String toString() {
        return roleName;
    }

    /** Builds the sets above; a class of its own so that it is initialized before the roles are. */
    private static final class Grants {

        static final Set<AclOperation> EVERY_ACL = Collections.unmodifiableSet(
                EnumSet.complementOf(EnumSet.of(AclOperation.UNKNOWN, AclOperation.ANY, AclOperation.ALL)));

        static Set<AclOperation> acl(AclOperation... operations) {
            EnumSet<AclOperation> set = EnumSet.noneOf(AclOperation.class);
            Collections.addAll(set, operations);
            return Collections.unmodifiableSet(set);
        }

        static Set<AclOperation> everyAcl() {
            return EVERY_ACL;
        }

        static Set<SubjectOperation> subject(SubjectOperation... operations) {
            EnumSet<SubjectOperation> set = EnumSet.noneOf(SubjectOperation.class);
            Collections.addAll(set, operations);
            return Collections.unmodifiableSet(set);
        }

        static Set<SubjectOperation> everySubject() {
            return Collections.unmodifiableSet(EnumSet.allOf(SubjectOperation.class));
        }

        static Set<ConnectorOperation> connector(ConnectorOperation... operations) {
            EnumSet<ConnectorOperation> set = EnumSet.noneOf(ConnectorOperation.class);
            Collections.addAll(set, operations);
            return Collections.unmodifiableSet(set);
        }

        static Set<ConnectorOperation> everyConnector() {
            return Collections.unmodifiableSet(EnumSet.allOf(ConnectorOperation.class));
        }
    }
}
