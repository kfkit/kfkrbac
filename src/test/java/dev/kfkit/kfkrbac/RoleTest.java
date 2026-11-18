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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The roles in code against the table operators read in
 * {@code docs/roles.md}: change one without the other and this fails.
 */
class RoleTest {

    private static final Path ROLES_DOC = Path.of("docs/roles.md");
    private static final Path README = Path.of("README.md");

    private static final List<String> HEADER = List.of("Role", "Bound to", "Topic", "Group", "TransactionalId",
            "Cluster", "Subject", "Connector");
    private static final List<ResourceType> BROKER_KINDS = List.of(ResourceType.TOPIC, ResourceType.GROUP,
            ResourceType.TRANSACTIONAL_ID, ResourceType.CLUSTER);

    /** AC1, AC2: every role, every kind, exactly as the table says, and no role the table does not have. */
    @Test
    void theTableInTheDocsIsTheCode() throws IOException {
        Map<String, List<String>> rows = rolesTable();
        assertEquals(Role.NAMES, rows.keySet(), "the roles in docs/roles.md and in Role");
        for (Role role : Role.values()) {
            List<String> row = rows.get(role.roleName());
            String where = role.roleName() + " in docs/roles.md";
            assertEquals(row.get(1), role.scope().name().toLowerCase(Locale.ROOT), where + ", Bound to");
            for (int i = 0; i < BROKER_KINDS.size(); i++) {
                assertEquals(operations(row.get(2 + i), AclOperation.class, Role.everyAclOperation()),
                        role.operations(BROKER_KINDS.get(i)), where + ", " + HEADER.get(2 + i));
            }
            assertEquals(operations(row.get(6), SubjectOperation.class, EnumSet.allOf(SubjectOperation.class)),
                    role.subjectOperations(), where + ", Subject");
            assertEquals(operations(row.get(7), ConnectorOperation.class, EnumSet.allOf(ConnectorOperation.class)),
                    role.connectorOperations(), where + ", Connector");
        }
    }

    /** AC1: the operations of each kind, as the docs list them, are the ones the code knows. */
    @Test
    void theKindsInTheDocsAreTheCode() throws IOException {
        Map<String, String> kinds = new LinkedHashMap<>();
        for (List<String> row : table("| Kind | Operations |")) {
            kinds.put(row.get(0), row.get(1));
        }
        String broker = kinds.get("Topic, Group, TransactionalId, Cluster");
        assertEquals(Role.everyAclOperation(),
                operations(broker.substring(broker.indexOf(':') + 1).trim(), AclOperation.class, Set.of()));
        assertEquals(EnumSet.allOf(SubjectOperation.class),
                operations(kinds.get("Subject"), SubjectOperation.class, Set.of()));
        assertEquals(EnumSet.allOf(ConnectorOperation.class),
                operations(kinds.get("Connector"), ConnectorOperation.class, Set.of()));
    }

    /** AC2. */
    @Test
    void fiveRolesAreBoundToAPatternAndTwoToTheCluster() {
        assertEquals(EnumSet.of(Role.DEVELOPER_READ, Role.DEVELOPER_WRITE, Role.DEVELOPER_MANAGE,
                Role.RESOURCE_OWNER, Role.RESOURCE_VIEWER), rolesBoundTo(Role.Scope.PATTERN));
        assertEquals(EnumSet.of(Role.CLUSTER_ADMIN, Role.SYSTEM_ADMIN), rolesBoundTo(Role.Scope.CLUSTER));
    }

    /** AC3. */
    @ParameterizedTest
    @EnumSource(Role.class)
    void aRoleIsFoundByItsExactName(Role role) {
        assertEquals(Optional.of(role), Role.named(role.roleName()));
    }

    /** AC3: the same lookups, one letter or one space off. */
    @ParameterizedTest
    @ValueSource(strings = {"developerread", "DEVELOPERREAD", "Developerread", "DEVELOPER_READ", " DeveloperRead",
            "DeveloperRead ", "Developer", "Member", "User", "Group", "*", ""})
    void aNameThatIsNotARoleIsNotFound(String name) {
        assertEquals(Optional.empty(), Role.named(name));
    }

    @Test
    void noNameIsNotARole() {
        assertEquals(Optional.empty(), Role.named(null));
    }

    @Test
    void theReservedNamesAreTheSevenRoles() {
        assertEquals(Set.of("DeveloperRead", "DeveloperWrite", "DeveloperManage", "ResourceOwner",
                "ResourceViewer", "ClusterAdmin", "SystemAdmin"), Role.NAMES);
        assertThrows(UnsupportedOperationException.class, () -> Role.NAMES.add("Admin"));
    }

    @Test
    void aRoleGrantsWhatItHasAndNothingElse() {
        assertTrue(Role.DEVELOPER_READ.grants(AclOperation.READ, ResourceType.TOPIC));
        assertFalse(Role.DEVELOPER_READ.grants(AclOperation.WRITE, ResourceType.TOPIC));
        assertTrue(Role.DEVELOPER_WRITE.grants(AclOperation.WRITE, ResourceType.TRANSACTIONAL_ID));
        assertFalse(Role.DEVELOPER_WRITE.grants(AclOperation.READ, ResourceType.GROUP));
        assertTrue(Role.DEVELOPER_READ.grants(SubjectOperation.READ));
        assertFalse(Role.DEVELOPER_READ.grants(SubjectOperation.WRITE));
        assertTrue(Role.DEVELOPER_MANAGE.grants(ConnectorOperation.PAUSE));
        assertFalse(Role.DEVELOPER_MANAGE.grants(ConnectorOperation.READ_CONFIG));
    }

    /** Only the four broker kinds of the table: not users, not delegation tokens, not even for SystemAdmin. */
    @ParameterizedTest
    @EnumSource(Role.class)
    void noRoleGrantsOnOtherBrokerResourceTypes(Role role) {
        for (ResourceType type : ResourceType.values()) {
            if (!BROKER_KINDS.contains(type)) {
                assertEquals(Set.of(), role.operations(type), role + " on " + type);
                for (AclOperation operation : AclOperation.values()) {
                    assertFalse(role.grants(operation, type), role + " " + operation + " on " + type);
                }
            }
        }
    }

    /** UNKNOWN, ANY and ALL are filter values, never a request: "all" in the table does not include them. */
    @Test
    void everyOperationIsNotTheFilterValues() {
        for (ResourceType type : BROKER_KINDS) {
            assertTrue(Role.SYSTEM_ADMIN.grants(AclOperation.CLUSTER_ACTION, type));
            assertFalse(Role.SYSTEM_ADMIN.grants(AclOperation.ALL, type));
            assertFalse(Role.SYSTEM_ADMIN.grants(AclOperation.ANY, type));
            assertFalse(Role.SYSTEM_ADMIN.grants(AclOperation.UNKNOWN, type));
        }
    }

    @ParameterizedTest
    @EnumSource(Role.class)
    void whatARoleGrantsCannotBeChanged(Role role) {
        assertThrows(UnsupportedOperationException.class,
                () -> role.operations(ResourceType.TOPIC).add(AclOperation.WRITE));
        assertThrows(UnsupportedOperationException.class,
                () -> role.subjectOperations().add(SubjectOperation.DELETE));
        assertThrows(UnsupportedOperationException.class,
                () -> role.connectorOperations().add(ConnectorOperation.DELETE));
    }

    /** AC4: one sentence per role on what it is for. */
    @Test
    void theDocsSayWhatEachRoleIsFor() throws IOException {
        String doc = Files.readString(ROLES_DOC, StandardCharsets.UTF_8);
        for (String name : Role.NAMES) {
            assertTrue(doc.contains("\n- `" + name + "`: "), name + " has no line of its own in docs/roles.md");
        }
    }

    /** AC5. */
    @Test
    void theReadmeNamesTheRolesAndLinksTheTable() throws IOException {
        String readme = Files.readString(README, StandardCharsets.UTF_8);
        assertTrue(readme.contains("(docs/roles.md)"), "README links docs/roles.md");
        for (String name : Role.NAMES) {
            assertTrue(readme.contains("`" + name + "`"), "README names " + name);
        }
    }

    private static Set<Role> rolesBoundTo(Role.Scope scope) {
        return Arrays.stream(Role.values()).filter(r -> r.scope() == scope)
                .collect(Collectors.toCollection(() -> EnumSet.noneOf(Role.class)));
    }

    /** The table of roles, by role name; cells as written, backticks around the name dropped. */
    private static Map<String, List<String>> rolesTable() throws IOException {
        Map<String, List<String>> rows = new LinkedHashMap<>();
        for (List<String> row : table("| " + String.join(" | ", HEADER) + " |")) {
            assertEquals(HEADER.size(), row.size(), "cells in " + row);
            String name = row.get(0).replace("`", "");
            assertEquals(null, rows.put(name, row), name + " twice in docs/roles.md");
        }
        return rows;
    }

    /** The rows of the table under the header line, without the separator line. */
    private static List<List<String>> table(String header) throws IOException {
        List<String> lines = Files.readAllLines(ROLES_DOC, StandardCharsets.UTF_8);
        int at = lines.indexOf(header);
        assertTrue(at >= 0, "no table '" + header + "' in docs/roles.md");
        List<List<String>> rows = new ArrayList<>();
        for (int i = at + 2; i < lines.size() && lines.get(i).startsWith("|"); i++) {
            String line = lines.get(i).trim();
            rows.add(Arrays.stream(line.substring(1, line.length() - 1).split("\\|"))
                    .map(String::trim).toList());
        }
        return rows;
    }

    /** A cell: "all", "—", or operations by their names in the docs, {@code DescribeConfigs} for {@code DESCRIBE_CONFIGS}. */
    private static <E extends Enum<E>> Set<E> operations(String cell, Class<E> type, Set<E> all) {
        if (cell.equals("all")) {
            return all;
        }
        if (cell.equals("—")) {
            return Set.of();
        }
        Function<String, E> byDocName = name -> Enum.valueOf(type,
                name.replaceAll("([a-z])([A-Z])", "$1_$2").toUpperCase(Locale.ROOT));
        EnumSet<E> set = EnumSet.noneOf(type);
        for (String name : cell.split(",")) {
            assertTrue(set.add(byDocName.apply(name.trim())), name + " twice in '" + cell + "'");
        }
        return set;
    }
}
