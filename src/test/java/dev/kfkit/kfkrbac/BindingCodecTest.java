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

import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.DELEGATION_TOKEN;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.apache.kafka.common.resource.ResourceType.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** An ACL record read as what it stands for, in both of the shapes it comes in. */
class BindingCodecTest {

    private static final KafkaPrincipal ALICE = new KafkaPrincipal("User", "alice");

    // --- well-formed bindings ---------------------------------------------------

    @Test
    void aTopicRecordNamesEveryTypeTheRoleGrants() {
        assertEquals(new RoleBinding(Role.DEVELOPER_WRITE, ALICE, RoleBinding.Target.NAMES, "billing.", PREFIXED),
                BindingCodec.decode(binding("DeveloperWrite:User:alice", TOPIC, "billing.", PREFIXED)));
    }

    @Test
    void aGroupOrTransactionalIdRecordIsNarrowedToThatType() {
        assertEquals(new RoleBinding(Role.DEVELOPER_READ, ALICE, RoleBinding.Target.GROUPS, "billing.", PREFIXED),
                BindingCodec.decode(binding("DeveloperRead:User:alice", GROUP, "billing.", PREFIXED)));
        assertEquals(new RoleBinding(Role.DEVELOPER_READ, ALICE, RoleBinding.Target.TRANSACTIONAL_IDS, "orders",
                LITERAL), BindingCodec.decode(binding("DeveloperRead:User:alice", TRANSACTIONAL_ID, "orders",
                        LITERAL)));
    }

    @Test
    void aClusterRecordBindsAClusterRole() {
        assertEquals(new RoleBinding(Role.CLUSTER_ADMIN, ALICE, RoleBinding.Target.CLUSTER, "kafka-cluster", LITERAL),
                BindingCodec.decode(binding("ClusterAdmin:User:alice", CLUSTER, "kafka-cluster", LITERAL)));
    }

    @Test
    void aNameWithItsTypeInFrontIsForSubjectsOrConnectors() {
        assertEquals(new RoleBinding(Role.DEVELOPER_READ, ALICE, RoleBinding.Target.SUBJECTS, "orders-", PREFIXED),
                BindingCodec.decode(binding("DeveloperRead:User:alice", TOPIC, "Subject:orders-", PREFIXED)));
        assertEquals(new RoleBinding(Role.DEVELOPER_MANAGE, ALICE, RoleBinding.Target.CONNECTORS, "billing-sink",
                LITERAL), BindingCodec.decode(binding("DeveloperManage:User:alice", TOPIC, "Connector:billing-sink",
                        LITERAL)));
    }

    @Test
    void theMemberIsATypeAndAName() {
        RoleBinding group = assertInstanceOf(RoleBinding.class,
                BindingCodec.decode(binding("DeveloperRead:Group:payments", TOPIC, "billing.", PREFIXED)));
        assertEquals(new KafkaPrincipal("Group", "payments"), group.member());

        RoleBinding colons = assertInstanceOf(RoleBinding.class,
                BindingCodec.decode(binding("DeveloperRead:User:CN=alice:1", TOPIC, "billing.", PREFIXED)));
        assertEquals(new KafkaPrincipal("User", "CN=alice:1"), colons.member());
    }

    // --- what is not a binding --------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {"User:alice", "User:*", "Group:payments", "DeveloperDelete:User:alice",
        "developerread:User:alice", "alice", ""})
    void aPrincipalTypeThatIsNotARoleIsAnOrdinaryAcl(String principal) {
        assertInstanceOf(BindingCodec.Ordinary.class,
                BindingCodec.decode(binding(principal, TOPIC, "billing.", PREFIXED)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Group:payments", "Group:*", "Group:a:b"})
    void anAllowAclOfAGroupIsAnOrdinaryAcl(String principal) {
        for (AclOperation operation : new AclOperation[] {READ, ALL}) {
            assertInstanceOf(BindingCodec.Ordinary.class,
                    BindingCodec.decode(acl(principal, TOPIC, "reports", LITERAL, "127.0.0.1", operation, ALLOW)));
        }
    }

    // --- group membership ---------------------------------------------------------

    @Test
    void aMemberRecordPutsItsMemberInTheGroupItNames() {
        assertEquals(new BindingCodec.Membership(ALICE, new KafkaPrincipal("Group", "payments")),
                BindingCodec.decode(binding("Member:User:alice", TOPIC, "Group:payments", LITERAL)));
        assertEquals(new BindingCodec.Membership(new KafkaPrincipal("User", "CN=alice:1"),
                new KafkaPrincipal("Group", "eu:payments")),
                BindingCodec.decode(binding("Member:User:CN=alice:1", TOPIC, "Group:eu:payments", LITERAL)));
    }

    static Stream<Arguments> malformedMemberships() {
        return Stream.of(
                Arguments.of("operation not ALL",
                        acl("Member:User:alice", TOPIC, "Group:payments", LITERAL, "*", READ, ALLOW)),
                Arguments.of("permission DENY",
                        acl("Member:User:alice", TOPIC, "Group:payments", LITERAL, "*", ALL, DENY)),
                Arguments.of("host not *",
                        acl("Member:User:alice", TOPIC, "Group:payments", LITERAL, "127.0.0.1", ALL, ALLOW)),
                Arguments.of("a group as the member",
                        binding("Member:Group:billing", TOPIC, "Group:payments", LITERAL)),
                Arguments.of("a member without a type",
                        binding("Member:alice", TOPIC, "Group:payments", LITERAL)),
                Arguments.of("a member without a name",
                        binding("Member:User:", TOPIC, "Group:payments", LITERAL)),
                Arguments.of("no member",
                        binding("Member:", TOPIC, "Group:payments", LITERAL)),
                Arguments.of("a name that is not a group",
                        binding("Member:User:alice", TOPIC, "payments", LITERAL)),
                Arguments.of("a group in another case",
                        binding("Member:User:alice", TOPIC, "group:payments", LITERAL)),
                Arguments.of("a group without a name",
                        binding("Member:User:alice", TOPIC, "Group:", LITERAL)),
                Arguments.of("an empty name",
                        binding("Member:User:alice", TOPIC, "", LITERAL)),
                Arguments.of("a prefix of groups",
                        binding("Member:User:alice", TOPIC, "Group:pay", PREFIXED)),
                Arguments.of("a pattern type no record has",
                        binding("Member:User:alice", TOPIC, "Group:payments", PatternType.MATCH)),
                Arguments.of("a group record",
                        binding("Member:User:alice", GROUP, "Group:payments", LITERAL)),
                Arguments.of("the cluster",
                        binding("Member:User:alice", CLUSTER, "kafka-cluster", LITERAL)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedMemberships")
    void aMemberRecordThatTheFormatDoesNotAllowIsMalformed(String shape, StandardAcl record) {
        BindingCodec.Malformed malformed = assertInstanceOf(BindingCodec.Malformed.class,
                BindingCodec.decode(record));
        assertEquals(BindingCodec.Malformed.Kind.MEMBERSHIP, malformed.kind());
        assertFalse(malformed.reason().isBlank());
    }

    @Test
    void aGroupIsNotAMemberOfAGroupAndTheRecordSaysSo() {
        BindingCodec.Malformed nested = assertInstanceOf(BindingCodec.Malformed.class,
                BindingCodec.decode(binding("Member:Group:billing", TOPIC, "Group:payments", LITERAL)));
        assertEquals("the member is a group, and a group is not a member of a group", nested.reason());
    }

    @Test
    void aDenyAclOfAGroupIsMalformedAndOfAUserIsNot() {
        BindingCodec.Malformed deny = assertInstanceOf(BindingCodec.Malformed.class,
                BindingCodec.decode(acl("Group:payments", TOPIC, "reports", LITERAL, "*", READ, DENY)));
        assertEquals(BindingCodec.Malformed.Kind.GROUP_DENY, deny.kind());
        assertTrue(deny.reason().contains("deny the users by name"), deny.reason());

        assertInstanceOf(BindingCodec.Ordinary.class,
                BindingCodec.decode(acl("User:alice", TOPIC, "reports", LITERAL, "*", READ, DENY)));
        assertInstanceOf(BindingCodec.Ordinary.class,
                BindingCodec.decode(acl("Groups:payments", TOPIC, "reports", LITERAL, "*", READ, DENY)));
    }

    static Stream<Arguments> malformed() {
        return Stream.of(
                Arguments.of("operation not ALL",
                        acl("DeveloperRead:User:alice", TOPIC, "billing.", PREFIXED, "*", READ, ALLOW)),
                Arguments.of("permission DENY",
                        acl("DeveloperRead:User:alice", TOPIC, "billing.", PREFIXED, "*", ALL, DENY)),
                Arguments.of("host not *",
                        acl("DeveloperRead:User:alice", TOPIC, "billing.", PREFIXED, "127.0.0.1", ALL, ALLOW)),
                Arguments.of("a cluster role on a name pattern",
                        binding("ClusterAdmin:User:alice", TOPIC, "billing.", PREFIXED)),
                Arguments.of("a cluster role on a group pattern",
                        binding("SystemAdmin:User:alice", GROUP, "*", LITERAL)),
                Arguments.of("a pattern role on the cluster",
                        binding("DeveloperRead:User:alice", CLUSTER, "kafka-cluster", LITERAL)),
                Arguments.of("a cluster of another name",
                        binding("ClusterAdmin:User:alice", CLUSTER, "billing", LITERAL)),
                Arguments.of("a prefix of the cluster's name",
                        binding("ClusterAdmin:User:alice", CLUSTER, "kafka-cluster", PREFIXED)),
                Arguments.of("a member without a type",
                        binding("DeveloperRead:alice", TOPIC, "billing.", PREFIXED)),
                Arguments.of("a member without a name",
                        binding("DeveloperRead:User:", TOPIC, "billing.", PREFIXED)),
                Arguments.of("a member with an empty type",
                        binding("DeveloperRead::alice", TOPIC, "billing.", PREFIXED)),
                Arguments.of("no member",
                        binding("DeveloperRead:", TOPIC, "billing.", PREFIXED)),
                Arguments.of("a delegation token",
                        binding("ResourceOwner:User:alice", DELEGATION_TOKEN, "billing.", PREFIXED)),
                Arguments.of("a user resource",
                        binding("SystemAdmin:User:alice", USER, "alice", LITERAL)),
                Arguments.of("an empty name",
                        binding("DeveloperRead:User:alice", TOPIC, "", PREFIXED)),
                Arguments.of("a pattern type no record has",
                        binding("DeveloperRead:User:alice", TOPIC, "billing.", PatternType.MATCH)),
                Arguments.of("subjects on a group record",
                        binding("DeveloperRead:User:alice", GROUP, "Subject:orders-", PREFIXED)),
                Arguments.of("connectors on a transactional ID record",
                        binding("DeveloperRead:User:alice", TRANSACTIONAL_ID, "Connector:billing-", PREFIXED)),
                Arguments.of("subjects without a name",
                        binding("DeveloperRead:User:alice", TOPIC, "Subject:", PREFIXED)),
                Arguments.of("a name reserved for a group",
                        binding("DeveloperRead:User:alice", TOPIC, "Group:payments", LITERAL)),
                Arguments.of("a prefix reserved for groups",
                        binding("ResourceOwner:User:alice", TOPIC, "Group:", PREFIXED)),
                Arguments.of("a name reserved for a group on a group record",
                        binding("DeveloperRead:User:alice", GROUP, "Group:payments", LITERAL)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformed")
    void aRecordOfARoleThatTheFormatDoesNotAllowIsMalformed(String shape, StandardAcl record) {
        BindingCodec.Malformed malformed = assertInstanceOf(BindingCodec.Malformed.class,
                BindingCodec.decode(record));
        assertEquals(BindingCodec.Malformed.Kind.BINDING, malformed.kind());
        assertFalse(malformed.reason().isBlank());
    }

    @Test
    void aNameThatStartsWithGroupIsReservedAndSaysSo() {
        BindingCodec.Malformed reserved = assertInstanceOf(BindingCodec.Malformed.class,
                BindingCodec.decode(binding("DeveloperRead:User:alice", TOPIC, "Group:payments", LITERAL)));
        assertTrue(reserved.reason().contains("Group:"), reserved.reason());

        // Without the colon it is a name like any other, as with Subject and Connector.
        assertEquals(new RoleBinding(Role.DEVELOPER_READ, ALICE, RoleBinding.Target.NAMES, "Group", PREFIXED),
                BindingCodec.decode(binding("DeveloperRead:User:alice", TOPIC, "Group", PREFIXED)));
    }

    // --- the two shapes ----------------------------------------------------------

    static Stream<StandardAcl> records() {
        // A pattern of type MATCH is a filter: no client can list a record that has it.
        Stream<StandardAcl> malformed = Stream.concat(malformed(), malformedMemberships())
                .map(arguments -> (StandardAcl) arguments.get()[1])
                .filter(record -> record.patternType() != PatternType.MATCH);
        return Stream.concat(malformed, Stream.of(
                binding("DeveloperWrite:User:alice", TOPIC, "billing.", PREFIXED),
                binding("DeveloperRead:User:alice", GROUP, "*", LITERAL),
                binding("SystemAdmin:Group:payments", CLUSTER, "kafka-cluster", LITERAL),
                binding("ResourceOwner:User:alice", TOPIC, "Subject:orders-", PREFIXED),
                binding("Member:User:alice", TOPIC, "Group:payments", LITERAL),
                acl("Group:payments", TOPIC, "reports", LITERAL, "*", READ, ALLOW),
                acl("Group:payments", TOPIC, "reports", LITERAL, "*", READ, DENY),
                acl("User:alice", TOPIC, "orders", LITERAL, "*", READ, ALLOW)));
    }

    @ParameterizedTest
    @MethodSource("records")
    void whatAClientListsIsReadAsWhatTheBrokerDelivers(StandardAcl record) {
        AclBinding listed = new AclBinding(
                new ResourcePattern(record.resourceType(), record.resourceName(), record.patternType()),
                new AccessControlEntry(record.principal(), record.host(), record.operation(),
                        record.permissionType()));

        assertEquals(BindingCodec.decode(record), BindingCodec.decode(listed));
    }

    private static StandardAcl binding(String principal, ResourceType type, String name, PatternType patternType) {
        return acl(principal, type, name, patternType, "*", ALL, ALLOW);
    }

    private static StandardAcl acl(String principal, ResourceType type, String name, PatternType patternType,
            String host, AclOperation operation, AclPermissionType permission) {
        return new StandardAcl(type, name, patternType, principal, host, operation, permission);
    }
}
