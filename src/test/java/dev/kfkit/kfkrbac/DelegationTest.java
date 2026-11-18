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

import static dev.kfkit.kfkrbac.Delegation.Outcome.ALLOWED;
import static dev.kfkit.kfkrbac.Delegation.Outcome.FORBIDDEN;
import static dev.kfkit.kfkrbac.Delegation.Outcome.MALFORMED;
import static dev.kfkit.kfkrbac.Delegation.Outcome.WITH_THE_REQUEST;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** The rules of who may create and delete which record, without an authorizer. */
class DelegationTest {

    private static final KafkaPrincipal CAROL = new KafkaPrincipal("User", "carol");

    private static final AclBinding OWNS_BILLING = binding("ResourceOwner:User:carol", TOPIC, "billing.", PREFIXED);
    private static final AclBinding ALICE_READS_BILLING =
            binding("DeveloperRead:User:alice", TOPIC, "billing.", PREFIXED);
    private static final AclBinding ALICE_READS_PAYMENTS =
            binding("DeveloperRead:User:alice", TOPIC, "payments.", PREFIXED);
    private static final AclBinding BOB_READS_ORDERS = new AclBinding(
            new ResourcePattern(TOPIC, "billing.orders", LITERAL),
            new AccessControlEntry("User:bob", "*", AclOperation.READ, AclPermissionType.ALLOW));
    private static final AclBinding ALICE_IN_PAYMENTS = binding("Member:User:alice", TOPIC, "Group:payments", LITERAL);
    private static final AclBinding ALICE_ADMINISTERS =
            binding("ClusterAdmin:User:alice", CLUSTER, "kafka-cluster", LITERAL);
    private static final AclBinding NOT_A_BINDING = new AclBinding(
            new ResourcePattern(TOPIC, "billing.", PREFIXED),
            new AccessControlEntry("DeveloperRead:User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW));

    // --- what a ResourceOwner binding covers --------------------------------------

    static Stream<Arguments> covered() {
        return Stream.of(
                Arguments.of(TOPIC, "billing.", PREFIXED, TOPIC, "billing.", PREFIXED),
                Arguments.of(TOPIC, "billing.", PREFIXED, TOPIC, "billing.eu.", PREFIXED),
                Arguments.of(TOPIC, "billing.", PREFIXED, TOPIC, "billing.orders", LITERAL),
                Arguments.of(TOPIC, "billing.", PREFIXED, TOPIC, "billing.", LITERAL),
                Arguments.of(TOPIC, "billing.", PREFIXED, GROUP, "billing.invoicing", LITERAL),
                Arguments.of(TOPIC, "billing.", PREFIXED, TRANSACTIONAL_ID, "billing.", PREFIXED),
                Arguments.of(TOPIC, "billing.orders", LITERAL, TOPIC, "billing.orders", LITERAL),
                Arguments.of(TOPIC, "billing.orders", LITERAL, GROUP, "billing.orders", LITERAL),
                Arguments.of(TOPIC, "*", LITERAL, TOPIC, "*", LITERAL),
                Arguments.of(TOPIC, "*", PREFIXED, TOPIC, "*orders", LITERAL),
                Arguments.of(GROUP, "billing.", PREFIXED, GROUP, "billing.eu.", PREFIXED),
                Arguments.of(TRANSACTIONAL_ID, "billing.", PREFIXED, TRANSACTIONAL_ID, "billing.tx", LITERAL),
                Arguments.of(TOPIC, "Subject:orders-", PREFIXED, TOPIC, "Subject:orders-value", LITERAL),
                Arguments.of(TOPIC, "Connector:billing-", PREFIXED, TOPIC, "Connector:billing-sink", LITERAL));
    }

    @ParameterizedTest(name = "{0} {2} {1} covers {3} {5} {4}")
    @MethodSource("covered")
    void anOwnerBindingCovers(ResourceType ownerType, String ownerName, PatternType ownerPattern,
            ResourceType type, String name, PatternType pattern) {
        assertTrue(Delegation.covers(read(binding("ResourceOwner:User:carol", ownerType, ownerName, ownerPattern)),
                read(binding("DeveloperRead:User:alice", type, name, pattern))));
    }

    static Stream<Arguments> notCovered() {
        return Stream.of(
                // another name, a shorter name, and the name that is every name
                Arguments.of(TOPIC, "billing.", PREFIXED, TOPIC, "payments.", PREFIXED),
                Arguments.of(TOPIC, "billing.", PREFIXED, TOPIC, "bill", PREFIXED),
                Arguments.of(TOPIC, "billing.", PREFIXED, TOPIC, "billing", LITERAL),
                Arguments.of(TOPIC, "billing.", PREFIXED, TOPIC, "*", LITERAL),
                Arguments.of(TOPIC, "*", PREFIXED, TOPIC, "*", LITERAL),
                Arguments.of(TOPIC, "*", PREFIXED, GROUP, "*", LITERAL),
                // a literal owner covers its own literal name and nothing else
                Arguments.of(TOPIC, "billing.orders", LITERAL, TOPIC, "billing.orders", PREFIXED),
                Arguments.of(TOPIC, "billing.orders", LITERAL, TOPIC, "billing.orders.eu", LITERAL),
                Arguments.of(TOPIC, "*", LITERAL, TOPIC, "billing.orders", LITERAL),
                Arguments.of(TOPIC, "*", LITERAL, TOPIC, "billing.", PREFIXED),
                // a narrowed owner covers only bindings narrowed to its type
                Arguments.of(GROUP, "billing.", PREFIXED, TOPIC, "billing.", PREFIXED),
                Arguments.of(GROUP, "billing.", PREFIXED, TRANSACTIONAL_ID, "billing.", PREFIXED),
                Arguments.of(TRANSACTIONAL_ID, "billing.", PREFIXED, GROUP, "billing.", PREFIXED),
                // subjects and connectors are not topics, nor each other
                Arguments.of(TOPIC, "orders-", PREFIXED, TOPIC, "Subject:orders-value", LITERAL),
                Arguments.of(TOPIC, "Sub", PREFIXED, TOPIC, "Subject:orders-value", LITERAL),
                Arguments.of(TOPIC, "Subject:orders-", PREFIXED, TOPIC, "orders-value", LITERAL),
                Arguments.of(TOPIC, "Subject:orders-", PREFIXED, TOPIC, "Connector:orders-sink", LITERAL),
                Arguments.of(TOPIC, "Subject:orders-", PREFIXED, GROUP, "orders-1", LITERAL),
                Arguments.of(TOPIC, "Subject:orders-", PREFIXED, TOPIC, "Subject:*", LITERAL));
    }

    @ParameterizedTest(name = "{0} {2} {1} does not cover {3} {5} {4}")
    @MethodSource("notCovered")
    void anOwnerBindingDoesNotCover(ResourceType ownerType, String ownerName, PatternType ownerPattern,
            ResourceType type, String name, PatternType pattern) {
        assertFalse(Delegation.covers(read(binding("ResourceOwner:User:carol", ownerType, ownerName, ownerPattern)),
                read(binding("DeveloperRead:User:alice", type, name, pattern))));
    }

    @Test
    void onlyAResourceOwnerBindingCoversAnything() {
        for (String role : List.of("DeveloperRead", "DeveloperWrite", "DeveloperManage", "ResourceViewer")) {
            assertFalse(Delegation.covers(read(binding(role + ":User:carol", TOPIC, "billing.", PREFIXED)),
                    read(ALICE_READS_BILLING)), role);
        }
        assertFalse(Delegation.covers(read(binding("SystemAdmin:User:carol", CLUSTER, "kafka-cluster", LITERAL)),
                read(ALICE_READS_BILLING)));
    }

    @Test
    void nothingCoversABindingToTheCluster() {
        assertFalse(Delegation.covers(read(binding("ResourceOwner:User:carol", TOPIC, "*", LITERAL)),
                read(ALICE_ADMINISTERS)));
        assertFalse(Delegation.covers(read(binding("ResourceOwner:User:carol", TOPIC, "kafka-", PREFIXED)),
                read(ALICE_ADMINISTERS)));
    }

    // --- creating -------------------------------------------------------------------

    @Test
    void whoeverTheAclsAllowAndASystemAdminCreateAnythingWellFormed() {
        List<AclBinding> records = List.of(ALICE_READS_BILLING, BOB_READS_ORDERS, ALICE_IN_PAYMENTS,
                ALICE_ADMINISTERS);
        for (Delegation.Caller caller : List.of(allowedByTheAcls(), holding(systemAdmin()))) {
            Delegation.Decision decision = Delegation.create(caller, records);

            assertTrue(decision.allowed());
            assertEquals(List.of(ALLOWED, ALLOWED, ALLOWED, ALLOWED), outcomes(decision));
            assertTrue(decision.refusal().isEmpty());
        }
    }

    @Test
    void aMalformedBindingIsRefusedWhoeverAsks() {
        for (Delegation.Caller caller : List.of(allowedByTheAcls(), holding(systemAdmin()), holding(OWNS_BILLING),
                holding())) {
            Delegation.Decision decision = Delegation.create(caller, List.of(NOT_A_BINDING));

            assertFalse(decision.allowed());
            assertEquals(List.of(MALFORMED), outcomes(decision));
            String reason = decision.answers().get(0).reason();
            assertTrue(reason.contains("DeveloperRead:User:alice on the TOPIC prefix 'billing.'"), reason);
            assertTrue(reason.contains("the operation is READ, a binding has ALL"), reason);
        }
    }

    @Test
    void anOwnerCreatesBindingsInsideWhatItOwns() {
        Delegation.Decision decision = Delegation.create(holding(OWNS_BILLING), List.of(ALICE_READS_BILLING,
                binding("ResourceOwner:User:dave", TOPIC, "billing.eu.", PREFIXED),
                binding("DeveloperWrite:Group:payments", TOPIC, "billing.orders", LITERAL)));

        assertTrue(decision.allowed());
    }

    @Test
    void anOwnerIsRefusedOutsideWhatItOwnsAndToldWhatItOwns() {
        Delegation.Decision decision = Delegation.create(
                holding(OWNS_BILLING, binding("ResourceOwner:User:carol", GROUP, "invoicing-", PREFIXED)),
                List.of(ALICE_READS_PAYMENTS));

        assertEquals(List.of(FORBIDDEN), outcomes(decision));
        assertEquals("User:carol may not create DeveloperRead:User:alice on the TOPIC prefix 'payments.': none of"
                + " the ResourceOwner bindings of User:carol covers that pattern; it owns the TOPIC prefix"
                + " 'billing.', the GROUP prefix 'invoicing-'", decision.answers().get(0).reason());
    }

    @Test
    void anOwnerOfManyPatternsIsShownAFewOfThem() {
        List<AclBinding> held = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            held.add(binding("ResourceOwner:User:carol", TOPIC, "team" + i + ".", PREFIXED));
        }
        String reason = Delegation.create(holding(held.toArray(new AclBinding[0])), List.of(ALICE_READS_PAYMENTS))
                .answers().get(0).reason();

        assertTrue(reason.endsWith("the TOPIC prefix 'team4.' and 3 more"), reason);
    }

    @Test
    void anOwnerCreatesNoClusterBindingNoMembershipAndNoOrdinaryAcl() {
        Delegation.Caller owner = holding(OWNS_BILLING);

        String cluster = onlyRefusal(Delegation.create(owner, List.of(ALICE_ADMINISTERS)));
        assertTrue(cluster.startsWith("User:carol may not create ClusterAdmin:User:alice on the CLUSTER name"
                + " 'kafka-cluster': ClusterAdmin is bound to the cluster"), cluster);

        String member = onlyRefusal(Delegation.create(owner, List.of(ALICE_IN_PAYMENTS)));
        assertTrue(member.startsWith("User:carol may not create Member:User:alice on the TOPIC name"
                + " 'Group:payments': a record of group membership"), member);

        String ordinary = onlyRefusal(Delegation.create(owner, List.of(BOB_READS_ORDERS)));
        assertTrue(ordinary.startsWith("User:carol may not create the ACL User:bob ALLOW READ from host * on the"
                + " TOPIC name 'billing.orders': it is an ordinary ACL"), ordinary);
    }

    @Test
    void whoeverHoldsNeitherRoleCreatesNothing() {
        for (Delegation.Caller caller : List.of(holding(), holding(ALICE_READS_BILLING),
                holding(binding("DeveloperManage:User:carol", TOPIC, "billing.", PREFIXED)),
                holding(binding("ClusterAdmin:User:carol", CLUSTER, "kafka-cluster", LITERAL)))) {
            for (AclBinding record : List.of(ALICE_READS_BILLING, BOB_READS_ORDERS, ALICE_IN_PAYMENTS,
                    ALICE_ADMINISTERS)) {
                String reason = onlyRefusal(Delegation.create(caller, List.of(record)));
                assertTrue(reason.startsWith("User:carol may not create "), reason);
                assertTrue(reason.endsWith("User:carol is none of them"), reason);
            }
        }
    }

    /**
     * Both ways. A member of a group that an ACL lets alter the cluster is
     * told that the ACL is there and why it does not count; a caller without
     * such a group is told nothing of groups.
     */
    @Test
    void aGroupThatAnAclLetsAlterTheClusterIsNamedInTheReason() {
        KafkaPrincipal admins = new KafkaPrincipal("Group", "admins");
        String byAGroup = "; an ACL of its group Group:admins allows it ALTER on the cluster, but an ACL of a group"
                + " does not make its members managers of access";
        for (List<AclBinding> held : List.of(List.<AclBinding>of(), List.of(OWNS_BILLING))) {
            Delegation.Caller member = new Delegation.Caller(CAROL, false,
                    held.stream().map(DelegationTest::read).toList(), admins);
            Delegation.Caller nobody = new Delegation.Caller(CAROL, false,
                    held.stream().map(DelegationTest::read).toList());
            for (AclBinding record : List.of(BOB_READS_ORDERS, ALICE_IN_PAYMENTS, ALICE_ADMINISTERS)) {
                String told = onlyRefusal(Delegation.create(member, List.of(record)));
                assertTrue(told.endsWith(byAGroup), told);
                assertEquals(onlyRefusal(Delegation.create(nobody, List.of(record))) + byAGroup, told);

                String deleting = Delegation.delete(member, List.of(List.of(record))).answers().get(0).reason();
                assertTrue(deleting.endsWith(byAGroup), deleting);
                assertEquals(Delegation.delete(nobody, List.of(List.of(record))).answers().get(0).reason() + byAGroup,
                        deleting);
            }
            String notTold = onlyRefusal(Delegation.create(nobody, List.of(BOB_READS_ORDERS)));
            assertFalse(notTold.contains("group"), notTold);
        }
        // What the group does not give, the owner's own binding still does.
        Delegation.Caller owner = new Delegation.Caller(CAROL, false, List.of(read(OWNS_BILLING)), admins);
        assertTrue(Delegation.create(owner, List.of(ALICE_READS_BILLING)).allowed());
    }

    @Test
    void oneRefusedRecordRefusesTheRequestAndEveryAnswerSaysWhy() {
        Delegation.Decision decision = Delegation.create(holding(OWNS_BILLING),
                List.of(ALICE_READS_BILLING, ALICE_READS_PAYMENTS, NOT_A_BINDING));

        assertFalse(decision.allowed());
        assertEquals(List.of(WITH_THE_REQUEST, FORBIDDEN, MALFORMED), outcomes(decision));
        String refused = decision.answers().get(1).reason();
        assertEquals(refused, decision.refusal().orElseThrow().reason());
        assertEquals("Not created, because the request was refused as a whole: " + refused,
                decision.answers().get(0).reason());
    }

    @Test
    void anEmptyRequestIsAllowed() {
        assertTrue(Delegation.create(holding(), List.of()).allowed());
        assertTrue(Delegation.delete(holding(), List.of()).allowed());
    }

    // --- deleting -------------------------------------------------------------------

    @Test
    void whoeverTheAclsAllowAndASystemAdminDeleteWhateverTheirFiltersMatch() {
        for (Delegation.Caller caller : List.of(allowedByTheAcls(), holding(systemAdmin()))) {
            assertTrue(Delegation.delete(caller, List.of(List.of(ALICE_READS_BILLING, BOB_READS_ORDERS,
                    ALICE_IN_PAYMENTS, ALICE_ADMINISTERS, NOT_A_BINDING), List.of())).allowed());
        }
    }

    @Test
    void anOwnerDeletesBindingsItCouldHaveCreated() {
        assertTrue(Delegation.delete(holding(OWNS_BILLING), List.of(List.of(ALICE_READS_BILLING,
                binding("ResourceOwner:User:dave", TOPIC, "billing.eu.", PREFIXED)), List.of())).allowed());
    }

    @Test
    void anOwnerDeletesNothingWhenAFilterMatchesWhatItCouldNotHaveCreated() {
        Delegation.Decision decision = Delegation.delete(holding(OWNS_BILLING), List.of(
                List.of(ALICE_READS_BILLING),
                List.of(ALICE_READS_BILLING, ALICE_READS_PAYMENTS, BOB_READS_ORDERS, NOT_A_BINDING),
                List.of()));

        assertFalse(decision.allowed());
        assertEquals(List.of(WITH_THE_REQUEST, FORBIDDEN, WITH_THE_REQUEST), outcomes(decision));
        String refused = decision.answers().get(1).reason();
        assertEquals("User:carol may not delete DeveloperRead:User:alice on the TOPIC prefix 'payments.', which the"
                + " filter matches: none of the ResourceOwner bindings of User:carol covers that pattern; it owns"
                + " the TOPIC prefix 'billing.' (and 2 more of the 4 records the filter matches)", refused);
        assertEquals("Nothing deleted, because the request was refused as a whole: " + refused,
                decision.answers().get(0).reason());
    }

    @Test
    void anOwnerDeletesNoOrdinaryAclNoMembershipNoClusterBindingAndNoMalformedRecord() {
        Delegation.Caller owner = holding(OWNS_BILLING);
        for (AclBinding record : List.of(BOB_READS_ORDERS, ALICE_IN_PAYMENTS, ALICE_ADMINISTERS, NOT_A_BINDING)) {
            String reason = onlyRefusal(Delegation.delete(owner, List.of(List.of(record))));
            assertTrue(reason.startsWith("User:carol may not delete "), reason);
            assertTrue(reason.endsWith("a principal that the ACLs allow ALTER on the cluster, as they do a super"
                    + " user, or a SystemAdmin"), reason);
        }
    }

    @Test
    void whoeverHoldsNeitherRoleDeletesNothingEvenWhenNothingMatches() {
        for (Delegation.Caller caller : List.of(holding(), holding(ALICE_READS_BILLING))) {
            Delegation.Decision decision = Delegation.delete(caller, List.of(List.of(ALICE_READS_BILLING),
                    List.of()));

            assertEquals(List.of(FORBIDDEN, FORBIDDEN), outcomes(decision));
            assertTrue(decision.answers().get(1).reason().startsWith("User:carol may not delete ACLs or role"
                    + " bindings: "), decision.answers().get(1).reason());
        }
    }

    // --- helpers --------------------------------------------------------------------

    /** Carol as a super user is, or as an ACL that allows her {@code ALTER} on the cluster makes her. */
    private static Delegation.Caller allowedByTheAcls() {
        return new Delegation.Caller(CAROL, true, List.of());
    }

    /** Carol, who is no super user, with these bindings applying to her. */
    private static Delegation.Caller holding(AclBinding... records) {
        List<RoleBinding> held = new ArrayList<>();
        for (AclBinding record : records) {
            held.add(read(record));
        }
        return new Delegation.Caller(CAROL, false, held);
    }

    private static AclBinding systemAdmin() {
        return binding("SystemAdmin:User:carol", CLUSTER, "kafka-cluster", LITERAL);
    }

    private static RoleBinding read(AclBinding record) {
        return assertInstanceOf(RoleBinding.class, BindingCodec.decode(record));
    }

    private static List<Delegation.Outcome> outcomes(Delegation.Decision decision) {
        return decision.answers().stream().map(Delegation.Answer::outcome).toList();
    }

    /** The reason of a request of one record, or one filter, that was refused for being forbidden. */
    private static String onlyRefusal(Delegation.Decision decision) {
        assertFalse(decision.allowed());
        assertEquals(List.of(FORBIDDEN), outcomes(decision));
        return decision.answers().get(0).reason();
    }

    private static AclBinding binding(String principal, ResourceType type, String name, PatternType patternType) {
        return new AclBinding(new ResourcePattern(type, name, patternType),
                new AccessControlEntry(principal, "*", AclOperation.ALL, AclPermissionType.ALLOW));
    }
}
