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

import static dev.kfkit.kfkrbac.RbacAuthorizerTest.acl;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.allow;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.deny;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.literal;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.prefixed;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.stderrOf;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Groups: the authorizer as the broker creates it, fed the way the broker
 * feeds it and asked the way a broker and a controller ask it, with the real
 * {@link StandardAuthorizer} underneath, a {@link RecordingMutator} where the
 * controller would be, and no broker.
 *
 * <p>{@code User:alice} is in {@code Group:payments} and {@code User:bob} is
 * in no group. {@code User:admin} is the super user, {@code User:erin} holds
 * {@code SystemAdmin}, {@code User:frank} holds {@code ClusterAdmin},
 * {@code User:carol} holds {@code ResourceOwner} on the prefix
 * {@code billing.}, and an ordinary ACL allows {@code User:grace}
 * {@code ALTER} on the cluster.
 */
class RbacAuthorizerGroupsTest {

    private static final String ORDERS = "billing.orders";
    private static final String INVOICES = "billing.invoices";
    private static final String REPORTS = "reports";
    private static final ResourcePattern BILLING = prefixed(TOPIC, "billing.");
    private static final ResourcePattern THE_CLUSTER = literal(CLUSTER, "kafka-cluster");

    private static final StandardAcl ALICE_IN_PAYMENTS = member("User:alice", "Group:payments");
    private static final StandardAcl PAYMENTS_READS_BILLING = binding("DeveloperRead:Group:payments", BILLING);
    private static final StandardAcl PAYMENTS_READS_REPORTS = allow("Group:payments", literal(TOPIC, REPORTS), READ);

    private static final StandardAcl ERIN_ADMINISTERS = binding("SystemAdmin:User:erin", THE_CLUSTER);
    private static final StandardAcl FRANK_RUNS_THE_CLUSTER = binding("ClusterAdmin:User:frank", THE_CLUSTER);
    private static final StandardAcl CAROL_OWNS_BILLING = binding("ResourceOwner:User:carol", BILLING);
    private static final StandardAcl GRACE_ALTERS_THE_CLUSTER = allow("User:grace", THE_CLUSTER, ALTER);

    private static final String NOT_A_MEMBERSHIP = "is not a group membership";
    private static final String A_DENY_FOR_A_GROUP = "is a DENY for a group";

    /** Where the authorizer registers its plugin metrics, as the broker would have it. */
    private Metrics metrics;
    private RbacAuthorizer authorizer;
    private RecordingMutator mutator;

    @AfterEach
    void close() {
        if (authorizer == null) {
            return;
        }
        try {
            authorizer.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            authorizer = null;
            metrics.close();
        }
    }

    // --- AC1: a role bound to a group -------------------------------------------------------

    @Test
    void aRoleBoundToAGroupIsHeldByItsMembersUntilTheMembershipIsRemoved() {
        start(PAYMENTS_READS_BILLING);
        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));

        Uuid membership = Uuid.randomUuid();
        authorizer.addAcl(membership, ALICE_IN_PAYMENTS);

        assertEquals(ALLOWED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("alice", READ, GROUP, "billing.invoicing"));
        assertEquals(DENIED, can("alice", WRITE, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", READ, TOPIC, "payments.orders"));
        assertEquals(DENIED, can("bob", READ, TOPIC, ORDERS));

        authorizer.removeAcl(membership);
        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
    }

    @Test
    void inASnapshotTheSameUntilASnapshotWithoutTheMembership() {
        start(ALICE_IN_PAYMENTS, PAYMENTS_READS_BILLING);

        assertEquals(ALLOWED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(DENIED, can("bob", READ, TOPIC, ORDERS));

        authorizer.loadSnapshot(snapshot(PAYMENTS_READS_BILLING, member("User:bob", "Group:payments")));

        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("bob", READ, TOPIC, ORDERS));
    }

    @Test
    void aMemberOfAnotherGroupAndTheGroupItselfHoldNothing() {
        start(PAYMENTS_READS_BILLING, member("User:alice", "Group:invoicing"), member("User:bob", "Group:Payments"));

        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(DENIED, can("bob", READ, TOPIC, ORDERS));
        // A user who happens to be called like the group is not the group.
        assertEquals(DENIED, can("payments", READ, TOPIC, ORDERS));
        assertEquals(DENIED, can("Group:payments", READ, TOPIC, ORDERS));
    }

    @Test
    void aMemberOfTwoGroupsHoldsWhatEitherHoldsAndItsOwn() {
        start(ALICE_IN_PAYMENTS, member("User:alice", "Group:invoicing"), PAYMENTS_READS_BILLING,
                binding("DeveloperWrite:Group:invoicing", prefixed(TOPIC, "invoices.")),
                binding("DeveloperRead:User:alice", prefixed(TOPIC, "audit.")));

        assertEquals(ALLOWED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("alice", WRITE, TOPIC, "invoices.eu"));
        assertEquals(ALLOWED, can("alice", READ, TOPIC, "audit.log"));
        assertEquals(DENIED, can("alice", WRITE, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", READ, TOPIC, "invoices.eu"));
    }

    // --- AC2: an ordinary ACL that names a group ----------------------------------------------

    @Test
    void anAllowAclOfAGroupAllowsItsMembersUntilItOrTheMembershipIsRemoved() {
        start();
        Uuid membership = Uuid.randomUuid();
        Uuid reports = Uuid.randomUuid();
        authorizer.addAcl(reports, PAYMENTS_READS_REPORTS);
        assertEquals(DENIED, can("alice", READ, TOPIC, REPORTS));

        authorizer.addAcl(membership, ALICE_IN_PAYMENTS);

        assertEquals(ALLOWED, can("alice", READ, TOPIC, REPORTS));
        assertEquals(DENIED, can("alice", WRITE, TOPIC, REPORTS));
        assertEquals(DENIED, can("alice", READ, TOPIC, "reports.eu"));
        assertEquals(DENIED, can("alice", READ, GROUP, REPORTS));
        assertEquals(DENIED, can("bob", READ, TOPIC, REPORTS));

        authorizer.removeAcl(reports);
        assertEquals(DENIED, can("alice", READ, TOPIC, REPORTS));

        authorizer.addAcl(reports, PAYMENTS_READS_REPORTS);
        assertEquals(ALLOWED, can("alice", READ, TOPIC, REPORTS));
        authorizer.removeAcl(membership);
        assertEquals(DENIED, can("alice", READ, TOPIC, REPORTS));
    }

    @Test
    void inASnapshotTheSameUntilASnapshotWithoutTheAcl() {
        start(ALICE_IN_PAYMENTS, PAYMENTS_READS_REPORTS);

        assertEquals(ALLOWED, can("alice", READ, TOPIC, REPORTS));
        assertEquals(DENIED, can("bob", READ, TOPIC, REPORTS));

        authorizer.loadSnapshot(snapshot(ALICE_IN_PAYMENTS, allow("Group:invoicing", literal(TOPIC, REPORTS), READ)));
        assertEquals(DENIED, can("alice", READ, TOPIC, REPORTS));
    }

    @Test
    void theHostOfAnAclOfAGroupIsHonoured() throws UnknownHostException {
        start(ALICE_IN_PAYMENTS, acl("Group:payments", literal(TOPIC, REPORTS), READ, "10.0.0.7", ALLOW));
        InetAddress named = InetAddress.getByName("10.0.0.7");
        InetAddress other = InetAddress.getByName("10.0.0.8");

        assertEquals(ALLOWED, from(named, "alice", READ, TOPIC, REPORTS));
        assertEquals(DENIED, from(other, "alice", READ, TOPIC, REPORTS));
        assertEquals(DENIED, from(named, "bob", READ, TOPIC, REPORTS));
    }

    @Test
    void anAclOfAGroupMatchesWhatTheSameAclOfAUserWould() {
        start(ALICE_IN_PAYMENTS, allow("Group:payments", prefixed(TOPIC, "reports."), WRITE),
                allow("Group:payments", literal(GROUP, "*"), ALL),
                allow("User:bob", prefixed(TOPIC, "reports."), WRITE), allow("User:bob", literal(GROUP, "*"), ALL));

        for (String user : List.of("alice", "bob")) {
            assertEquals(ALLOWED, can(user, WRITE, TOPIC, "reports.eu"), user);
            // The broker's rule: whoever may write may describe.
            assertEquals(ALLOWED, can(user, DESCRIBE, TOPIC, "reports.eu"), user);
            assertEquals(DENIED, can(user, READ, TOPIC, "reports.eu"), user);
            assertEquals(DENIED, can(user, WRITE, TOPIC, REPORTS), user);
            assertEquals(ALLOWED, can(user, READ, GROUP, "any-group"), user);
            assertEquals(DENIED, can(user, READ, ResourceType.TRANSACTIONAL_ID, "any-group"), user);
        }
        assertEquals(DENIED, can("dave", WRITE, TOPIC, "reports.eu"));
    }

    @Test
    void theParentAloneMakesNothingOfAnAclOfAGroup() throws IOException {
        try (StandardAuthorizer parent = new StandardAuthorizer(); Metrics parentMetrics = new Metrics()) {
            parent.configure(Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, "User:admin"));
            parent.withPluginMetrics(new PluginMetricsImpl(parentMetrics, Map.of()));
            parent.loadSnapshot(snapshot(ALICE_IN_PAYMENTS, PAYMENTS_READS_REPORTS));
            parent.completeInitialLoad();

            assertEquals(List.of(DENIED), parent.authorize(RequestContext.of("alice", "billing-1", ApiKeys.FETCH),
                    List.of(new Action(READ, literal(TOPIC, REPORTS), 1, true, true))));
        }
    }

    @Test
    void aGroupAmongTheSuperUsersMakesNoMemberASuperUser() {
        start(Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, "User:admin;Group:payments"), ALICE_IN_PAYMENTS);

        assertEquals(ALLOWED, can("admin", READ, TOPIC, REPORTS));
        assertEquals(DENIED, can("alice", READ, TOPIC, REPORTS));
        assertEquals(DENIED, byType("alice", WRITE, TOPIC));
    }

    @Test
    void whereEveryoneIsAllowedForWantOfAnAclAGroupIsNotAllowedWhatNoAclOfItsOwnNames() {
        start(Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, "User:admin",
                StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG, "true"),
                ALICE_IN_PAYMENTS, PAYMENTS_READS_REPORTS, allow("User:dave", literal(TOPIC, ORDERS), READ));

        // An ACL names orders, for dave, so the parent denies everyone else; no ACL of any group
        // names it, and that is no reason to allow the group.
        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("alice", READ, TOPIC, REPORTS));
        assertEquals(ALLOWED, can("alice", READ, TOPIC, "a-topic-no-acl-names"));
        assertEquals(ALLOWED, can("bob", READ, TOPIC, "a-topic-no-acl-names"));
    }

    // --- AC3: a deny ACL that names the user beats both ---------------------------------------

    @Test
    void aDenyAclOfTheUserBeatsTheRoleOfItsGroup() {
        start(ALICE_IN_PAYMENTS, PAYMENTS_READS_BILLING, deny("User:alice", literal(TOPIC, ORDERS), READ, "*"));

        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("alice", READ, TOPIC, INVOICES));
    }

    @Test
    void aDenyAclOfTheUserBeatsTheAclOfItsGroup() {
        start(ALICE_IN_PAYMENTS, allow("Group:payments", BILLING, READ),
                deny("User:alice", literal(TOPIC, ORDERS), READ, "*"));

        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("alice", READ, TOPIC, INVOICES));
    }

    @Test
    void aDenyAclOfEveryoneBeatsBoth() {
        start(ALICE_IN_PAYMENTS, PAYMENTS_READS_BILLING, allow("Group:payments", literal(TOPIC, REPORTS), READ),
                deny("User:*", literal(TOPIC, ORDERS), READ, "*"), deny("User:*", literal(TOPIC, REPORTS), ALL, "*"));

        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", READ, TOPIC, REPORTS));
        assertEquals(ALLOWED, can("alice", READ, TOPIC, INVOICES));
    }

    @Test
    void aDenyAclOfAnotherUserLeavesBoth() {
        start(ALICE_IN_PAYMENTS, PAYMENTS_READS_BILLING, PAYMENTS_READS_REPORTS,
                deny("User:bob", literal(TOPIC, ORDERS), READ, "*"),
                deny("User:bob", literal(TOPIC, REPORTS), READ, "*"));

        assertEquals(ALLOWED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("alice", READ, TOPIC, REPORTS));
    }

    // --- AC4: the broker's second question ----------------------------------------------------

    @Test
    void byResourceTypeARoleHeldThroughAGroupAnswers() {
        start(ALICE_IN_PAYMENTS, binding("DeveloperWrite:Group:payments", BILLING));

        assertEquals(ALLOWED, byType("alice", WRITE, TOPIC));
        assertEquals(DENIED, byType("alice", READ, TOPIC));
        assertEquals(DENIED, byType("bob", WRITE, TOPIC));
    }

    @Test
    void byResourceTypeADenyOfTheUserThatCoversTheRoleOfItsGroupDenies() {
        start(ALICE_IN_PAYMENTS, binding("DeveloperWrite:Group:payments", BILLING),
                deny("User:alice", BILLING, WRITE, "*"));
        assertEquals(DENIED, byType("alice", WRITE, TOPIC));
        close();

        start(ALICE_IN_PAYMENTS, binding("DeveloperWrite:Group:payments", BILLING),
                deny("User:alice", literal(TOPIC, ORDERS), WRITE, "*"));
        assertEquals(ALLOWED, byType("alice", WRITE, TOPIC));
    }

    @Test
    void byResourceTypeAnAclOfAGroupAnswers() throws UnknownHostException {
        start(ALICE_IN_PAYMENTS, acl("Group:payments", literal(TOPIC, ORDERS), WRITE, "10.0.0.7", ALLOW),
                allow("Group:invoicing", BILLING, READ));
        InetAddress named = InetAddress.getByName("10.0.0.7");
        InetAddress other = InetAddress.getByName("10.0.0.8");

        assertEquals(ALLOWED, byTypeFrom(named, "alice", WRITE, TOPIC));
        assertEquals(ALLOWED, byTypeFrom(named, "alice", DESCRIBE, TOPIC));
        assertEquals(DENIED, byTypeFrom(other, "alice", WRITE, TOPIC));
        assertEquals(DENIED, byTypeFrom(named, "alice", READ, TOPIC));
        assertEquals(DENIED, byTypeFrom(named, "alice", WRITE, GROUP));
        assertEquals(DENIED, byTypeFrom(named, "bob", WRITE, TOPIC));
    }

    static Stream<Arguments> aclsOfTheGroupAndDeniesThatCoverThem() {
        return Stream.of(
                Arguments.of(literal(TOPIC, ORDERS), literal(TOPIC, ORDERS), true),
                Arguments.of(literal(TOPIC, ORDERS), BILLING, true),
                Arguments.of(literal(TOPIC, ORDERS), literal(TOPIC, INVOICES), false),
                Arguments.of(BILLING, BILLING, true),
                Arguments.of(BILLING, prefixed(TOPIC, "bill"), true),
                Arguments.of(BILLING, literal(TOPIC, "*"), true),
                Arguments.of(BILLING, literal(TOPIC, ORDERS), false),
                Arguments.of(BILLING, prefixed(TOPIC, "billing.eu."), false),
                Arguments.of(literal(TOPIC, "*"), literal(TOPIC, "*"), true),
                Arguments.of(literal(TOPIC, "*"), BILLING, false));
    }

    @ParameterizedTest(name = "allowed on {0}, denied on {1}: covered {2}")
    @MethodSource("aclsOfTheGroupAndDeniesThatCoverThem")
    void byResourceTypeADenyOfTheUserThatCoversTheAclOfItsGroupDenies(ResourcePattern allowed,
            ResourcePattern denied, boolean covered) {
        start(ALICE_IN_PAYMENTS, allow("Group:payments", allowed, WRITE), deny("User:alice", denied, WRITE, "*"));

        assertEquals(covered ? DENIED : ALLOWED, byType("alice", WRITE, TOPIC));
    }

    // --- AC5: a DENY ACL that names a group ---------------------------------------------------

    @Test
    void aDenyAclOfAGroupIsRefusedForEveryoneWithTheReason() {
        start(ERIN_ADMINISTERS, CAROL_OWNS_BILLING, GRACE_ALTERS_THE_CLUSTER, ALICE_IN_PAYMENTS);
        AclBinding denial = deny("Group:payments", literal(TOPIC, ORDERS), READ, "*").toBinding();

        for (String user : List.of("admin", "erin", "grace", "carol", "alice", "bob")) {
            ApiException refused = refused(create(user, denial), 0);

            assertInstanceOf(InvalidRequestException.class, refused, user);
            assertEquals("the ACL Group:payments DENY READ from host * on the TOPIC name 'billing.orders' is a DENY"
                    + " for a group: a group only adds access, and a DENY for a group cannot take away what an ACL"
                    + " allows a member by name; deny the users by name instead", refused.getMessage(), user);
        }
        assertEquals(List.of(), mutator.created);
    }

    @Test
    void theSameAclThatAllowsIsCreatedByWhoeverCreatesAcls() {
        start(ERIN_ADMINISTERS, CAROL_OWNS_BILLING, GRACE_ALTERS_THE_CLUSTER, ALICE_IN_PAYMENTS);
        AclBinding allowance = allow("Group:payments", literal(TOPIC, ORDERS), READ).toBinding();

        for (String user : List.of("admin", "erin", "grace")) {
            assertCreated(create(user, allowance), 1);
            assertEquals(List.of(allowance), deleted(delete(user, allowance.toFilter()), 0), user);
        }
        for (String user : List.of("carol", "alice", "bob")) {
            assertInstanceOf(ClusterAuthorizationException.class, refused(create(user, allowance), 0), user);
        }
        assertEquals(3, mutator.created.size());
    }

    @Test
    void aDenyAclOfAGroupRefusesTheWholeRequest() {
        start();
        AclBinding denial = deny("Group:payments", literal(TOPIC, ORDERS), READ, "*").toBinding();

        List<AclCreateResult> results = create("admin", PAYMENTS_READS_REPORTS.toBinding(), denial);

        assertInstanceOf(InvalidRequestException.class, refused(results, 0));
        assertTrue(refused(results, 0).getMessage().startsWith("Not created, because the request was refused as a"
                + " whole: "), refused(results, 0).getMessage());
        assertInstanceOf(InvalidRequestException.class, refused(results, 1));
        assertEquals(List.of(), mutator.created);
    }

    @Test
    void aDenyAclOfAGroupThatArrivesAnywayIsLoggedCountedAndTakesAwayWhatRolesAndGroupsGive() {
        start(ALICE_IN_PAYMENTS, PAYMENTS_READS_BILLING, allow("Group:payments", literal(TOPIC, REPORTS), READ),
                binding("DeveloperWrite:User:alice", BILLING), allow("User:alice", literal(TOPIC, ORDERS), DESCRIBE),
                member("User:bob", "Group:invoicing"), binding("DeveloperRead:Group:invoicing", BILLING));
        Uuid id = Uuid.randomUuid();

        String log = stderrOf(() -> authorizer.addAcl(id,
                deny("Group:payments", literal(TOPIC, "*"), ALL, "*")));

        assertEquals(1, occurrences(log, A_DENY_FOR_A_GROUP), log);
        assertTrue(log.contains(id.toString()), log);
        assertEquals(1, malformedRecords());
        // What the group's role, the group's ACL and the member's own role gave is gone ...
        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", READ, TOPIC, REPORTS));
        assertEquals(DENIED, can("alice", WRITE, TOPIC, ORDERS));
        assertEquals(DENIED, byType("alice", READ, TOPIC));
        // ... what an ACL allows the member by name is not, and nobody else is denied anything.
        assertEquals(ALLOWED, can("alice", DESCRIBE, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("bob", READ, TOPIC, ORDERS));

        authorizer.removeAcl(id);
        assertEquals(0, malformedRecords());
        assertEquals(ALLOWED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("alice", READ, TOPIC, REPORTS));
        assertEquals(ALLOWED, can("alice", WRITE, TOPIC, ORDERS));
        assertEquals(ALLOWED, byType("alice", READ, TOPIC));
    }

    @Test
    void aDenyAclOfAGroupInSnapshotsIsLoggedOnceAndMatchesAsADenyOfAUserWould() {
        start();
        Map<Uuid, StandardAcl> records = snapshot(ALICE_IN_PAYMENTS, PAYMENTS_READS_BILLING,
                deny("Group:payments", literal(TOPIC, ORDERS), READ, "*"));

        String log = stderrOf(() -> {
            authorizer.loadSnapshot(records);
            authorizer.loadSnapshot(new HashMap<>(records));
        });

        assertEquals(1, occurrences(log, A_DENY_FOR_A_GROUP), log);
        assertEquals(1, malformedRecords());
        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("alice", READ, TOPIC, INVOICES));
        assertEquals(ALLOWED, byType("alice", READ, TOPIC));

        authorizer.loadSnapshot(snapshot(ALICE_IN_PAYMENTS, PAYMENTS_READS_BILLING));
        assertEquals(0, malformedRecords());
        assertEquals(ALLOWED, can("alice", READ, TOPIC, ORDERS));
    }

    // --- AC6: who manages membership ------------------------------------------------------------

    @Test
    void aSuperUserAndASystemAdminCreateAndDeleteAMembership() {
        for (String user : List.of("admin", "erin")) {
            start(ERIN_ADMINISTERS, PAYMENTS_READS_BILLING);
            assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));

            assertCreated(create(user, ALICE_IN_PAYMENTS.toBinding()), 1);
            assertEquals(ALLOWED, can("alice", READ, TOPIC, ORDERS), user);

            assertEquals(List.of(ALICE_IN_PAYMENTS.toBinding()),
                    deleted(delete(user, ALICE_IN_PAYMENTS.toBinding().toFilter()), 0), user);
            assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS), user);
            close();
        }
    }

    @Test
    void aResourceOwnerAndEveryoneElseCreateNoMembership() {
        start(ERIN_ADMINISTERS, CAROL_OWNS_BILLING, FRANK_RUNS_THE_CLUSTER, PAYMENTS_READS_BILLING,
                binding("ResourceOwner:User:dave", literal(TOPIC, "*")), member("User:heidi", "Group:payments"));

        for (String user : List.of("carol", "dave", "frank", "heidi", "alice", "bob")) {
            for (String member : List.of("User:alice", "User:" + user)) {
                ApiException refused = refused(create(user, member(member, "Group:payments").toBinding()), 0);

                assertInstanceOf(ClusterAuthorizationException.class, refused, user);
                assertTrue(refused.getMessage().startsWith("User:" + user + " may not create Member:" + member
                        + " on the TOPIC name 'Group:payments': "), refused.getMessage());
            }
        }
        assertEquals(List.of(), mutator.created);
        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
    }

    @Test
    void aResourceOwnerAndEveryoneElseDeleteNoMembership() {
        start(ERIN_ADMINISTERS, CAROL_OWNS_BILLING, FRANK_RUNS_THE_CLUSTER, PAYMENTS_READS_BILLING,
                ALICE_IN_PAYMENTS);

        for (String user : List.of("carol", "frank", "alice", "bob")) {
            ApiException refused = refused(delete(user, ALICE_IN_PAYMENTS.toBinding().toFilter()).get(0));

            assertInstanceOf(ClusterAuthorizationException.class, refused, user);
            assertTrue(refused.getMessage().startsWith("User:" + user + " may not delete "), refused.getMessage());
        }
        assertEquals(List.of(), mutator.deleted);
        assertEquals(ALLOWED, can("alice", READ, TOPIC, ORDERS));
    }

    // --- AC7: records that the format does not allow ---------------------------------------------

    static Stream<Arguments> bindingsNamedLikeAGroup() {
        return Stream.of(
                Arguments.of(binding("DeveloperRead:User:alice", literal(TOPIC, "Group:payments"))),
                Arguments.of(binding("DeveloperRead:User:alice", prefixed(TOPIC, "Group:"))),
                Arguments.of(binding("ResourceOwner:User:alice", prefixed(TOPIC, "Group:pay"))),
                Arguments.of(binding("DeveloperRead:Group:payments", literal(GROUP, "Group:payments"))));
    }

    @ParameterizedTest
    @MethodSource("bindingsNamedLikeAGroup")
    void aBindingWhoseNameStartsWithGroupIsRefusedWhenCreated(StandardAcl record) {
        start(ERIN_ADMINISTERS, binding("ResourceOwner:User:carol", prefixed(TOPIC, "G")), ALICE_IN_PAYMENTS);

        for (String user : List.of("admin", "erin", "carol", "bob")) {
            ApiException refused = refused(create(user, record.toBinding()), 0);

            assertInstanceOf(InvalidRequestException.class, refused, user);
            assertTrue(refused.getMessage().endsWith("is not a role binding: the name starts with Group:, which is"
                    + " reserved for the groups of membership records"), refused.getMessage());
        }
        assertEquals(List.of(), mutator.created);
    }

    @ParameterizedTest
    @MethodSource("bindingsNamedLikeAGroup")
    void aBindingWhoseNameStartsWithGroupThatArrivesAnywayGrantsNothingIsLoggedAndCounted(StandardAcl record) {
        start(PAYMENTS_READS_BILLING, PAYMENTS_READS_REPORTS);
        Uuid id = Uuid.randomUuid();

        String log = stderrOf(() -> authorizer.addAcl(id, record));

        assertEquals(1, occurrences(log, "is not a role binding"), log);
        assertTrue(log.contains(id.toString()), log);
        assertEquals(1, malformedRecords());
        for (AclOperation operation : Role.everyAclOperation()) {
            for (ResourceType type : List.of(TOPIC, GROUP)) {
                assertEquals(DENIED, can("alice", operation, type, "Group:payments"), operation + " " + type);
            }
        }
        // It is not a membership either: nobody is in the group it names.
        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", READ, TOPIC, REPORTS));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dev.kfkit.kfkrbac.BindingCodecTest#malformedMemberships")
    void aMalformedMembershipIsRefusedWhenCreatedForASuperUserToo(String shape, StandardAcl record) {
        if (record.patternType() == PatternType.MATCH) {
            return; // a pattern of type MATCH is a filter: no request can carry it
        }
        start(ERIN_ADMINISTERS, CAROL_OWNS_BILLING, GRACE_ALTERS_THE_CLUSTER);
        String wrong = assertInstanceOf(BindingCodec.Malformed.class, BindingCodec.decode(record)).reason();

        for (String user : List.of("admin", "grace", "erin", "carol", "bob")) {
            ApiException refused = refused(create(user, record.toBinding()), 0);

            assertInstanceOf(InvalidRequestException.class, refused, user);
            assertTrue(refused.getMessage().startsWith(record.principal() + " on the "), refused.getMessage());
            assertTrue(refused.getMessage().endsWith(" is not a group membership: " + wrong), refused.getMessage());
        }
        assertEquals(List.of(), mutator.created);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dev.kfkit.kfkrbac.BindingCodecTest#malformedMemberships")
    void aMalformedMembershipThatArrivesAnywayPutsNobodyInAGroupIsLoggedOnceAndCounted(String shape,
            StandardAcl record) {
        if (record.patternType() == PatternType.MATCH) {
            return; // a pattern of type MATCH is a filter: no record has it
        }
        // Whatever group the record could be taken to mean holds a role and has an ACL.
        start(PAYMENTS_READS_BILLING, PAYMENTS_READS_REPORTS, binding("DeveloperRead:Group:pay", BILLING),
                binding("DeveloperRead:Group:billing", BILLING), member("User:alice", "Group:billing-readers"),
                member("User:bob", "Group:billing"));
        Uuid id = Uuid.randomUuid();

        String log = stderrOf(() -> authorizer.addAcl(id, record));

        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", READ, TOPIC, REPORTS));
        assertEquals(DENIED, byType("alice", READ, TOPIC));
        assertEquals(1, occurrences(log, NOT_A_MEMBERSHIP), log);
        assertTrue(log.contains(id.toString()), log);
        assertEquals(1, malformedRecords());

        // In a snapshot it is the same, and it is not logged again.
        Map<Uuid, StandardAcl> records = snapshot(PAYMENTS_READS_BILLING, PAYMENTS_READS_REPORTS);
        records.put(id, record);
        String quiet = stderrOf(() -> authorizer.loadSnapshot(records));
        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(0, occurrences(quiet, NOT_A_MEMBERSHIP), quiet);
        assertEquals(1, malformedRecords());

        // The twin: the record as the format has it puts alice in the group.
        authorizer.removeAcl(id);
        assertEquals(0, malformedRecords());
        String silent = stderrOf(() -> authorizer.addAcl(id, ALICE_IN_PAYMENTS));
        assertEquals(ALLOWED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(ALLOWED, can("alice", READ, TOPIC, REPORTS));
        assertEquals(0, occurrences(silent, NOT_A_MEMBERSHIP), silent);
        assertEquals(0, malformedRecords());
    }

    @Test
    void aGroupIsNotAMemberOfAGroup() {
        start(PAYMENTS_READS_BILLING, PAYMENTS_READS_REPORTS, member("User:alice", "Group:billing"));

        authorizer.addAcl(Uuid.randomUuid(), member("Group:billing", "Group:payments"));

        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(DENIED, can("alice", READ, TOPIC, REPORTS));
        assertEquals(1, malformedRecords());
    }

    // --- AC8: a ResourceOwner binds a role to a group inside what it owns --------------------------

    @Test
    void aResourceOwnerBindsARoleToAGroupInsideWhatItOwnsAndDeletesTheBinding() {
        start(CAROL_OWNS_BILLING, ALICE_IN_PAYMENTS);
        AclBinding inside = binding("DeveloperRead:Group:payments", prefixed(TOPIC, "billing.eu.")).toBinding();

        assertCreated(create("carol", inside), 1);

        assertEquals(List.of(inside), mutator.created);
        assertEquals(ALLOWED, can("alice", READ, TOPIC, "billing.eu.orders"));
        assertEquals(DENIED, can("alice", READ, TOPIC, ORDERS));
        assertEquals(DENIED, can("bob", READ, TOPIC, "billing.eu.orders"));

        assertEquals(List.of(inside), deleted(delete("carol", inside.toFilter()), 0));
        assertEquals(DENIED, can("alice", READ, TOPIC, "billing.eu.orders"));
    }

    @Test
    void aResourceOwnerBindsNoRoleToAGroupOutsideWhatItOwns() {
        start(CAROL_OWNS_BILLING, ALICE_IN_PAYMENTS);

        for (AclBinding outside : List.of(
                binding("DeveloperRead:Group:payments", prefixed(TOPIC, "payments.")).toBinding(),
                binding("DeveloperRead:Group:payments", literal(TOPIC, "*")).toBinding(),
                binding("ClusterAdmin:Group:payments", THE_CLUSTER).toBinding())) {
            ApiException refused = refused(create("carol", outside), 0);

            assertInstanceOf(ClusterAuthorizationException.class, refused);
            assertTrue(refused.getMessage().startsWith("User:carol may not create "), refused.getMessage());
        }
        assertEquals(List.of(), mutator.created);
        // Whoever owns nothing binds nothing to a group, inside billing or anywhere.
        refused(create("bob", PAYMENTS_READS_BILLING.toBinding()), 0);
    }

    // --- what a group does not make of its members ---------------------------------------------------

    @Test
    void aRoleHeldThroughAGroupDelegatesAsOneHeldByName() {
        start(ALICE_IN_PAYMENTS, binding("ResourceOwner:Group:payments", BILLING),
                member("User:dave", "Group:admins"), binding("SystemAdmin:Group:admins", THE_CLUSTER));
        AclBinding inside = binding("DeveloperRead:User:bob", BILLING).toBinding();
        AclBinding outside = binding("DeveloperRead:User:bob", prefixed(TOPIC, "payments.")).toBinding();

        assertEquals(ALLOWED, gate("alice", ApiKeys.CREATE_ACLS));
        assertCreated(create("alice", inside), 1);
        refused(create("alice", outside), 0);
        refused(create("alice", member("User:bob", "Group:payments").toBinding()), 0);

        assertCreated(create("dave", outside, member("User:bob", "Group:payments").toBinding()), 2);
        assertEquals(ALLOWED, gate("dave", ApiKeys.ALTER_USER_SCRAM_CREDENTIALS));
        assertEquals(DENIED, gate("mallory", ApiKeys.CREATE_ACLS));
    }

    @Test
    void anAclThatAllowsAGroupToAlterTheClusterMakesNoMemberAManagerOfAccess() {
        start(ALICE_IN_PAYMENTS, allow("Group:payments", THE_CLUSTER, ALTER));

        // As with ClusterAdmin: the operation is allowed, the requests that decide who has access are not.
        assertEquals(ALLOWED, gate("alice", ApiKeys.ALTER_PARTITION_REASSIGNMENTS));
        assertEquals(DENIED, gate("alice", ApiKeys.ALTER_USER_SCRAM_CREDENTIALS));
        assertEquals(DENIED, authorizer.authorizeByResourceType(
                RequestContext.of("alice", "admin-1", ApiKeys.ALTER_USER_SCRAM_CREDENTIALS), ALTER, CLUSTER));
        assertEquals(DENIED, gate("bob", ApiKeys.ALTER_PARTITION_REASSIGNMENTS));

        for (AclBinding record : List.of(PAYMENTS_READS_BILLING.toBinding(), PAYMENTS_READS_REPORTS.toBinding(),
                member("User:bob", "Group:payments").toBinding())) {
            ApiException refused = refused(create("alice", record), 0);
            assertInstanceOf(ClusterAuthorizationException.class, refused);
        }
        refused(delete("alice", ALICE_IN_PAYMENTS.toBinding().toFilter()).get(0));
        assertEquals(List.of(), mutator.created);
        assertEquals(List.of(), mutator.deleted);
    }

    // --- records change while requests are answered ------------------------------------------------------

    @Test
    void groupsChangeWhileOtherThreadsAuthorize() throws Exception {
        StandardAcl bobInInvoicing = member("User:bob", "Group:invoicing");
        StandardAcl invoicingReadsBilling = binding("DeveloperRead:Group:invoicing", BILLING);
        StandardAcl invoicingReadsReports = allow("Group:invoicing", literal(TOPIC, REPORTS), READ);
        StandardAcl aliceInInvoicing = member("User:alice", "Group:invoicing");
        StandardAcl erinInPayments = member("User:erin", "Group:payments");
        StandardAcl daveInAudit = member("User:dave", "Group:audit");
        StandardAcl auditReadsLogs = binding("DeveloperRead:Group:audit", prefixed(TOPIC, "logs."));
        start(bobInInvoicing, invoicingReadsBilling, invoicingReadsReports, PAYMENTS_READS_BILLING,
                PAYMENTS_READS_REPORTS);
        Uuid firstId = Uuid.randomUuid();
        authorizer.addAcl(firstId, ALICE_IN_PAYMENTS);

        // alice is in payments or in invoicing, which hold the same, in every state the writer
        // leaves behind and bob always in invoicing; dave is in a group that holds nothing on billing,
        // or in none, and mallory in none; erin comes and goes.
        int readers = 4;
        AtomicBoolean done = new AtomicBoolean();
        Queue<String> wrong = new ConcurrentLinkedQueue<>();
        CountDownLatch reading = new CountDownLatch(readers);
        ExecutorService pool = Executors.newFixedThreadPool(readers);
        try {
            for (int i = 0; i < readers; i++) {
                pool.execute(() -> {
                    boolean counted = false;
                    try {
                        while (!done.get() && wrong.isEmpty()) {
                            for (String member : List.of("alice", "bob")) {
                                expect(ALLOWED, can(member, READ, TOPIC, ORDERS), member + " reads by role", wrong);
                                expect(ALLOWED, can(member, READ, TOPIC, REPORTS), member + " reads by ACL", wrong);
                                expect(ALLOWED, byType(member, READ, TOPIC), member + " reads some topic", wrong);
                            }
                            for (String stranger : List.of("dave", "mallory")) {
                                expect(DENIED, can(stranger, READ, TOPIC, ORDERS), stranger + " reads by role", wrong);
                                expect(DENIED, can(stranger, READ, TOPIC, REPORTS), stranger + " reads by ACL", wrong);
                            }
                            expect(DENIED, byType("mallory", READ, TOPIC), "mallory reads some topic", wrong);
                            can("erin", READ, TOPIC, ORDERS);
                            can("erin", READ, TOPIC, REPORTS);
                            if (!counted) {
                                counted = true;
                                reading.countDown();
                            }
                        }
                    } catch (Throwable e) {
                        wrong.add(e.toString());
                    } finally {
                        if (!counted) {
                            reading.countDown();
                        }
                    }
                });
            }
            assertTrue(reading.await(30, TimeUnit.SECONDS), "the readers started");

            for (int i = 0; i < 2000 && wrong.isEmpty(); i++) {
                Uuid secondId = Uuid.randomUuid();
                Uuid erinsId = Uuid.randomUuid();
                authorizer.addAcl(secondId, aliceInInvoicing);
                authorizer.removeAcl(firstId);
                authorizer.addAcl(erinsId, erinInPayments);
                firstId = Uuid.randomUuid();
                authorizer.addAcl(firstId, ALICE_IN_PAYMENTS);
                authorizer.removeAcl(secondId);
                authorizer.removeAcl(erinsId);
            }
            for (int i = 0; i < 500 && wrong.isEmpty(); i++) {
                // In one snapshot dave is in a group that holds nothing on billing; in the other the
                // group holds billing and dave is not in it. Neither lets dave read, and what the two
                // have in common, alice in payments and bob in invoicing, holds throughout.
                authorizer.loadSnapshot(snapshot(bobInInvoicing, invoicingReadsBilling, invoicingReadsReports,
                        PAYMENTS_READS_BILLING, PAYMENTS_READS_REPORTS, ALICE_IN_PAYMENTS, erinInPayments,
                        daveInAudit, auditReadsLogs));
                authorizer.loadSnapshot(snapshot(bobInInvoicing, invoicingReadsBilling, invoicingReadsReports,
                        PAYMENTS_READS_BILLING, PAYMENTS_READS_REPORTS, ALICE_IN_PAYMENTS,
                        binding("DeveloperRead:Group:audit", BILLING),
                        allow("Group:audit", literal(TOPIC, REPORTS), READ)));
            }
        } finally {
            done.set(true);
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "the readers stopped");
        }

        assertEquals(List.of(), new ArrayList<>(wrong));
    }

    // --- helpers -----------------------------------------------------------------------------------------

    /** The authorizer as a controller has it: configured, with a mutator, and loaded with these records. */
    private void start(StandardAcl... records) {
        start(Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, "User:admin"), records);
    }

    private void start(Map<String, ?> configs, StandardAcl... records) {
        metrics = new Metrics();
        authorizer = new RbacAuthorizer();
        authorizer.configure(configs);
        authorizer.withPluginMetrics(new PluginMetricsImpl(metrics, Map.of()));
        mutator = new RecordingMutator(authorizer);
        authorizer.setAclMutator(mutator);
        mutator.load(records);
        authorizer.completeInitialLoad();
    }

    private static Map<Uuid, StandardAcl> snapshot(StandardAcl... records) {
        Map<Uuid, StandardAcl> snapshot = new HashMap<>();
        for (StandardAcl record : records) {
            snapshot.put(Uuid.randomUuid(), record);
        }
        return snapshot;
    }

    /** A membership record as it is created: {@code TOPIC}, literal, {@code ALL}, {@code ALLOW}, host {@code *}. */
    private static StandardAcl member(String member, String group) {
        return acl("Member:" + member, literal(TOPIC, group), ALL, "*", ALLOW);
    }

    /** A role binding as it is created: operation {@code ALL}, permission {@code ALLOW}, host {@code *}. */
    private static StandardAcl binding(String roleAndMember, ResourcePattern pattern) {
        return acl(roleAndMember, pattern, ALL, "*", ALLOW);
    }

    private AuthorizationResult can(String user, AclOperation operation, ResourceType type, String name) {
        return from(InetAddress.getLoopbackAddress(), user, operation, type, name);
    }

    private AuthorizationResult from(InetAddress address, String user, AclOperation operation, ResourceType type,
            String name) {
        return authorizer.authorize(context(address, user, ApiKeys.FETCH),
                List.of(new Action(operation, literal(type, name), 1, true, true))).get(0);
    }

    private AuthorizationResult byType(String user, AclOperation operation, ResourceType type) {
        return byTypeFrom(InetAddress.getLoopbackAddress(), user, operation, type);
    }

    private AuthorizationResult byTypeFrom(InetAddress address, String user, AclOperation operation,
            ResourceType type) {
        return authorizer.authorizeByResourceType(context(address, user, ApiKeys.INIT_PRODUCER_ID), operation, type);
    }

    /** The broker's check of {@code ALTER} on the cluster before a request of that type. */
    private AuthorizationResult gate(String user, ApiKeys api) {
        return authorizer.authorize(RequestContext.of(user, "admin-1", api),
                List.of(new Action(ALTER, THE_CLUSTER, 1, true, true))).get(0);
    }

    private static RequestContext context(InetAddress address, String user, ApiKeys api) {
        return new RequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user), "billing-1", address, api.id);
    }

    /** A {@code CreateAcls} request as the controller hands it over, and its results once they are known. */
    private List<AclCreateResult> create(String user, AclBinding... records) {
        return results(authorizer.createAcls(RequestContext.of(user, "admin-1", ApiKeys.CREATE_ACLS),
                Arrays.asList(records)));
    }

    /** A {@code DeleteAcls} request as the controller hands it over, and its results once they are known. */
    private List<AclDeleteResult> delete(String user, AclBindingFilter... filters) {
        return results(authorizer.deleteAcls(RequestContext.of(user, "admin-1", ApiKeys.DELETE_ACLS),
                Arrays.asList(filters)));
    }

    private static <T> List<T> results(List<? extends CompletionStage<T>> stages) {
        List<T> results = new ArrayList<>();
        for (CompletionStage<T> stage : stages) {
            assertTrue(stage.toCompletableFuture().isDone(), "answered at once");
            results.add(stage.toCompletableFuture().join());
        }
        return results;
    }

    private static void assertCreated(List<AclCreateResult> results, int count) {
        assertEquals(count, results.size());
        for (AclCreateResult result : results) {
            assertEquals(Optional.empty(), result.exception().map(Throwable::getMessage));
        }
    }

    /** The error of the record at that position of a create that must have been refused. */
    private static ApiException refused(List<AclCreateResult> results, int position) {
        return results.get(position).exception()
                .orElseThrow(() -> new AssertionError("record " + position + " was not refused"));
    }

    private static ApiException refused(AclDeleteResult result) {
        return result.exception().orElseThrow(() -> new AssertionError("the filter was not refused"));
    }

    /** What the filter at that position deleted, when it was not refused and nothing failed. */
    private static List<AclBinding> deleted(List<AclDeleteResult> results, int position) {
        AclDeleteResult result = results.get(position);
        assertEquals(Optional.empty(), result.exception().map(Throwable::getMessage));
        List<AclBinding> deleted = new ArrayList<>();
        result.aclBindingDeleteResults().forEach(one -> {
            assertEquals(Optional.empty(), one.exception().map(Throwable::getMessage));
            deleted.add(one.aclBinding());
        });
        return deleted;
    }

    /** The gauge the authorizer registered with the broker's plugin metrics. */
    private int malformedRecords() {
        for (KafkaMetric metric : metrics.metrics().values()) {
            if (metric.metricName().name().equals(RbacAuthorizer.MALFORMED_BINDINGS_METRIC)) {
                return (Integer) metric.metricValue();
            }
        }
        throw new AssertionError("no metric " + RbacAuthorizer.MALFORMED_BINDINGS_METRIC);
    }

    private static void expect(AuthorizationResult expected, AuthorizationResult actual, String what,
            Queue<String> wrong) {
        if (actual != expected) {
            wrong.add(what + ": " + actual);
        }
    }

    private static int occurrences(String text, String part) {
        int count = 0;
        for (int at = text.indexOf(part); at >= 0; at = text.indexOf(part, at + part.length())) {
            count++;
        }
        return count;
    }
}
