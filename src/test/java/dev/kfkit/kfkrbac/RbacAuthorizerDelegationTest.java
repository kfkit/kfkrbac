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

import static dev.kfkit.kfkrbac.RbacAuthorizerTest.allow;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.deny;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.literal;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.prefixed;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
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
 * Who may create and delete bindings: the authorizer as the broker creates
 * it, asked the way a controller asks it, with a {@link RecordingMutator}
 * where the controller would be.
 *
 * <p>{@code User:admin} is the super user, {@code User:erin} holds
 * {@code SystemAdmin}, {@code User:frank} holds {@code ClusterAdmin},
 * {@code User:carol} holds {@code ResourceOwner} on the prefix
 * {@code billing.}, an ordinary ACL allows {@code User:grace} {@code ALTER}
 * on the cluster, and {@code User:bob} holds nothing.
 */
class RbacAuthorizerDelegationTest {

    private static final ResourcePattern BILLING = prefixed(TOPIC, "billing.");
    private static final ResourcePattern PAYMENTS = prefixed(TOPIC, "payments.");
    private static final ResourcePattern THE_CLUSTER = literal(CLUSTER, "kafka-cluster");

    private static final StandardAcl ERIN_ADMINISTERS = record(binding("SystemAdmin:User:erin", THE_CLUSTER));
    private static final StandardAcl CAROL_OWNS_BILLING = record(binding("ResourceOwner:User:carol", BILLING));
    private static final StandardAcl FRANK_RUNS_THE_CLUSTER = record(binding("ClusterAdmin:User:frank", THE_CLUSTER));
    private static final StandardAcl GRACE_ALTERS_THE_CLUSTER = allow("User:grace", THE_CLUSTER, ALTER);

    /** The request types for which, of all the roles, only {@code SystemAdmin} grants {@code ALTER} on the cluster. */
    private static final List<ApiKeys> KEPT_FOR_SYSTEM_ADMIN = List.of(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS);

    private static final AclBinding ALICE_READS_BILLING = binding("DeveloperRead:User:alice", BILLING);
    private static final AclBinding ALICE_READS_PAYMENTS = binding("DeveloperRead:User:alice", PAYMENTS);
    private static final AclBinding BOB_READS_ORDERS = new AclBinding(literal(TOPIC, "billing.orders"),
            new AccessControlEntry("User:bob", "*", READ, ALLOW));
    private static final AclBinding ALICE_IN_PAYMENTS =
            binding("Member:User:alice", literal(TOPIC, "Group:payments"));
    private static final AclBinding ALICE_ADMINISTERS = binding("ClusterAdmin:User:alice", THE_CLUSTER);

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

    // --- AC1, AC12: a super user and a SystemAdmin create anything well-formed ------------

    @Test
    void aSuperUserCreatesABindingAndAnOrdinaryAcl() {
        start();

        List<AclCreateResult> results = create("admin", ALICE_READS_BILLING, BOB_READS_ORDERS);

        assertCreated(results, 2);
        assertEquals(List.of(ALICE_READS_BILLING, BOB_READS_ORDERS), mutator.created);
        assertEquals(List.of(new KafkaPrincipal("User", "admin")), mutator.callers);
    }

    @Test
    void aSystemAdminCreatesABindingAndAnOrdinaryAcl() {
        start(ERIN_ADMINISTERS);

        List<AclCreateResult> results = create("erin", ALICE_READS_BILLING, BOB_READS_ORDERS);

        assertCreated(results, 2);
        assertEquals(List.of(ALICE_READS_BILLING, BOB_READS_ORDERS), mutator.created);
    }

    @Test
    void aSuperUserAndASystemAdminCreateAClusterBindingAndAMembership() {
        start(ERIN_ADMINISTERS);

        assertCreated(create("admin", ALICE_ADMINISTERS, ALICE_IN_PAYMENTS), 2);
        assertCreated(create("erin", binding("SystemAdmin:User:dave", THE_CLUSTER),
                binding("Member:User:dave", literal(TOPIC, "Group:payments"))), 2);
        assertEquals(4, mutator.created.size());
    }

    @Test
    void whoeverHoldsNeitherRoleCreatesNothing() {
        start(record(binding("DeveloperRead:User:bob", BILLING)), FRANK_RUNS_THE_CLUSTER,
                allow("User:heidi", THE_CLUSTER, DESCRIBE), allow("User:heidi", literal(TOPIC, "*"), ALTER));

        for (String user : List.of("bob", "frank", "heidi", "nobody")) {
            for (AclBinding record : List.of(ALICE_READS_BILLING, BOB_READS_ORDERS)) {
                ApiException refused = refused(create(user, record), 0);
                assertInstanceOf(ClusterAuthorizationException.class, refused);
                assertTrue(refused.getMessage().startsWith("User:" + user + " may not create "),
                        refused.getMessage());
            }
        }
        assertEquals(List.of(), mutator.created);
    }

    @Test
    void aSuperUserIsWhoeverTheParentTakesForOne() {
        metrics = new Metrics();
        authorizer = new RbacAuthorizer();
        authorizer.configure(Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, " User:admin ;; User:root;"));
        authorizer.withPluginMetrics(new PluginMetricsImpl(metrics, Map.of()));
        mutator = new RecordingMutator(authorizer);
        authorizer.setAclMutator(mutator);
        mutator.load();
        authorizer.completeInitialLoad();

        assertCreated(create("admin", ALICE_READS_BILLING), 1);
        assertCreated(create("root", ALICE_READS_PAYMENTS), 1);
        assertEquals(ALLOWED, gate("root", ALTER, ApiKeys.CREATE_ACLS));
        refused(create("adm", BOB_READS_ORDERS), 0);
        refused(create("User:admin", BOB_READS_ORDERS), 0);
    }

    // --- ACLs stay: whoever the ACLs alone allow ALTER on the cluster creates and deletes anything -----

    @Test
    void aPrincipalAnAclAllowsAlterOnTheClusterCreatesAndDeletesAnything() {
        start(GRACE_ALTERS_THE_CLUSTER);
        List<AclBinding> records = List.of(BOB_READS_ORDERS, ALICE_READS_BILLING, ALICE_ADMINISTERS,
                ALICE_IN_PAYMENTS);
        assertEquals(ALLOWED, gate("grace", ALTER, ApiKeys.CREATE_ACLS));
        assertEquals(ALLOWED, gate("grace", ALTER, ApiKeys.DELETE_ACLS));

        assertCreated(create("grace", records.toArray(new AclBinding[0])), 4);
        assertEquals(records, mutator.created);

        AclBindingFilter[] filters = records.stream().map(AclBinding::toFilter).toArray(AclBindingFilter[]::new);
        List<AclDeleteResult> results = delete("grace", filters);
        for (int i = 0; i < records.size(); i++) {
            assertEquals(List.of(records.get(i)), deleted(results, i));
        }
        // The filters go to the controller as they were sent, as a super user's do.
        assertEquals(List.of(filters), mutator.deleted);
        assertEquals(List.of(GRACE_ALTERS_THE_CLUSTER.toBinding()), mutator.records());
    }

    @Test
    void anAclForEveryoneOrForEveryOperationOnTheClusterAllowsTheSame() {
        start(allow("User:*", THE_CLUSTER, ALL), record(BOB_READS_ORDERS));

        assertCreated(create("nobody", ALICE_ADMINISTERS, ALICE_IN_PAYMENTS), 2);
        assertEquals(List.of(BOB_READS_ORDERS), deleted(delete("nobody", BOB_READS_ORDERS.toFilter()), 0));
    }

    @Test
    void anAclThatAllowsSomethingElseIsNotEnough() {
        start(allow("User:heidi", THE_CLUSTER, DESCRIBE), allow("User:heidi", literal(TOPIC, "*"), ALL),
                allow("User:heidi", literal(CLUSTER, "billing."), ALTER), record(BOB_READS_ORDERS));

        assertEquals(DENIED, gate("heidi", ALTER, ApiKeys.CREATE_ACLS));
        refused(create("heidi", ALICE_READS_BILLING), 0);
        refused(delete("heidi", BOB_READS_ORDERS.toFilter()).get(0));
        assertEquals(List.of(), mutator.created);
        assertEquals(List.of(), mutator.deleted);
    }

    @Test
    void aDenyAclOnTheClusterClosesTheGateToAPrincipalAnAclAllowsAndItManagesNothing() {
        start(GRACE_ALTERS_THE_CLUSTER, deny("User:grace", THE_CLUSTER, ALTER, "*"), record(BOB_READS_ORDERS));

        assertEquals(DENIED, gate("grace", ALTER, ApiKeys.CREATE_ACLS));
        assertEquals(DENIED, gate("grace", ALTER, ApiKeys.DELETE_ACLS));
        assertTrue(refused(create("grace", ALICE_READS_BILLING), 0).getMessage()
                .startsWith("User:grace may not create "));
        assertTrue(refused(delete("grace", BOB_READS_ORDERS.toFilter()).get(0)).getMessage()
                .startsWith("User:grace may not delete "));
        assertEquals(List.of(), mutator.created);
        assertEquals(List.of(), mutator.deleted);
    }

    @Test
    void aClusterAdminPassesTheGateByItsRoleAndIsRefusedWithAReason() {
        start(FRANK_RUNS_THE_CLUSTER, record(BOB_READS_ORDERS));
        assertEquals(ALLOWED, gate("frank", ALTER, ApiKeys.CREATE_ACLS));
        assertEquals(ALLOWED, gate("frank", ALTER, ApiKeys.DELETE_ACLS));

        for (AclBinding record : List.of(BOB_READS_ORDERS, ALICE_READS_BILLING, ALICE_ADMINISTERS,
                ALICE_IN_PAYMENTS)) {
            ApiException refused = refused(create("frank", record), 0);
            assertInstanceOf(ClusterAuthorizationException.class, refused);
            assertTrue(refused.getMessage().startsWith("User:frank may not create "), refused.getMessage());
            assertTrue(refused.getMessage().endsWith("User:frank is none of them"), refused.getMessage());
        }
        ApiException refused = refused(delete("frank", BOB_READS_ORDERS.toFilter()).get(0));
        assertTrue(refused.getMessage().startsWith("User:frank may not delete "), refused.getMessage());
        assertEquals(List.of(), mutator.created);
        assertEquals(List.of(), mutator.deleted);
    }

    // --- a role is not a way to become a super user ------------------------------------------------------

    static List<ApiKeys> keptForSystemAdmin() {
        return KEPT_FOR_SYSTEM_ADMIN;
    }

    @ParameterizedTest
    @MethodSource("keptForSystemAdmin")
    void noRoleButSystemAdminGrantsAlterOnTheClusterForARequestThatChangesWhoCanAuthenticate(ApiKeys api) {
        start(ERIN_ADMINISTERS, FRANK_RUNS_THE_CLUSTER, GRACE_ALTERS_THE_CLUSTER,
                record(binding("ClusterAdmin:User:heidi", THE_CLUSTER)),
                record(binding("SystemAdmin:User:heidi", THE_CLUSTER)));

        assertEquals(DENIED, gate("frank", ALTER, api));
        assertEquals(ALLOWED, gate("erin", ALTER, api));
        assertEquals(ALLOWED, gate("heidi", ALTER, api));
        // What the ACLs allow is not touched: an ordinary ACL, and a super user.
        assertEquals(ALLOWED, gate("grace", ALTER, api));
        assertEquals(ALLOWED, gate("admin", ALTER, api));
        assertEquals(DENIED, gate("bob", ALTER, api));

        assertEquals(DENIED, anyCluster("frank", api));
        assertEquals(ALLOWED, anyCluster("erin", api));
        assertEquals(ALLOWED, anyCluster("grace", api));

        // The other questions of the same request are answered as before.
        assertEquals(ALLOWED, gate("frank", DESCRIBE, api));
    }

    @ParameterizedTest
    @MethodSource("keptForSystemAdmin")
    void aDenyAclBeatsASystemAdminThereToo(ApiKeys api) {
        start(ERIN_ADMINISTERS, deny("User:erin", THE_CLUSTER, ALTER, "*"));

        assertEquals(DENIED, gate("erin", ALTER, api));
        assertEquals(DENIED, anyCluster("erin", api));
    }

    @Test
    void aClusterAdminKeepsAlterOnTheClusterForEveryOtherRequest() {
        start(FRANK_RUNS_THE_CLUSTER);

        for (ApiKeys api : ApiKeys.values()) {
            if (!KEPT_FOR_SYSTEM_ADMIN.contains(api)) {
                assertEquals(ALLOWED, gate("frank", ALTER, api), api.toString());
                assertEquals(ALLOWED, anyCluster("frank", api), api.toString());
            }
        }
    }

    // --- AC2: a malformed binding is refused, whoever asks ----------------------------------

    /** The malformed shapes the codec knows, but the one no request can carry: a pattern of type MATCH is a filter. */
    static Stream<Arguments> malformedBindings() {
        return BindingCodecTest.malformed()
                .filter(shape -> ((StandardAcl) shape.get()[1]).patternType() != PatternType.MATCH);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedBindings")
    void aMalformedBindingIsRefusedForASuperUserToo(String shape, StandardAcl record) {
        start(ERIN_ADMINISTERS, CAROL_OWNS_BILLING, GRACE_ALTERS_THE_CLUSTER);
        String wrong = assertInstanceOf(BindingCodec.Malformed.class, BindingCodec.decode(record)).reason();

        for (String user : List.of("admin", "grace", "erin", "carol", "bob")) {
            ApiException refused = refused(create(user, record.toBinding()), 0);

            assertInstanceOf(InvalidRequestException.class, refused, user);
            assertTrue(refused.getMessage().contains(record.principal()), refused.getMessage());
            assertTrue(refused.getMessage().endsWith("is not a role binding: " + wrong), refused.getMessage());
        }
        assertEquals(List.of(), mutator.created);
    }

    // --- AC3, AC4: an owner binds inside what it owns, and nowhere else -----------------------

    @Test
    void anOwnerOfAPrefixBindsOnThePrefixOnALongerPrefixAndOnALiteralNameInsideIt() {
        start(CAROL_OWNS_BILLING);
        List<AclBinding> inside = List.of(ALICE_READS_BILLING,
                binding("DeveloperRead:User:alice", prefixed(TOPIC, "billing.eu.")),
                binding("DeveloperRead:User:alice", literal(TOPIC, "billing.orders")));

        for (AclBinding record : inside) {
            assertCreated(create("carol", record), 1);
        }

        assertEquals(inside, mutator.created);
        assertEquals(new KafkaPrincipal("User", "carol"), mutator.callers.get(0));
    }

    @Test
    void theSameBindingsAreRefusedToWhoeverOwnsNothing() {
        start(CAROL_OWNS_BILLING);

        refused(create("bob", ALICE_READS_BILLING), 0);
        refused(create("bob", binding("DeveloperRead:User:alice", prefixed(TOPIC, "billing.eu."))), 0);
        refused(create("bob", binding("DeveloperRead:User:alice", literal(TOPIC, "billing.orders"))), 0);
        assertEquals(List.of(), mutator.created);
    }

    @Test
    void anOwnerOfAPrefixIsRefusedAnotherPrefixAShorterPrefixAndEveryName() {
        start(CAROL_OWNS_BILLING);

        for (ResourcePattern outside : List.of(PAYMENTS, prefixed(TOPIC, "bill"), literal(TOPIC, "*"))) {
            ApiException refused = refused(create("carol", binding("DeveloperRead:User:alice", outside)), 0);

            assertInstanceOf(ClusterAuthorizationException.class, refused);
            assertTrue(refused.getMessage().startsWith("User:carol may not create DeveloperRead:User:alice on the"
                    + " TOPIC " + (outside.patternType() == PatternType.PREFIXED ? "prefix" : "name") + " '"
                    + outside.name() + "': "), refused.getMessage());
            assertTrue(refused.getMessage().endsWith("it owns the TOPIC prefix 'billing.'"), refused.getMessage());
        }
        assertEquals(List.of(), mutator.created);
    }

    // --- AC5: what an owner never creates -----------------------------------------------------

    @Test
    void anOwnerCreatesNoOrdinaryAclNoClusterBindingAndNoMembership() {
        start(CAROL_OWNS_BILLING);

        for (AclBinding record : List.of(BOB_READS_ORDERS, ALICE_ADMINISTERS, ALICE_IN_PAYMENTS)) {
            ApiException refused = refused(create("carol", record), 0);

            assertInstanceOf(ClusterAuthorizationException.class, refused);
            assertTrue(refused.getMessage().startsWith("User:carol may not create "), refused.getMessage());
            assertTrue(refused.getMessage().contains(record.entry().principal()), refused.getMessage());
            assertTrue(refused.getMessage().contains("'" + record.pattern().name() + "'"), refused.getMessage());
        }
        assertEquals(List.of(), mutator.created);
    }

    // --- AC6: an owner hands ownership on, and no further than it was handed ---------------------

    @Test
    void anOwnerMakesAnOwnerOfAPartOfWhatItOwns() {
        start(CAROL_OWNS_BILLING);
        AclBinding inEurope = binding("DeveloperRead:User:alice", literal(TOPIC, "billing.eu.orders"));
        AclBinding inAmerica = binding("DeveloperRead:User:alice", literal(TOPIC, "billing.us.orders"));
        refused(create("dave", inEurope), 0);
        assertEquals(DENIED, gate("dave", ALTER, ApiKeys.CREATE_ACLS));

        AclBinding daveOwnsEurope = binding("ResourceOwner:User:dave", prefixed(TOPIC, "billing.eu."));
        assertCreated(create("carol", daveOwnsEurope), 1);

        assertEquals(ALLOWED, gate("dave", ALTER, ApiKeys.CREATE_ACLS));
        assertCreated(create("dave", inEurope), 1);
        ApiException refused = refused(create("dave", inAmerica), 0);
        assertTrue(refused.getMessage().contains("User:dave"), refused.getMessage());
        assertTrue(refused.getMessage().contains("'billing.us.orders'"), refused.getMessage());
        refused(create("dave", ALICE_READS_BILLING), 0);
        assertEquals(List.of(daveOwnsEurope, inEurope), mutator.created);
    }

    // --- AC7: an owner narrowed to a resource type ------------------------------------------------

    @Test
    void anOwnerNarrowedToGroupsCreatesOnlyBindingsNarrowedToGroups() {
        start(record(binding("ResourceOwner:User:carol", prefixed(GROUP, "billing."))));
        AclBinding groups = binding("DeveloperRead:User:alice", prefixed(GROUP, "billing.eu."));

        assertCreated(create("carol", groups), 1);
        refused(create("carol", binding("DeveloperRead:User:alice", prefixed(TOPIC, "billing.eu."))), 0);
        refused(create("carol", binding("DeveloperRead:User:alice", prefixed(TRANSACTIONAL_ID, "billing.eu."))), 0);
        refused(create("carol", binding("DeveloperRead:User:alice", prefixed(GROUP, "payments."))), 0);

        assertEquals(List.of(groups), mutator.created);
    }

    // --- AC8: all of a request or none of it --------------------------------------------------------

    @Test
    void oneRefusedRecordRefusesTheWholeRequestAndEveryResultSaysWhy() {
        start(CAROL_OWNS_BILLING);

        List<AclCreateResult> results = create("carol", ALICE_READS_BILLING, ALICE_READS_PAYMENTS);

        String refused = refused(results, 1).getMessage();
        assertTrue(refused.startsWith("User:carol may not create DeveloperRead:User:alice on the TOPIC prefix"
                + " 'payments.': "), refused);
        assertEquals("Not created, because the request was refused as a whole: " + refused,
                refused(results, 0).getMessage());
        assertInstanceOf(ClusterAuthorizationException.class, refused(results, 0));
        assertEquals(List.of(), mutator.created);

        assertCreated(create("carol", ALICE_READS_BILLING), 1);
    }

    @Test
    void aMalformedRecordRefusesTheWholeRequestOfASuperUser() {
        start();
        AclBinding malformed = new AclBinding(BILLING,
                new AccessControlEntry("DeveloperRead:User:alice", "*", READ, ALLOW));

        List<AclCreateResult> results = create("admin", BOB_READS_ORDERS, malformed);

        assertInstanceOf(InvalidRequestException.class, refused(results, 1));
        assertInstanceOf(InvalidRequestException.class, refused(results, 0));
        assertTrue(refused(results, 0).getMessage().startsWith("Not created, because the request was refused as a"
                + " whole: DeveloperRead:User:alice on the TOPIC prefix 'billing.' is not a role binding: "),
                refused(results, 0).getMessage());
        assertEquals(List.of(), mutator.created);
    }

    @Test
    void aRequestThatCannotBeDecidedIsRefusedNotThrown() {
        start();

        List<AclCreateResult> results = create("admin", ALICE_READS_BILLING, null);

        assertEquals(2, results.size());
        assertTrue(refused(results, 0).getMessage().contains("could not be decided"));
        assertTrue(refused(results, 1).getMessage().contains("User:admin"));
        assertEquals(List.of(), mutator.created);
    }

    // --- AC9, AC10: the gates open the ACL API, to an owner, and nothing else ---------------------------

    @Test
    void anOwnerPassesTheGatesOfTheAclApi() {
        start(CAROL_OWNS_BILLING, record(binding("ResourceOwner:User:dave", literal(GROUP, "billing.invoicing"))));

        for (String owner : List.of("carol", "dave")) {
            assertEquals(ALLOWED, gate(owner, ALTER, ApiKeys.CREATE_ACLS));
            assertEquals(ALLOWED, gate(owner, ALTER, ApiKeys.DELETE_ACLS));
            assertEquals(ALLOWED, gate(owner, DESCRIBE, ApiKeys.DESCRIBE_ACLS));
        }
    }

    @Test
    void whoeverHoldsNeitherRoleIsDeniedAtTheGates() {
        start(CAROL_OWNS_BILLING, record(binding("DeveloperManage:User:bob", BILLING)),
                record(binding("ResourceViewer:User:bob", literal(TOPIC, "*"))));

        for (String user : List.of("bob", "nobody")) {
            assertEquals(DENIED, gate(user, ALTER, ApiKeys.CREATE_ACLS));
            assertEquals(DENIED, gate(user, ALTER, ApiKeys.DELETE_ACLS));
            assertEquals(DENIED, gate(user, DESCRIBE, ApiKeys.DESCRIBE_ACLS));
        }
    }

    @Test
    void aSystemAdminPassesTheGatesAsItPassesEverything() {
        start(ERIN_ADMINISTERS);

        assertEquals(ALLOWED, gate("erin", ALTER, ApiKeys.CREATE_ACLS));
        assertEquals(ALLOWED, gate("erin", ALTER, ApiKeys.DELETE_ACLS));
        assertEquals(ALLOWED, gate("erin", DESCRIBE, ApiKeys.DESCRIBE_ACLS));
        assertEquals(ALLOWED, gate("erin", ALTER, ApiKeys.ALTER_PARTITION_REASSIGNMENTS));
    }

    @Test
    void theSameQuestionWithAnyOtherRequestTypeIsAnsweredAsIfTheOwnerHeldNoRole() {
        start(CAROL_OWNS_BILLING);

        for (String user : List.of("carol", "bob")) {
            for (ApiKeys api : ApiKeys.values()) {
                boolean aclApi = api == ApiKeys.CREATE_ACLS || api == ApiKeys.DELETE_ACLS;
                if (!aclApi) {
                    assertEquals(DENIED, gate(user, ALTER, api), user + " ALTER " + api);
                }
                if (api != ApiKeys.DESCRIBE_ACLS) {
                    assertEquals(DENIED, gate(user, DESCRIBE, api), user + " DESCRIBE " + api);
                }
            }
        }
    }

    @Test
    void theGatesOpenNothingButTheirOwnQuestion() {
        start(CAROL_OWNS_BILLING);

        for (ApiKeys api : List.of(ApiKeys.CREATE_ACLS, ApiKeys.DELETE_ACLS, ApiKeys.DESCRIBE_ACLS)) {
            for (AclOperation operation : Role.everyAclOperation()) {
                boolean theQuestion = api == ApiKeys.DESCRIBE_ACLS ? operation == DESCRIBE : operation == ALTER;
                assertEquals(theQuestion ? ALLOWED : DENIED, gate("carol", operation, api), operation + " " + api);
            }
            assertEquals(DENIED, ask("carol", api, ALTER, literal(TOPIC, "payments.refunds")));
            assertEquals(DENIED, ask("carol", api, DESCRIBE, literal(TOPIC, "payments.refunds")));
            assertEquals(DENIED, ask("carol", api, ALTER, literal(CLUSTER, "billing.")));
        }
        assertEquals(DENIED, authorizer.authorizeByResourceType(
                RequestContext.of("carol", "admin-1", ApiKeys.CREATE_ACLS), ALTER, CLUSTER));
    }

    // --- AC13: a deny ACL on the cluster closes the gate ------------------------------------------------

    @Test
    void aDenyAclOnTheClusterClosesTheGateToAnOwner() {
        start(CAROL_OWNS_BILLING, deny("User:carol", THE_CLUSTER, ALTER, "*"));

        assertEquals(DENIED, gate("carol", ALTER, ApiKeys.CREATE_ACLS));
        assertEquals(DENIED, gate("carol", ALTER, ApiKeys.DELETE_ACLS));
        assertEquals(ALLOWED, gate("carol", DESCRIBE, ApiKeys.DESCRIBE_ACLS));
    }

    @Test
    void aDenyAclForDescribeClosesTheListingAndForAllClosesEveryGate() {
        start(CAROL_OWNS_BILLING, record(binding("ResourceOwner:User:dave", BILLING)),
                deny("User:carol", THE_CLUSTER, DESCRIBE, "*"), deny("User:dave", THE_CLUSTER, ALL, "*"));

        assertEquals(ALLOWED, gate("carol", ALTER, ApiKeys.CREATE_ACLS));
        assertEquals(DENIED, gate("carol", DESCRIBE, ApiKeys.DESCRIBE_ACLS));
        assertEquals(DENIED, gate("dave", ALTER, ApiKeys.CREATE_ACLS));
        assertEquals(DENIED, gate("dave", ALTER, ApiKeys.DELETE_ACLS));
        assertEquals(DENIED, gate("dave", DESCRIBE, ApiKeys.DESCRIBE_ACLS));
    }

    @Test
    void aDenyAclOnTheClusterClosesTheGateToASystemAdminToo() {
        start(ERIN_ADMINISTERS, deny("User:erin", THE_CLUSTER, ALTER, "*"));

        assertEquals(DENIED, gate("erin", ALTER, ApiKeys.CREATE_ACLS));
        assertEquals(DENIED, gate("erin", ALTER, ApiKeys.DELETE_ACLS));
    }

    // --- AC11, AC12: deleting ------------------------------------------------------------------------------

    @Test
    void anOwnerDeletesABindingInsideWhatItOwns() {
        start(CAROL_OWNS_BILLING, record(ALICE_READS_BILLING), record(ALICE_READS_PAYMENTS));

        List<AclDeleteResult> results = delete("carol", ALICE_READS_BILLING.toFilter());

        assertEquals(List.of(ALICE_READS_BILLING), deleted(results, 0));
        assertEquals(List.of(ALICE_READS_BILLING.toFilter()), mutator.deleted);
        assertFalse(mutator.records().contains(ALICE_READS_BILLING));
        assertTrue(mutator.records().contains(ALICE_READS_PAYMENTS));
    }

    @Test
    void theSameDeleteIsRefusedToWhoeverOwnsNothing() {
        start(CAROL_OWNS_BILLING, record(ALICE_READS_BILLING));

        ApiException refused = refused(delete("bob", ALICE_READS_BILLING.toFilter()).get(0));

        assertInstanceOf(ClusterAuthorizationException.class, refused);
        assertTrue(refused.getMessage().startsWith("User:bob may not delete "), refused.getMessage());
        assertEquals(List.of(), mutator.deleted);
        assertTrue(mutator.records().contains(ALICE_READS_BILLING));
    }

    @Test
    void anOwnerDeletesExactlyWhatAWideFilterMatchedWhenAllOfItIsInside() {
        AclBinding bobWrites = binding("DeveloperWrite:User:bob", literal(TOPIC, "billing.orders"));
        start(CAROL_OWNS_BILLING, record(ALICE_READS_BILLING), record(bobWrites));
        AclBindingFilter everythingOfAlice = principal("DeveloperRead:User:alice");
        AclBindingFilter everythingOnOrders = new AclBindingFilter(
                new ResourcePatternFilter(TOPIC, "billing.orders", PatternType.LITERAL), AccessControlEntryFilter.ANY);

        List<AclDeleteResult> results = delete("carol", everythingOfAlice, everythingOnOrders,
                ALICE_READS_BILLING.toFilter());

        assertEquals(List.of(ALICE_READS_BILLING), deleted(results, 0));
        assertEquals(List.of(bobWrites), deleted(results, 1));
        assertEquals(List.of(ALICE_READS_BILLING), deleted(results, 2));
        // Each record that was checked goes to the controller once, as a filter that matches it alone.
        assertEquals(List.of(ALICE_READS_BILLING.toFilter(), bobWrites.toFilter()), mutator.deleted);
        assertEquals(List.of(CAROL_OWNS_BILLING.toBinding()), mutator.records());
    }

    @Test
    void anOwnerDeletesNothingWhenItsFilterAlsoMatchesABindingOutside() {
        start(CAROL_OWNS_BILLING, record(ALICE_READS_BILLING), record(ALICE_READS_PAYMENTS));

        List<AclDeleteResult> results = delete("carol", ALICE_READS_BILLING.toFilter(),
                principal("DeveloperRead:User:alice"));

        String refused = refused(results.get(1)).getMessage();
        assertTrue(refused.startsWith("User:carol may not delete DeveloperRead:User:alice on the TOPIC prefix"
                + " 'payments.', which the filter matches: "), refused);
        assertEquals("Nothing deleted, because the request was refused as a whole: " + refused,
                refused(results.get(0)).getMessage());
        assertEquals(List.of(), mutator.deleted);
        assertEquals(3, mutator.records().size());
    }

    @Test
    void anOwnerDeletesNothingWhenItsFilterAlsoMatchesAnOrdinaryAcl() {
        AclBinding bobReadsBilling = new AclBinding(BILLING, new AccessControlEntry("User:bob", "*", READ, ALLOW));
        start(CAROL_OWNS_BILLING, record(ALICE_READS_BILLING), record(bobReadsBilling));
        AclBindingFilter everythingOnBilling = new AclBindingFilter(
                new ResourcePatternFilter(TOPIC, "billing.", PatternType.PREFIXED), AccessControlEntryFilter.ANY);

        ApiException refused = refused(delete("carol", everythingOnBilling).get(0));

        assertInstanceOf(ClusterAuthorizationException.class, refused);
        assertTrue(refused.getMessage().startsWith("User:carol may not delete the ACL User:bob ALLOW READ from host"
                + " * on the TOPIC prefix 'billing.', which the filter matches: it is an ordinary ACL"),
                refused.getMessage());
        assertEquals(List.of(), mutator.deleted);
        assertEquals(3, mutator.records().size());
    }

    @Test
    void anOwnerDeletesNeitherAMembershipNorAClusterBindingNorAMalformedRecord() {
        AclBinding malformed = new AclBinding(BILLING,
                new AccessControlEntry("DeveloperRead:User:alice", "*", READ, ALLOW));
        start(CAROL_OWNS_BILLING, record(ALICE_IN_PAYMENTS), record(ALICE_ADMINISTERS), record(malformed));

        for (AclBinding record : List.of(ALICE_IN_PAYMENTS, ALICE_ADMINISTERS, malformed)) {
            ApiException refused = refused(delete("carol", record.toFilter()).get(0));
            assertTrue(refused.getMessage().startsWith("User:carol may not delete "), refused.getMessage());
        }
        assertEquals(List.of(), mutator.deleted);
        assertEquals(4, mutator.records().size());
    }

    @Test
    void aFilterThatMatchesNothingIsNotAnError() {
        start(ERIN_ADMINISTERS, CAROL_OWNS_BILLING, record(ALICE_READS_BILLING));

        for (String user : List.of("carol", "admin", "erin")) {
            List<AclDeleteResult> results = delete(user, ALICE_READS_PAYMENTS.toFilter(),
                    principal("DeveloperWrite:User:alice"));

            assertEquals(List.of(), deleted(results, 0), user);
            assertEquals(List.of(), deleted(results, 1), user);
        }
        assertEquals(3, mutator.records().size());
    }

    @Test
    void aSuperUserAndASystemAdminDeleteWhateverTheirFiltersMatch() {
        AclBinding malformed = new AclBinding(BILLING,
                new AccessControlEntry("DeveloperRead:User:alice", "*", READ, ALLOW));
        for (String user : List.of("admin", "erin")) {
            start(ERIN_ADMINISTERS, CAROL_OWNS_BILLING, record(ALICE_READS_BILLING), record(ALICE_READS_PAYMENTS),
                    record(BOB_READS_ORDERS), record(ALICE_IN_PAYMENTS), record(ALICE_ADMINISTERS),
                    record(malformed));
            AclBindingFilter everyTopicRecord = new AclBindingFilter(
                    new ResourcePatternFilter(TOPIC, null, PatternType.ANY), AccessControlEntryFilter.ANY);

            List<AclDeleteResult> results = delete(user, everyTopicRecord, ALICE_ADMINISTERS.toFilter());

            assertEquals(6, deleted(results, 0).size(), user);
            assertEquals(List.of(ALICE_ADMINISTERS), deleted(results, 1), user);
            // The filters go to the controller as they were sent.
            assertEquals(List.of(everyTopicRecord, ALICE_ADMINISTERS.toFilter()), mutator.deleted, user);
            assertEquals(List.of(ERIN_ADMINISTERS.toBinding()), mutator.records(), user);
            close();
        }
    }

    @Test
    void withoutAControllerBehindItNothingIsCreatedOrDeleted() {
        start(CAROL_OWNS_BILLING, record(ALICE_READS_BILLING));
        authorizer.setAclMutator(null);

        assertThrows(NotControllerException.class, () -> create("admin", ALICE_READS_PAYMENTS));
        assertThrows(NotControllerException.class, () -> delete("carol", ALICE_READS_BILLING.toFilter()));
        refused(create("carol", ALICE_READS_PAYMENTS), 0);
    }

    // --- helpers -----------------------------------------------------------------------------------------

    /** The authorizer as a controller has it: configured, with a mutator, and loaded with these records. */
    private void start(StandardAcl... records) {
        metrics = new Metrics();
        authorizer = new RbacAuthorizer();
        authorizer.configure(Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, "User:admin"));
        authorizer.withPluginMetrics(new PluginMetricsImpl(metrics, Map.of()));
        mutator = new RecordingMutator(authorizer);
        authorizer.setAclMutator(mutator);
        mutator.load(records);
        authorizer.completeInitialLoad();
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

    /** The broker's check before a request to the ACL API: the operation on the cluster. */
    private AuthorizationResult gate(String user, AclOperation operation, ApiKeys api) {
        return ask(user, api, operation, THE_CLUSTER);
    }

    /** The broker's other question: {@code ALTER} on some cluster resource, for a request of that type. */
    private AuthorizationResult anyCluster(String user, ApiKeys api) {
        return authorizer.authorizeByResourceType(RequestContext.of(user, "admin-1", api), ALTER, CLUSTER);
    }

    private AuthorizationResult ask(String user, ApiKeys api, AclOperation operation, ResourcePattern resource) {
        return authorizer.authorize(RequestContext.of(user, "admin-1", api),
                List.of(new Action(operation, resource, 1, true, true))).get(0);
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

    /** A role binding as it is created: operation {@code ALL}, permission {@code ALLOW}, host {@code *}. */
    private static AclBinding binding(String roleAndMember, ResourcePattern pattern) {
        return new AclBinding(pattern, new AccessControlEntry(roleAndMember, "*", ALL, ALLOW));
    }

    private static StandardAcl record(AclBinding binding) {
        return StandardAcl.fromAclBinding(binding);
    }

    /** Every record of that principal, whatever it is on. */
    private static AclBindingFilter principal(String principal) {
        return new AclBindingFilter(ResourcePatternFilter.ANY,
                new AccessControlEntryFilter(principal, null, AclOperation.ANY, AclPermissionType.ANY));
    }
}
