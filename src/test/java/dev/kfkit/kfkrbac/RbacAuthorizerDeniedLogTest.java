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
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * What the authorizer logs when it denies, read from the logger it writes
 * to: the authorizer as the broker creates it, with bindings that are ACL
 * records and the real {@link StandardAuthorizer} underneath.
 *
 * <p>{@code User:bob} is a consumer that holds {@code DeveloperRead} on the
 * prefix {@code billing.}; {@code User:alice} holds roles of her own, which
 * no line about bob may show.
 */
class RbacAuthorizerDeniedLogTest {

    private static final ResourcePattern BILLING = prefixed(TOPIC, "billing.");
    private static final StandardAcl BOB_READS_BILLING = binding("DeveloperRead:User:bob", BILLING);
    private static final StandardAcl ALICE_WRITES_ORDERS = binding("DeveloperWrite:User:alice",
            prefixed(TOPIC, "orders"));

    private Metrics metrics;
    private RbacAuthorizer authorizer;

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

    // --- AC1: one line, with the cause, of the three ---------------------------------

    @Test
    void aRequestNoAclAndNoRoleAllowsSaysSoAndWhoAskedForWhat() {
        start(BOB_READS_BILLING, ALICE_WRITES_ORDERS);

        List<Logs.Line> lines = Logs.of(DeniedLog.LOGGER,
                () -> assertEquals(DENIED, can("bob", ApiKeys.JOIN_GROUP, READ, GROUP, "invoicing")));

        assertEquals(List.of(new Logs.Line("INFO", "dev.kfkit.kfkrbac.denied",
                "Denied User:bob READ on GROUP 'invoicing' (request JoinGroup): no ACL and no role allows it; "
                        + "roles held for GROUP: DeveloperRead on prefix billing.")), lines);
    }

    @Test
    void aRequestADenyAclMatchesSaysSo() {
        start(BOB_READS_BILLING, deny("User:bob", prefixed(TOPIC, "billing.payroll."), READ, "*"));

        List<String> lines = denied(() -> assertEquals(DENIED, can("bob", READ, TOPIC, "billing.payroll.2026")));

        assertEquals(List.of("Denied User:bob READ on TOPIC 'billing.payroll.2026' (request Fetch): "
                + "a deny ACL matches"), lines);
    }

    @Test
    void aDenyAclIsTheCauseWithoutAnyRoleToo() {
        start(deny("User:*", literal(TOPIC, "orders"), ALL, "*"));

        List<String> lines = denied(() -> assertEquals(DENIED, can("bob", READ, TOPIC, "orders")));

        assertEquals(List.of("Denied User:bob READ on TOPIC 'orders' (request Fetch): a deny ACL matches"), lines);
    }

    @Test
    void rolesThatCannotBeConsultedSaySo() {
        start(new RbacAuthorizer(new Failing()));

        List<String> lines = denied(() -> assertEquals(DENIED, can("bob", READ, TOPIC, "orders")));

        assertEquals(List.of("Denied User:bob READ on TOPIC 'orders' (request Fetch): "
                + "the roles could not be consulted, see \"Role lookup failed\" in the broker's log"), lines);
    }

    @Test
    void eachDeniedActionOfARequestHasItsLine() {
        start(BOB_READS_BILLING);
        List<Action> actions = List.of(
                new Action(READ, literal(TOPIC, "orders"), 1, true, true),
                new Action(READ, literal(TOPIC, "billing.invoices"), 1, true, true),
                new Action(READ, literal(TOPIC, "payments"), 1, true, true));

        List<String> lines = denied(() -> assertEquals(List.of(DENIED, ALLOWED, DENIED),
                authorizer.authorize(RequestContext.of("bob", "billing-1", ApiKeys.FETCH), actions)));

        assertEquals(2, lines.size(), lines.toString());
        assertTrue(lines.get(0).startsWith("Denied User:bob READ on TOPIC 'orders' "), lines.get(0));
        assertTrue(lines.get(1).startsWith("Denied User:bob READ on TOPIC 'payments' "), lines.get(1));
    }

    // --- AC2: what the principal does hold --------------------------------------------

    @Test
    void aBindingOnTheWrongPrefixShowsAtOnce() {
        start(BOB_READS_BILLING, ALICE_WRITES_ORDERS);

        List<String> lines = denied(() -> assertEquals(DENIED, can("bob", READ, TOPIC, "biling.invoices")));

        assertEquals(List.of("Denied User:bob READ on TOPIC 'biling.invoices' (request Fetch): "
                + "no ACL and no role allows it; roles held for TOPIC: DeveloperRead on prefix billing."), lines);
    }

    @Test
    void aPrincipalThatHoldsNoRoleIsToldSo() {
        start(BOB_READS_BILLING, ALICE_WRITES_ORDERS);

        List<String> lines = denied(() -> assertEquals(DENIED, can("dave", READ, TOPIC, "billing.invoices")));

        assertEquals(List.of("Denied User:dave READ on TOPIC 'billing.invoices' (request Fetch): "
                + "no ACL and no role allows it; roles held for TOPIC: none"), lines);
    }

    @Test
    void aRoleOfTheWrongKindShowsAtOnce() {
        start(BOB_READS_BILLING);

        List<String> lines = denied(() -> assertEquals(DENIED, can("bob", WRITE, TOPIC, "billing.invoices")));

        assertEquals(List.of("Denied User:bob WRITE on TOPIC 'billing.invoices' (request Fetch): "
                + "no ACL and no role allows it; roles held for TOPIC: "
                + "DeveloperRead on prefix billing. (does not grant WRITE on TOPIC)"), lines);
    }

    @Test
    void aRoleHeldAsAMemberOfAGroupNamesTheGroup() {
        start(binding("DeveloperRead:Group:payments", BILLING), member("User:bob", "Group:payments"),
                member("User:alice", "Group:audit"), binding("DeveloperRead:Group:audit", prefixed(TOPIC, "audit.")));

        List<String> lines = denied(() -> assertEquals(DENIED, can("bob", READ, TOPIC, "orders")));

        assertEquals(List.of("Denied User:bob READ on TOPIC 'orders' (request Fetch): no ACL and no role allows it; "
                + "roles held for TOPIC: DeveloperRead on prefix billing. as Group:payments"), lines);
    }

    @Test
    void theLineSaysNothingOfTheHostTheClientOrAnyOtherPrincipal() {
        start(BOB_READS_BILLING, ALICE_WRITES_ORDERS, allow("User:carol", literal(TOPIC, "orders"), READ));

        List<String> lines = denied(() -> assertEquals(DENIED, can("bob", READ, TOPIC, "orders")));

        assertEquals(1, lines.size());
        String line = lines.get(0);
        assertFalse(line.contains("127.0.0.1"), line);
        assertFalse(line.contains("billing-1"), line);
        assertFalse(line.contains("alice"), line);
        assertFalse(line.contains("carol"), line);
        assertFalse(line.contains("DeveloperWrite"), line);
    }

    // --- AC3: not once per retry -------------------------------------------------------

    @Test
    void aRequestDeniedAgainAndAgainIsLoggedOnce() {
        start(BOB_READS_BILLING);

        List<String> lines = denied(() -> {
            for (int i = 0; i < 100; i++) {
                assertEquals(DENIED, can("bob", READ, TOPIC, "orders"));
            }
        });

        assertEquals(1, lines.size(), lines.toString());
    }

    @Test
    void withAnIntervalOfZeroEveryDenialIsLogged() {
        start(new RbacAuthorizer(), Map.of(DeniedLog.INTERVAL_CONFIG, "0"), BOB_READS_BILLING);

        List<String> lines = denied(() -> {
            for (int i = 0; i < 3; i++) {
                assertEquals(DENIED, can("bob", READ, TOPIC, "orders"));
            }
        });

        assertEquals(3, lines.size(), lines.toString());
    }

    @Test
    void aWrongIntervalFailsConfigureAndNamesTheSetting() {
        RbacAuthorizer created = new RbacAuthorizer();
        try {
            ConfigException refused = assertThrows(ConfigException.class, () -> created.configure(
                    Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, "User:admin", DeniedLog.INTERVAL_CONFIG, "soon")));

            assertTrue(refused.getMessage().contains("kfkrbac.denied.log.interval.seconds"), refused.getMessage());
        } finally {
            closeQuietly(created);
        }
    }

    // --- AC4: an allowed action writes nothing -----------------------------------------

    @Test
    void anActionAnAclAllowsWritesNothing() {
        start(BOB_READS_BILLING, allow("User:bob", literal(TOPIC, "orders"), READ));

        assertEquals(List.of(), denied(() -> assertEquals(ALLOWED, can("bob", READ, TOPIC, "orders"))));
        assertEquals(List.of(), denied(() -> assertEquals(ALLOWED, can("admin", READ, TOPIC, "orders"))));
    }

    @Test
    void anActionARoleAllowsWritesNothing() {
        start(BOB_READS_BILLING);

        assertEquals(List.of(), denied(() -> assertEquals(ALLOWED, can("bob", READ, TOPIC, "billing.invoices"))));
        assertEquals(List.of(), denied(() -> assertEquals(ALLOWED, can("bob", READ, GROUP, "billing.invoicing"))));
    }

    @Test
    void theSameActionsWithoutTheAclAndTheRoleWriteALineEach() {
        start();

        List<String> lines = denied(() -> {
            assertEquals(DENIED, can("bob", READ, TOPIC, "orders"));
            assertEquals(DENIED, can("bob", READ, TOPIC, "billing.invoices"));
            assertEquals(DENIED, can("bob", READ, GROUP, "billing.invoicing"));
        });

        assertEquals(3, lines.size(), lines.toString());
    }

    // --- AC5: a probe is not logged -----------------------------------------------------

    @Test
    void aDenialTheBrokerDoesNotWantLoggedWritesNothing() {
        start(BOB_READS_BILLING);
        List<Action> probe = List.of(new Action(READ, literal(TOPIC, "orders"), 1, true, false));
        List<Action> wanted = List.of(new Action(READ, literal(TOPIC, "orders"), 1, true, true));

        for (String user : List.of("bob", "dave")) {
            assertEquals(List.of(), denied(() -> assertEquals(List.of(DENIED),
                    authorizer.authorize(RequestContext.of(user, "billing-1", ApiKeys.METADATA), probe))), user);
            assertEquals(1, denied(() -> assertEquals(List.of(DENIED),
                    authorizer.authorize(RequestContext.of(user, "billing-1", ApiKeys.FETCH), wanted))).size(), user);
        }
    }

    // --- the second question: any resource of the type ----------------------------------

    @Test
    void theSecondQuestionDeniedSaysAnyResourceOfTheTypeAndWhatIsHeldForIt() {
        start(BOB_READS_BILLING, ALICE_WRITES_ORDERS);

        List<String> lines = denied(() -> {
            assertEquals(DENIED, byType("bob", WRITE, TOPIC));
            assertEquals(DENIED, byType("bob", WRITE, TOPIC));
        });

        assertEquals(List.of("Denied User:bob WRITE on any TOPIC (request InitProducerId): "
                + "no ACL and no role allows it; roles held for TOPIC: "
                + "DeveloperRead on prefix billing. (does not grant WRITE on TOPIC)"), lines);
    }

    @Test
    void theSecondQuestionAllowedWritesNothing() {
        start(BOB_READS_BILLING, ALICE_WRITES_ORDERS, allow("User:carol", literal(TOPIC, "orders"), WRITE));

        assertEquals(List.of(), denied(() -> {
            assertEquals(ALLOWED, byType("alice", WRITE, TOPIC));
            assertEquals(ALLOWED, byType("carol", WRITE, TOPIC));
            assertEquals(ALLOWED, byType("admin", WRITE, TOPIC));
        }));
    }

    @Test
    void theSecondQuestionDeniedByDenyAclsSaysSo() {
        start(ALICE_WRITES_ORDERS, deny("User:alice", prefixed(TOPIC, "ord"), WRITE, "*"));

        List<String> lines = denied(() -> assertEquals(DENIED, byType("alice", WRITE, TOPIC)));

        assertEquals(List.of("Denied User:alice WRITE on any TOPIC (request InitProducerId): "
                + "deny ACLs cover everything a role or a group allows it on; roles held for TOPIC: "
                + "DeveloperWrite on prefix orders"), lines);
    }

    @Test
    void theSecondQuestionDeniedByDenyAclsOverAGroupsAclSaysSo() {
        start(member("User:bob", "Group:payments"), allow("Group:payments", literal(TOPIC, "orders"), WRITE),
                deny("User:bob", literal(TOPIC, "orders"), WRITE, "*"));

        List<String> lines = denied(() -> assertEquals(DENIED, byType("bob", WRITE, TOPIC)));

        assertEquals(List.of("Denied User:bob WRITE on any TOPIC (request InitProducerId): "
                + "deny ACLs cover everything a role or a group allows it on; roles held for TOPIC: none"), lines);
    }

    @Test
    void theSecondQuestionWithRolesThatCannotBeConsultedSaysSo() {
        start(new RbacAuthorizer(new Failing()));

        List<String> lines = denied(() -> assertEquals(DENIED, byType("bob", WRITE, TOPIC)));

        assertEquals(List.of("Denied User:bob WRITE on any TOPIC (request InitProducerId): "
                + "the roles could not be consulted, see \"Role lookup failed\" in the broker's log"), lines);
    }

    // --- helpers --------------------------------------------------------------------------

    /** The authorizer as the broker creates it. */
    private void start(StandardAcl... records) {
        start(new RbacAuthorizer(), records);
    }

    private void start(RbacAuthorizer created, StandardAcl... records) {
        start(created, Map.of(), records);
    }

    private void start(RbacAuthorizer created, Map<String, String> settings, StandardAcl... records) {
        metrics = new Metrics();
        authorizer = created;
        Map<String, String> configs = new HashMap<>(settings);
        configs.put(StandardAuthorizer.SUPER_USERS_CONFIG, "User:admin");
        created.configure(configs);
        created.withPluginMetrics(new PluginMetricsImpl(metrics, Map.of()));
        Map<Uuid, StandardAcl> snapshot = new HashMap<>();
        for (StandardAcl record : records) {
            snapshot.put(Uuid.randomUuid(), record);
        }
        created.loadSnapshot(snapshot);
        created.completeInitialLoad();
    }

    private static void closeQuietly(RbacAuthorizer created) {
        try {
            created.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The lines for denied requests that the code makes the authorizer write. */
    private static List<String> denied(Runnable code) {
        return Logs.messages(DeniedLog.LOGGER, code);
    }

    private AuthorizationResult can(String user, AclOperation operation, ResourceType type, String name) {
        return can(user, ApiKeys.FETCH, operation, type, name);
    }

    private AuthorizationResult can(String user, ApiKeys request, AclOperation operation, ResourceType type,
            String name) {
        return authorizer.authorize(RequestContext.of(user, "billing-1", request),
                List.of(new Action(operation, literal(type, name), 1, true, true))).get(0);
    }

    private AuthorizationResult byType(String user, AclOperation operation, ResourceType type) {
        return authorizer.authorizeByResourceType(RequestContext.of(user, "billing-1", ApiKeys.INIT_PRODUCER_ID),
                operation, type);
    }

    /** A membership as it is created: the member is in the group. */
    private static StandardAcl member(String member, String group) {
        return RbacAuthorizerTest.acl("Member:" + member, literal(TOPIC, group), ALL, "*", AclPermissionType.ALLOW);
    }

    /** A role binding as it is created: operation {@code ALL}, permission {@code ALLOW}, host {@code *}. */
    static StandardAcl binding(String roleAndMember, ResourcePattern pattern) {
        return RbacAuthorizerTest.acl(roleAndMember, pattern, ALL, "*", AclPermissionType.ALLOW);
    }

    /** Roles whose store cannot be read. */
    private static final class Failing implements Roles {
        @Override
        public boolean grants(KafkaPrincipal principal, Action action) {
            throw new IllegalStateException("bindings not loaded");
        }

        @Override
        public Collection<ResourcePattern> grantedPatterns(KafkaPrincipal principal, AclOperation operation,
                ResourceType resourceType) {
            throw new IllegalStateException("bindings not loaded");
        }
    }
}
