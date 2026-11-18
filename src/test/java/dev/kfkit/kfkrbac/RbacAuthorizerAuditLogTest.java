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

import static dev.kfkit.kfkrbac.RbacAuthorizerDeniedLogTest.binding;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.allow;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.deny;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.literal;
import static dev.kfkit.kfkrbac.RbacAuthorizerTest.prefixed;
import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.server.authorizer.AuthorizationResult.ALLOWED;
import static org.apache.kafka.server.authorizer.AuthorizationResult.DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAuthorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * What the broker's authorizer log, the logger {@code kafka.authorizer.logger},
 * says of a request: what was decided, once. The parent writes "Denied"
 * before any role is asked; for a request a role allows that line must not
 * be there, and "Allowed" must, as the parent writes it for a request an ACL
 * allows.
 *
 * <p>Each test asks the authorizer as the broker creates it and, where the
 * parent's own line is expected, the broker's {@link StandardAuthorizer}
 * holding the same records: the lines must be the same.
 */
class RbacAuthorizerAuditLogTest {

    private static final ResourcePattern BILLING = prefixed(TOPIC, "billing.");
    private static final StandardAcl BOB_READS_BILLING = binding("DeveloperRead:User:bob", BILLING);
    private static final ResourcePattern INVOICES = literal(TOPIC, "billing.invoices");
    private static final ResourcePattern ORDERS = literal(TOPIC, "orders");
    private static final StandardAcl ALICE_IN_PAYMENTS = member("User:alice", "Group:payments");

    private final List<Metrics> metrics = new ArrayList<>();
    private final List<StandardAuthorizer> started = new ArrayList<>();
    private RbacAuthorizer authorizer;
    private StandardAuthorizer parent;

    @AfterEach
    void close() {
        try {
            for (StandardAuthorizer one : started) {
                one.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            metrics.forEach(Metrics::close);
        }
    }

    // --- a role allows: "Allowed", and never "Denied" --------------------------------

    @Test
    void aRequestARoleAllowsIsLoggedAsAllowedWithItsBinding() {
        start(BOB_READS_BILLING);

        List<Logs.Line> lines = auditAtDebug(authorizer, "bob", action(READ, INVOICES, true, true), ALLOWED);

        assertEquals(List.of(new Logs.Line("DEBUG", "kafka.authorizer.logger",
                "Principal = User:bob is Allowed operation = READ from host = 127.0.0.1 "
                        + "on resource = Topic:LITERAL:billing.invoices for request = Fetch with resourceRefCount = 1 "
                        + "based on rule RoleBinding(role=DeveloperRead, member=User:bob, resourceType=TOPIC, "
                        + "resourceName=billing., patternType=PREFIXED)")), lines);
    }

    @Test
    void itsLineIsTheParentsLineForAnAllowedRequestUpToTheRule() {
        start(BOB_READS_BILLING, allow("User:carol", INVOICES, READ));

        String byRole = auditAtDebug(authorizer, "bob", action(READ, INVOICES, true, true), ALLOWED).get(0).message();
        String byAcl = auditAtDebug(parent, "carol", action(READ, INVOICES, true, true), ALLOWED).get(0).message();

        String rule = " based on rule ";
        assertEquals(byAcl.substring(0, byAcl.indexOf(rule)).replace("User:carol", "User:bob"),
                byRole.substring(0, byRole.indexOf(rule)));
    }

    @Test
    void atTheBrokersOwnLevelARequestARoleAllowsWritesNothing() {
        start(BOB_READS_BILLING);

        assertEquals(List.of(), audit(authorizer, "bob", action(READ, INVOICES, true, true), ALLOWED));
        assertEquals(List.of(), audit(authorizer, "bob", action(READ, literal(GROUP, "billing.invoicing"), true, true),
                ALLOWED));
    }

    @Test
    void anAllowedRequestTheBrokerDoesNotWantLoggedIsNotLoggedAtDebug() {
        start(BOB_READS_BILLING);

        assertEquals(List.of(), auditAtDebug(authorizer, "bob", action(READ, INVOICES, false, true), ALLOWED));
        assertEquals(List.of(), auditAtDebug(parent, "admin", action(READ, INVOICES, false, true), ALLOWED),
                "as the parent does not log it either");
    }

    @Test
    void theRoleThatOpensTheAclApiIsNamed() {
        start(binding("ResourceOwner:User:bob", BILLING));
        Action alterTheCluster = action(ALTER, literal(CLUSTER, "kafka-cluster"), true, true);

        List<Logs.Line> lines = new ArrayList<>();
        Logs.atDebug(RoleAudit.LOGGER, () -> lines.addAll(Logs.of(RoleAudit.LOGGER, () -> assertEquals(
                List.of(ALLOWED), authorizer.authorize(RequestContext.of("bob", "billing-1", ApiKeys.CREATE_ACLS),
                        List.of(alterTheCluster))))));

        assertEquals(List.of(new Logs.Line("DEBUG", "kafka.authorizer.logger",
                "Principal = User:bob is Allowed operation = ALTER from host = 127.0.0.1 "
                        + "on resource = Cluster:LITERAL:kafka-cluster for request = CreateAcls "
                        + "with resourceRefCount = 1 based on rule RoleBinding(role=ResourceOwner, member=User:bob, "
                        + "resourceType=TOPIC, resourceName=billing., patternType=PREFIXED)")), lines);
    }

    // --- an ACL of a group allows: "Allowed", and never "Denied" -----------------------

    @Test
    void aRequestAnAclOfAGroupAllowsIsLoggedAsAllowedWithTheGroup() {
        start(ALICE_IN_PAYMENTS, allow("Group:payments", ORDERS, READ));

        assertEquals(List.of(), audit(authorizer, "alice", action(READ, ORDERS, true, true), ALLOWED));
        assertEquals(List.of(new Logs.Line("DEBUG", "kafka.authorizer.logger",
                "Principal = User:alice is Allowed operation = READ from host = 127.0.0.1 "
                        + "on resource = Topic:LITERAL:orders for request = Fetch with resourceRefCount = 1 "
                        + "based on rule GroupAcl(group=Group:payments)")),
                auditAtDebug(authorizer, "alice", action(READ, ORDERS, true, true), ALLOWED));
    }

    @Test
    void withoutTheMembershipTheSameRequestIsLoggedAsDeniedByTheParent() {
        start(member("User:bob", "Group:payments"), allow("Group:payments", ORDERS, READ));

        List<Logs.Line> lines = audit(authorizer, "alice", action(READ, ORDERS, true, true), DENIED);

        assertEquals(1, lines.size(), lines.toString());
        assertEquals(audit(parent, "alice", action(READ, ORDERS, true, true), DENIED), lines);
    }

    @Test
    void aMemberOfAGroupThatIsDeniedIsLoggedAsDeniedOnceAsTheParentDoes() {
        start(ALICE_IN_PAYMENTS, allow("Group:payments", ORDERS, READ));

        List<Logs.Line> lines = audit(authorizer, "alice", action(READ, INVOICES, true, true), DENIED);

        assertEquals(1, lines.size(), lines.toString());
        assertEquals(audit(parent, "alice", action(READ, INVOICES, true, true), DENIED), lines);
    }

    @Test
    void aRoleHeldAsAMemberOfAGroupIsNamedWithTheGroup() {
        start(ALICE_IN_PAYMENTS, binding("DeveloperRead:Group:payments", BILLING));

        List<Logs.Line> lines = auditAtDebug(authorizer, "alice", action(READ, INVOICES, true, true), ALLOWED);

        assertEquals(1, lines.size(), lines.toString());
        assertTrue(lines.get(0).message().endsWith("based on rule RoleBinding(role=DeveloperRead, "
                + "member=Group:payments, resourceType=TOPIC, resourceName=billing., patternType=PREFIXED)"),
                lines.get(0).message());
    }

    @Test
    void aRoleThatDidNotAllowTheRequestIsNotNamedForWhatAGroupAllowed() {
        start(ALICE_IN_PAYMENTS, allow("Group:payments", ORDERS, READ), binding("ResourceOwner:User:alice", BILLING),
                binding("DeveloperWrite:User:alice", literal(TOPIC, "orders")));

        List<Logs.Line> lines = auditAtDebug(authorizer, "alice", action(READ, ORDERS, true, true), ALLOWED);

        assertEquals(1, lines.size(), lines.toString());
        assertTrue(lines.get(0).message().endsWith("based on rule GroupAcl(group=Group:payments)"),
                lines.get(0).message());
    }

    // --- denied for good: the parent's own line, once ---------------------------------

    @Test
    void withoutTheRoleTheSameRequestIsLoggedAsDeniedByTheParent() {
        start(binding("DeveloperRead:User:alice", BILLING));

        List<Logs.Line> lines = audit(authorizer, "bob", action(READ, INVOICES, true, true), DENIED);

        assertEquals(List.of(new Logs.Line("INFO", "kafka.authorizer.logger",
                "Principal = User:bob is Denied operation = READ from host = 127.0.0.1 "
                        + "on resource = Topic:LITERAL:billing.invoices for request = Fetch with resourceRefCount = 1 "
                        + "based on rule DefaultDeny")), lines);
        assertEquals(audit(parent, "bob", action(READ, INVOICES, true, true), DENIED), lines);
    }

    @Test
    void aHolderOfARoleThatDoesNotCoverTheRequestIsLoggedAsDeniedOnceAsTheParentDoes() {
        start(BOB_READS_BILLING);

        List<Logs.Line> lines = audit(authorizer, "bob", action(READ, ORDERS, true, true), DENIED);

        assertEquals(1, lines.size(), lines.toString());
        assertEquals(audit(parent, "bob", action(READ, ORDERS, true, true), DENIED), lines);
    }

    @Test
    void aDenyAclThatBeatsTheRoleIsLoggedAsTheParentLogsIt() {
        start(BOB_READS_BILLING, deny("User:bob", INVOICES, READ, "*"));

        List<Logs.Line> lines = audit(authorizer, "bob", action(READ, INVOICES, true, true), DENIED);

        assertEquals(1, lines.size(), lines.toString());
        assertEquals("INFO", lines.get(0).level());
        assertTrue(lines.get(0).message().contains("is Denied operation = READ"), lines.get(0).message());
        assertTrue(lines.get(0).message().contains("based on rule MatchingAcl(acl=StandardAcl"),
                lines.get(0).message());
        assertEquals(audit(parent, "bob", action(READ, INVOICES, true, true), DENIED), lines);
    }

    @Test
    void aDenialTheBrokerDoesNotWantLoggedIsNotLoggedForAHolderOfARoleEither() {
        start(BOB_READS_BILLING);

        assertEquals(List.of(), audit(authorizer, "bob", action(READ, ORDERS, true, false), DENIED));
        assertEquals(List.of(), audit(authorizer, "dave", action(READ, ORDERS, true, false), DENIED));
    }

    // --- an ACL allows: the parent's own line, untouched -------------------------------

    @Test
    void aRequestAnAclAllowsIsLoggedByTheParentAloneWhoeverHoldsARole() {
        start(BOB_READS_BILLING, allow("User:bob", ORDERS, READ), allow("User:dave", ORDERS, READ));

        for (String user : List.of("bob", "dave", "admin")) {
            List<Logs.Line> lines = auditAtDebug(authorizer, user, action(READ, ORDERS, true, true), ALLOWED);

            assertEquals(1, lines.size(), lines.toString());
            assertEquals(auditAtDebug(parent, user, action(READ, ORDERS, true, true), ALLOWED), lines, user);
        }
    }

    // --- each action of a request ---------------------------------------------------------

    @Test
    void eachActionOfARequestIsLoggedAsItWasDecided() {
        start(BOB_READS_BILLING, allow("User:bob", ORDERS, READ));
        List<Action> actions = List.of(action(READ, literal(TOPIC, "payments"), true, true),
                action(READ, INVOICES, true, true), action(READ, ORDERS, true, true),
                action(READ, literal(TOPIC, "shipping"), true, false));

        List<Logs.Line> lines = new ArrayList<>();
        Logs.atDebug(RoleAudit.LOGGER, () -> lines.addAll(Logs.of(RoleAudit.LOGGER, () -> assertEquals(
                List.of(DENIED, ALLOWED, ALLOWED, DENIED),
                authorizer.authorize(RequestContext.of("bob", "billing-1", ApiKeys.FETCH), actions)))));

        List<String> verdicts = lines.stream().map(line -> line.level() + " "
                + line.message().replaceAll(".* is (\\w+) .* on resource = (\\S+) .* based on rule (\\w+).*",
                        "$1 $2 $3")).sorted().toList();
        assertEquals(List.of(
                "DEBUG Allowed Topic:LITERAL:billing.invoices RoleBinding",
                "DEBUG Allowed Topic:LITERAL:orders MatchingAcl",
                "INFO Denied Topic:LITERAL:payments DefaultDeny"), verdicts);
    }

    // --- the second question ----------------------------------------------------------------

    @Test
    void theSecondQuestionWritesNoDeniedLineWhetherARoleAllowsItOrNot() {
        start(binding("DeveloperWrite:User:bob", BILLING));

        assertEquals(List.of(), byTypeAudit(authorizer, "bob", ALLOWED));
        assertEquals(List.of(), byTypeAudit(authorizer, "dave", DENIED));
        assertEquals(List.of(), byTypeAudit(parent, "dave", DENIED), "as the parent writes none");
    }

    // --- at TRACE: the deny ACLs and the ACLs of groups are asked without a line --------------

    @Test
    void theDenyAclsAskedAfterARoleWriteNothingAtTrace() {
        start(BOB_READS_BILLING, deny("User:bob", literal(TOPIC, "billing.payroll"), READ, "*"),
                deny("User:dave", INVOICES, READ, "*"));

        List<String> lines = verdicts(auditAtTrace(authorizer, "bob", action(READ, INVOICES, true, true), ALLOWED));

        // The parent's own answer, asked without its "Denied" line, and the role's: nothing of the deny ACLs.
        assertEquals(List.of("TRACE User:bob Denied DefaultDeny", "DEBUG User:bob Allowed RoleBinding"), lines);
    }

    @Test
    void aDenyAclThatMatchesAfterARoleWritesOnlyTheParentsLineAtTrace() {
        start(BOB_READS_BILLING, deny("User:bob", INVOICES, READ, "*"));

        List<String> lines = verdicts(auditAtTrace(authorizer, "bob", action(READ, INVOICES, true, true), DENIED));

        // The parent's own answers: asked without its "Denied" line, then again for that line, now final.
        assertEquals(List.of("TRACE User:bob Denied MatchingAcl", "INFO User:bob Denied MatchingAcl"), lines);
    }

    @Test
    void theAclsOfGroupsWriteNothingAtTrace() {
        start(ALICE_IN_PAYMENTS, allow("Group:payments", ORDERS, READ),
                deny("Group:payments", literal(TOPIC, "orders.archive"), READ, "*"));

        List<String> lines = verdicts(auditAtTrace(authorizer, "alice", action(READ, ORDERS, true, true), ALLOWED));

        assertEquals(List.of("TRACE User:alice Denied DefaultDeny", "DEBUG User:alice Allowed GroupAcl"), lines);
    }

    @Test
    void aDenyAclOfAGroupThatMatchesWritesOnlyTheParentsLineAtTrace() {
        start(ALICE_IN_PAYMENTS, allow("Group:payments", ORDERS, READ), deny("Group:payments", ORDERS, READ, "*"));

        List<String> lines = verdicts(auditAtTrace(authorizer, "alice", action(READ, ORDERS, true, true), DENIED));

        assertEquals(List.of("TRACE User:alice Denied DefaultDeny", "INFO User:alice Denied DefaultDeny"), lines);
    }

    @Test
    void theSecondQuestionWritesAtTraceWhatTheParentWritesWhenAGroupAllowsIt() {
        start(ALICE_IN_PAYMENTS, allow("Group:payments", ORDERS, WRITE), deny("User:alice", ORDERS, READ, "*"));

        List<Logs.Line> byGroup = new ArrayList<>();
        Logs.atTrace(RoleAudit.LOGGER, () -> byGroup.addAll(byTypeAudit(authorizer, "alice", ALLOWED)));
        List<Logs.Line> byParent = new ArrayList<>();
        Logs.atTrace(RoleAudit.LOGGER, () -> byParent.addAll(byTypeAudit(parent, "alice", DENIED)));

        assertTrue(!byParent.isEmpty(), "the parent writes its own answers at TRACE");
        assertEquals(byParent, byGroup);
    }

    @Test
    void theSecondQuestionWritesAtTraceWhatTheParentWritesWhenARoleAllowsIt() {
        start(binding("DeveloperWrite:User:bob", BILLING), deny("User:bob", literal(TOPIC, "billing.payroll"),
                WRITE, "*"));

        List<Logs.Line> byRole = new ArrayList<>();
        Logs.atTrace(RoleAudit.LOGGER, () -> byRole.addAll(byTypeAudit(authorizer, "bob", ALLOWED)));
        List<Logs.Line> byParent = new ArrayList<>();
        Logs.atTrace(RoleAudit.LOGGER, () -> byParent.addAll(byTypeAudit(parent, "bob", DENIED)));

        assertTrue(!byParent.isEmpty(), "the parent writes its own answers at TRACE");
        assertEquals(byParent, byRole);
    }

    // --- helpers --------------------------------------------------------------------------

    /** The authorizer as the broker creates it, and the broker's own with the same records. */
    private void start(StandardAcl... records) {
        authorizer = started(new RbacAuthorizer(), records);
        parent = started(new StandardAuthorizer(), records);
    }

    private <A extends StandardAuthorizer> A started(A created, StandardAcl... records) {
        Metrics its = new Metrics();
        metrics.add(its);
        started.add(created);
        created.configure(Map.of(StandardAuthorizer.SUPER_USERS_CONFIG, "User:admin"));
        created.withPluginMetrics(new PluginMetricsImpl(its, Map.of()));
        Map<Uuid, StandardAcl> snapshot = new HashMap<>();
        for (StandardAcl record : records) {
            snapshot.put(Uuid.randomUuid(), record);
        }
        created.loadSnapshot(snapshot);
        created.completeInitialLoad();
        return created;
    }

    /** A membership as it is created: the member is in the group. */
    private static StandardAcl member(String member, String group) {
        return RbacAuthorizerTest.acl("Member:" + member, literal(TOPIC, group), AclOperation.ALL, "*",
                AclPermissionType.ALLOW);
    }

    private static Action action(AclOperation operation, ResourcePattern resource,
            boolean logIfAllowed, boolean logIfDenied) {
        return new Action(operation, resource, 1, logIfAllowed, logIfDenied);
    }

    /** What the broker's authorizer log gets, at its usual level, when the user sends a Fetch with the action. */
    private static List<Logs.Line> audit(StandardAuthorizer asked, String user, Action action,
            AuthorizationResult expected) {
        AuthorizableRequestContext context = RequestContext.of(user, "billing-1", ApiKeys.FETCH);
        return Logs.of(RoleAudit.LOGGER,
                () -> assertEquals(List.of(expected), asked.authorize(context, List.of(action)), user));
    }

    /** The same with the log at {@code DEBUG}, where allowed requests are written. */
    private static List<Logs.Line> auditAtDebug(StandardAuthorizer asked, String user, Action action,
            AuthorizationResult expected) {
        List<Logs.Line> lines = new ArrayList<>();
        Logs.atDebug(RoleAudit.LOGGER, () -> lines.addAll(audit(asked, user, action, expected)));
        return lines;
    }

    /** The same with the log at {@code TRACE}, where the broker's authorizer writes every answer. */
    private static List<Logs.Line> auditAtTrace(StandardAuthorizer asked, String user, Action action,
            AuthorizationResult expected) {
        List<Logs.Line> lines = new ArrayList<>();
        Logs.atTrace(RoleAudit.LOGGER, () -> lines.addAll(audit(asked, user, action, expected)));
        return lines;
    }

    /** Each line as its level, its principal, its verdict and the kind of its rule. */
    private static List<String> verdicts(List<Logs.Line> lines) {
        return lines.stream().map(line -> line.level() + " "
                + line.message().replaceAll("Principal = (\\S+) is (\\w+) .* based on rule (\\w+).*", "$1 $2 $3"))
                .toList();
    }

    private static List<Logs.Line> byTypeAudit(StandardAuthorizer asked, String user, AuthorizationResult expected) {
        AuthorizableRequestContext context = RequestContext.of(user, "billing-1", ApiKeys.INIT_PRODUCER_ID);
        return Logs.of(RoleAudit.LOGGER,
                () -> assertEquals(expected, asked.authorizeByResourceType(context, WRITE, TOPIC), user));
    }
}
