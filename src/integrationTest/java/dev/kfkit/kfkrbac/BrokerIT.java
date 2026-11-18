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

import static dev.kfkit.kfkrbac.Node.ADMIN;
import static dev.kfkit.kfkrbac.Node.ALICE;
import static dev.kfkit.kfkrbac.Node.BOB;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Network;

/**
 * The jar that ships as the authorizer of a real broker: one node that is
 * broker and controller, of the release {@code -PbrokerVersion} names. Each
 * test has a node of its own, so no test sees what another bound.
 *
 * <p>{@code alice} and {@code bob} are users no ACL names; {@code admin} is
 * a super user and creates the topics, the bindings and the ACLs. A binding
 * is in force on the broker a moment after {@code AdminClient} has created
 * it, so the first request that depends on it is asked until it goes
 * through, with a bound; what follows from the same record is asked once.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class BrokerIT {

    private static final String PREFIX = "billing.";
    private static final String TOPIC = "billing.orders";
    private static final String OTHER_TOPIC = "billing.invoices";
    private static final String ELSEWHERE = "payments.orders";

    /** How long a binding or an ACL may take from {@code AdminClient}'s answer to being in force. */
    private static final Duration IN_FORCE = Duration.ofSeconds(30);

    private Network network;
    private Node node;
    private Clients clients;

    @BeforeEach
    void start() {
        Node.requireDocker();
        network = Network.newNetwork();
        node = Node.combined(network);
        Node.start(node);
        clients = new Clients(node.bootstrap());
        clients.createTopics(TOPIC, OTHER_TOPIC, ELSEWHERE);
    }

    @AfterEach
    void stop(TestInfo test) {
        if (node != null) {
            node.close(test.getTestMethod().orElseThrow().getName());
        }
        if (network != null) {
            network.close();
        }
    }

    /** AC1. */
    @Test
    void aWriteBindingLetsProduceIdempotentlyAndInATransaction() {
        clients.produce(ALICE, TOPIC).assertRefused("no binding and no ACL names her");
        clients.produceInTransaction(ALICE, "billing.tx", TOPIC)
                .assertRefused("no binding and no ACL names her");

        clients.createAcls(Clients.binding("DeveloperWrite", ALICE, PREFIX));

        Await.allowed(IN_FORCE, "she holds DeveloperWrite on " + PREFIX, () -> clients.produce(ALICE, TOPIC));
        clients.produceInTransaction(ALICE, "billing.tx", TOPIC)
                .assertAllowed("she holds DeveloperWrite on " + PREFIX + ", which covers the transactional ID");

        clients.produceInTransaction(ALICE, "payments.tx", TOPIC)
                .assertRefused(TransactionalIdAuthorizationException.class,
                        "the transactional ID is outside " + PREFIX);
        clients.produce(ALICE, ELSEWHERE).assertRefused("the topic is outside " + PREFIX);
        clients.produce(BOB, TOPIC).assertRefused("the binding is hers, not his");
    }

    /** AC2. */
    @Test
    void oneReadBindingCoversTheTopicsAndTheGroupsOfThePrefix() {
        clients.produce(ADMIN, TOPIC).assertAllowed("he is a super user");
        clients.consume(ALICE, "billing.app", TOPIC).assertRefused("no binding and no ACL names her");

        clients.createAcls(Clients.binding("DeveloperRead", ALICE, PREFIX));

        Await.allowed(IN_FORCE, "she holds DeveloperRead on " + PREFIX,
                () -> clients.consume(ALICE, "billing.app", TOPIC));
        clients.consume(ALICE, "payments.app", TOPIC)
                .assertRefused(GroupAuthorizationException.class, "the group is outside " + PREFIX);
        clients.produce(ALICE, TOPIC).assertRefused("DeveloperRead does not grant writing");
    }

    /** AC3. */
    @Test
    void aDenyAclStopsWhatTheBindingAllows() {
        AclBinding binding = Clients.binding("DeveloperWrite", ALICE, PREFIX);
        clients.createAcls(binding);
        Await.allowed(IN_FORCE, "she holds DeveloperWrite on " + PREFIX, () -> clients.produce(ALICE, TOPIC));

        AclBinding deny = Clients.deny(ALICE, TOPIC);
        clients.createAcls(deny);

        Await.refused(IN_FORCE, "an ACL denies her that topic", () -> clients.produce(ALICE, TOPIC));
        // The binding stays next to the deny ACL.
        awaitListed(AclBindingFilter.ANY, Set.of(binding, deny));
        clients.produce(ALICE, OTHER_TOPIC)
                .assertAllowed("the binding is still in force and the deny ACL names another topic");
    }

    /** AC6. */
    @Test
    void bindingsAreListedAsAclsWithTheRoleInThePrincipal() {
        AclBinding write = Clients.binding("DeveloperWrite", ALICE, PREFIX);
        AclBinding read = Clients.binding("DeveloperRead", BOB, PREFIX);
        assertEquals("DeveloperWrite:User:alice", write.entry().principal());
        assertEquals("DeveloperRead:User:bob", read.entry().principal());
        assertEquals(Set.of(), clients.describeAcls(AclBindingFilter.ANY), "the ACLs of a new cluster");

        clients.createAcls(write, read);

        // The broker that answers lists a record once it has it, a moment after it was created.
        awaitListed(AclBindingFilter.ANY, Set.of(write, read));
        awaitListed(new AclBindingFilter(ResourcePatternFilter.ANY, new AccessControlEntryFilter(
                "DeveloperRead:User:bob", null, AclOperation.ANY, AclPermissionType.ANY)), Set.of(read));
    }

    /** AC7. */
    @Test
    void bindingsAreInForceWhenTheBrokerIsBackAfterARestart() {
        AclBinding binding = Clients.binding("DeveloperWrite", ALICE, PREFIX);
        clients.createAcls(binding);
        Await.allowed(IN_FORCE, "she holds DeveloperWrite on " + PREFIX, () -> clients.produce(ALICE, TOPIC));

        node.restart();

        // Asked once, and first: nothing has spoken to the broker since it started.
        clients.produce(ALICE, TOPIC)
                .assertAllowed("the binding was created before the restart and the broker serves no request "
                        + "before it has loaded its ACL records");
        clients.produce(BOB, TOPIC).assertRefused("the binding is hers, not his");
        assertEquals(Set.of(binding), clients.describeAcls(AclBindingFilter.ANY),
                "the ACLs of the cluster after the restart");
    }

    private void awaitListed(AclBindingFilter filter, Set<AclBinding> expected) {
        Await.until(IN_FORCE, () -> clients.describeAcls(filter), expected::equals,
                listed -> "expected describeAcls(" + filter + ") to list " + expected + " within "
                        + IN_FORCE.toSeconds() + " s; the broker listed: " + listed);
    }
}
