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
import static dev.kfkit.kfkrbac.Worker.CAROL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Network;

/**
 * The jar that ships as the extension of a real Kafka Connect cluster, two
 * workers of the release {@code -PbrokerVersion} names, and as the
 * authorizer of the broker they use. Each test has a broker and workers of
 * its own.
 *
 * <p>The workers authenticate callers with HTTP basic authentication;
 * {@code admin} is a super user of the broker and of the extension and
 * creates the bindings, {@code alice}, {@code bob} and {@code carol} are
 * callers no ACL names. A binding is in force in a worker once the broker
 * has it and the extension has read it again, every
 * {@link Worker#REFRESH}; so the first request that depends on it is asked
 * until it goes through, with a bound.
 */
@Timeout(value = 6, unit = TimeUnit.MINUTES)
class ConnectIT {

    private static final String TOPIC = "billing.orders";

    /** How long a binding may take from {@code AdminClient}'s answer to being in force in a worker. */
    private static final Duration IN_FORCE = Duration.ofSeconds(20);

    private Network network;
    private Node node;
    private final List<Worker> workers = new ArrayList<>();
    private Worker worker1;
    private Worker worker2;
    private Clients clients;

    @BeforeEach
    void start() {
        Node.requireDocker();
        network = Network.newNetwork();
        node = Node.combined(network);
        Node.start(node);
        clients = new Clients(node.bootstrap());
        clients.createTopics(TOPIC);
        worker1 = new Worker(1, node, network);
        worker2 = new Worker(2, node, network);
        workers.addAll(List.of(worker1, worker2));
        Worker.start(worker1, worker2);
    }

    @AfterEach
    void stop(TestInfo test) {
        String name = test.getTestMethod().orElseThrow().getName();
        for (Worker worker : workers) {
            worker.close(name);
        }
        if (node != null) {
            node.close(name);
        }
        if (network != null) {
            network.close();
        }
    }

    /** AC8, AC1 and AC7. */
    @Test
    void developerManageOnTheBillingPrefixCreatesBillingSinkAndNotPaymentsSink() {
        assertRefused(worker1.createSink(ALICE, "billing-sink", TOPIC), "Create", "billing-sink",
                "no binding names her; that the extension says so is that it loaded");

        clients.createAcls(Clients.connectorBinding("DeveloperManage", "User:" + ALICE, "billing-"));

        Worker.Reply created = Await.until(IN_FORCE, () -> worker1.createSink(ALICE, "billing-sink", TOPIC),
                reply -> reply.status() != 403 && reply.status() != 409,
                last -> "expected the binding to be in force within " + IN_FORCE.toSeconds() + " s; last: " + last);
        assertEquals(201, created.status(), created.toString());

        assertRefused(worker1.createSink(ALICE, "payments-sink", TOPIC), "Create", "payments-sink",
                "her binding is on Connector:billing-");
        assertRefused(worker1.request(CAROL, "GET", "/connectors/billing-sink/status", null), "ReadStatus",
                "billing-sink", "the binding is alice's");

        assertEquals(201, worker1.createSink(ADMIN, "payments-sink", TOPIC).status(), "admin is a super user");
        for (Worker worker : workers) {
            Worker.Reply list = Await.until(IN_FORCE, () -> worker.request(ALICE, "GET", "/connectors", null),
                    reply -> reply.status() == 200 && reply.body().contains("billing-sink"),
                    last -> "expected her list to show billing-sink; last: " + last);
            assertTrue(!list.body().contains("payments-sink"), "her list shows only what she may see: " + list);
            Worker.Reply all = Await.until(IN_FORCE, () -> worker.request(ADMIN, "GET", "/connectors", null),
                    reply -> reply.status() == 200 && reply.body().contains("payments-sink"),
                    last -> "expected the super user's list to show payments-sink; last: " + last);
            assertTrue(all.body().contains("billing-sink"), all.toString());
        }
    }

    /** AC5: a role bound to a group applies to the group's members, and only once the membership exists. */
    @Test
    void aRoleHeldThroughAGroupPasses() {
        clients.createAcls(Clients.connectorBinding("DeveloperManage", "Group:payments", "payments-"));
        clients.createAcls(Clients.connectorBinding("DeveloperManage", "User:" + CAROL, "nothing-"));
        // The binding of carol's is a marker: once she is refused saying her binding is not for this connector,
        // the group's binding, created before it, is in force too.
        Await.until(IN_FORCE, () -> worker1.createSink(CAROL, "nothing-sink", TOPIC),
                reply -> reply.status() == 201 || reply.status() == 409 && reply.body().contains("already"),
                last -> "expected the marker binding to be in force; last: " + last);
        assertRefused(worker1.createSink(BOB, "payments-sink", TOPIC), "Create", "payments-sink",
                "he is not a member of Group:payments yet");

        clients.createAcls(Clients.membership(BOB, "payments"));

        Worker.Reply created = Await.until(IN_FORCE, () -> worker1.createSink(BOB, "payments-sink", TOPIC),
                reply -> reply.status() != 403 && reply.status() != 409,
                last -> "expected his membership to be in force within " + IN_FORCE.toSeconds() + " s; last: "
                        + last);
        assertEquals(201, created.status(), created.toString());
        assertRefused(worker1.createSink(CAROL, "payments-other", TOPIC), "Create", "payments-other",
                "she is not a member of Group:payments");
    }

    /**
     * AC4: the caller is the basic-authentication user; an anonymous request
     * gets 401; a request that a follower forwards to the leader is decided
     * there as the user again. Of two workers one is not the leader, so one
     * of the two creates is forwarded.
     */
    @Test
    void theCallerIsTheBasicAuthenticationUserOnEveryWorker() {
        for (Worker worker : workers) {
            Worker.Reply anonymous = worker.request(null, "GET", "/connectors", null);
            assertEquals(401, anonymous.status(), anonymous.toString());
        }
        clients.createAcls(Clients.connectorBinding("DeveloperManage", "User:" + ALICE, "billing-"));

        int number = 0;
        for (Worker worker : workers) {
            String name = "billing-sink-" + ++number;
            Worker.Reply created = Await.until(IN_FORCE, () -> worker.createSink(ALICE, name, TOPIC),
                    reply -> reply.status() != 403 && reply.status() != 409,
                    last -> "expected her binding to be in force within " + IN_FORCE.toSeconds() + " s; last: "
                            + last);
            assertEquals(201, created.status(), "decided as alice where it arrived and where it was forwarded: "
                    + created);
            assertRefused(worker.createSink(BOB, "billing-other", TOPIC), "Create", "billing-other",
                    "the binding is alice's, and bob is who authenticated");
        }
    }

    private static void assertRefused(Worker.Reply reply, String operation, String connector, String why) {
        assertEquals(403, reply.status(), "expected a refusal, " + why + ": " + reply);
        String user = reply.request().substring(0, reply.request().indexOf(' '));
        String missing = "User:" + user + " may not " + operation + " connector " + connector;
        assertTrue(reply.body().contains(missing), "expected the refusal to say \"" + missing + "\": " + reply);
    }
}
