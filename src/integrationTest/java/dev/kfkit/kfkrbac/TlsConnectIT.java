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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

/**
 * AC4 of #20 with mutual TLS: two real Connect workers of the release
 * {@code -PbrokerVersion} names take their REST callers by client
 * certificate, with the built jar as their extension and as the authorizer
 * of the broker they use.
 *
 * <p>The certificates are {@code CN=<name>,O=example}, signed by an
 * authority of the test's own: {@code admin}, {@code alice} and {@code bob}
 * call, {@code worker-1} and {@code worker-2} are the workers, which
 * present theirs when one forwards a request to the leader. Of two workers
 * one is not the leader, so of a create sent to each, one is forwarded.
 */
@Timeout(value = 6, unit = TimeUnit.MINUTES)
class TlsConnectIT {

    private static final String TOPIC = "billing.orders";

    /** How long a binding may take from {@code AdminClient}'s answer to being in force in a worker. */
    private static final Duration IN_FORCE = Duration.ofSeconds(20);

    private static Certificates certificates;

    private Network network;
    private Node node;
    private final List<Worker> workers = new ArrayList<>();
    private Clients clients;

    @BeforeAll
    static void certificates() {
        Node.requireDocker();
        Set<String> hosts = new LinkedHashSet<>(List.of(Worker.alias(1), Worker.alias(2), "localhost", "127.0.0.1"));
        hosts.add(DockerClientFactory.instance().dockerHostIpAddress());
        certificates = new Certificates(List.of(ADMIN, ALICE, BOB, Worker.alias(1), Worker.alias(2)),
                List.copyOf(hosts));
    }

    /** A broker and two workers whose extension passes these principals. */
    private void start(List<String> superUsers) {
        network = Network.newNetwork();
        node = Node.combined(network);
        Node.start(node);
        clients = new Clients(node.bootstrap());
        clients.createTopics(TOPIC);
        Worker worker1 = Worker.tls(1, node, network, certificates, superUsers);
        Worker worker2 = Worker.tls(2, node, network, certificates, superUsers);
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

    /**
     * The caller is the certificate's subject; a caller without a
     * certificate gets 401; a request a follower forwards passes the leader
     * as the forwarding worker, whose principal is a super user.
     */
    @Test
    void theCallerIsTheCertificateSubjectAndAForwardedRequestPassesAsAWorkerThatIsASuperUser() {
        start(List.of(user(ADMIN), user(Worker.alias(1)), user(Worker.alias(2))));
        for (Worker worker : workers) {
            Worker.Reply anonymous = worker.request(null, "GET", "/connectors", null);
            assertEquals(401, anonymous.status(), "no certificate, no caller: " + anonymous);
        }
        Worker first = workers.get(0);
        assertRefused(first, first.createSink(ALICE, "billing-sink-0", TOPIC), ALICE, "Create", "billing-sink-0",
                "no binding names her subject yet");

        clients.createAcls(Clients.connectorBinding("DeveloperManage", user(ALICE), "billing-"));

        int number = 0;
        for (Worker worker : workers) {
            String name = "billing-sink-" + ++number;
            Worker.Reply created = Await.until(IN_FORCE, () -> worker.createSink(ALICE, name, TOPIC),
                    reply -> reply.status() != 403 && reply.status() != 409,
                    last -> "expected her binding to be in force within " + IN_FORCE.toSeconds() + " s; last: "
                            + last);
            assertEquals(201, created.status(), "decided as her subject where it arrived; where it was forwarded, "
                    + "the forwarding worker is a super user: " + created);
            assertRefused(worker, worker.createSink(BOB, "billing-other", TOPIC), BOB, "Create", "billing-other",
                    "the binding is alice's, and bob's certificate is what was presented");
        }
        Worker.Reply list = workers.get(1).request(ALICE, "GET", "/connectors", null);
        assertEquals(200, list.status(), list.toString());
        assertTrue(list.body().contains("billing-sink-1") && list.body().contains("billing-sink-2"),
                "both were created: " + list);
    }

    /**
     * The twin: when the workers' principal is not a super user, the leader
     * refuses what a follower forwards, naming the follower; what arrives at
     * the leader itself is decided as the caller and passes.
     */
    @Test
    void aForwardedRequestIsRefusedWhenTheWorkerIsNotASuperUser() {
        start(List.of(user(ADMIN)));
        clients.createAcls(Clients.connectorBinding("DeveloperManage", user(ALICE), "billing-"));

        Map<Integer, Worker.Reply> replies = new TreeMap<>();
        for (int id = 1; id <= workers.size(); id++) {
            Worker worker = workers.get(id - 1);
            String name = "billing-sink-" + id;
            replies.put(id, Await.until(IN_FORCE, () -> worker.createSink(ALICE, name, TOPIC),
                    reply -> reply.status() != 409 && !(reply.status() == 403 && reply.body().contains(user(ALICE))),
                    last -> "expected her binding to be in force within " + IN_FORCE.toSeconds() + " s; last: "
                            + last));
        }

        List<Integer> created = replies.entrySet().stream().filter(entry -> entry.getValue().status() == 201)
                .map(Map.Entry::getKey).toList();
        assertEquals(1, created.size(), "exactly one worker is the leader and decides her request itself: "
                + replies);
        int follower = created.get(0) == 1 ? 2 : 1;
        Worker.Reply forwarded = replies.get(follower);
        assertEquals(403, forwarded.status(), "forwarded as worker-" + follower + ": " + forwarded);
        String missing = user(Worker.alias(follower)) + " may not Create connector billing-sink-" + follower;
        assertTrue(forwarded.body().contains(missing), "expected the leader's refusal to say \"" + missing + "\": "
                + forwarded);
    }

    private static String user(String name) {
        return "User:" + Certificates.subject(name);
    }

    private static void assertRefused(Worker worker, Worker.Reply reply, String user, String operation,
            String connector, String why) {
        assertEquals(403, reply.status(), "expected a refusal, " + why + ": " + reply);
        String missing = worker.principal(user) + " may not " + operation + " connector " + connector;
        assertTrue(reply.body().contains(missing), "expected the refusal to say \"" + missing + "\": " + reply);
    }
}
