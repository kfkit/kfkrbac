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

import static dev.kfkit.kfkrbac.Node.ALICE;
import static dev.kfkit.kfkrbac.Node.BOB;

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
 * The jar that ships as the authorizer of every node of a cluster: a
 * controller and two brokers, each a container of its own on one network,
 * of the newest release unless {@code -PbrokerVersion} names another. Each
 * test has a cluster of its own.
 *
 * <p>A request is authorized by the broker that receives it, and a record is
 * produced to the broker that leads its partition. So each topic here has
 * one partition on one broker, named after it: producing to
 * {@code billing.on-3} asks the authorizer of broker 3.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class ClusterIT {

    private static final String PREFIX = "billing.";

    /** How long a binding may take from {@code AdminClient}'s answer to being in force on every broker. */
    private static final Duration IN_FORCE = Duration.ofSeconds(10);

    private Network network;
    private final List<Node> nodes = new ArrayList<>();
    private Node broker2;
    private Node broker3;
    /** Clients that know the cluster by broker 2, which no test stops. */
    private Clients clients;

    @BeforeEach
    void start() {
        Node.requireDocker();
        network = Network.newNetwork();
        Node controller = Node.controller(network);
        broker2 = Node.broker(2, network);
        broker3 = Node.broker(3, network);
        nodes.addAll(List.of(controller, broker2, broker3));
        Node.start(controller, broker2, broker3);
        clients = new Clients(broker2.bootstrap());
        clients.createTopicOn(2, "billing.on-2");
        clients.createTopicOn(3, "billing.on-3");
    }

    @AfterEach
    void stop(TestInfo test) {
        for (Node node : nodes) {
            node.close(test.getTestMethod().orElseThrow().getName());
        }
        if (network != null) {
            network.close();
        }
    }

    /** AC8. */
    @Test
    void aBindingReachesEveryBrokerAndTheOneStartedAfterIt() {
        clients.produce(ALICE, "billing.on-2").assertRefused("no binding and no ACL names her");
        clients.produce(ALICE, "billing.on-3").assertRefused("no binding and no ACL names her");

        clients.createAcls(Clients.binding("DeveloperWrite", ALICE, PREFIX));

        Await.allowed(IN_FORCE, "she holds DeveloperWrite on " + PREFIX + " and broker 2 has the record",
                () -> clients.produce(ALICE, "billing.on-2"));
        Await.allowed(IN_FORCE, "she holds DeveloperWrite on " + PREFIX + " and broker 3 has the record",
                () -> clients.produce(ALICE, "billing.on-3"));

        Node broker4 = Node.broker(4, network);
        nodes.add(broker4);
        Node.start(broker4);

        // Asked once, and first: nothing has spoken to broker 4 since it started.
        Clients onlyBroker4 = new Clients(broker4.bootstrap());
        onlyBroker4.describe(ALICE, "billing.on-2")
                .assertAllowed("the binding was created before broker 4 started and a broker serves no request "
                        + "before it has loaded its ACL records");
        onlyBroker4.describe(BOB, "billing.on-2").assertRefused("the binding is hers, not his");

        clients.createTopicOn(4, "billing.on-4");
        clients.produce(ALICE, "billing.on-4")
                .assertAllowed("she holds DeveloperWrite on " + PREFIX + " and broker 4 leads the partition");
        clients.produce(BOB, "billing.on-4").assertRefused("the binding is hers, not his");
    }

    /** AC9. */
    @Test
    void aBrokerRestartedWhileTheOthersRunAuthorizesWithRolesWhenItIsBack() {
        clients.createTopicOn(3, "payments.on-3");
        clients.createAcls(Clients.binding("DeveloperWrite", ALICE, PREFIX));
        Await.allowed(IN_FORCE, "she holds DeveloperWrite on " + PREFIX + " and broker 2 has the record",
                () -> clients.produce(ALICE, "billing.on-2"));
        Await.allowed(IN_FORCE, "she holds DeveloperWrite on " + PREFIX + " and broker 3 has the record",
                () -> clients.produce(ALICE, "billing.on-3"));
        clients.produce(BOB, "payments.on-3").assertRefused("no binding and no ACL names him");

        broker3.stop();
        // While broker 3 is away the cluster goes on, and a binding is created that it has never seen.
        clients.produce(ALICE, "billing.on-2").assertAllowed("broker 2 runs and has the binding");
        clients.createAcls(Clients.binding("DeveloperWrite", BOB, "payments."));
        broker3.startAgain();

        // Each asked once: nothing has spoken to broker 3 since it started.
        clients.produce(ALICE, "billing.on-3")
                .assertAllowed("the binding was created before broker 3 stopped and a broker serves no request "
                        + "before it has loaded its ACL records");
        clients.produce(BOB, "payments.on-3")
                .assertAllowed("he was bound DeveloperWrite on payments. while broker 3 was away and a broker "
                        + "serves no request before it has caught up with the ACL records");
        clients.produce(BOB, "billing.on-3").assertRefused("his binding is on payments., not on " + PREFIX);
    }
}
