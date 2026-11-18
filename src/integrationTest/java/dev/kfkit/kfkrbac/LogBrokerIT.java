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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Network;

/**
 * What a real broker's logs say with the jar that ships as its authorizer:
 * the broker's own log of its authorizer's decisions,
 * {@code kafka-authorizer.log}, and the line that says what was missing
 * when a request is denied, which goes where the broker's other messages
 * go. One node that is broker and controller, of the release
 * {@code -PbrokerVersion} names, with the logging configuration the image
 * ships.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class LogBrokerIT {

    private static final String PREFIX = "billing.";
    private static final String TOPIC = "billing.orders";

    /** How long a binding may take to be in force, and a line to reach the log. */
    private static final Duration SOON = Duration.ofSeconds(30);

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
        clients.createTopics(TOPIC);
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

    @Test
    void theLogsSayWhatWasDecidedAndWhatWasMissing() {
        clients.produce(ADMIN, TOPIC).assertAllowed("he is a super user");
        clients.createAcls(Clients.binding("DeveloperRead", ALICE, PREFIX));
        Await.allowed(SOON, "she holds DeveloperRead on " + PREFIX,
                () -> clients.consume(ALICE, "billing.app", TOPIC));

        // The binding is in force: a group no request has named yet is allowed by the role alone.
        clients.consume(ALICE, "billing.audit", TOPIC).assertAllowed("the role covers the group");
        clients.consume(ALICE, "payments.app", TOPIC)
                .assertRefused(GroupAuthorizationException.class, "the group is outside " + PREFIX);

        Pattern whatWasMissing = Pattern.compile("Denied User:alice \\w+ on GROUP 'payments\\.app' \\(request \\w+\\): "
                + "no ACL and no role allows it; roles held for GROUP: DeveloperRead on prefix billing\\.");
        Await.until(SOON, node::log, log -> whatWasMissing.matcher(log).find(),
                log -> "expected the node's log to say what " + ALICE + " was missing for the group payments.app, "
                        + "as " + whatWasMissing + "; it says of denials: " + lines(log, "Denied User:"));

        String decisions = Await.until(SOON, node::authorizerLog,
                log -> !lines(log, "Group:LITERAL:payments.app").isEmpty(),
                log -> "expected the authorizer log to name the group payments.app; it says:\n" + log);
        assertEquals(List.of(), lines(decisions, "Group:LITERAL:billing.audit"),
                "what the authorizer log says of the group a role allowed her: nothing at the level the broker "
                        + "logs at, and never that she was denied");
        List<String> refused = lines(decisions, "Group:LITERAL:payments.app");
        assertTrue(refused.stream().allMatch(line -> line.contains("Principal = User:alice is Denied operation = ")
                && line.contains(" based on rule DefaultDeny")),
                "the authorizer log says of the group she was refused, in the broker's own words: " + refused);
    }

    private static List<String> lines(String log, String containing) {
        return log.lines().filter(line -> line.contains(containing)).toList();
    }
}
