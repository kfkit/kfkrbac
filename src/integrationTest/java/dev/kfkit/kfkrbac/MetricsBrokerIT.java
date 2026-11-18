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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Network;

/**
 * kfkwho's access metrics from the jar that ships, on a real broker: one
 * node that is broker and controller, of the release {@code -PbrokerVersion}
 * names, with {@code kfkrbac.metrics.enabled=true}. The JMX Exporter agent
 * runs in the broker with the rules kfkwho released, unchanged, and the
 * tests read what an operator would scrape.
 *
 * <p>{@code alice} holds {@code DeveloperWrite} on {@code billing.} and no
 * ACL names her; {@code bob} has nothing.
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class MetricsBrokerIT {

    private static final String PREFIX = "billing.";
    private static final String TOPIC = "billing.orders";
    private static final String SERIES = "kfkwho_access_requests_total{";

    /** How long a binding may take to be in force, and a series to be created and scraped. */
    private static final Duration IN_FORCE = Duration.ofSeconds(30);

    private Network network;
    private Node node;
    private Clients clients;

    @BeforeEach
    void network() {
        Node.requireDocker();
        network = Network.newNetwork();
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

    /** AC5. */
    @Test
    void theExporterShowsWhatARoleAllowedAndWhatWasDenied() {
        start(Node.combined(network).withMetrics());

        aliceProducesByHerRoleAndBobIsRefused();

        awaitTheirSeries();
        assertNoErrors();
    }

    /** AC6. */
    @Test
    void theBrokerRunsWithKfkwhosOwnJarNextToThisOne() {
        start(Node.combined(network).withMetrics().withKfkwhoInLibs());
        assertEquals(List.of("kfkrbac.jar", "kfkwho.jar"), node.libs("kfk*.jar"), "the jars of ours in libs/");

        aliceProducesByHerRoleAndBobIsRefused();

        awaitTheirSeries();
        assertNoErrors();
    }

    private void start(Node created) {
        node = created;
        Node.start(node);
        clients = new Clients(node.bootstrap());
        clients.createTopics(TOPIC);
    }

    private void aliceProducesByHerRoleAndBobIsRefused() {
        clients.createAcls(Clients.binding("DeveloperWrite", ALICE, PREFIX));
        Await.allowed(IN_FORCE, "she holds DeveloperWrite on " + PREFIX, () -> clients.produce(ALICE, TOPIC));
        clients.produce(BOB, TOPIC).assertRefused("no binding and no ACL names him");
    }

    /**
     * Scrapes until the series of both are there: hers with the verdict of
     * her role, his denied, and none of his allowed.
     */
    private void awaitTheirSeries() {
        Map<String, String> hers = Map.of("principal", "User:" + ALICE, "resource_type", "TOPIC", "resource", TOPIC,
                "operation", "WRITE", "api", "PRODUCE", "result", "ALLOWED");
        Map<String, String> his = Map.of("principal", "User:" + BOB, "resource_type", "TOPIC", "resource", TOPIC,
                "result", "DENIED");
        List<String> series = Await.until(IN_FORCE,
                () -> node.scrape().lines().filter(line -> line.startsWith(SERIES)).toList(),
                scraped -> scraped.stream().anyMatch(line -> has(line, hers))
                        && scraped.stream().anyMatch(line -> has(line, his)),
                scraped -> "expected the exporter to show a series with " + hers + " and one with " + his
                        + " within " + IN_FORCE.toSeconds() + " s; it showed:\n" + String.join("\n", scraped));

        Map<String, String> hisAllowed = Map.of("principal", "User:" + BOB, "resource", TOPIC, "result", "ALLOWED");
        assertEquals(List.of(), series.stream().filter(line -> has(line, hisAllowed)).toList(),
                "series that say " + BOB + " was allowed something on " + TOPIC);
    }

    private static boolean has(String series, Map<String, String> labels) {
        return labels.entrySet().stream()
                .allMatch(label -> series.contains(label.getKey() + "=\"" + label.getValue() + "\""));
    }

    /** A class that could not be loaded or linked, and anything the authorizer or the recorder logged as an error. */
    private void assertNoErrors() {
        List<String> errors = node.log().lines()
                .filter(line -> line.contains("NoClassDefFoundError") || line.contains("LinkageError")
                        || line.contains("ERROR") && (line.toLowerCase(Locale.ROOT).contains("kfkwho")
                                || line.toLowerCase(Locale.ROOT).contains("kfkrbac")))
                .toList();
        assertEquals(List.of(), errors, "errors in the log of the node");
    }
}
