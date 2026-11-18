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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * One node of a cluster in a container: the {@code apache/kafka} image with
 * the built jar on its classpath as the authorizer. A node is a controller,
 * a broker, or both.
 *
 * <p>Clients are different principals: the listener they use is
 * SASL_PLAINTEXT with the mechanism PLAIN, and the users are in a JAAS file
 * the JVM is pointed to. The brokers speak to each other on a listener of
 * their own, as {@code User:admin}; the controller's listener is plaintext,
 * so {@code User:ANONYMOUS} is a super user next to {@code User:admin}.
 *
 * <p>The clients' port is the same on the host for as long as the node
 * exists, so the address the broker advertises stays true when the node is
 * stopped and started again.
 *
 * <p>Gradle passes the image ({@code -PbrokerVersion}) and the jar as system
 * properties.
 */
final class Node implements AutoCloseable {

    /** The super user the tests manage the cluster as. */
    static final String ADMIN = "admin";
    static final String ALICE = "alice";
    static final String BOB = "bob";

    /** The node ID of the controller, in a cluster and on one node alike. */
    static final int CONTROLLER_ID = 1;

    private static final String NO_DOCKER =
            "Docker is not available; the broker integration tests need it and are skipped";

    private static final String DIR = "/opt/kfkrbac/";
    private static final int CLIENT_PORT = 9092;
    private static final int CONTROLLER_PORT = 9093;
    private static final int INTERNAL_PORT = 19092;
    private static final String STARTED = "Kafka Server started";
    private static final String AUTHORIZER_LOG = "/opt/kafka/logs/kafka-authorizer.log";
    private static final Duration START_TIMEOUT = Duration.ofMinutes(3);
    /** How long a node gets to shut down in an orderly way before it is killed. */
    private static final int STOP_TIMEOUT_SECONDS = 60;

    private static final String JAAS = """
            KafkaServer {
              org.apache.kafka.common.security.plain.PlainLoginModule required
              username="%1$s"
              password="%2$s"
              user_%1$s="%2$s"
              user_%3$s="%4$s"
              user_%5$s="%6$s";
            };
            """.formatted(ADMIN, password(ADMIN), ALICE, password(ALICE), BOB, password(BOB));

    /** The ports given to nodes: a port is free again the moment it was found, so it could be found twice. */
    private static final Set<Integer> PORTS_GIVEN = ConcurrentHashMap.newKeySet();

    private final int id;
    private final GenericContainer<?> container;
    /** The clients' port on the host; 0 for a node that is only a controller. */
    private final int port;
    /** How many times the node was started: as many times its log must say so. */
    private int starts;

    private Node(int id, boolean controller, boolean broker, Network network) {
        this.id = id;
        this.port = broker ? freePort() : 0;
        Map<String, String> env = new LinkedHashMap<>();
        env.put("CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw");
        env.put("KAFKA_NODE_ID", Integer.toString(id));
        env.put("KAFKA_PROCESS_ROLES", Stream.of(broker ? "broker" : null, controller ? "controller" : null)
                .filter(role -> role != null).collect(Collectors.joining(",")));
        env.put("KAFKA_CONTROLLER_QUORUM_VOTERS", CONTROLLER_ID + "@" + alias(CONTROLLER_ID) + ":" + CONTROLLER_PORT);
        env.put("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
        env.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "CLIENT:SASL_PLAINTEXT,INTERNAL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT");
        List<String> listeners = new ArrayList<>();
        if (broker) {
            listeners.add("CLIENT://:" + CLIENT_PORT);
            listeners.add("INTERNAL://:" + INTERNAL_PORT);
            env.put("KAFKA_ADVERTISED_LISTENERS",
                    "CLIENT://" + DockerClientFactory.instance().dockerHostIpAddress() + ":" + port
                            + ",INTERNAL://" + alias(id) + ":" + INTERNAL_PORT);
            env.put("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL");
            env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN");
            env.put("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN");
            // Per-listener JAAS through the environment does not work in this image.
            env.put("KAFKA_OPTS", "-Djava.security.auth.login.config=" + DIR + "jaas.conf");
            env.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
            env.put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
            env.put("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
            env.put("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
            env.put("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
        }
        if (controller) {
            listeners.add("CONTROLLER://:" + CONTROLLER_PORT);
        }
        env.put("KAFKA_LISTENERS", String.join(",", listeners));
        // By name: the node loads the class from the jar, not from this test's classpath.
        env.put("KAFKA_AUTHORIZER_CLASS_NAME", "dev.kfkit.kfkrbac.RbacAuthorizer");
        env.put("KAFKA_SUPER_USERS", "User:" + ADMIN + ";User:ANONYMOUS");
        env.put("CLASSPATH", DIR + "kfkrbac.jar");

        container = new GenericContainer<>(DockerImageName.parse(property("kfkrbac.it.image")))
                .withNetwork(network)
                .withNetworkAliases(alias(id))
                .withCopyFileToContainer(MountableFile.forHostPath(Path.of(property("kfkrbac.it.jar"))),
                        DIR + "kfkrbac.jar")
                .withCopyToContainer(Transferable.of(JAAS), DIR + "jaas.conf")
                .withEnv(env)
                .waitingFor(Wait.forLogMessage(".*" + STARTED + ".*\\n", 1).withStartupTimeout(START_TIMEOUT));
        if (broker) {
            container.setPortBindings(List.of(port + ":" + CLIENT_PORT));
        }
    }

    /** A node that is the controller and a broker at once: a cluster of one. */
    static Node combined(Network network) {
        return new Node(CONTROLLER_ID, true, true, network);
    }

    /** A node that is only the controller. */
    static Node controller(Network network) {
        return new Node(CONTROLLER_ID, true, false, network);
    }

    /** A node that is only a broker. */
    static Node broker(int id, Network network) {
        if (id == CONTROLLER_ID) {
            throw new IllegalArgumentException(id + " is the controller's node ID");
        }
        return new Node(id, false, true, network);
    }

    /**
     * Skips the test when there is no Docker; in CI, where a skip would pass
     * the job without running anything, fails it.
     */
    static void requireDocker() {
        boolean available = DockerClientFactory.instance().isDockerAvailable();
        assertFalse(!available && "true".equals(System.getenv("GITHUB_ACTIONS")), NO_DOCKER);
        assumeTrue(available, NO_DOCKER);
    }

    /** Starts the nodes at once and returns when each has said it started. */
    static void start(Node... nodes) {
        Startables.deepStart(Stream.of(nodes).map(node -> node.container)).join();
        for (Node node : nodes) {
            node.starts++;
        }
    }

    /** Shuts the node down, in an orderly way; its data stays. */
    void stop() {
        container.getDockerClient().stopContainerCmd(container.getContainerId())
                .withTimeout(STOP_TIMEOUT_SECONDS).exec();
    }

    /** Starts the node after {@link #stop} and returns when it has said it started, before any request to it. */
    void startAgain() {
        starts++;
        container.getDockerClient().startContainerCmd(container.getContainerId()).exec();
        Await.until(START_TIMEOUT, container::getLogs, log -> timesStarted(log) >= starts,
                log -> "expected node " + id + " to say \"" + STARTED + "\" " + starts + " times within "
                        + START_TIMEOUT.toSeconds() + " s; it said so " + timesStarted(log)
                        + " times, and its log ends with:\n" + tail(log));
    }

    void restart() {
        stop();
        startAgain();
    }

    int id() {
        return id;
    }

    /** The address clients reach this broker at, and no other broker. */
    String bootstrap() {
        if (port == 0) {
            throw new IllegalStateException("node " + id + " is not a broker");
        }
        return DockerClientFactory.instance().dockerHostIpAddress() + ":" + port;
    }

    /**
     * The address a container on the same network reaches this broker at,
     * on the brokers' own listener: SASL_PLAINTEXT, mechanism PLAIN, as
     * {@code admin}.
     */
    String internalBootstrap() {
        if (port == 0) {
            throw new IllegalStateException("node " + id + " is not a broker");
        }
        return alias(id) + ":" + INTERNAL_PORT;
    }

    /** What the node has logged since it was created, restarts included. */
    String log() {
        return container.getLogs();
    }

    /** The password of a user in the JAAS file. */
    static String password(String user) {
        return user + "-secret";
    }

    /**
     * Keeps what the node logged under {@code build/integration-logs}, its
     * authorizer's own log of what it denied next to it, and removes the
     * node.
     */
    void close(String test) {
        try {
            if (container.getContainerId() != null) {
                Path dir = Path.of(property("kfkrbac.it.logs"));
                Files.createDirectories(dir);
                String name = test + "-node-" + id;
                Files.writeString(dir.resolve(name + ".log"), container.getLogs());
                container.copyFileFromContainer(AUTHORIZER_LOG, dir.resolve(name + "-authorizer.log").toString());
            }
        } catch (IOException | RuntimeException e) {
            // The logs are there to explain a failure; not having them is not one.
            System.err.println("could not keep the logs of node " + id + " after " + test + ": " + e);
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        container.stop();
    }

    private static String alias(int id) {
        return "node-" + id;
    }

    private static int timesStarted(String log) {
        int times = 0;
        for (int at = log.indexOf(STARTED); at >= 0; at = log.indexOf(STARTED, at + 1)) {
            times++;
        }
        return times;
    }

    private static String tail(String log) {
        List<String> lines = log.lines().toList();
        return String.join("\n", lines.subList(Math.max(0, lines.size() - 40), lines.size()));
    }

    /** A port that is free on this host now and that no other node of these tests was given. */
    private static int freePort() {
        while (true) {
            try (ServerSocket socket = new ServerSocket(0)) {
                if (PORTS_GIVEN.add(socket.getLocalPort())) {
                    return socket.getLocalPort();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static String property(String name) {
        String value = System.getProperty(name);
        if (value == null) {
            throw new IllegalStateException(name + " is not set; run the tests through ./gradlew integrationTest");
        }
        return value;
    }
}
