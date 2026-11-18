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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * The one-node cluster the documents' commands are run in: the container
 * of the {@code apache/kafka} image, and a shell inside it.
 *
 * <p>The commands of a page are written for {@code localhost:9092} and for
 * three files of client properties, {@code admin.properties},
 * {@code alice.properties} and {@code bob.properties}. Inside the container
 * that is all true as written: the clients' listener is advertised as
 * {@code localhost:9092}, the files are in the directory the commands run
 * in, next to the built jar under the name it was built with, and
 * {@code KAFKA_HOME} is the distribution of the image with its {@code bin}
 * on the {@code PATH}. So a command is run as the page has it.
 *
 * <p>The node is started in one of two ways, which are the two ways
 * {@code docs/install.md} installs the jar:
 * <ul>
 * <li>{@linkplain #fromTheDistribution as from the unpacked distribution}:
 * a properties file of its own and {@code kafka-server-start.sh}, so that a
 * page can copy the jar to {@code libs/}, change the file and have the node
 * restarted, which the image's own start script would undo;
 * <li>{@linkplain #ofTheImage as the image starts it}: its start script and
 * environment variables.
 * </ul>
 *
 * <p>{@link Node} cannot be this: it advertises the address of the host,
 * which a command inside the container does not reach, and keeps its
 * container to itself. The users, their passwords and the way a restart is
 * awaited are {@code Node}'s, the first two by calling it and the last
 * copied.
 */
final class DocBroker implements AutoCloseable {

    /** The directory the commands of a page run in. */
    static final String WORK = "/work";

    private static final String KAFKA_HOME = "/opt/kafka";
    private static final String STARTED = "Kafka Server started";
    private static final Duration START_TIMEOUT = Duration.ofMinutes(3);
    private static final int STOP_TIMEOUT_SECONDS = 60;

    /** What every block of commands starts with: where it runs, and its output as one stream. */
    private static final String SHELL = "cd " + WORK + "\nexport KAFKA_HOME=" + KAFKA_HOME + " PATH=" + KAFKA_HOME
            + "/bin:$PATH\nexec 2>&1\nset -e -o pipefail\n";

    /** The settings of a node that is broker and controller, without an authorizer. */
    private static final String SERVER_PROPERTIES = """
            process.roles=broker,controller
            node.id=1
            controller.quorum.voters=1@localhost:9093
            listeners=CLIENT://:9092,CONTROLLER://:9093
            advertised.listeners=CLIENT://localhost:9092
            listener.security.protocol.map=CLIENT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT
            inter.broker.listener.name=CLIENT
            controller.listener.names=CONTROLLER
            sasl.enabled.mechanisms=PLAIN
            sasl.mechanism.inter.broker.protocol=PLAIN
            log.dirs=/tmp/kfkrbac-docs/data
            auto.create.topics.enable=false
            offsets.topic.replication.factor=1
            transaction.state.log.replication.factor=1
            transaction.state.log.min.isr=1
            group.initial.rebalance.delay.ms=0
            """;

    /**
     * Starts the node the way an operator of the unpacked distribution
     * does. The properties file is copied once, when the storage is
     * formatted, so what a page changes in it is there after a restart. The
     * JMX port is the node's own and not the container's: a tool run next
     * to it would try to open it too.
     */
    private static final String START = """
            #!/usr/bin/env bash
            set -e
            cd %1$s
            if [ ! -f /tmp/kfkrbac-docs/data/meta.properties ]; then
              cp %2$s/server.properties config/server.properties
              bin/kafka-storage.sh format -t 4L6g3nShT-eMCtK--X86sw -c config/server.properties
            fi
            export KAFKA_OPTS=-Djava.security.auth.login.config=%2$s/jaas.conf
            export JMX_PORT=9999
            exec bin/kafka-server-start.sh config/server.properties
            """.formatted(KAFKA_HOME, WORK);

    private static final String JAAS = """
            KafkaServer {
              org.apache.kafka.common.security.plain.PlainLoginModule required
              username="%1$s"
              password="%2$s"
              user_%1$s="%2$s"
              user_%3$s="%4$s"
              user_%5$s="%6$s";
            };
            """.formatted(ADMIN, Node.password(ADMIN), ALICE, Node.password(ALICE), BOB, Node.password(BOB));

    /** What a block of commands did. */
    record Result(int exit, String output) {
    }

    private final GenericContainer<?> container;
    private int starts;

    private DocBroker(GenericContainer<?> container) {
        this.container = container
                .withCopyFileToContainer(MountableFile.forHostPath(jar()), WORK + "/" + jar().getFileName())
                .withCopyToContainer(Transferable.of(JAAS), WORK + "/jaas.conf")
                .waitingFor(Wait.forLogMessage(".*" + STARTED + ".*\\n", 1).withStartupTimeout(START_TIMEOUT));
        for (String user : List.of(ADMIN, ALICE, BOB)) {
            this.container.withCopyToContainer(Transferable.of(clientProperties(user)),
                    WORK + "/" + user + ".properties");
        }
    }

    /**
     * A node started from the distribution with these lines after the
     * settings every node of these tests has: its authorizer, if it has
     * one, and its super users.
     */
    static DocBroker fromTheDistribution(String... settings) {
        String properties = SERVER_PROPERTIES + String.join("\n", settings) + "\n";
        return new DocBroker(new GenericContainer<>(image())
                .withCopyToContainer(Transferable.of(properties), WORK + "/server.properties")
                .withCopyToContainer(Transferable.of(START, 0100755), WORK + "/start.sh")
                .withCreateContainerCmdModifier(create -> create.withEntrypoint(WORK + "/start.sh")));
    }

    /**
     * A node the image starts, with these variables next to the ones that
     * make it a node at all. {@code CLASSPATH} names where the jar is put.
     */
    static DocBroker ofTheImage(Map<String, String> variables) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw");
        env.put("KAFKA_NODE_ID", "1");
        env.put("KAFKA_PROCESS_ROLES", "broker,controller");
        env.put("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093");
        env.put("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
        env.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CLIENT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT");
        env.put("KAFKA_LISTENERS", "CLIENT://:9092,CONTROLLER://:9093");
        env.put("KAFKA_ADVERTISED_LISTENERS", "CLIENT://localhost:9092");
        env.put("KAFKA_INTER_BROKER_LISTENER_NAME", "CLIENT");
        env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN");
        env.put("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN");
        env.put("KAFKA_OPTS", "-Djava.security.auth.login.config=" + WORK + "/jaas.conf");
        env.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        env.put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        env.put("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
        env.put("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        env.put("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
        // The controller's listener is plaintext, so the node speaks to itself as User:ANONYMOUS.
        env.put("KAFKA_SUPER_USERS", "User:" + ADMIN + ";User:ANONYMOUS");
        env.putAll(variables);
        String classpath = variables.get("CLASSPATH");
        if (classpath == null) {
            throw new AssertionError("the environment has no CLASSPATH to put the jar at: " + variables);
        }
        return new DocBroker(new GenericContainer<>(image())
                .withCopyFileToContainer(MountableFile.forHostPath(jar()), classpath)
                .withEnv(env));
    }

    /** The release of the image: {@code 4.4.0}. */
    static String release() {
        String image = property("kfkrbac.it.image");
        return image.substring(image.lastIndexOf(':') + 1);
    }

    void start() {
        container.start();
        starts++;
    }

    /** Shuts the node down in an orderly way and starts it again; its data and its files stay. */
    void restart() {
        container.getDockerClient().stopContainerCmd(container.getContainerId())
                .withTimeout(STOP_TIMEOUT_SECONDS).exec();
        starts++;
        container.getDockerClient().startContainerCmd(container.getContainerId()).exec();
        Await.until(START_TIMEOUT, container::getLogs, log -> timesStarted(log) >= starts,
                log -> "expected the node to say \"" + STARTED + "\" " + starts + " times within "
                        + START_TIMEOUT.toSeconds() + " s; it said so " + timesStarted(log)
                        + " times, and its log ends with:\n" + tail(log));
    }

    /**
     * Runs shell commands in the container, in the directory with the
     * client properties, and stops at the first that fails.
     *
     * @return the exit status, and what the commands printed on both streams, in the order it was printed
     */
    Result run(String commands) {
        try {
            ExecResult result = container.execInContainer("bash", "-c", SHELL + commands);
            return new Result(result.getExitCode(), result.getStdout() + result.getStderr());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", e);
        }
    }

    /** Runs commands that prepare the cluster for a page; fails when they fail. */
    void prepare(String commands) {
        Result result = run(commands);
        if (result.exit() != 0) {
            throw new AssertionError("preparing the cluster failed with status " + result.exit() + ":\n" + commands
                    + "\nprinted:\n" + result.output());
        }
    }

    /** Keeps what the node logged under {@code build/integration-logs} and removes the node. */
    void close(String name) {
        try {
            if (container.getContainerId() != null) {
                Path dir = Path.of(property("kfkrbac.it.logs"));
                Files.createDirectories(dir);
                Files.writeString(dir.resolve(name + ".log"), container.getLogs());
            }
        } catch (IOException | RuntimeException e) {
            // The log is there to explain a failure; not having it is not one.
            System.err.println("could not keep the log of " + name + ": " + e);
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        container.stop();
    }

    private static String clientProperties(String user) {
        return """
                security.protocol=SASL_PLAINTEXT
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
                username="%s" password="%s";
                """.formatted(user, Node.password(user));
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

    private static DockerImageName image() {
        return DockerImageName.parse(property("kfkrbac.it.image"));
    }

    private static Path jar() {
        return Path.of(property("kfkrbac.it.jar"));
    }

    private static String property(String name) {
        String value = System.getProperty(name);
        if (value == null) {
            throw new IllegalStateException(name + " is not set; run the tests through ./gradlew integrationTest");
        }
        return value;
    }
}
