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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * A Kafka Connect worker in a container: the {@code apache/kafka} image of
 * the release under test, running {@code connect-distributed.sh}, with the
 * built jar on its class path and the extension switched on by its one
 * line. Workers created with the same group form one Connect cluster.
 *
 * <p>The worker authenticates its REST callers with HTTP basic
 * authentication, the extension that ships with Apache Kafka, against a
 * file of users and passwords: {@code admin}, {@code alice}, {@code bob}
 * and {@code carol}, each with {@link Node#password}. {@code User:admin}
 * is in the extension's {@code kfkrbac.super.users}. The worker connects to
 * the broker as {@code admin} on the brokers' own listener, so the
 * extension reads the bindings as a super user.
 */
final class Worker implements AutoCloseable {

    static final String CAROL = "carol";

    /** How often the extension reads the bindings again, in these tests. */
    static final Duration REFRESH = Duration.ofSeconds(1);

    private static final String DIR = "/opt/kfkrbac/";
    private static final int PORT = 8083;
    private static final Duration START_TIMEOUT = Duration.ofMinutes(3);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private static final List<String> USERS = List.of(Node.ADMIN, Node.ALICE, Node.BOB, CAROL);

    private static final String JAAS = """
            KafkaConnect {
              org.apache.kafka.connect.rest.basic.auth.extension.PropertyFileLoginModule required
              file="%spasswords.properties";
            };
            """.formatted(DIR);

    /** The file connectors ship in the distribution but not on the worker's class path: put them on its plugin path. */
    private static final String START = "mkdir -p /tmp/plugins && cp /opt/kafka/libs/connect-file-*.jar /tmp/plugins/"
            + " && exec /opt/kafka/bin/connect-distributed.sh " + DIR + "worker.properties";

    private final int id;
    private final GenericContainer<?> container;
    private final HttpClient http = HttpClient.newBuilder()
            .proxy(HttpClient.Builder.NO_PROXY)
            .connectTimeout(REQUEST_TIMEOUT)
            .build();

    /**
     * A worker of the Connect cluster {@code kfkrbac-connect} that reads and
     * writes its own topics on the broker.
     *
     * @param id this worker's number, which names it on the network
     * @param broker the broker it connects to, on the same network
     */
    Worker(int id, Node broker, Network network) {
        this.id = id;
        String jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + Node.ADMIN
                + "\" password=\"" + Node.password(Node.ADMIN) + "\";";
        String client = """
                security.protocol=SASL_PLAINTEXT
                sasl.mechanism=PLAIN
                sasl.jaas.config=%s
                """.formatted(jaas);
        String properties = """
                bootstrap.servers=%1$s
                %2$s
                %3$s
                %4$s
                group.id=kfkrbac-connect
                config.storage.topic=connect-configs
                offset.storage.topic=connect-offsets
                status.storage.topic=connect-status
                config.storage.replication.factor=1
                offset.storage.replication.factor=1
                status.storage.replication.factor=1
                key.converter=org.apache.kafka.connect.storage.StringConverter
                value.converter=org.apache.kafka.connect.storage.StringConverter
                listeners=http://0.0.0.0:%5$d
                rest.advertised.host.name=%6$s
                rest.advertised.port=%5$d
                plugin.path=/tmp/plugins
                rest.extension.classes=org.apache.kafka.connect.rest.basic.auth.extension.BasicAuthSecurityRestExtension,\
                dev.kfkit.kfkrbac.connect.RbacConnectRestExtension
                kfkrbac.super.users=User:%7$s
                kfkrbac.refresh.interval.ms=%8$d
                """.formatted(broker.internalBootstrap(), client, prefixed("producer.", client),
                        prefixed("consumer.", client), PORT, alias(id), Node.ADMIN, REFRESH.toMillis());
        String passwords = USERS.stream().map(user -> user + ": " + Node.password(user))
                .collect(Collectors.joining("\n", "", "\n"));

        container = new GenericContainer<>(DockerImageName.parse(property("kfkrbac.it.image")))
                .withNetwork(network)
                .withNetworkAliases(alias(id))
                .withCopyFileToContainer(MountableFile.forHostPath(Path.of(property("kfkrbac.it.jar"))),
                        DIR + "kfkrbac.jar")
                .withCopyToContainer(Transferable.of(properties), DIR + "worker.properties")
                .withCopyToContainer(Transferable.of(JAAS), DIR + "connect-jaas.conf")
                .withCopyToContainer(Transferable.of(passwords), DIR + "passwords.properties")
                .withEnv("CLASSPATH", DIR + "kfkrbac.jar")
                .withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=" + DIR + "connect-jaas.conf")
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("sh", "-c", START))
                .withExposedPorts(PORT)
                // "/" answers 200 to an authenticated caller only once the extension has read the bindings: 503 before.
                .waitingFor(Wait.forHttp("/").forPort(PORT)
                        .withBasicCredentials(Node.ADMIN, Node.password(Node.ADMIN))
                        .forStatusCode(200)
                        .withStartupTimeout(START_TIMEOUT));
    }

    /** Starts the workers at once and returns when each answers. */
    static void start(Worker... workers) {
        Startables.deepStart(Stream.of(workers).map(worker -> worker.container)).join();
    }

    /** What the worker answered: the status and the body. */
    record Reply(String request, int status, String body) {

        @Override
        public String toString() {
            return request + ": " + status + " " + body;
        }
    }

    /**
     * Sends one request to this worker's REST interface.
     *
     * @param user the user it authenticates as with basic authentication, or {@code null} for none
     * @param body JSON, or {@code null} for none
     */
    Reply request(String user, String method, String path, String body) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(
                "http://" + container.getHost() + ":" + container.getMappedPort(PORT) + path))
                .timeout(REQUEST_TIMEOUT)
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body));
        if (body != null) {
            request.header("Content-Type", "application/json");
        }
        if (user != null) {
            String credentials = user + ":" + Node.password(user);
            request.header("Authorization",
                    "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));
        }
        String described = (user == null ? "anonymous" : user) + " " + method + " " + path + " on worker " + id;
        try {
            HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
            return new Reply(described, response.statusCode(), response.body());
        } catch (IOException e) {
            throw new AssertionError(described + " failed: " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while " + described, e);
        }
    }

    /** {@code POST /connectors}: a file sink of that name that reads the topic. */
    Reply createSink(String user, String name, String topic) {
        return request(user, "POST", "/connectors", """
                {"name":"%s","config":{"connector.class":"org.apache.kafka.connect.file.FileStreamSinkConnector",\
                "tasks.max":"1","topics":"%s","file":"/tmp/%s.txt"}}""".formatted(name, topic, name));
    }

    /** Keeps what the worker logged under {@code build/integration-logs} and removes it. */
    void close(String test) {
        try {
            if (container.getContainerId() != null) {
                Path dir = Path.of(property("kfkrbac.it.logs"));
                Files.createDirectories(dir);
                Files.writeString(dir.resolve(test + "-worker-" + id + ".log"), container.getLogs());
            }
        } catch (IOException | RuntimeException e) {
            System.err.println("could not keep the log of worker " + id + " after " + test + ": " + e);
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        container.stop();
    }

    private static String prefixed(String prefix, String settings) {
        return settings.lines().map(line -> prefix + line).collect(Collectors.joining("\n"));
    }

    private static String alias(int id) {
        return "worker-" + id;
    }

    private static String property(String name) {
        String value = System.getProperty(name);
        if (value == null) {
            throw new IllegalStateException(name + " is not set; run the tests through ./gradlew integrationTest");
        }
        return value;
    }
}
