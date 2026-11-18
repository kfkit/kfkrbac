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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The shell commands of the operator documentation, run as the pages have
 * them against a real broker with the jar, and what they print compared
 * with what the pages show. Change a command or its output in a page and
 * this fails.
 *
 * <p>{@link DocSteps} says how a page marks its blocks, and
 * {@link DocBroker} where they run: inside the broker's container, where
 * {@code localhost:9092} and the files of client properties the pages name
 * are what they say. A page tells its reader what the cluster of its
 * examples has before the first command; each test here makes that true
 * first.
 *
 * <p>A record is in force a moment after the command that created it has
 * returned, so a block that does not do what the page says is run again,
 * with a bound, before the test fails: as {@code BrokerIT} asks again. The
 * commands of the pages can be repeated: creating what exists and binding
 * what is bound change nothing.
 *
 * <p>What the output of a run and a page may differ in: the time and the
 * ID of a record in a line of the broker's log, white space at the end of a
 * line, empty lines at the end, a tab, which a page shows as four spaces,
 * and the brackets around an ACL record in a line of the broker's log,
 * which Kafka 4.1 prints round and the newest release, as the pages do,
 * square.
 */
@Timeout(value = 15, unit = TimeUnit.MINUTES)
class DocsIT {

    private static final Path DOCS = Path.of("docs");

    /** How long a block may take to do what the page says, from the first time it is run. */
    private static final Duration IN_FORCE = Duration.ofSeconds(30);
    private static final Duration PAUSE = Duration.ofMillis(500);

    private static final String BOOTSTRAP = "--bootstrap-server localhost:9092 --command-config admin.properties";
    private static final String PLAIN_AUTHORIZER =
            "authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer";
    /** By name: the node loads the class from the jar, not from this test's classpath. */
    private static final String RBAC_AUTHORIZER = "dev.kfkit.kfkrbac.RbacAuthorizer";
    private static final String SUPER_USERS = "super.users=User:admin;User:ANONYMOUS";

    private final Map<String, DocBroker> nodes = new LinkedHashMap<>();
    private String page;

    @BeforeEach
    void requireDocker() {
        Node.requireDocker();
    }

    @AfterEach
    void stop() {
        nodes.forEach((name, node) -> node.close("docs-" + page + "-" + name));
    }

    /**
     * {@code docs/install.md}: a node that runs the plain authorizer and has
     * an ACL gets the jar and the line, and a node that ran without an
     * authorizer gets them with {@code allow.everyone.if.no.acl.found}.
     */
    @Test
    void theInstallationGuide() {
        DocBroker main = node("main", DocBroker.fromTheDistribution(PLAIN_AUTHORIZER, SUPER_USERS));
        DocBroker open = node("open", DocBroker.fromTheDistribution());
        // What the page says its example cluster has.
        main.prepare("""
                kafka-topics.sh %1$s --create --topic orders
                kafka-topics.sh %1$s --create --topic billing.orders
                kafka-acls.sh %1$s --add --allow-principal User:alice --operation Read --topic orders
                """.formatted(BOOTSTRAP));
        open.prepare("""
                kafka-topics.sh %1$s --create --topic orders
                kafka-topics.sh %1$s --create --topic billing.orders
                """.formatted(BOOTSTRAP));

        follow("install");
    }

    /**
     * {@code docs/bindings.md}, on a node the image starts with the
     * environment {@code docs/install.md} gives for it, and, for the record
     * that looks like a binding and is not one, on a node whose metadata
     * has such a record, written while it ran the plain authorizer: this
     * authorizer refuses to create one.
     */
    @Test
    void theBindingsGuide() {
        node("main", DocBroker.ofTheImage(environment()));
        DocBroker old = node("old", DocBroker.fromTheDistribution(PLAIN_AUTHORIZER, SUPER_USERS));
        old.prepare("""
                kafka-topics.sh %1$s --create --topic orders
                kafka-acls.sh %1$s --add --allow-principal DeveloperRead:User:bob --operation Read --topic orders
                cp kfkrbac-*.jar "$KAFKA_HOME/libs/"
                sed -i 's/^authorizer\\.class\\.name=.*/authorizer.class.name=%2$s/' \
                "$KAFKA_HOME/config/server.properties"
                """.formatted(BOOTSTRAP, RBAC_AUTHORIZER));
        old.restart();

        follow("bindings");
    }

    /**
     * {@code docs/delegation.md}, on a node the image starts with the
     * environment {@code docs/install.md} gives for it.
     */
    @Test
    void theDelegationGuide() {
        node("main", DocBroker.ofTheImage(environment()));

        follow("delegation");
    }

    /** The variables {@code docs/install.md} gives for the image: they are what makes a node refuse anything. */
    private static Map<String, String> environment() {
        Map<String, String> environment = DocSteps.of(DOCS.resolve("install.md")).environment();
        assertEquals(Set.of("CLASSPATH", "KAFKA_AUTHORIZER_CLASS_NAME"), environment.keySet(),
                "the variables of the block docs/install.md marks with <!-- environment -->");
        return environment;
    }

    private DocBroker node(String name, DocBroker node) {
        nodes.put(name, node);
        node.start();
        return node;
    }

    /** Does what the page does, in its order, on the nodes of this test. */
    private void follow(String name) {
        page = name;
        forget();
        Path document = DOCS.resolve(name + ".md");
        DocSteps read = DocSteps.of(document);
        List<String> session = new ArrayList<>(); // one shell session for the page, whichever node a block is for
        int ran = 0;
        for (DocSteps.Step step : read.steps()) {
            DocBroker node = nodes.get(step.node());
            if (node == null) {
                throw new AssertionError(document + ":" + step.line() + ": there is no node '" + step.node()
                        + "' in this test; it has " + nodes.keySet());
            }
            if (step instanceof DocSteps.Restart) {
                node.restart();
                continue;
            }
            DocSteps.Run run = (DocSteps.Run) step;
            String commands = String.join("", session) + asOn(DocBroker.release(), run.commands());
            run(document, run, node, commands);
            if (run.session()) {
                session.add(run.commands());
            }
            ran++;
        }
        assertFalse(ran == 0, document + " has no block of commands to run");
    }

    /** Runs a block until it does what the page says, and fails with what it did when it does not in time. */
    private void run(Path document, DocSteps.Run run, DocBroker node, String commands) {
        long deadline = System.nanoTime() + IN_FORCE.toNanos();
        int attempts = 0;
        while (true) {
            DocBroker.Result result = node.run(commands);
            attempts++;
            String wrong = wrong(run, result);
            keep(document, run, result, attempts, wrong);
            if (wrong == null) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                throw new AssertionError(document + ":" + run.line() + ": " + wrong + " (run " + attempts
                        + " times in " + IN_FORCE.toSeconds() + " s)\n--- the block\n" + run.commands()
                        + "--- exit status " + result.exit() + ", and it printed\n" + shown(result.output()));
            }
            pause();
        }
    }

    /** What a run of the block did that the page does not say, or {@code null}. */
    private static String wrong(DocSteps.Run run, DocBroker.Result result) {
        if (result.exit() != run.exit()) {
            return "the block exits with " + result.exit() + ", and the page says " + run.exit();
        }
        if (run.output() == null) {
            return null;
        }
        String printed = shown(result.output());
        String page = shown(run.output().text());
        switch (run.output().shows()) {
            case NOTHING:
                return printed.isEmpty() ? null : "the block prints something, and the page says nothing";
            case EVERYTHING:
                return printed.equals(page) ? null
                        : "the block does not print what the page shows:\n" + page.stripTrailing();
            default:
                int from = 0;
                List<String> lines = printed.lines().toList();
                for (String wanted : page.lines().filter(line -> !line.isBlank()).toList()) {
                    while (from < lines.size() && !lines.get(from).contains(wanted.strip())) {
                        from++;
                    }
                    if (from == lines.size()) {
                        return "the block does not print, after the lines before it, this line of the page:\n"
                                + wanted;
                    }
                    from++;
                }
                return null;
        }
    }

    /**
     * Output as a page shows it: a tab as four spaces, nothing at the end
     * of a line, no empty line at the end, and in a line of the broker's
     * log neither the time nor the ID of the record, which no two runs
     * share, and the record in square brackets, as the newest release
     * prints it.
     */
    static String shown(String output) {
        StringBuilder shown = new StringBuilder();
        for (String line : output.lines().toList()) {
            shown.append(line.replace("\t", "    ")
                    .replaceAll("^\\[\\d{4}-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d,\\d{3}\\]", "[<time>]")
                    .replaceAll("\\bACL [A-Za-z0-9_-]{22} has\\b", "ACL <id> has")
                    .replaceAll("StandardAcl\\((.*)\\) \\(dev\\.kfkit", "StandardAcl[$1] (dev.kfkit")
                    .stripTrailing()).append('\n');
        }
        String text = shown.toString().stripTrailing();
        return text.isEmpty() ? "" : text + "\n";
    }

    /**
     * The block as an operator of that release types it. The pages are
     * written for the newest one and say where an older one differs: the
     * console producer and consumer of Kafka 4.1 take the client properties
     * as {@code --producer.config} and {@code --consumer.config}.
     */
    static String asOn(String release, String commands) {
        if (!release.startsWith("4.1.")) {
            return commands;
        }
        return commands
                .replaceAll("(?s)(kafka-console-producer\\.sh.*?)--command-config", "$1--producer.config")
                .replaceAll("(?s)(kafka-console-consumer\\.sh.*?)--command-config", "$1--consumer.config");
    }

    /** Keeps what the block printed next to the brokers' logs: what a page is updated from. */
    private void keep(Path document, DocSteps.Run run, DocBroker.Result result, int attempt, String wrong) {
        try {
            Path dir = Path.of(System.getProperty("kfkrbac.it.logs")).resolve("docs-output").resolve(page);
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("line-%04d.txt".formatted(run.line())),
                    "# " + document + ":" + run.line() + ", run " + attempt + " time(s), exit status "
                            + result.exit() + (wrong == null ? "" : ", NOT what the page says: " + wrong) + "\n"
                            + run.commands() + "# printed\n" + shown(result.output()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Removes what an earlier run kept for the page: its blocks may be at other lines now. */
    private void forget() {
        Path dir = Path.of(System.getProperty("kfkrbac.it.logs")).resolve("docs-output").resolve(page);
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (Stream<Path> kept = Files.list(dir)) {
            for (Path file : kept.toList()) {
                Files.delete(file);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void pause() {
        try {
            Thread.sleep(PAUSE.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting", e);
        }
    }
}
