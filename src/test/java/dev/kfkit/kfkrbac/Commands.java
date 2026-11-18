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

import static dev.kfkit.kfkrbac.RbacAuthorizerTest.acl;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.internals.PluginMetricsImpl;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizationResult;

/**
 * What the tests of the command share: running it against ACL records held
 * in memory, and asking a real {@link RbacAuthorizer} that was fed the same
 * records what it answers.
 */
final class Commands {

    /** What one run of the command left behind. */
    record Ran(int status, String out, String err) {
    }

    private Commands() {
    }

    /**
     * Runs the command as typed, with the cluster to connect to after it,
     * against those records.
     */
    static Ran run(FakeAclRecords cluster, String... typed) {
        List<String> args = new ArrayList<>(List.of(typed));
        args.add("--bootstrap-server");
        args.add("localhost:9092");
        return ran(cluster, args.toArray(new String[0]));
    }

    /** Runs the command exactly as typed, against those records. */
    static Ran ran(FakeAclRecords cluster, String... args) {
        cluster.described = 0;
        cluster.created = 0;
        cluster.deleted = 0;
        cluster.closed = false;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int status = RbacCommand.run(args, new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8), properties -> cluster);
        return new Ran(status, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    /**
     * A binding, or a membership, as the record it is: the principal on the
     * pattern, {@code ALL}, {@code ALLOW}, {@code *}.
     */
    static AclBinding record(String principal, ResourcePattern pattern) {
        return acl(principal, pattern, ALL, "*", ALLOW).toBinding();
    }

    /** An ordinary ACL that allows the principal the operation on the pattern, from every host. */
    static AclBinding allow(String principal, ResourcePattern pattern, AclOperation operation) {
        return acl(principal, pattern, operation, "*", ALLOW).toBinding();
    }

    /** What an authorizer answers a user's ordinary request that a broker fed those records. */
    static boolean authorizerAllows(Collection<AclBinding> records, String user, AclOperation operation,
            ResourcePattern resource) {
        return authorizerAllows(records, user, operation, resource, "127.0.0.1");
    }

    /**
     * What a document shows as the output of a command: the text block it
     * marks as the output of the {@code bash} block that is this command
     * and nothing else. {@code DocsIT} runs the same block against a broker.
     */
    static String documented(Path document, String command) throws IOException {
        String page = Files.readString(document);
        String block = "```bash\n" + command + "\n```\n\n<!-- output -->\n```text\n";
        int at = page.indexOf(block);
        if (at < 0) {
            throw new AssertionError(document + " does not show `" + command + "` with its output");
        }
        int from = at + block.length();
        return page.substring(from, page.indexOf("```", from));
    }

    /** What an authorizer answers a user's ordinary request from that address that a broker fed those records. */
    static boolean authorizerAllows(Collection<AclBinding> records, String user, AclOperation operation,
            ResourcePattern resource, String from) {
        Metrics metrics = new Metrics();
        try (RbacAuthorizer authorizer = new RbacAuthorizer()) {
            authorizer.configure(Map.of());
            authorizer.withPluginMetrics(new PluginMetricsImpl(metrics, Map.of()));
            Map<Uuid, StandardAcl> snapshot = new HashMap<>();
            for (AclBinding record : records) {
                snapshot.put(Uuid.randomUuid(), StandardAcl.fromAclBinding(record));
            }
            authorizer.loadSnapshot(snapshot);
            authorizer.completeInitialLoad();
            RequestContext context = new RequestContext(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user),
                    "billing-1", InetAddress.getByName(from), ApiKeys.FETCH.id);
            return authorizer.authorize(context, List.of(new Action(operation, resource, 1, false, false)))
                    .get(0) == AuthorizationResult.ALLOWED;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            metrics.close();
        }
    }
}
