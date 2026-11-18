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

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * The clients of the tests: each call is one client, authenticated as the
 * user it names, that asks the cluster once and reports its {@link Answer}.
 * Anything but "allowed" or "refused" fails the test with what happened.
 */
final class Clients {

    /** The cluster as a resource of ACLs and of the bindings of cluster roles. */
    private static final ResourcePattern CLUSTER =
            new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL);

    /** How long a client waits for the cluster before the test fails. */
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final String bootstrap;

    /** Clients that know the cluster by these addresses. */
    Clients(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    // --- managing the cluster, as the super user -------------------------------

    /** A role binding as the ACL it is: {@code DeveloperWrite:User:alice} on the prefix {@code billing.}. */
    static AclBinding binding(String role, String user, String prefix) {
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, prefix, PatternType.PREFIXED),
                new AccessControlEntry(role + ":User:" + user, "*", AclOperation.ALL, AclPermissionType.ALLOW));
    }

    /** A role binding on connectors: {@code DeveloperManage:User:alice} on the prefix {@code Connector:billing-}. */
    static AclBinding connectorBinding(String role, String member, String prefix) {
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, "Connector:" + prefix, PatternType.PREFIXED),
                new AccessControlEntry(role + ":" + member, "*", AclOperation.ALL, AclPermissionType.ALLOW));
    }

    /** The record that puts the user in the group: {@code Member:User:bob} on {@code Group:payments}. */
    static AclBinding membership(String user, String group) {
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, "Group:" + group, PatternType.LITERAL),
                new AccessControlEntry("Member:User:" + user, "*", AclOperation.ALL, AclPermissionType.ALLOW));
    }

    /** An ACL that denies the user everything on the topic of that name. */
    static AclBinding deny(String user, String topic) {
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                new AccessControlEntry("User:" + user, "*", AclOperation.ALL, AclPermissionType.DENY));
    }

    /** A binding of a cluster role as the ACL it is: {@code ClusterAdmin:User:alice} on the cluster. */
    static AclBinding clusterBinding(String role, String user) {
        return new AclBinding(CLUSTER,
                new AccessControlEntry(role + ":User:" + user, "*", AclOperation.ALL, AclPermissionType.ALLOW));
    }

    /** An ordinary ACL that allows the user the operation on the topic of that name. */
    static AclBinding allow(String user, AclOperation operation, String topic) {
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                new AccessControlEntry("User:" + user, "*", operation, AclPermissionType.ALLOW));
    }

    /** An ordinary ACL that allows the user the operation on the cluster. */
    static AclBinding allowOnCluster(String user, AclOperation operation) {
        return new AclBinding(CLUSTER,
                new AccessControlEntry("User:" + user, "*", operation, AclPermissionType.ALLOW));
    }

    /** Creates the ACLs, role bindings among them, through {@code AdminClient}. */
    void createAcls(AclBinding... acls) {
        try (Admin admin = admin()) {
            admin.createAcls(List.of(acls)).all().get();
        } catch (ExecutionException e) {
            throw new AssertionError("creating " + List.of(acls) + " as " + Node.ADMIN + " failed: " + e.getCause(),
                    e.getCause());
        } catch (InterruptedException e) {
            throw interrupted(e);
        }
    }

    /** Every ACL of the cluster as {@code AdminClient} lists it, role bindings among them. */
    Set<AclBinding> describeAcls(AclBindingFilter filter) {
        try (Admin admin = admin()) {
            Collection<AclBinding> acls = admin.describeAcls(filter).values().get();
            return new HashSet<>(acls);
        } catch (ExecutionException e) {
            throw new AssertionError("listing the ACLs as " + Node.ADMIN + " failed: " + e.getCause(), e.getCause());
        } catch (InterruptedException e) {
            throw interrupted(e);
        }
    }

    /** The users that have SCRAM credentials, as {@code AdminClient} lists them for the super user. */
    Set<String> usersWithScramCredentials() {
        try (Admin admin = admin()) {
            return new HashSet<>(admin.describeUserScramCredentials().all().get().keySet());
        } catch (ExecutionException e) {
            throw new AssertionError("listing the SCRAM credentials as " + Node.ADMIN + " failed: " + e.getCause(),
                    e.getCause());
        } catch (InterruptedException e) {
            throw interrupted(e);
        }
    }

    /** Creates topics of one partition, each on the one broker of this cluster. */
    void createTopics(String... topics) {
        try (Admin admin = admin()) {
            admin.createTopics(List.of(topics).stream().map(topic -> new NewTopic(topic, 1, (short) 1)).toList())
                    .all().get();
        } catch (ExecutionException e) {
            throw new AssertionError("creating " + List.of(topics) + " failed: " + e.getCause(), e.getCause());
        } catch (InterruptedException e) {
            throw interrupted(e);
        }
    }

    /**
     * Creates a topic whose one partition is on that broker, so that
     * producing to it asks that broker's authorizer. Waits for the broker to
     * be in the cluster: one that has just started may not be yet.
     */
    void createTopicOn(int broker, String topic) {
        try (Admin admin = admin()) {
            Await.until(TIMEOUT, () -> {
                try {
                    admin.createTopics(List.of(new NewTopic(topic, Map.of(0, List.of(broker))))).all().get();
                    return "created";
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof InvalidReplicaAssignmentException) {
                        return e.getCause().toString();
                    }
                    throw new AssertionError("creating " + topic + " on broker " + broker + " failed: "
                            + e.getCause(), e.getCause());
                } catch (InterruptedException e) {
                    throw interrupted(e);
                }
            }, "created"::equals, last -> "expected " + topic + " to be created on broker " + broker + " within "
                    + TIMEOUT.toSeconds() + " s; the cluster's last answer: " + last);
        }
    }

    // --- asking as a user ------------------------------------------------------

    /** Creates the ACLs, role bindings among them, through an {@code AdminClient} of the user's own. */
    Answer createAcls(String user, AclBinding... acls) {
        String request = user + " creating " + List.of(acls);
        try (Admin admin = admin(user)) {
            admin.createAcls(List.of(acls)).all().get();
            return Answer.allowed(request);
        } catch (ExecutionException e) {
            return refusal(request, e);
        } catch (InterruptedException e) {
            throw interrupted(e);
        }
    }

    /**
     * Deletes the ACLs the filters match, role bindings among them, through
     * an {@code AdminClient} of the user's own.
     */
    Deletion deleteAcls(String user, AclBindingFilter... filters) {
        String request = user + " deleting the ACLs that match " + List.of(filters);
        try (Admin admin = admin(user)) {
            Collection<AclBinding> deleted = admin.deleteAcls(List.of(filters)).all().get();
            return new Deletion(Answer.allowed(request), new HashSet<>(deleted));
        } catch (ExecutionException e) {
            return new Deletion(refusal(request, e), Set.of());
        } catch (InterruptedException e) {
            throw interrupted(e);
        }
    }

    /**
     * Replaces the SCRAM credentials of {@code of}, which is how a password
     * is set: whoever may do that to a super user can become one.
     */
    Answer replaceScramCredentials(String user, String of) {
        String request = user + " replacing the SCRAM credentials of " + of;
        try (Admin admin = admin(user)) {
            admin.alterUserScramCredentials(List.of(new UserScramCredentialUpsertion(of,
                    new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), "chosen-by-" + user))).all().get();
            return Answer.allowed(request);
        } catch (ExecutionException e) {
            return refusal(request, e);
        } catch (InterruptedException e) {
            throw interrupted(e);
        }
    }

    /** Sends one record with a producer of the default configuration, which is idempotent. */
    Answer produce(String user, String topic) {
        String request = user + " producing to " + topic;
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(producer(user), new StringSerializer(), new StringSerializer());
        try {
            producer.send(new ProducerRecord<>(topic, "key", "value")).get();
            return Answer.allowed(request);
        } catch (ExecutionException | KafkaException e) {
            return refusal(request, e);
        } catch (InterruptedException e) {
            throw interrupted(e);
        } finally {
            producer.close(TIMEOUT);
        }
    }

    /** Sends one record in a transaction and commits it. */
    Answer produceInTransaction(String user, String transactionalId, String topic) {
        String request = user + " producing to " + topic + " in a transaction of " + transactionalId;
        Map<String, Object> config = producer(user);
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(config, new StringSerializer(), new StringSerializer());
        try {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, "key", "value")).get();
            producer.commitTransaction();
            return Answer.allowed(request);
        } catch (ExecutionException | KafkaException e) {
            return refusal(request, e);
        } catch (InterruptedException e) {
            throw interrupted(e);
        } finally {
            producer.close(TIMEOUT);
        }
    }

    /** Subscribes to the topic in the group, reads until a record arrives and commits its offset. */
    Answer consume(String user, String group, String topic) {
        String request = user + " consuming " + topic + " in the group " + group;
        Map<String, Object> config = consumer(user);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer());
        try {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                if (!consumer.poll(Duration.ofMillis(500)).isEmpty()) {
                    consumer.commitSync();
                    return Answer.allowed(request);
                }
            }
            throw new AssertionError(request + " was neither refused nor given a record within "
                    + TIMEOUT.toSeconds() + " s");
        } catch (KafkaException e) {
            return refusal(request, e);
        } finally {
            consumer.close();
        }
    }

    /**
     * Asks for the partitions of the topic. It is the first thing this
     * client asks, and a client that knows the cluster by one address asks
     * that broker.
     */
    Answer describe(String user, String topic) {
        String request = user + " asking " + bootstrap + " for the partitions of " + topic;
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(consumer(user), new StringDeserializer(), new StringDeserializer());
        try {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic, TIMEOUT);
            if (partitions == null || partitions.isEmpty()) {
                throw new AssertionError(request + " was neither refused nor given partitions: " + partitions);
            }
            return Answer.allowed(request);
        } catch (KafkaException e) {
            return refusal(request, e);
        } finally {
            consumer.close();
        }
    }

    // ---------------------------------------------------------------------------

    private Admin admin() {
        return admin(Node.ADMIN);
    }

    private Admin admin(String user) {
        return Admin.create(client(user));
    }

    private Map<String, Object> producer(String user) {
        Map<String, Object> config = client(user);
        // The defaults, idempotence among them; only how long a failure takes to show is bounded.
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (int) TIMEOUT.toMillis());
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) TIMEOUT.toMillis());
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TIMEOUT.toMillis() / 3);
        return config;
    }

    private Map<String, Object> consumer(String user) {
        Map<String, Object> config = client(user);
        config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) TIMEOUT.toMillis());
        return config;
    }

    private Map<String, Object> client(String user) {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        config.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + user
                        + "\" password=\"" + Node.password(user) + "\";");
        return config;
    }

    /** The refusal behind what the client threw; fails the test when it threw for another reason. */
    private static Answer refusal(String request, Exception thrown) {
        for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
            if (cause instanceof AuthorizationException refused) {
                return Answer.refused(request, refused);
            }
        }
        throw new AssertionError(request + " failed, and not because the cluster refused it: " + thrown, thrown);
    }

    private static AssertionError interrupted(InterruptedException e) {
        Thread.currentThread().interrupt();
        return new AssertionError("interrupted", e);
    }
}
