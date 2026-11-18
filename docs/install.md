# Installation

kfkrbac is one jar and one line of configuration on every node of a cluster
of Apache Kafka®, controllers included. This page says what changes on the
cluster when the line is set, and gives the steps for a node started from
the Kafka distribution, for the `apache/kafka` container image and for
Strimzi. It ends with how to take the jar out again. The workers of Kafka
Connect take the same jar with a line of their own, which is in
[Kafka Connect](connect.md).

> Status: there is no release yet, so the jar is built from this
> repository. The commands for a node from the distribution, and the
> variables for the `apache/kafka` image, are run by a test of the
> integration suite (`DocsIT`) on a broker of the newest supported release,
> and the output shown is what they printed there. The image build and the
> Strimzi steps have not been run; the page says so where they are.

## Before you start

- **Kafka 4.1 or later, in KRaft mode.** See
  [Compatibility](../README.md#compatibility).
- **The jar.** `./gradlew jar` writes `build/libs/kfkrbac-<version>.jar`. It
  needs nothing next to it: every class it uses besides its own is in the
  Kafka distribution. Once there are releases, each will carry the jar, its
  SHA-256 and a signature, and [Provenance](provenance/README.md) has the
  commands that verify them: run them before the jar goes on a node.
- **Every node, controllers included.** A role binding is a record in the
  cluster's metadata, like an ACL. A broker reads it to decide a request,
  and the controller decides who may create one. A controller that runs
  the plain authorizer applies none of the rules for bindings, and a broker
  that runs it does not honour the roles.
- **The jar before the line.** A node whose configuration names the class
  and that cannot find the jar does not start: it stops with
  `java.lang.ClassNotFoundException: dev.kfkit.kfkrbac.RbacAuthorizer`.
- **Somebody who may create ACLs.** The first role is bound by a principal
  that is in `super.users`, or that an ACL allows to alter the cluster: the
  same principals that manage ACLs today.

## What changes when the line is set

`RbacAuthorizer` is the broker's `StandardAuthorizer` with roles added. It
asks `StandardAuthorizer` first about every request, and looks at the roles
only for a request the ACLs did not allow.

### A cluster that already runs `StandardAuthorizer`

Nothing changes until the first role is bound or the first member is put
in a group ([Groups](groups.md)). The ACLs, `super.users` and
`allow.everyone.if.no.acl.found` mean what they meant, the same principals
create and delete ACLs, and with no binding and no membership in the
metadata no request is answered differently.

Two things are different from the first start, and neither changes who may
do what:

- A few principal types are reserved
  ([Bindings](bindings.md#reserved-names)). An ACL whose principal type is
  the name of one of the seven [roles](roles.md), such as
  `DeveloperRead:User:alice`, is refused when it is created unless it is a
  well-formed role binding; one whose principal type is `Member`, unless it
  is a well-formed group membership; and a `DENY` ACL whose principal type
  is `Group`, always.
- The broker's own authorizer metrics are in an MBean named after the
  class: `kafka.server:type=plugins,config=authorizer.class.name,class=StandardAuthorizer,...`
  becomes `...,class=RbacAuthorizer,...`. A dashboard or an exporter rule
  that names the class has to follow.

### A cluster that ran without an authorizer

Such a cluster starts enforcing ACLs the moment it gets an authorizer, this
one or the plain one: with no ACL in the metadata, every request of a
principal that is not a super user is denied, the brokers' own requests to
each other included.

So decide first what the cluster should do with the clients it has:

- To enforce from the start, put the principals the brokers and controllers
  use between themselves in `super.users`, and create the ACLs or bind the
  roles the clients need as soon as the nodes are back.
- To keep every client working and close the cluster step by step, also set
  `allow.everyone.if.no.acl.found=true`. Then whatever no ACL names stays
  open to everyone.

With `allow.everyone.if.no.acl.found=true` a binding closes what it names
to everyone else. That is how the setting works for any ACL: it opens a
resource only as long as no ACL names it, whoever that ACL is for, and a
binding is an ACL record. [The example below](#a-cluster-that-ran-without-an-authorizer-the-steps)
shows it. A binding names the resources of the type its record has:

| Binding | What stops being open to everyone |
|---|---|
| on a name or a prefix | the topics the pattern covers; the groups and the transactional IDs it covers stay open |
| narrowed to groups or to transactional IDs | the groups, or the transactional IDs, the pattern covers |
| of `ClusterAdmin` or `SystemAdmin` | the cluster itself: what a request needs a right on the cluster for, such as creating and deleting ACLs |

The last row has a consequence for the time before it: while
`allow.everyone.if.no.acl.found=true` and no ACL names the cluster, every
principal may alter the cluster, and so may create and delete ACLs and role
bindings. That is the setting's doing, with this authorizer and with the
plain one. It is also why the nodes' own principals belong in `super.users`
even with this setting: the first ACL or binding on the cluster takes away
what they had by default.

The setting is a way in, not a way to run with roles. When every client has
its ACLs or its roles, set it back to `false`, which is its default.

## A node from the Kafka distribution

The commands assume that `KAFKA_HOME` is the directory the distribution is
unpacked in and that its `bin` is on the `PATH`. The cluster of the examples
is one node at `localhost:9092` with three users: `User:admin`, who is in
`super.users`, `User:alice` and `User:bob`. Each has a file of client
properties, `admin.properties`, `alice.properties` and `bob.properties`, of
this kind:

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="alice-secret";
```

The cluster has two topics, `orders` and `billing.orders`, runs
`StandardAuthorizer`, and has one ACL: `User:alice` may read `orders`. So
she sees that topic:

<!-- run -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config alice.properties --topic orders
```

<!-- output -->
```text
orders:0:0
```

On every node, copy the jar to `libs/` and set the line in the properties
file the node is started with:

<!-- run -->
```bash
cp kfkrbac-*.jar "$KAFKA_HOME/libs/"
sed -i 's/^authorizer\.class\.name=.*/authorizer.class.name=dev.kfkit.kfkrbac.RbacAuthorizer/' \
  "$KAFKA_HOME/config/server.properties"
```

Then restart the nodes one at a time, as for any change of a setting that
is read at start. Bind the first role when every node is back: a node that
still runs the plain authorizer ignores bindings, and a controller that
still runs it applies none of the rules for creating them.

<!-- restart -->

A node prints its configuration when it starts, so its log says which
authorizer it runs:

<!-- run -->
```bash
grep 'authorizer.class.name = ' "$KAFKA_HOME/logs/server.log" | tail -n 1
```

<!-- output -->
```text
    authorizer.class.name = dev.kfkit.kfkrbac.RbacAuthorizer
```

The ACL works as before:

<!-- run -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config alice.properties --topic orders
```

<!-- output -->
```text
orders:0:0
```

### The first binding

No ACL lets `User:alice` see `billing.orders`, and to her the topic does
not exist:

<!-- run: exit 1 -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config alice.properties --topic billing.orders
```

<!-- output -->
```text
Error occurred: Could not match any topic-partitions with the specified filters
```

Bind a role to her, as `User:admin`, with the command that is in the jar
([The command](command.md)):

<!-- run -->
```bash
kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand bind \
  --bootstrap-server localhost:9092 --command-config admin.properties \
  --role DeveloperRead --member User:alice --prefix billing.
```

<!-- output -->
```text
Bound DeveloperRead to User:alice on prefix billing.
```

Now she sees it:

<!-- run -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config alice.properties --topic billing.orders
```

<!-- output -->
```text
billing.orders:0:0
```

What a binding is and how to manage bindings is in
[Bindings](bindings.md); who may bind what is in
[Delegation](delegation.md).

### A cluster that ran without an authorizer: the steps

The second cluster of the examples is like the first, with the same two
topics, and has no authorizer and no ACL. It gets the jar, the line, a
super user and `allow.everyone.if.no.acl.found=true`, so that its clients
keep working:

<!-- run: on open -->
```bash
cp kfkrbac-*.jar "$KAFKA_HOME/libs/"
cat >> "$KAFKA_HOME/config/server.properties" <<'PROPERTIES'
authorizer.class.name=dev.kfkit.kfkrbac.RbacAuthorizer
super.users=User:admin
allow.everyone.if.no.acl.found=true
PROPERTIES
```

<!-- restart: on open -->

After the restart `User:bob`, whom no ACL names, still sees
`billing.orders`, because no ACL names that topic either:

<!-- run: on open -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config bob.properties --topic billing.orders
```

<!-- output -->
```text
billing.orders:0:0
```

The first binding on the prefix is for `User:alice`:

<!-- run: on open -->
```bash
kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand bind \
  --bootstrap-server localhost:9092 --command-config admin.properties \
  --role DeveloperRead --member User:alice --prefix billing.
```

<!-- output -->
```text
Bound DeveloperRead to User:alice on prefix billing.
```

It names the topics that start with `billing.`, so they are no longer open
to everyone, and `User:bob` has lost `billing.orders`:

<!-- run: on open, exit 1 -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config bob.properties --topic billing.orders
```

<!-- output -->
```text
Error occurred: Could not match any topic-partitions with the specified filters
```

`orders`, which nothing names, is still open to him:

<!-- run: on open -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config bob.properties --topic orders
```

<!-- output -->
```text
orders:0:0
```

So on such a cluster, before the first binding on a pattern, bind or allow
everyone who uses what the pattern covers.

## The `apache/kafka` image

Put the jar in an image of your own, or mount it:

<!-- not run: the tests copy the jar into a container of the stock image and build none -->
```dockerfile
FROM apache/kafka:4.4.0
COPY kfkrbac-<version>.jar /opt/kfkrbac/kfkrbac.jar
```

This image build has not been run by the tests: they copy the jar to that
path in a container of the stock image.

Add two variables to the environment the node already has:

<!-- environment -->
```properties
CLASSPATH=/opt/kfkrbac/kfkrbac.jar
KAFKA_AUTHORIZER_CLASS_NAME=dev.kfkit.kfkrbac.RbacAuthorizer
```

The image's start script turns every `KAFKA_*` variable into a setting of
the node, so `super.users` is `KAFKA_SUPER_USERS`, and Kafka's scripts add
`CLASSPATH` to the classpath of the broker and of every tool run in the
container, the command of this jar among them. The image uses its built-in
configuration of a single node only when no `KAFKA_*` variable is set: with
just the two variables above it stops with `Missing required configuration
"process.roles"`. They go next to a full configuration (`KAFKA_NODE_ID`,
`KAFKA_PROCESS_ROLES`, `KAFKA_LISTENERS` and the rest), not instead of one.

The integration tests start every broker this way, on each supported
release, and the examples of [Delegation](delegation.md) run on a node
started with exactly these two variables.

## Strimzi

These steps follow Strimzi's documentation,
[`KafkaAuthorizationCustom` schema reference](https://strimzi.io/docs/operators/latest/configuring.html#type-KafkaAuthorizationCustom-reference),
and have not been run: not by the tests and not by hand.

Strimzi runs its own images, so the jar goes in an image built on one of
them, in the directory Strimzi's documentation gives for the jar of a custom
authorizer:

<!-- not run: it needs Strimzi, which the tests do not have -->
```dockerfile
FROM quay.io/strimzi/kafka:<strimzi-version>-kafka-<kafka-version>
USER root:root
COPY kfkrbac-<version>.jar /opt/kafka/libs/
USER 1001
```

The `Kafka` resource names that image, the class and the super users:

<!-- not run: it needs a Kubernetes cluster with Strimzi, which the tests do not have -->
```yaml
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: my-cluster
spec:
  kafka:
    image: registry.example.com/kafka-kfkrbac:<tag>
    authorization:
      type: custom
      authorizerClass: dev.kfkit.kfkrbac.RbacAuthorizer
      supportsAdminApi: true
      superUsers:
        - admin
```

- `superUsers` is the only place for super users: Strimzi ignores
  `super.users` in `spec.kafka.config`. The names are written as in the
  examples of Strimzi's documentation, without `User:` in front.
- `supportsAdminApi: true` tells Strimzi that ACLs are managed through
  Kafka's Admin API, which is how this authorizer and `StandardAuthorizer`
  manage them. Strimzi's default is `false`.
- Not verified: that Strimzi sets the authorizer on its controller nodes as
  well as on its brokers; that its User Operator works with this authorizer
  as it does with `type: simple`; and what the User Operator does with role
  bindings when it reconciles the ACLs of its users
  ([Bindings](bindings.md#tools-that-make-the-acls-equal-to-a-list)).

## Removing it

Set `authorizer.class.name` back to what it was, restart the nodes one at a
time, and delete the jar when every node is back. On the first cluster of
the examples, which ran `StandardAuthorizer` before:

<!-- run -->
```bash
sed -i 's/^authorizer\.class\.name=.*/authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer/' \
  "$KAFKA_HOME/config/server.properties"
```

<!-- restart -->

<!-- run -->
```bash
rm "$KAFKA_HOME"/libs/kfkrbac-*.jar
```

A cluster that had no authorizer before gets the line removed instead, and
then enforces nothing again.

The ACLs are in the cluster's metadata, not in kfkrbac, and stay as they
are. So do the role bindings: they are ACL records, and the plain
authorizer lists them as ACLs.

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --list --topic billing. --resource-pattern-type prefixed
```

<!-- output -->
```text
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=billing., patternType=PREFIXED)`:
    (principal=DeveloperRead:User:alice, host=*, operation=ALL, permissionType=ALLOW)
```

To the plain authorizer they mean nothing: no client is a principal of type
`DeveloperRead`, so the record matches no request, and `User:alice` has
lost what the role gave her and kept what her ACL gives her.

<!-- run: exit 1 -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config alice.properties --topic billing.orders
```

<!-- output -->
```text
Error occurred: Could not match any topic-partitions with the specified filters
```

<!-- run -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config alice.properties --topic orders
```

<!-- output -->
```text
orders:0:0
```

The records can stay for the day the authorizer comes back, or be deleted
with the ACL tool like any ACL:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --remove --force --allow-principal DeveloperRead:User:alice --operation All \
  --topic billing. --resource-pattern-type prefixed
```

With `allow.everyone.if.no.acl.found=true` delete them: for as long as a
record is there, what it names is closed to everyone it does not name, and
without this authorizer it opens it to nobody.
