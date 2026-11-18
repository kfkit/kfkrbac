# Troubleshooting

What to look at when a client of a cluster of Apache Kafka® that runs
`RbacAuthorizer` is refused, or a binding does not do what was meant:

- [A client is "not authorized"](#a-client-is-not-authorized): the line
  the broker logs for a denied request, and the commands to read it with.
- [A consumer that was given the topic and not the group](#a-consumer-that-was-given-the-topic-and-not-the-group)
- [A producer gets "Cluster authorization failed"](#a-producer-gets-cluster-authorization-failed)
- [Creating a binding "needs ALTER permission"](#creating-a-binding-needs-alter-permission)
- [The authorizer is "not ready"](#the-authorizer-is-not-ready)
- [A record that looks like a binding and grants nothing](#a-record-that-looks-like-a-binding-and-grants-nothing)
- [Bindings disappear](#bindings-disappear)
- [A binding took access away from everyone else](#a-binding-took-access-away-from-everyone-else)
- [Where each line goes](#where-each-line-goes)

> Status: the commands of this page are run by a test of the integration
> suite (`DocsIT`) on a broker of the newest supported release, and the
> output shown, the log lines included, is what they printed there. The
> lines under "The roles the line names", "on any TOPIC" and "How often a
> line is written" are from the unit tests of the authorizer.

## The cluster of the examples

One node at `localhost:9092`, started from the Kafka distribution in
`KAFKA_HOME`, that runs `RbacAuthorizer` ([Installation](install.md)), with
three users: `User:admin`, who is in `super.users`, `User:alice` and
`User:bob`, whom no ACL names. Each has a file of client properties for the
tools: `admin.properties`, `alice.properties` and `bob.properties`. The
cluster has a topic `billing.orders` with one record in it, and one
binding: `User:alice` holds `DeveloperRead` on the prefix `billing.`.

The examples write `rbac` for the command of the jar
([The command](command.md)):

<!-- run: session -->
```bash
rbac() { kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand "$@"; }
```

On Kafka 4.1 the console producer and the console consumer take the client
properties as `--producer.config` and `--consumer.config`, not as
`--command-config`.

## A client is "not authorized"

The client's error names a topic or a group and nothing more. Here
`User:alice` consumes `billing.orders`, which her role covers, in the
consumer group `billing-1`:

<!-- run -->
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --command-config alice.properties \
  --topic billing.orders --group billing-1 --from-beginning --max-messages 1 --timeout-ms 20000
```

<!-- output: excerpt -->
```text
org.apache.kafka.common.errors.GroupAuthorizationException: Not authorized to access group: billing-1
```

The broker's log has the rest: one line for the denied request, on the
logger `dev.kfkit.kfkrbac.denied`, that says who asked, for what, and what
was missing.

<!-- run -->
```bash
grep 'dev.kfkit.kfkrbac.denied' "$KAFKA_HOME/logs/server.log"
```

<!-- output -->
```text
[2026-11-18 13:32:28,167] INFO Denied User:alice DESCRIBE on GROUP 'billing-1' (request FindCoordinator): no ACL and no role allows it; roles held for GROUP: DeveloperRead on prefix billing. (dev.kfkit.kfkrbac.denied)
```

Read it left to right:

| Part | Meaning |
|---|---|
| `User:alice` | the principal that asked |
| `DESCRIBE on GROUP 'billing-1'` | the operation, the resource type and the name of the resource, as the broker asked the authorizer |
| `(request FindCoordinator)` | the request the client sent |
| `no ACL and no role allows it` | the cause, one of the three below |
| `roles held for GROUP: ...` | what the principal does hold for that resource type |

### The cause

| The line says | What happened | What to do |
|---|---|---|
| `no ACL and no role allows it` | no ACL allows the request and no role bound to the principal grants it | read the roles the line names, below |
| `a deny ACL matches` | a deny ACL matches the request; a deny ACL beats every role and every allow ACL | find the ACL in the broker's authorizer log, see [Where each line goes](#where-each-line-goes); no binding helps until it is deleted |
| `the roles could not be consulted, see "Role lookup failed" in the broker's log` | the role lookup failed, and nothing is granted on failure | the `ERROR` line `Role lookup failed` before it has the exception; a lookup that keeps failing writes that line once per interval, see [How often a line is written](#how-often-a-line-is-written) |

For `ALTER` on the cluster with the request `AlterUserScramCredentials` the
first cause reads `no ACL allows it, and for this request no role but
SystemAdmin does`: see [Roles](roles.md), `ClusterAdmin`.

### The roles the line names

After `no ACL and no role allows it` the line lists every binding of the
principal that names resources of that type, the cluster roles among them:

```
Denied User:bob READ on TOPIC 'biling.invoices' (request Fetch): no ACL and no role allows it; roles held for TOPIC: DeveloperRead on prefix billing.
Denied User:bob WRITE on TOPIC 'billing.invoices' (request Fetch): no ACL and no role allows it; roles held for TOPIC: DeveloperRead on prefix billing. (does not grant WRITE on TOPIC)
Denied User:dave READ on TOPIC 'billing.invoices' (request Fetch): no ACL and no role allows it; roles held for TOPIC: none
```

- A binding with nothing after it grants the operation, on a pattern that
  does not cover the resource: the first line is a topic name that was
  mistyped, `biling.` for `billing.`.
- `(does not grant WRITE on TOPIC)`: the role is the wrong one for the
  operation, whatever its pattern.
- `none`: the principal holds no role for that resource type. When it
  holds roles for others, the line says how many, `none (its 2 role
  bindings are for other resource types)`: a binding on groups only is not
  listed for a topic, and a binding on topics is not listed for the
  cluster.
- `as Group:payments` after a binding: the principal holds it as a member
  of that [group](groups.md).

A binding is written as [the command](command.md) writes it: `prefix
billing.`, `name orders`, `every name`, `groups with prefix billing.`,
`the cluster`. At most 20 are named, and the line says how many more there
are.

### The commands to read it with

The line says what one request lacked. The command says what a principal
has, and who has what the request asked for, before a client fails. Both
read the ACL records of the cluster and need a principal that may list
them.

<!-- run -->
```bash
rbac what-can --bootstrap-server localhost:9092 --command-config admin.properties \
  --principal User:alice
```

<!-- output -->
```text
User:alice may:
  topics with prefix billing.             Read, Describe  by binding DeveloperRead to User:alice on prefix billing.
  groups with prefix billing.             Read, Describe  by binding DeveloperRead to User:alice on prefix billing.
  transactional IDs with prefix billing.  Describe        by binding DeveloperRead to User:alice on prefix billing.

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

<!-- run -->
```bash
rbac who-can --bootstrap-server localhost:9092 --command-config admin.properties \
  --operation Describe --group billing-1
```

<!-- output -->
```text
Describe on group billing-1:

Nobody, by the roles and the ACLs.

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

## A consumer that was given the topic and not the group

That is the example above, and the most common refusal of a consumer: it
may read the topic and may not use its consumer group. A binding on a name
or a prefix covers the topics, the consumer groups and the transactional
IDs of that name, so the role is right and the pattern is not: the binding
is on `billing.` and the group is `billing-1`.

Call the group `billing.app` and nothing more is needed. Where the name of
the group is fixed, bind the role on the group as well
([Bindings](bindings.md#narrowing-a-binding)):

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role DeveloperRead --member User:alice --name billing-1 --group-only
```

<!-- output -->
```text
Bound DeveloperRead to User:alice on group billing-1
```

<!-- run -->
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --command-config alice.properties \
  --topic billing.orders --group billing-1 --from-beginning --max-messages 1 --timeout-ms 20000
```

<!-- output: excerpt -->
```text
first order
Processed a total of 1 messages
```

## A producer gets "Cluster authorization failed"

A producer is idempotent by default, and an idempotent producer asks the
broker for a producer ID before it sends anything. The broker gives one to
a principal that may `IdempotentWrite` on the cluster or that may write
some topic, whichever topic. A principal that may write none is refused
there, and the error does not name a topic. `User:alice` may read
`billing.orders` and may write nothing:

<!-- run -->
```bash
echo 'second order' | kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --command-config alice.properties --topic billing.orders
```

<!-- output: excerpt -->
```text
org.apache.kafka.common.errors.ClusterAuthorizationException: Cluster authorization failed.
```

The broker's log has the line of the question that was denied, which is
about any topic, and the role that does not answer it:

<!-- run -->
```bash
grep 'dev.kfkit.kfkrbac.denied' "$KAFKA_HOME/logs/server.log" | grep 'InitProducerId'
```

<!-- output -->
```text
[2026-11-18 13:32:33,406] INFO Denied User:alice WRITE on any TOPIC (request InitProducerId): no ACL and no role allows it; roles held for TOPIC: DeveloperRead on prefix billing. (does not grant WRITE on TOPIC) (dev.kfkit.kfkrbac.denied)
```

A principal that may not even describe the topic is refused one step
earlier, with an error that does name the topic. `User:bob` holds nothing:

<!-- run -->
```bash
echo 'second order' | kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --command-config bob.properties --topic billing.orders
```

<!-- output: excerpt -->
```text
org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [billing.orders]
```

<!-- run -->
```bash
grep 'dev.kfkit.kfkrbac.denied' "$KAFKA_HOME/logs/server.log" | grep 'User:bob'
```

<!-- output -->
```text
[2026-11-18 13:32:34,281] INFO Denied User:bob DESCRIBE on TOPIC 'billing.orders' (request Metadata): no ACL and no role allows it; roles held for TOPIC: none (dev.kfkit.kfkrbac.denied)
[2026-11-18 13:32:34,281] INFO Denied User:bob WRITE on any TOPIC (request InitProducerId): no ACL and no role allows it; roles held for TOPIC: none (dev.kfkit.kfkrbac.denied)
```

What a producer needs is a role that grants `Write` on some topic:
`DeveloperWrite` on the prefix of its topics, which is also what lets it
write them.

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role DeveloperWrite --member User:bob --prefix billing.
```

<!-- output -->
```text
Bound DeveloperWrite to User:bob on prefix billing.
```

<!-- run -->
```bash
echo 'second order' | kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --command-config bob.properties --topic billing.orders
```

<!-- prints nothing -->

The broker asks about `IdempotentWrite` on the cluster first, and no role
for an application grants that. That question leaves no line in the log,
with or without the role. With the access metrics on it is counted as
denied each time a producer starts ([Metrics](metrics.md#what-is-recorded)).

### "on any TOPIC"

That second question, whether a principal may do something on any resource
of a type, has no resource to name, and the line says `on any TOPIC`:

```
Denied User:bob WRITE on any TOPIC (request InitProducerId): no ACL and no role allows it; roles held for TOPIC: DeveloperRead on prefix billing. (does not grant WRITE on TOPIC)
Denied User:alice WRITE on any TOPIC (request InitProducerId): deny ACLs cover everything a role or a group allows it on; roles held for TOPIC: DeveloperWrite on prefix orders
```

The second cause is the one a deny ACL has here: a role of the principal
grants the operation, or an ACL of one of its [groups](groups.md) allows
it, and deny ACLs cover every resource it is granted on. An allow ACL that
a deny ACL cancels is not told apart for this question: it reads `no ACL
and no role allows it`.

## Creating a binding "needs ALTER permission"

A principal that may not create a binding is refused in one of two ways,
and only one of them is in the words of this authorizer
([Delegation](delegation.md)).

A principal that holds `ResourceOwner`, or that an ACL or a role lets alter
the cluster, reaches the rules for bindings. When they refuse, the reason
that comes back says what was asked, why not, and what the caller owns.

A principal with neither is stopped before that, by the check the broker
makes of every request that creates or deletes ACLs, and the client gets
the broker's own message. `User:alice` holds `DeveloperRead` on two
patterns, and that does not let her bind:

<!-- run: exit 1 -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config alice.properties \
  --role DeveloperRead --member User:bob --prefix billing.
```

<!-- output: excerpt -->
```text
needs ALTER permission.
```

The same check left its line in the broker's log. It says that she holds
no role for the cluster, and that the two bindings she does hold are for
other resource types, which is not what the check asks about:

<!-- run -->
```bash
grep 'dev.kfkit.kfkrbac.denied' "$KAFKA_HOME/logs/server.log" | grep 'CreateAcls'
```

<!-- output -->
```text
[2026-11-18 13:32:36,917] INFO Denied User:alice ALTER on CLUSTER 'kafka-cluster' (request CreateAcls): no ACL and no role allows it; roles held for CLUSTER: none (its 2 role bindings are for other resource types) (dev.kfkit.kfkrbac.denied)
```

She needs `ResourceOwner` on a pattern that covers what she binds, or
`SystemAdmin`, or an ACL that allows her `Alter` on the cluster.
`rbac list`, `who-can` and `what-can` are refused the same way, with
`needs DESCRIBE permission.`, for a principal that may not list the ACLs.

## The authorizer is "not ready"

kfkrbac has no start-up rule of its own. Bindings and memberships are ACL
records, a node gets them with the ACLs, and the rule of the broker's
`StandardAuthorizer` covers them: until a node has loaded the ACL records,
its authorizer answers the principals in `super.users` and nobody else, and
every other request is refused with `AuthorizerNotReadyException` rather
than decided. So no request is ever decided from half of the bindings. The
exception is in the unit tests of the authorizer. What a client sees of it,
and how long a node stays in that state, is the broker's doing and not
verified here.

In a worker of Kafka Connect the same state has a different face: until the
extension has read the bindings for the first time, every request but
`GET /health` gets 503 with `kfkrbac: the role bindings have not been read
from the cluster yet; try again shortly` ([Kafka Connect](connect.md)).

## A record that looks like a binding and grants nothing

A record whose principal type is a role and which is not a binding, an
operation other than `All` for instance, grants nothing. The authorizer
refuses to create one, with the reason. One that was written before the jar
was installed stays in the metadata, and shows in three places:
a `WARN` line of the logger `dev.kfkit.kfkrbac.BindingIndex` on every node,
the gauge `malformed-bindings`, which is 0 on a healthy cluster, and a
warning of `rbac list`. [Bindings](bindings.md#a-record-that-is-not-a-binding)
has all three, run on a broker, and the command that deletes the record.
Records of groups that are not what they say are counted in the same gauge
([Groups](groups.md#a-record-that-is-not-a-membership)).

## Bindings disappear

A tool that keeps the ACLs of a cluster equal to a list deletes what its
list does not have, and a binding is an ACL record. The tool runs as a
principal that manages every ACL, so its delete is not refused, and nothing
of kfkrbac logs it. Put the bindings in the tool's list, or make it leave
alone every ACL whose principal type is a role or `Member`:
[Bindings](bindings.md#tools-that-make-the-acls-equal-to-a-list).

The ACL tool with a wide filter does the same: `--remove --topic billing.
--resource-pattern-type prefixed` removes every record on that prefix, the
bindings among them.

## A binding took access away from everyone else

With `allow.everyone.if.no.acl.found=true` the broker allows whatever no
ACL names. A binding is an ACL record and names its pattern, so the first
binding on a pattern closes it to everyone the binding is not for.
[Installation](install.md#a-cluster-that-ran-without-an-authorizer) says
which resources a binding names, and shows it on a broker. Before the first
binding on a pattern, bind or allow everyone who uses what it covers; when
every client has its ACLs or its roles, set the setting back to `false`.

## How often a line is written

A client retries a denied request many times a second. The line for the
same principal, operation and resource is written at most once in 60
seconds, and the line that follows says what was left out:

```
Denied User:bob READ on TOPIC 'orders' (request Fetch): no ACL and no role allows it; roles held for TOPIC: none; denied 42 more times since this was last logged
```

The interval is the setting `kfkrbac.denied.log.interval.seconds`
([Settings](settings.md)); `0` logs every denial.

The same interval bounds the `ERROR` line `Role lookup failed`, which
carries the exception: a lookup that fails once usually fails for every
request until what broke is mended. The first failure is written with its
stack trace, the ones in the rest of the interval are counted, and the next
line says how many there were. Every one of those requests is denied all
the same:

```
Role lookup failed for User:bob READ on ResourcePattern(resourceType=TOPIC, name=orders, patternType=LITERAL); denied; 4 more lookups failed and were denied since this was last logged
```

Only denials the broker itself wants logged are written: a request that
only asks what a client may see, such as a listing of all topics, is not a
failure and leaves no line.

The authorizer remembers at most 10,000 different denied requests at a
time, each by a principal and a resource name of at most 256 characters; a
longer name is shown cut, and told apart from others by its first 256
characters. When more are denied, the ones not denied for an interval are
forgotten first, and then all of them. A line says so, and a request that
was forgotten is logged again the next time it is denied. This one is from
a test, where the limit is 16:

```
Forgot 16 of the denied requests remembered so as not to log them again, to stay within 16; 3 denials that were not logged will not be counted in a later line
```

## Where each line goes

| Logger | Level | What |
|---|---|---|
| `dev.kfkit.kfkrbac.denied` | `INFO` | a request that was denied, and why |
| `kafka.authorizer.logger` | `INFO` | the broker's own line for a denied request, unchanged |
| `kafka.authorizer.logger` | `DEBUG` | the broker's own line for a request an ACL allows, and the same line for a request a role or an ACL of a group allows |
| `dev.kfkit.kfkrbac.RbacAuthorizer` | `ERROR` | `Role lookup failed`, with the exception, at most once per interval |
| `dev.kfkit.kfkrbac.RbacAuthorizer` | `INFO` | `Refused to create` and `Refused to delete`, on the controller: a request for ACL records that the rules for bindings refused, with the reason the caller got |
| `dev.kfkit.kfkrbac.BindingIndex` | `WARN` | a record that looks like a binding or a membership and is not one |

`dev.kfkit.kfkrbac.denied` is a logger of its own so that these lines can
be sent to a file of their own or silenced without touching anything else:
give the logger of that name an appender, or the level `OFF`, in the
broker's logging configuration. With the configuration a broker ships it
goes where the broker's other messages go, `logs/server.log`.
The line names the principal, the operation, the resource, the request, the
cause and the roles of that principal: no host, no client ID, and nothing
about any other principal.

`kafka.authorizer.logger` is the broker's log of what its authorizer
decided, `kafka-authorizer.log` in the configuration a broker ships. It says
what was decided in the end, once for each resource of a request:

```
Principal = User:bob is Denied operation = READ from host = 127.0.0.1 on resource = Topic:LITERAL:orders for request = Fetch with resourceRefCount = 1 based on rule DefaultDeny
Principal = User:bob is Allowed operation = READ from host = 127.0.0.1 on resource = Topic:LITERAL:billing.invoices for request = Fetch with resourceRefCount = 1 based on rule RoleBinding(role=DeveloperRead, member=User:bob, resourceType=TOPIC, resourceName=billing., patternType=PREFIXED)
```

The first is the broker's own line, at `INFO`, as it is without roles; a
deny ACL that matched is named in it, as `MatchingAcl(...)`. The second is
written at `DEBUG`, where the broker writes the requests an ACL allows: a
request a role allows reads the same up to the rule, which is the binding,
with the resource type, the name and the pattern type of its ACL record. A
request that an ACL of a group allows its member names the group instead:
`based on rule GroupAcl(group=Group:payments)`. Neither ever leaves a
`Denied` line there. These two lines are from the unit tests of the
authorizer.

At `TRACE` the broker writes there every answer its authorizer gives,
those it was asked not to log included. This authorizer asks the parent
first, and the parent's answer is written as it is without roles. For a
principal that holds a binding or is in a group, the parent is first asked
not to log a denial, which leaves a `TRACE` line, and asked again, for its
`INFO` line, only when the request stays denied. Whether a deny ACL
matches, and what an ACL of a group allows, is found without a line in
that log.
