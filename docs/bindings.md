# Bindings

A role binding gives a member a role on a name pattern or on the cluster:
`User:alice` holds `DeveloperRead` on every name that starts with
`billing.`. The member is a user, as on this page, or a group
([Groups](groups.md)). This page shows what a binding is in the metadata of a cluster
of Apache Kafka®, and how to create, list and remove bindings with Kafka's
own ACL tool and with the command of this jar. What each role grants is in
[Roles](roles.md), every option of the command in [The command](command.md),
and who may bind what in [Delegation](delegation.md).

> Status: every command of this page is run by a test of the integration
> suite (`DocsIT`) on a broker of the newest supported release, and the
> output shown is what the commands printed there.

## What a binding is

A binding is an ACL record whose principal names the role and the member.
There is no other store: the record lives in the cluster's metadata next to
the ACLs, reaches every node the way an ACL does, and is created, listed
and deleted through Kafka's ACL API. So the ACL tool, the admin client and
whatever manages your ACLs today manage bindings as well; the command of
this jar writes the same records in fewer words.

| Field of the record | In a binding |
|---|---|
| principal | `<Role>:<member>`: `DeveloperRead:User:alice`, or `DeveloperRead:Group:payments` for a group |
| resource type | `TOPIC` for a pattern that covers topics, groups and transactional IDs; `GROUP` or `TRANSACTIONAL_ID` for a pattern narrowed to that type; `CLUSTER` for a role bound to the cluster |
| name and pattern type | the name with `LITERAL`, the prefix with `PREFIXED`; `kafka-cluster` with `LITERAL` for the cluster |
| operation, permission, host | always `All`, `Allow` and `*` |

The broker's own authorizer never matches such a record to a request,
because no client is a principal of type `DeveloperRead`. `RbacAuthorizer`
reads it as "the member holds the role on that pattern".

## The cluster of the examples

One node at `localhost:9092` that runs `RbacAuthorizer`
([Installation](install.md)), with three users: `User:admin`, who is in
`super.users`, `User:alice` and `User:bob`, whom no ACL names. Each has a
file of client properties for the tools: `admin.properties`,
`alice.properties` and `bob.properties`. Kafka's `bin` is on the `PATH`.

The examples write `rbac` for the command of the jar:

<!-- run: session -->
```bash
rbac() { kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand "$@"; }
```

They need a topic with a record in it:

<!-- run -->
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --create --topic billing.orders
echo 'first order' | kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --command-config admin.properties --topic billing.orders
```

On Kafka 4.1 the console producer and the console consumer take the client
properties as `--producer.config` and `--consumer.config`, not as
`--command-config`.

## One binding for an application

An application that consumes needs to read its topics and to use its
consumer group. Without a binding `User:alice` may do neither:

<!-- run -->
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --command-config alice.properties \
  --topic billing.orders --group billing.app --from-beginning --max-messages 1 --timeout-ms 20000
```

<!-- output: excerpt -->
```text
org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [billing.orders]
```

One binding is enough, because a pattern covers every kind of resource the
role grants something on: `DeveloperRead` on the prefix `billing.` is the
topics and the consumer groups that start with `billing.`. With the ACL
tool it is an ACL for the principal `DeveloperRead:User:alice` on the
prefixed topic pattern, with the operation `All`:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal DeveloperRead:User:alice --operation All \
  --topic billing. --resource-pattern-type prefixed
```

The same binding with the command:

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role DeveloperRead --member User:alice --prefix billing.
```

<!-- output -->
```text
Bound DeveloperRead to User:alice on prefix billing.
```

Binding what is bound already changes nothing, so after both there is one
record:

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

Now the application reads the topic in a group under the prefix:

<!-- run -->
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --command-config alice.properties \
  --topic billing.orders --group billing.app --from-beginning --max-messages 1 --timeout-ms 20000
```

<!-- output: excerpt -->
```text
first order
Processed a total of 1 messages
```

`DeveloperRead` does not grant writing, so producing is still refused. The
console producer is idempotent, as producers are by default, and the broker
gives a producer ID only to a principal that may write some topic: she may
write none, and is refused before she sends anything.

<!-- run -->
```bash
echo 'not written' | kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --command-config alice.properties --topic billing.orders
```

<!-- output: excerpt -->
```text
org.apache.kafka.common.errors.ClusterAuthorizationException: Cluster authorization failed.
```

An application that produces gets `DeveloperWrite` the same way:

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

The console producer prints nothing when the record is written. That one
binding also covers the transactional IDs under the prefix, for a producer
that writes in transactions:

<!-- run -->
```bash
rbac what-can --bootstrap-server localhost:9092 --command-config admin.properties \
  --principal User:bob
```

<!-- output -->
```text
User:bob may:
  topics with prefix billing.             Write, Describe  by binding DeveloperWrite to User:bob on prefix billing.
  transactional IDs with prefix billing.  Write, Describe  by binding DeveloperWrite to User:bob on prefix billing.

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

## Narrowing a binding

A name that does not follow the prefix needs a binding of its own. Give the
names of an application one prefix and it needs one binding; where a name
is already fixed, bind the role to that name, for that kind of resource
only.

The group `billing-1` does not start with `billing.`, so the binding above
does not cover it:

<!-- run -->
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --command-config alice.properties \
  --topic billing.orders --group billing-1 --from-beginning --max-messages 1 --timeout-ms 20000
```

<!-- output: excerpt -->
```text
org.apache.kafka.common.errors.GroupAuthorizationException: Not authorized to access group: billing-1
```

`--group-only` binds the role on groups and nothing else:

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role DeveloperRead --member User:alice --name billing-1 --group-only
```

<!-- output -->
```text
Bound DeveloperRead to User:alice on group billing-1
```

With the ACL tool the same binding is the same principal on the group:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal DeveloperRead:User:alice --operation All --group billing-1
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

`--transactional-id-only` does the same for transactional IDs, and the ACL
tool's `--transactional-id` writes the same record:

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role DeveloperWrite --member User:bob --name billing-1 --transactional-id-only
```

<!-- output -->
```text
Bound DeveloperWrite to User:bob on transactional ID billing-1
```

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal DeveloperWrite:User:bob --operation All --transactional-id billing-1
```

`--name '*'` is every name, as `--topic '*'` is for the ACL tool.

## Cluster roles

`ClusterAdmin` and `SystemAdmin` are bound to the cluster and take no
pattern:

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role ClusterAdmin --member User:bob
```

<!-- output -->
```text
Bound ClusterAdmin to User:bob on the cluster
```

With the ACL tool it is the same principal on the cluster:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal ClusterAdmin:User:bob --operation All --cluster
```

A cluster role with a pattern, or one of the other five roles without, is
not a binding. The command says so before it sends anything, with the
status 2 of wrong usage:

<!-- run: exit 2 -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role ClusterAdmin --member User:bob --prefix billing.
```

<!-- output: excerpt -->
```text
Not a binding: ClusterAdmin is bound to the cluster, not to a name pattern
```

## Listing

`rbac list` shows the bindings as bindings, and no ordinary ACL:

<!-- run -->
```bash
rbac list --bootstrap-server localhost:9092 --command-config admin.properties
```

<!-- output -->
```text
ROLE            MEMBER      ON
DeveloperRead   User:alice  prefix billing.
DeveloperWrite  User:bob    prefix billing.
DeveloperRead   User:alice  group billing-1
ClusterAdmin    User:bob    the cluster
DeveloperWrite  User:bob    transactional ID billing-1
```

It takes `--role`, `--member`, `--prefix` and `--name` to select, and
`--output json` for a program ([The command](command.md#list)). The ACL
tool's `--list` shows the same records as the ACLs they are, as above.
Which bindings reach a given topic, and what a principal may do in all, are
the command's `who-can` and `what-can`.

## Removing

`unbind` takes the options of `bind` and removes that one binding:

<!-- run -->
```bash
rbac unbind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role ClusterAdmin --member User:bob
```

<!-- output -->
```text
Unbound ClusterAdmin to User:bob on the cluster
```

It fails, with the status 1, when there is no such binding:

<!-- run: exit 1 -->
```bash
rbac unbind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role ClusterAdmin --member User:bob
```

<!-- output -->
```text
error: no such binding: ClusterAdmin to User:bob on the cluster
```

With the ACL tool a binding is removed like any ACL, by naming it in full:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --remove --force --allow-principal DeveloperWrite:User:bob --operation All \
  --transactional-id billing-1
```

## A deny ACL beats a role

A role only adds access. What a deny ACL denies stays denied, whatever
roles the principal holds, so denying is done with ACLs:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --deny-principal User:alice --operation Read --topic billing.orders
```

<!-- run -->
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --command-config alice.properties \
  --topic billing.orders --group billing.app --from-beginning --max-messages 1 --timeout-ms 20000
```

<!-- output: excerpt -->
```text
org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [billing.orders]
```

`who-can` shows both the ACL that stops her and the binding that would
have let her:

<!-- run -->
```bash
rbac who-can --bootstrap-server localhost:9092 --command-config admin.properties \
  --operation Read --topic billing.orders
```

<!-- output -->
```text
Read on topic billing.orders:

User:alice  denied
    denied by ACL deny User:alice Read on topic billing.orders
    without that, allowed by binding DeveloperRead to User:alice on prefix billing.

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

Remove the deny ACL and the binding is in force again:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --remove --force --deny-principal User:alice --operation Read --topic billing.orders
```

## A record that is not a binding

A record whose principal type is a role, and which is not as the table at
the top has it, is not a binding and grants nothing. It is one of these:

- an operation other than `All`, a permission other than `Allow`, or a host
  other than `*`;
- a member that is not a type and a name, as in `User:alice`;
- `ClusterAdmin` or `SystemAdmin` on a name pattern, or one of the other
  five roles on the cluster;
- a cluster resource that is not the literal name `kafka-cluster`;
- a resource type other than topic, group, transactional ID and cluster, a
  pattern type other than literal and prefixed, or an empty name;
- a name that starts with `Group:`, or one that starts with `Subject:` or
  `Connector:` on a group or a transactional ID
  ([Reserved names](#reserved-names)).

`RbacAuthorizer` refuses to create such a record, for a super user too,
and says what is wrong with it. An operator who meant "let `User:bob` write
`billing.orders`" and took the role for a principal gets:

<!-- run: exit 1 -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal DeveloperWrite:User:bob --operation Write --topic billing.orders
```

<!-- output: excerpt -->
```text
Error while executing ACL command: org.apache.kafka.common.errors.InvalidRequestException: DeveloperWrite:User:bob on the TOPIC name 'billing.orders' is not a role binding: the operation is WRITE, a binding has ALL
```

Such a record can still be in the metadata: written while the cluster ran
the plain authorizer, which takes it for an ACL like any other. A second
cluster of the examples, like the first, has one and no other record: an
ACL for `DeveloperRead:User:bob` with the operation `Read` on the topic
`orders`, written before the jar was installed. On that node `KAFKA_HOME`
is the directory of the distribution and the JMX port is 9999. The record
shows in three places.

Each authorizer logs the record at `WARN` when it first reads it, which is
once after every start of the node, with the ID of the record and what is
wrong with it. A node that is broker and controller has two authorizers, so
its log has the line twice:

<!-- run: on old -->
```bash
grep 'is not a role binding' "$KAFKA_HOME/logs/server.log"
```

<!-- output -->
```text
[2026-11-18 11:24:33,803] WARN ACL EQbz60tBQVG9zZAm1ZlSHQ has a role as its principal type and is not a role binding, so it grants nothing: the operation is READ, a binding has ALL: StandardAcl[resourceType=TOPIC, resourceName=orders, patternType=LITERAL, principal=DeveloperRead:User:bob, host=*, operation=READ, permissionType=ALLOW] (dev.kfkit.kfkrbac.BindingIndex)
[2026-11-18 11:24:34,193] WARN ACL EQbz60tBQVG9zZAm1ZlSHQ has a role as its principal type and is not a role binding, so it grants nothing: the operation is READ, a binding has ALL: StandardAcl[resourceType=TOPIC, resourceName=orders, patternType=LITERAL, principal=DeveloperRead:User:bob, host=*, operation=READ, permissionType=ALLOW] (dev.kfkit.kfkrbac.BindingIndex)
```

Each authorizer counts such records, for as long as they exist, and with
them the records of groups that are not what they say
([Groups](groups.md)), in the gauge `malformed-bindings` of its MBean
`kafka.server:type=plugins,config=authorizer.class.name,class=RbacAuthorizer,role=<broker or controller>`.
Where the node's JMX port is open, Kafka's JMX tool reads it:

<!-- run: on old -->
```bash
kafka-jmx.sh --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi \
  --object-name 'kafka.server:type=plugins,config=authorizer.class.name,class=RbacAuthorizer,*' \
  --attributes malformed-bindings --one-time true --report-format properties
```

<!-- output: excerpt -->
```text
kafka.server:type=plugins,config=authorizer.class.name,class=RbacAuthorizer,role=broker:malformed-bindings=1
kafka.server:type=plugins,config=authorizer.class.name,class=RbacAuthorizer,role=controller:malformed-bindings=1
```

And `rbac list` leaves the record out and names it in a warning on standard
error:

<!-- run: on old -->
```bash
rbac list --bootstrap-server localhost:9092 --command-config admin.properties
```

<!-- output -->
```text
warning: not a binding, grants nothing: DeveloperRead:User:bob on topic orders: the operation is READ, a binding has ALL
No bindings.
```

`who-can` and `what-can` give the same warning.

Delete it with the ACL tool, by naming it in full, and bind the role that
was meant:

<!-- run: on old -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --remove --force --allow-principal DeveloperRead:User:bob --operation Read --topic orders
```

The gauge is back at 0:

<!-- run: on old -->
```bash
kafka-jmx.sh --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi \
  --object-name 'kafka.server:type=plugins,config=authorizer.class.name,class=RbacAuthorizer,*' \
  --attributes malformed-bindings --one-time true --report-format properties
```

<!-- output: excerpt -->
```text
kafka.server:type=plugins,config=authorizer.class.name,class=RbacAuthorizer,role=broker:malformed-bindings=0
kafka.server:type=plugins,config=authorizer.class.name,class=RbacAuthorizer,role=controller:malformed-bindings=0
```

## Reserved names

A binding is told from an ACL by its principal type, and from other
bindings by the start of its name, so a few names are taken:

- **The names of the seven roles, as principal types**: `DeveloperRead`,
  `DeveloperWrite`, `DeveloperManage`, `ResourceOwner`, `ResourceViewer`,
  `ClusterAdmin` and `SystemAdmin`. A record with one of them is a binding
  or is refused. Kafka's own principal builder makes every client a `User`;
  a principal builder of your own must not produce these types.
- **`Member` and `Group`, as principal types.** `Member` is the principal
  type of a group membership and `Group` that of a group
  ([Groups](groups.md)).
- **`Subject:` and `Connector:` in front of a name.** A binding whose name
  starts with one of them is for subjects of a schema registry or for
  connectors, and covers no topic, group or transactional ID: a colon is
  not legal in a topic name, so no topic is lost. The command writes such a
  name with `--subject` or `--connector` and refuses it in `--prefix` and
  `--name`. No broker reads these bindings: the ones for connectors are
  read by the extension for [Kafka Connect](connect.md), and nothing reads
  the ones for subjects.
- **`Group:` in front of a name.** It names the group of a membership
  record, so no binding may have such a name, and the cluster refuses the
  record:

<!-- run: exit 1 -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal DeveloperRead:User:bob --operation All --topic Group:billing-1
```

<!-- output: excerpt -->
```text
Error while executing ACL command: org.apache.kafka.common.errors.InvalidRequestException: DeveloperRead:User:bob on the TOPIC name 'Group:billing-1' is not a role binding: the name starts with Group:, which is reserved for the groups of membership records
```

## Tools that make the ACLs equal to a list

Some tools keep the ACLs of a cluster equal to a list in a file: they
create what is missing and delete what the list does not have. Bindings are
ACL records, so to such a tool a binding that is not in its list is an ACL
to delete, and a super user's delete is not refused.

Before you bind the first role on a cluster that has such a tool, do one of
two things: put the bindings in its list, as the ACLs they are, with the
principal `<Role>:<member>` and the fields of the table at the top; or
make the tool leave alone every ACL whose principal type is a role.

The ACL tool does the same when it is given a wide filter. This removes
every record on the prefix `billing.`, the ACLs and the bindings alike:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --remove --force --topic billing. --resource-pattern-type prefixed
```

<!-- run -->
```bash
rbac list --bootstrap-server localhost:9092 --command-config admin.properties
```

<!-- output -->
```text
ROLE           MEMBER      ON
DeveloperRead  User:alice  group billing-1
```
