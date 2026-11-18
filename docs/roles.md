# Roles

A role names a set of permissions once. A role binding hands it to a member,
a user (`User:alice`) or a group (`Group:payments`), on a name pattern or on
the cluster of Apache Kafka®. There are seven roles, and they only ever add
access: what a deny ACL denies stays denied.

A role's meaning never changes once it is released: a different set of
permissions gets a different name. Operators do not define roles of their
own; a group that ordinary ACLs name does that job. Groups are in
[groups.md](groups.md).

> Status: a unit test (`RoleTest`) fails when the table below and the code
> disagree. The broker's authorizer decides with the columns Topic, Group,
> TransactionalId and Cluster, and the extension for
> [Kafka Connect](connect.md) with the column Connector, on real brokers
> and workers of every supported release in the integration suite. Nothing
> decides with the column Subject: there is no extension for a schema
> registry, so a binding for subjects can be written and grants nothing
> anywhere. The two commands of this page are run by a test of that suite
> (`DocsIT`).

## What each role grants

"all" is every operation of that kind; "—" is nothing. A role bound to a
pattern grants on the resources whose names the pattern covers; a role bound
to the cluster grants on every resource of the kind, whatever its name.

| Role | Bound to | Topic | Group | TransactionalId | Cluster | Subject | Connector |
|---|---|---|---|---|---|---|---|
| `DeveloperRead` | pattern | Read, Describe | Read, Describe | Describe | — | Read, ReadCompatibility | ReadConfig, ReadStatus, ReadActiveTopics |
| `DeveloperWrite` | pattern | Write, Describe | — | Write, Describe | — | Write, ReadCompatibility | ReadStatus, ReadActiveTopics, Configure, ResetActiveTopics |
| `DeveloperManage` | pattern | Create, Delete, Describe, DescribeConfigs | Describe, Delete | Describe | — | ReadCompatibility, WriteCompatibility | Create, ReadStatus, ReadActiveTopics, Pause, Resume, Scale, ResetActiveTopics |
| `ResourceOwner` | pattern | all | all | all | — | all | all |
| `ResourceViewer` | pattern | Describe, DescribeConfigs | Describe | Describe | — | Read, ReadCompatibility | ReadStatus |
| `ClusterAdmin` | cluster | Create, Delete, Alter, AlterConfigs, Describe, DescribeConfigs | — | — | Create, Alter, AlterConfigs, Describe, DescribeConfigs | Read, Write, Delete, ReadCompatibility, WriteCompatibility | Create, ReadConfig, ReadStatus, ReadActiveTopics, Pause, Resume, Scale, Delete, ResetActiveTopics |
| `SystemAdmin` | cluster | all | all | all | all | all | all |

The kinds and their operations:

| Kind | Operations |
|---|---|
| Topic, Group, TransactionalId, Cluster | the broker's ACL operations: Read, Write, Create, Delete, Alter, Describe, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite, CreateTokens, DescribeTokens, TwoPhaseCommit |
| Subject | Read, Write, Delete, ReadCompatibility, WriteCompatibility |
| Connector | Create, Delete, ReadConfig, ReadStatus, ReadActiveTopics, Configure, Pause, Resume, Scale, ResetActiveTopics |

Connectors are the connectors of Kafka Connect and subjects the subjects of
a schema registry: the broker never sees those requests. For connectors the
same jar answers them in the workers, from the same bindings
([Kafka Connect](connect.md)). For subjects nothing does: the column says
what a role would grant there, and is kept so that a role's meaning does not
have to change when something reads it.

## What each role is for

- `DeveloperRead`: an application that consumes. It reads the topics and
  joins the consumer groups under its pattern.
- `DeveloperWrite`: an application that produces. It writes the topics
  under its pattern, transactionally if it wants.
- `DeveloperManage`: a team that creates and deletes its own topics and
  groups, and runs its own connectors, without reading or writing the
  data.
- `ResourceOwner`: the owner of a pattern. Everything on what the pattern
  covers, and the right to hand out roles inside it.
- `ResourceViewer`: someone who needs to see what exists under a pattern and
  how it is configured, without reading the data.
- `ClusterAdmin`: an operator who runs the cluster: brokers' configuration,
  topics everywhere and connectors, without reading or writing the data in
  topics.
  `ClusterAdmin` does not manage access. It creates and deletes no ACL, no
  role binding and no group membership, and its `Alter` on the cluster does
  not let it replace a user's SCRAM credentials: those take `SystemAdmin`, a
  super user, or an ACL that allows `Alter` on the cluster. It can change
  the brokers' configuration, how clients authenticate included: bind it
  only to those you trust with the cluster.
- `SystemAdmin`: everything, everywhere, including binding any role to
  anyone.

## A producer and a consumer

A typical producer needs one binding: `DeveloperWrite` on the prefix of its
topics. It writes them, it may be idempotent (the broker allows that to
whoever may write some topic), and a transactional producer whose
transactional ID starts with the same prefix may use it.

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal DeveloperWrite:User:alice --operation All \
  --topic billing. --resource-pattern-type prefixed
```

A typical consumer needs one binding too: `DeveloperRead` on the prefix of
its topics. It reads them and commits its offsets in a consumer group whose
ID starts with the same prefix, `billing.app` for instance.

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal DeveloperRead:User:bob --operation All \
  --topic billing. --resource-pattern-type prefixed
```

A consumer whose group does not start with the prefix of its topics needs a
second binding, on the group: give its groups the same prefix and it needs
one.

The two bindings, as the command of the jar lists them:

<!-- run -->
```bash
kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand list \
  --bootstrap-server localhost:9092 --command-config admin.properties
```

<!-- output -->
```text
ROLE            MEMBER      ON
DeveloperRead   User:bob    prefix billing.
DeveloperWrite  User:alice  prefix billing.
```

The commands are run on the cluster of [Bindings](bindings.md): one node at
`localhost:9092` with `RbacAuthorizer`, and `admin.properties` for
`User:admin`, who is in `super.users`. That page has the same bindings at
work, with a producer and a consumer.

## A record that is not a binding

A record whose principal type is a role, but which is not a binding as
above, grants nothing: an operation other than `All`, a `Deny`, a host other
than `*`, a cluster role on a name pattern or a pattern role on the cluster,
a member that is not `Type:name`. The authorizer refuses to create such a
record. One that is in the metadata anyway, written while the cluster ran
the plain authorizer, is logged at `WARN` with the ID of the record by each
authorizer when it first reads it. That is once after every start of a
node, and twice on a node that is both broker and controller, which has two
authorizers. Each authorizer counts such records in the gauge
`malformed-bindings` of its plugin metrics for as long as they exist. The
same gauge counts the records that are not group memberships and the `DENY`
ACLs of groups ([Groups](groups.md)).
[Bindings](bindings.md#a-record-that-is-not-a-binding) has the whole list,
the line of the log and the gauge as a broker shows them, and how to delete
the record.

## Reserved names

The names of the roles are reserved as principal types: no client may
authenticate as a principal of type `DeveloperRead`, so a binding never
matches as an ordinary ACL. So are `Member`, the principal type of a group
membership, and `Group`, the principal type of a group.

`Subject:`, `Connector:` and `Group:` are reserved in front of a name: the
first two say what a binding is for, and the third names the group of a
membership record, so no binding's name starts with it.
