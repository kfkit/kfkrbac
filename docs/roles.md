# Roles

A role names a set of permissions once. A role binding hands it to a member,
a user (`User:alice`) or a group (`Group:payments`), on a name pattern or on
the cluster. There are seven roles, and they only ever add access: what a
deny ACL denies stays denied.

A role's meaning never changes once it is released: a different set of
permissions gets a different name. Operators do not define roles of their
own; a group that ordinary ACLs name does that job. Groups are in
[groups.md](groups.md).

> Status: the authorizer reads bindings from ACL records and decides with
> them, on real brokers too in the integration tests; the extension for
> Kafka Connect decides with them for connectors, in real workers too
> ([connect.md](connect.md));
> nothing answers yet for subjects. The examples below are as designed.

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

Subjects are the subjects of a schema registry and connectors the connectors
of Kafka Connect: the broker never sees those requests, and the same jar
answers them there from the same bindings.

## What each role is for

- `DeveloperRead`: an application that consumes. It reads the topics, joins
  the consumer groups and reads the schemas under its pattern.
- `DeveloperWrite`: an application that produces. It writes the topics,
  transactionally if it wants, and registers schemas under its pattern.
- `DeveloperManage`: a team that creates and deletes its own topics and
  groups and evolves its schemas' compatibility, without reading or writing
  the data.
- `ResourceOwner`: the owner of a pattern. Everything on what the pattern
  covers, and the right to hand out roles inside it.
- `ResourceViewer`: someone who needs to see what exists under a pattern and
  how it is configured, without reading the data.
- `ClusterAdmin`: an operator who runs the cluster: brokers' configuration,
  topics everywhere, schemas and connectors, without reading or writing the
  data in topics.
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

```bash
kafka-acls.sh --bootstrap-server localhost:9092 --add \
  --allow-principal 'DeveloperWrite:User:alice' --operation All \
  --topic billing. --resource-pattern-type prefixed
```

A typical consumer needs one binding too: `DeveloperRead` on the prefix of
its topics. It reads them and commits its offsets in a consumer group whose
ID starts with the same prefix, `billing.invoicing` for instance.

```bash
kafka-acls.sh --bootstrap-server localhost:9092 --add \
  --allow-principal 'DeveloperRead:User:bob' --operation All \
  --topic billing. --resource-pattern-type prefixed
```

A consumer whose group does not start with the prefix of its topics needs a
second binding, on the group: give its groups the same prefix and it needs
one.

## A record that is not a binding

A record whose principal type is a role, but which is not a binding as
above, grants nothing: an operation other than `All`, a `Deny`, a host other
than `*`, a cluster role on a name pattern or a pattern role on the cluster,
a member that is not `Type:name`. The authorizer logs it once, at `WARN`,
with the ID of the record, and counts such records in the gauge
`malformed-bindings` of its plugin metrics for as long as they exist. The
same gauge counts the records that are not group memberships and the `DENY`
ACLs of groups: see [groups.md](groups.md).

## Reserved names

The names of the roles are reserved as principal types: no client may
authenticate as a principal of type `DeveloperRead`, so a binding never
matches as an ordinary ACL. So are `Member`, the principal type of a group
membership, and `Group`, the principal type of a group.

`Subject:`, `Connector:` and `Group:` are reserved in front of a name: the
first two say what a binding is for, and the third names the group of a
membership record, so no binding's name starts with it.
