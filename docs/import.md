# Bringing bindings over

`import` reads a file of role bindings that has another shape than the
command's own and writes what it holds to the cluster. It is a verb of the
command ([command.md](command.md)):

```bash
kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand import --bootstrap-server localhost:9092 \
  --file bindings.json --format role-bindings --dry-run
```

It reads files only, and two shapes of them:

| `--format` | The file is | It is written as |
|---|---|---|
| [`role-bindings`](#role-bindings) | a list of bindings of named roles | bindings |
| [`role-acls`](#role-acls) | a list of roles that are sets of ACLs | for each role, a group with the role's members, and ordinary `ALLOW` ACLs whose principal is the group ([groups.md](groups.md)) |

| Option | Meaning |
|---|---|
| `--file <file>` | the JSON file; required |
| `--format role-bindings\|role-acls` | the shape of the file; required |
| `--cluster-id <id>` | for `role-bindings`: only the bindings for that Kafka cluster, and those that name none |
| `--dry-run` | print what would be written and what is skipped, and change nothing |

The examples below leave out `--bootstrap-server` and `--command-config`,
and write `rbac` for `kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand`.

## Nothing is guessed

- **What the file must have and has not** stops the import before anything
  is written: a file that is not valid JSON, a required field that is
  missing, a value of another kind than the field takes. The message has the
  file, the line and the column, the way to the place from the top of the
  file, and the field; the exit status is 2.
- **What the file has and cannot be written exactly** is skipped and
  reported, with the place and the reason, and the rest is written: a role
  there is no such role for, a resource type nothing here authorizes, a
  scope a binding cannot be narrowed to, a role with a `DENY` ACL. Nothing
  is written that would give more access than the file gives, with the one
  exception that is the nature of a binding and is said below: a pattern for
  topics becomes a binding on that name.
- **Run it with `--dry-run` first.** The report of a dry run is the report
  of the run that writes, word for word, with `Would write` for `Wrote`.

The exit status is 0 when the import ran, whatever it skipped: read the
report. It is 1 when the cluster refused, with the cluster's reason, and
then nothing was written: the records of an import are created in one
request, which the authorizer applies as a whole or not at all.

Importing a file twice changes nothing: a record that exists is left as it
is. `import` only adds; it never deletes a binding, a membership or an ACL.
To make the cluster match a file, deletions included, use `apply` with a
file in the command's own shape ([command.md](command.md), "apply").

## role-bindings

A JSON list of objects, each a binding of a named role:

| Field | Value | |
|---|---|---|
| `principal` | who holds the role: `User:alice` or `Group:payments` | required |
| `role` | the name of the role | required |
| `resourcePatterns` | a list of the resource patterns the role is held on; each an object with the three fields below | may be left out, or empty, for a role that is bound to the cluster |
| `resourcePatterns[].resourceType` | `Topic`, `Group`, `TransactionalId`, `Cluster`, `Subject` or `Connector` | required |
| `resourcePatterns[].name` | the name, or the prefix | required |
| `resourcePatterns[].patternType` | `LITERAL` for a name, `PREFIXED` for a prefix | required |
| `scope` | where the binding applies: an object with `clusters` | optional |
| `scope.clusters` | an object whose entry `kafka-cluster` is the ID of the Kafka cluster the binding is for | optional |

Other fields of a binding and of a pattern are ignored. Other fields of
`scope`, and other entries of `scope.clusters`, are not: see below.

How a binding of the file becomes bindings of the cluster:

| In the file | Written as |
|---|---|
| a `role` that is one of the seven of [roles.md](roles.md) | that role; the name is taken as it is, in its exact spelling |
| a `principal` `User:<name>` or `Group:<name>` | the member of the binding |
| a pattern with `resourceType` `Topic` | a binding on that name or prefix. Such a binding is for the topics, the groups and the transactional IDs of that name ([roles.md](roles.md)): this is what a binding is, and it is the one case where what is written reaches further than the pattern of the file |
| a pattern `Group` or `TransactionalId` with the name and pattern type of a `Topic` pattern of the same principal and role | nothing more: the binding on that name covers it, and the report says `covered` |
| any other pattern `Group` or `TransactionalId` | a binding narrowed to groups, or to transactional IDs |
| a pattern `Subject` or `Connector` | a binding for subjects, or for connectors |
| a pattern `Cluster` with the literal name `kafka-cluster`, or no pattern at all, for `ClusterAdmin` or `SystemAdmin` | a binding to the cluster |

What is skipped and reported:

| In the file | Why |
|---|---|
| a `role` that is not one of the seven | there is no such role, and none is assumed to be like it |
| a `principal` that is neither `User:<name>` nor `Group:<name>` | |
| a `resourceType` other than the six | nothing here authorizes it |
| a `patternType` other than `LITERAL` and `PREFIXED` | |
| a role that is bound to a name pattern, without a pattern; a role that is bound to the cluster, with a pattern for something else; a pattern `Cluster` for a role that is bound to a name pattern | the record format does not allow it ([roles.md](roles.md)) |
| a name that is empty, or that starts with `Group:`, `Subject:` or `Connector:` | those are reserved in a record |
| a `scope` with a field other than `clusters`, or `scope.clusters` with an entry other than `kafka-cluster` | a binding here is for the Kafka cluster and for everything that reads its bindings; it cannot be narrowed further, and writing it anyway would give more than the file gives. Take that part of the scope out of the file to import the binding so |

A skipped pattern does not stop the other patterns of its binding from
being written.

### Which cluster

A file may hold the bindings of several clusters. `--cluster-id` selects:
the bindings whose `scope.clusters.kafka-cluster` is that ID are imported,
and so are the bindings that name no Kafka cluster; the bindings for another
cluster are left out and counted. Without `--cluster-id` a file that names
one Kafka cluster, or none, is imported whole, and a file that names several
is wrong usage: the command does not pick one.

### Example

[examples/role-bindings.json](examples/role-bindings.json) has six
bindings: one for a topic prefix and the groups of the same prefix, one for
a group of users, one with a resource type nothing here authorizes, one of a
role bound to the cluster, one of a role that does not exist here, and one
for another cluster.

```console
$ rbac import --file docs/examples/role-bindings.json --format role-bindings --cluster-id cluster-1
+ binding DeveloperRead to User:alice on prefix billing.
+ binding DeveloperWrite to Group:payments on name orders
+ binding DeveloperWrite to Group:payments on transactional ID billing-1
+ binding ResourceOwner to User:bob on prefix billing.
+ binding ResourceOwner to User:bob on subjects with prefix billing.
+ binding ClusterAdmin to User:carol on the cluster
covered [0].resourcePatterns[1] (line 7) DeveloperRead to User:alice on Group billing. PREFIXED: within binding DeveloperRead to User:alice on prefix billing.
skipped [2].resourcePatterns[2] (line 26) ResourceOwner to User:bob on Dashboard billing. PREFIXED: nothing here authorizes the resource type Dashboard
skipped [4] (line 35) Auditor to User:dave: there is no role Auditor here: the roles are DeveloperRead, DeveloperWrite, DeveloperManage, ResourceOwner, ResourceViewer, ClusterAdmin, SystemAdmin
left out [5] (line 43) DeveloperRead to User:erin: it is for the cluster cluster-2
Wrote 6 bindings; 1 covered by another; skipped 2; left out 1 for another cluster.
A binding on a name or a prefix is for the topics, the groups and the transactional IDs of that name: a pattern for topics is written as one.
```

`[2].resourcePatterns[2] (line 26)` is the place in the file: the third
pattern of the third binding, and the line it starts at.

Afterwards:

```console
$ rbac list
ROLE            MEMBER          ON
ResourceOwner   User:bob        subjects with prefix billing.
DeveloperRead   User:alice      prefix billing.
ResourceOwner   User:bob        prefix billing.
DeveloperWrite  Group:payments  name orders
ClusterAdmin    User:carol      the cluster
DeveloperWrite  Group:payments  transactional ID billing-1
```

## role-acls

A JSON list of objects, each a role: a name, who has it, and the ACLs it
stands for.

| Field | Value | |
|---|---|---|
| `name` | the name of the role | required |
| `members` | a list of the principals that have the role: `User:alice` | required; may be empty |
| `acls` | a list of the ACLs of the role; each an object with the six fields below | required; may be empty |
| `acls[].resourceType` | the resource type, as Kafka names it: `TOPIC`, `GROUP`, `CLUSTER`, `TRANSACTIONAL_ID`, `DELEGATION_TOKEN`, `USER` | required |
| `acls[].resourceName` | the name, or the prefix; `*` for every name | required |
| `acls[].patternType` | `LITERAL` or `PREFIXED` | required |
| `acls[].operation` | the operation, as Kafka names it: `READ`, `WRITE`, `DESCRIBE`, `DESCRIBE_CONFIGS`, `ALL`, ... | required |
| `acls[].permission` | `ALLOW` or `DENY` | required |
| `acls[].host` | the host the ACL is for; `*` for every host | required |

Other fields are ignored. The names of resource types, operations, pattern
types and permissions are read as Kafka's own classes read them, in upper or
lower case.

How a role of the file is written:

| In the file | Written as |
|---|---|
| a role | a group of the role's `name`: `Group:orders-readers`. A group is not a record of its own; it is there once it has a member ([groups.md](groups.md)) |
| a member | a membership of that group |
| an `ALLOW` ACL | an ordinary ACL with the same resource, pattern type, operation and host, whose principal is the group. The authorizer honours it for the members of the group |

What is skipped and reported:

| In the file | Why |
|---|---|
| a role with a `DENY` ACL, the whole role | a group only adds access: a `DENY` ACL whose principal is a group cannot be created, and leaving the `DENY` out would give the members more than the role gave. Deny the users by name, with ordinary ACLs, and import the role without it |
| a role with an ACL whose `permission` is neither `ALLOW` nor `DENY`, the whole role | it may be a denial |
| a role whose `name` is empty, the whole role | |
| a member that is not `<type>:<name>`, or that is a group | a group is not a member of a group; the other members and the ACLs are written |
| an `ALLOW` ACL with a resource type, an operation or a pattern type that an ACL cannot have, or with an empty name or host | the other ACLs and the members are written: the group then has less than the role had, never more |

Whoever imports this format creates ordinary ACLs and memberships, which
takes a principal that the ACLs allow to alter the cluster, or a
`SystemAdmin`.

### Example

[examples/role-acls.json](examples/role-acls.json) has two roles: one that
lets its two members read a topic in the consumer groups of a prefix, and
one with a `DENY` ACL.

```console
$ rbac import --file docs/examples/role-acls.json --format role-acls
+ membership of User:alice in Group:orders-readers
+ membership of User:bob in Group:orders-readers
+ ACL allow Group:orders-readers Read on topic orders
+ ACL allow Group:orders-readers Read on groups with prefix orders-
skipped [1] (line 10) role billing-writers: it has a DENY ACL ([1].acls[1] (line 15)), and a group cannot be denied: a DENY ACL names the users; nothing of the role is written
Wrote 4 records; skipped 1.
```

Afterwards `User:alice` and `User:bob` are allowed what the ACLs of the
role allowed, and the command says why:

```console
$ rbac group list
GROUP           MEMBER
orders-readers  User:alice
orders-readers  User:bob
$ rbac who-can --operation Read --topic orders
Read on topic orders:

User:alice  allowed
    allowed by ACL allow Group:orders-readers Read on topic orders, as a member of Group:orders-readers
User:bob  allowed
    allowed by ACL allow Group:orders-readers Read on topic orders, as a member of Group:orders-readers

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

## A file that cannot be read

```console
$ rbac import --file bindings.json --format role-bindings
error: bindings.json: line 14, column 3: [2]: "role" is required
$ echo $?
2
```

The place is the line and the column where the object starts that lacks the
field, and the way to it from the top of the file. Nothing was written, and
the cluster was not asked anything.

## What import uses

The JSON parser is the one the Kafka distribution has in `libs/`, in every
release from 4.1 on. The jar brings none.
