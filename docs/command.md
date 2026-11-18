# The command

`RbacCommand` manages role bindings and groups in an operator's own terms
and answers two questions: who can do this, and what may this principal do. It is a
class in the same jar as the authorizer, run with the script the Kafka
distribution ships:

```bash
kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand <verb> --bootstrap-server localhost:9092 [options]
```

The jar must be on the classpath of the tool, as it is on the broker's:
in `libs/`, or in `CLASSPATH`.

It talks to the cluster through the admin client only, with three calls:
list ACLs, create ACLs, delete ACLs. So it needs no access the ACL tool
would not need, the cluster refuses what the caller may not do, and
everything it does can be done with the ACL tool as well: a binding is an
ACL record ([roles.md](roles.md)), and so is a membership of a group
([groups.md](groups.md)). The command is a convenience, never the only way.

> Status: `bind`, `unbind`, `list`, `group add`, `group remove`,
> `group list`, `who-can` and `what-can` exist, and run against real brokers
> of every supported release in the integration suite. `apply` and `import`
> do not exist yet.

| Verb | Does |
|---|---|
| [`bind`](#bind) | binds a role to a member |
| [`unbind`](#unbind) | removes one binding |
| [`list`](#list) | lists the bindings |
| [`group add`](#group-add-group-remove) | puts a member in a group |
| [`group remove`](#group-add-group-remove) | takes a member out of a group |
| [`group list`](#group-list) | lists the groups and their members |
| [`who-can`](#who-can) | who may do an operation on a resource, and why |
| [`what-can`](#what-can) | what a principal may do, and why |

## Options of every verb

| Option | Meaning |
|---|---|
| `--bootstrap-server <host:port,...>` | the cluster to connect to; required |
| `--command-config <file>` | properties for the admin client: the security settings, as for the ACL tool |
| `--help` | print the usage of the verb and do nothing; without a verb, of every verb |

An option takes its value as the next word or after `=`: `--role DeveloperRead`
or `--role=DeveloperRead`. The verb comes first; `group add`, `group remove`
and `group list` are verbs of two words.

## Exit status

| Status | Meaning |
|---|---|
| 0 | done |
| 1 | the cluster refused, or the call failed; the reason the cluster returned is printed |
| 2 | wrong usage; what is wrong and the usage of the verb are printed |

The examples below leave out `--bootstrap-server` and `--command-config`,
and write `rbac` for `kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand`.

## bind

Binds a role to a member. Binding what is bound already changes nothing.

```console
$ rbac bind --role DeveloperRead --member User:alice --prefix billing.
Bound DeveloperRead to User:alice on prefix billing.
```

| Option | Meaning |
|---|---|
| `--role <role>` | one of the seven roles of [roles.md](roles.md); required |
| `--member <type:name>` | who gets the role: `User:alice`, or a group, `Group:payments`; required |
| `--prefix <prefix>` | every name that starts with the prefix |
| `--name <name>` | that one name; `*` is every name |
| `--group-only` | the pattern is for groups only |
| `--transactional-id-only` | the pattern is for transactional IDs only |
| `--subject` | the pattern is for subjects of a schema registry, and no broker resource |
| `--connector` | the pattern is for connectors, and no broker resource |

A role that is bound to a name pattern takes `--prefix` or `--name`, and the
pattern covers topics, groups and transactional IDs unless one of the last
four options narrows it. A role that is bound to the cluster takes neither:

```console
$ rbac bind --role ClusterAdmin --member User:carol
Bound ClusterAdmin to User:carol on the cluster
$ rbac bind --role DeveloperWrite --member User:bob --name billing-1 --transactional-id-only
Bound DeveloperWrite to User:bob on transactional ID billing-1
$ rbac bind --role DeveloperRead --member User:alice --prefix orders- --subject
Bound DeveloperRead to User:alice on subjects with prefix orders-
```

What it writes is the record of the binding, the one the ACL tool would
write with `--allow-principal 'DeveloperRead:User:alice' --operation All
--topic billing. --resource-pattern-type prefixed`.

A cluster that refuses says why, and the status is 1. Here `User:bob` holds
`ResourceOwner` on the prefix `billing.` and binds outside it:

```console
$ rbac bind --command-config bob.properties --role DeveloperWrite --member User:alice --prefix payments.
error: ClusterAuthorizationException: User:bob may not create DeveloperWrite:User:alice on the TOPIC prefix 'payments.': none of the ResourceOwner bindings of User:bob covers that pattern; it owns the TOPIC prefix 'billing.'
$ echo $?
1
```

## unbind

Takes the options of `bind` and removes exactly the binding that `bind`
with the same options creates, and nothing else. It fails when there is no
such binding.

```console
$ rbac unbind --role DeveloperRead --member User:alice --prefix billing.
Unbound DeveloperRead to User:alice on prefix billing.
$ rbac unbind --role DeveloperRead --member User:alice --prefix payments.
error: no such binding: DeveloperRead to User:alice on prefix payments.
$ echo $?
1
```

## list

Lists the role bindings, one on a line, as bindings: the ordinary ACLs are
not in it. It changes nothing.

```console
$ rbac list
ROLE            MEMBER      ON
DeveloperRead   User:alice  subjects with prefix orders-
DeveloperRead   User:alice  prefix billing.
DeveloperRead   User:carol  prefix billing.
ClusterAdmin    User:carol  the cluster
DeveloperWrite  User:bob    transactional ID billing-1
```

| Option | Meaning |
|---|---|
| `--role <role>` | only bindings of that role |
| `--member <type:name>` | only bindings of that member |
| `--prefix <prefix>` | only bindings on exactly that prefix |
| `--name <name>` | only bindings on exactly that name |
| `--output text\|json` | `text` by default |

`--prefix` and `--name` select the bindings that were bound with that very
pattern; which bindings reach a given topic is what `who-can` answers.

A record whose principal type is a role and which is not a binding grants
nothing ([roles.md](roles.md), "A record that is not a binding"). The
authorizer refuses to create one; where one is in the metadata all the
same, written before the authorizer was installed, it is not listed, and a
warning on standard error names it:

```console
$ rbac list --role DeveloperRead
warning: not a binding, grants nothing: DeveloperRead:User:dave on topics with prefix billing.: the operation is READ, a binding has ALL
ROLE           MEMBER      ON
DeveloperRead  User:alice  subjects with prefix orders-
DeveloperRead  User:alice  prefix billing.
DeveloperRead  User:carol  prefix billing.
```

### JSON

`--output json` prints one array with one object for each binding, and `[]`
when there is none. The shape is stable: fields may be added, these five
stay.

```console
$ rbac list --member User:alice --output json
[
  {"role":"DeveloperRead","member":"User:alice","resourceType":"TOPIC","name":"Subject:orders-","patternType":"PREFIXED"},
  {"role":"DeveloperRead","member":"User:alice","resourceType":"TOPIC","name":"billing.","patternType":"PREFIXED"}
]
```

| Field | Value |
|---|---|
| `role` | the role's name: `DeveloperRead` |
| `member` | who holds it: `User:alice` |
| `resourceType` | the resource type of the binding's record: `TOPIC` for a pattern that covers topics, groups and transactional IDs, and for subjects and connectors; `GROUP` or `TRANSACTIONAL_ID` for a pattern narrowed to that type; `CLUSTER` for a role bound to the cluster |
| `name` | the name of the record: the name or the prefix; with `Subject:` or `Connector:` in front for subjects and connectors; `kafka-cluster` for the cluster |
| `patternType` | `LITERAL` for a name, `PREFIXED` for a prefix |

The five fields are the record as it is in the cluster's metadata, so what
`list` prints is what any tool that manages ACLs sees. The order is fixed:
by resource type, name, pattern type, role and member.

## group add, group remove

`group add` puts a member in a group, and `group remove` takes it out and
does nothing else: the bindings and the ACLs of the group stay. What a group
is and what it gives its members is in [groups.md](groups.md).

```console
$ rbac group add --group payments --member User:alice
Added User:alice to Group:payments
$ rbac bind --role DeveloperRead --member Group:payments --prefix billing.
Bound DeveloperRead to Group:payments on prefix billing.
$ rbac group remove --group payments --member User:alice
Removed User:alice from Group:payments
$ rbac group remove --group payments --member User:alice
error: no such membership: User:alice in Group:payments
$ echo $?
1
```

| Option | Meaning |
|---|---|
| `--group <name>` | the name of the group, without `Group:` in front: `payments`; required |
| `--member <type:name>` | who joins or leaves it: `User:alice`; required |

A group is not created or deleted: it is there for as long as it has a
member. Adding who is in the group already changes nothing. A group is not a
member of a group, and `--member Group:billing` is wrong usage.

What `group add` writes is the membership record, the one the ACL tool
would write with `--allow-principal 'Member:User:alice' --operation All
--topic 'Group:payments'`.

## group list

Lists the members of the groups, one membership on a line. It changes
nothing.

```console
$ rbac group list
GROUP     MEMBER
audit     User:dave
payments  User:alice
payments  User:carol
$ rbac group list --group audit
GROUP  MEMBER
audit  User:dave
```

| Option | Meaning |
|---|---|
| `--group <name>` | only the members of that group |
| `--output text\|json` | `text` by default |

Without a membership it prints `No groups.`, and for a group nobody is in,
`Group:payments has no members.` A record whose principal type is `Member`
and which is not a membership puts nobody in a group
([groups.md](groups.md), "A record that is not a membership"); it is not
listed, and a warning on standard error names it.

`--output json` prints one array with one object for each membership, and
`[]` when there is none. The shape is stable: fields may be added, these two
stay. The order is fixed: by group, then by member.

```console
$ rbac group list --output json
[
  {"group":"audit","member":"User:dave"},
  {"group":"payments","member":"User:alice"},
  {"group":"payments","member":"User:carol"}
]
```

| Field | Value |
|---|---|
| `group` | the name of the group, as `--group` takes it: `payments` |
| `member` | who is in it: `User:alice` |

## who-can

Names every principal a role or an ACL lets do an operation on a resource,
with the binding or the ACL that lets it, and every principal a deny ACL
stops, with that ACL. It changes nothing.

```console
$ rbac who-can --operation Read --topic billing.orders
Read on topic billing.orders:

User:alice  allowed
    allowed by binding DeveloperRead to User:alice on prefix billing.
User:bob  allowed
    allowed by ACL allow User:bob Read on topic billing.orders
User:carol  denied
    denied by ACL deny User:carol Read on topic billing.orders
    without that, allowed by binding DeveloperRead to User:carol on prefix billing.

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

| Option | Meaning |
|---|---|
| `--operation <operation>` | Read, Write, Create, Delete, Alter, Describe, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite, CreateTokens, DescribeTokens, TwoPhaseCommit; required |
| `--topic <name>` | a topic, by its name |
| `--group <name>` | a group, by its name |
| `--transactional-id <name>` | a transactional ID |
| `--cluster` | the cluster |
| `--host <address>` | the address the request comes from; see below |

Exactly one of `--topic`, `--group`, `--transactional-id` and `--cluster` is
given. A principal that nothing lets do the operation is not named; when
nobody is, the answer is `Nobody, by the roles and the ACLs.`

`User:*` is everyone, as in an ACL. A principal that has only what everyone
has is not named again:

```console
$ rbac who-can --operation Read --topic public.news
Read on topic public.news:

User:*  allowed
    allowed by ACL allow User:* Read on topics with prefix public.
User:bob  allowed
    allowed by binding DeveloperRead to User:bob on prefix public.
    allowed by ACL allow User:* Read on topics with prefix public.
User:carol  denied
    denied by ACL deny User:carol Read on topic public.news
    without that, allowed by ACL allow User:* Read on topics with prefix public.

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

### Groups

A member of a group is named for what the group is given: a role bound to
the group, or an `ALLOW` ACL whose principal is the group. The line says
which group. The group itself is not named, since no client is a group, and
a group nobody is in gives nobody anything.

```console
$ rbac who-can --operation Read --topic billing.orders
Read on topic billing.orders:

User:alice  allowed
    allowed by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
User:bob  allowed
    allowed by binding DeveloperRead to User:bob on prefix billing.
User:carol  denied
    denied by ACL deny User:carol Read on topic billing.orders
    without that, allowed by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
User:dave  allowed
    allowed by ACL allow Group:audit Read on topic billing.orders, as a member of Group:audit

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

### How the answer is computed

The command lists the ACL records of the cluster and gives them to the
authorizer itself, inside the tool, the way a broker gives them to its own:
whether a principal is allowed is that authorizer's answer, by the same
code and the same rules of matching. The reasons are found by asking about
one record at a time. Which groups a principal is in, and whether an ACL of
a group applies to it, is asked of the classes a broker decides that with,
fed the membership records of the cluster.

The question is the operation on the resource, as an ordinary request asks
it. A few requests are decided by rules of their own, and `who-can` does not
answer for them: who may create and delete bindings and ACLs, who may list
them, and who may replace a user's SCRAM credentials are in
[roles.md](roles.md) and the README. So `who-can --operation Alter --cluster`
names a `ClusterAdmin`, who may not bind a role, and not a `ResourceOwner`,
who may, inside what it owns.

What it cannot see is the configuration of the brokers:

- principals in `super.users` may do everything and are not named;
- with `allow.everyone.if.no.acl.found=true` the brokers allow what no ACL
  names, and the command does not;
- an ACL may name a host. The question is asked as from an address no ACL
  names, so such ACLs do not apply, and a line says so; `--host` asks as
  from another address.

## what-can

Lists what a principal may do, with the binding or the ACL behind each
line, and what a deny ACL takes away. It changes nothing.

```console
$ rbac what-can --principal User:carol
User:carol may:
  topics with prefix billing.             Read, Describe                                                                                     by binding DeveloperRead to User:carol on prefix billing.
  groups with prefix billing.             Read, Describe                                                                                     by binding DeveloperRead to User:carol on prefix billing.
  transactional IDs with prefix billing.  Describe                                                                                           by binding DeveloperRead to User:carol on prefix billing.
  every topic                             Create, Delete, Alter, Describe, DescribeConfigs, AlterConfigs                                     by binding ClusterAdmin to User:carol on the cluster
  the cluster                             Create, Alter, Describe, DescribeConfigs, AlterConfigs                                             by binding ClusterAdmin to User:carol on the cluster
  every subject                           all                                                                                                by binding ClusterAdmin to User:carol on the cluster
  every connector                         Create, Delete, ReadConfig, ReadStatus, ReadActiveTopics, Pause, Resume, Scale, ResetActiveTopics  by binding ClusterAdmin to User:carol on the cluster
but not, whatever allows it:
  topic billing.orders  Read  by ACL deny User:carol Read on topic billing.orders

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

| Option | Meaning |
|---|---|
| `--principal <type:name>` | the principal: `User:alice`; `User:*` is what everyone may do, `Group:payments` what that group gives its members; required |
| `--host <address>` | the address the request comes from, as for `who-can` |

There is one line for each record and kind of resource: what the record
gives on the names its pattern covers. `all` is every operation of the
kind. An operation that Kafka allows along with another is listed with it:
an ACL that allows `Write` shows `Write, Describe`.

A deny ACL beats every role and every allow, so what is under `but not` is
denied on the names of that line whatever the lines above give there.

As for `who-can`, a line is an operation as an ordinary request asks it:
what the principal may bind, and whether it may list the ACLs, is not in
it.

The lines for subjects and connectors say what the role of the binding
grants there ([roles.md](roles.md)). A broker never decides those requests:
the extension for Kafka Connect answers for connectors
([connect.md](connect.md)), and nothing in this jar answers for subjects.

### Groups

What the principal gets as a member of a group is listed with the group, and
the first line names the groups it is in:

```console
$ rbac what-can --principal User:alice
User:alice is in Group:audit, Group:payments.
User:alice may:
  topics with prefix billing.             Read, Describe   by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
  groups with prefix billing.             Read, Describe   by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
  transactional IDs with prefix billing.  Describe         by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
  topic orders                            Write, Describe  by ACL allow User:alice Write on topic orders
  topic reports                           Read, Describe   by ACL allow Group:audit Read on topic reports, as a member of Group:audit
but not, whatever allows it:
  topic billing.secret  Read  by ACL deny User:alice Read on topic billing.secret

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

Asked about a group, it lists the members and what the group gives them:

```console
$ rbac what-can --principal Group:audit
Group:audit has the members User:alice, User:dave.
A member of Group:audit may:
  topic reports  Read, Describe  by ACL allow Group:audit Read on topic reports

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

A `DENY` ACL whose principal is a group cannot be created
([groups.md](groups.md), "Denying"). Where one is in the metadata all the
same, a warning on standard error names it, `who-can` names it as the reason
for the members it stops, and `what-can` lists it apart, under `and not by a
role or a group, only by an ACL that names User:alice`: it takes away what
roles and groups give, and nothing an ACL allows the principal by name.

A principal no record names may do nothing:

```console
$ rbac what-can --principal User:erin
User:erin may do nothing, by the roles and the ACLs.

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

## Wrong usage

What is wrong is printed first, then the usage of the verb, on standard
error, and the status is 2. Nothing is sent to the cluster.

```console
$ rbac bind --role DeveloperRead --member User:alice
DeveloperRead is bound to a name pattern: give --prefix or --name

Usage: kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand bind --bootstrap-server <host:port> [--command-config <file>]
           --role <role> --member <type:name> [--prefix <prefix> | --name <name>]
           [--group-only | --transactional-id-only | --subject | --connector]
...
$ echo $?
2
```

## Who may run it

The cluster decides, as it does for the ACL tool. `list`, `who-can`,
`what-can` and `group list` list the ACLs: whoever may describe the cluster,
and a `ResourceOwner`. `bind` and `unbind` create and delete one ACL record:
a principal the ACLs allow to alter the cluster and a `SystemAdmin` any
binding, a `ResourceOwner` bindings inside what it owns, everyone else
none. `group add` and `group remove` create and delete one membership
record: a principal the ACLs allow to alter the cluster and a `SystemAdmin`,
and nobody else. A refusal comes back with its reason.
