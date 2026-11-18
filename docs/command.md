# The command

`RbacCommand` manages role bindings and groups in an operator's own terms,
answers two questions, who can do this and what may this principal do,
keeps bindings in a file, and brings bindings over from a file of another
shape. It is a class in the same jar as the authorizer, run with the script
the distribution of Apache Kafka® ships:

```text
kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand <verb> --bootstrap-server localhost:9092 [options]
```

The jar must be on the classpath of the tool, as it is on the broker's:
in `libs/`, or in `CLASSPATH`.

It talks to the cluster through the admin client only, with three calls:
list ACLs, create ACLs, delete ACLs. So it needs no access the ACL tool
would not need, the cluster refuses what the caller may not do, and
everything it does can be done with the ACL tool as well: a binding is an
ACL record ([Bindings](bindings.md)), and so is a membership of a group
([Groups](groups.md)). The command is a convenience, never the only way.

> Status: every command of this page is run by a test of the integration
> suite (`DocsIT`) on a broker of the newest supported release, and the
> output shown is what the commands printed there.

| Verb | Does |
|---|---|
| [`bind`](#bind) | binds a role to a member |
| [`unbind`](#unbind) | removes one binding |
| [`list`](#list) | lists the bindings |
| [`group add`](#group-add) | puts a member in a group |
| [`group list`](#group-list) | lists the groups and their members |
| [`group remove`](#group-remove) | takes a member out of a group |
| [`who-can`](#who-can) | who may do an operation on a resource, and why |
| [`what-can`](#what-can) | what a principal may do, and why |
| [`apply`](#apply) | makes the bindings and the groups of the cluster those of a file |
| [`import`](#import) | writes the bindings of a file that has another shape |

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
| 2 | wrong usage: what is wrong and the usage of the verb are printed; or a file that `apply` or `import` cannot read as what it must be: the place and the field are printed |

## The cluster of the examples

One node at `localhost:9092` that runs `RbacAuthorizer`
([Installation](install.md)), with three users: `User:admin`, who is in
`super.users`, `User:alice` and `User:bob`, whom no ACL names. Each has a
file of client properties for the tools: `admin.properties`,
`alice.properties` and `bob.properties`. Kafka's `bin` is on the `PATH`, and
the commands are run in a copy of this repository, for the files of
`docs/examples/`. The cluster has no ACL and no binding.

The examples write `rbac` for the command with the connection of
`User:admin`, and `rbac_as bob` for the same as `User:bob`:

<!-- run: session -->
```bash
rbac_as() {
  user=$1; shift
  kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand "$@" \
    --bootstrap-server localhost:9092 --command-config "$user.properties"
}
rbac() { rbac_as admin "$@"; }
```

## bind

Binds a role to a member. Binding what is bound already changes nothing.

<!-- run -->
```bash
rbac bind --role DeveloperRead --member User:alice --prefix billing.
```

<!-- output -->
```text
Bound DeveloperRead to User:alice on prefix billing.
```

| Option | Meaning |
|---|---|
| `--role <role>` | one of the seven [roles](roles.md); required |
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

<!-- run -->
```bash
rbac bind --role ClusterAdmin --member User:bob
rbac bind --role DeveloperWrite --member User:bob --name billing-1 --transactional-id-only
rbac bind --role DeveloperManage --member User:alice --prefix billing- --connector
rbac bind --role DeveloperRead --member User:alice --prefix orders- --subject
```

<!-- output -->
```text
Bound ClusterAdmin to User:bob on the cluster
Bound DeveloperWrite to User:bob on transactional ID billing-1
Bound DeveloperManage to User:alice on connectors with prefix billing-
Bound DeveloperRead to User:alice on subjects with prefix orders-
```

A binding for connectors is read by the extension for
[Kafka Connect](connect.md). A binding for subjects can be written, listed
and removed, and nothing reads it: there is no extension for a schema
registry.

What `bind` writes is the record of the binding, the one the ACL tool would
write with `--allow-principal DeveloperRead:User:alice --operation All
--topic billing. --resource-pattern-type prefixed`
([Bindings](bindings.md)).

A cluster that refuses says why, and the status is 1. `User:bob` is made
the owner of the prefix `billing.` and binds outside it:

<!-- run -->
```bash
rbac bind --role ResourceOwner --member User:bob --prefix billing.
```

<!-- output -->
```text
Bound ResourceOwner to User:bob on prefix billing.
```

<!-- run: exit 1 -->
```bash
rbac_as bob bind --role DeveloperWrite --member User:alice --name orders
```

<!-- output -->
```text
error: ClusterAuthorizationException: User:bob may not create DeveloperWrite:User:alice on the TOPIC name 'orders': none of the ResourceOwner bindings of User:bob covers that pattern; it owns the TOPIC prefix 'billing.'
```

A principal that holds no role and that no ACL lets alter the cluster is
refused earlier, by the broker's own check and in the broker's own words:
see [Who may run it](#who-may-run-it).

## unbind

Takes the options of `bind` and removes exactly the binding that `bind`
with the same options creates, and nothing else.

<!-- run -->
```bash
rbac unbind --role ResourceOwner --member User:bob --prefix billing.
```

<!-- output -->
```text
Unbound ResourceOwner to User:bob on prefix billing.
```

It fails, with the status 1, when there is no such binding:

<!-- run: exit 1 -->
```bash
rbac unbind --role ResourceOwner --member User:bob --prefix billing.
```

<!-- output -->
```text
error: no such binding: ResourceOwner to User:bob on prefix billing.
```

## list

Lists the role bindings, one on a line, as bindings: the ordinary ACLs are
not in it. It changes nothing.

<!-- run -->
```bash
rbac list
```

<!-- output -->
```text
ROLE             MEMBER      ON
DeveloperManage  User:alice  connectors with prefix billing-
DeveloperRead    User:alice  subjects with prefix orders-
DeveloperRead    User:alice  prefix billing.
ClusterAdmin     User:bob    the cluster
DeveloperWrite   User:bob    transactional ID billing-1
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
Without a binding to show it prints `No bindings.`

A record whose principal type is a role and which is not a binding grants
nothing. The authorizer refuses to create one; where one is in the metadata
all the same, written before the authorizer was installed, it is not listed,
and a warning on standard error names it:
[Bindings](bindings.md#a-record-that-is-not-a-binding) has the example.

### JSON

`--output json` prints one array with one object for each binding, and `[]`
when there is none. The shape is stable: fields may be added, these five
stay.

<!-- run -->
```bash
rbac list --member User:alice --output json
```

<!-- output -->
```text
[
  {"role":"DeveloperManage","member":"User:alice","resourceType":"TOPIC","name":"Connector:billing-","patternType":"PREFIXED"},
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

## group add

Puts a member in a group. What a group is and what it gives its members is
in [Groups](groups.md).

<!-- run -->
```bash
rbac group add --group payments --member User:alice
rbac group add --group payments --member User:bob
rbac bind --role DeveloperRead --member Group:payments --prefix billing.
```

<!-- output -->
```text
Added User:alice to Group:payments
Added User:bob to Group:payments
Bound DeveloperRead to Group:payments on prefix billing.
```

| Option | Meaning |
|---|---|
| `--group <name>` | the name of the group, without `Group:` in front: `payments`; required |
| `--member <type:name>` | who joins or leaves it: `User:alice`; required |

A group is not created or deleted: it is there for as long as it has a
member. Adding who is in the group already changes nothing. A group is not a
member of a group, and `--member Group:billing` is wrong usage.

What `group add` writes is the membership record, the one the ACL tool
would write with `--allow-principal Member:User:alice --operation All
--topic Group:payments`.

## group list

Lists the members of the groups, one membership on a line. It changes
nothing.

<!-- run -->
```bash
rbac group list
```

<!-- output -->
```text
GROUP     MEMBER
payments  User:alice
payments  User:bob
```

| Option | Meaning |
|---|---|
| `--group <name>` | only the members of that group |
| `--output text\|json` | `text` by default |

Without a membership it prints `No groups.`, and for a group nobody is in:

<!-- run -->
```bash
rbac group list --group billing
```

<!-- output -->
```text
Group:billing has no members.
```

A record whose principal type is `Member` and which is not a membership
puts nobody in a group
([Groups](groups.md#a-record-that-is-not-a-membership)); it is not listed,
and a warning on standard error names it.

`--output json` prints one array with one object for each membership, and
`[]` when there is none. The shape is stable: fields may be added, these two
stay. The order is fixed: by group, then by member.

<!-- run -->
```bash
rbac group list --output json
```

<!-- output -->
```text
[
  {"group":"payments","member":"User:alice"},
  {"group":"payments","member":"User:bob"}
]
```

| Field | Value |
|---|---|
| `group` | the name of the group, as `--group` takes it: `payments` |
| `member` | who is in it: `User:alice` |

## group remove

Takes the options of `group add`, takes the member out of the group and
does nothing else: the bindings and the ACLs of the group stay.

<!-- run -->
```bash
rbac group remove --group payments --member User:bob
```

<!-- output -->
```text
Removed User:bob from Group:payments
```

It fails, with the status 1, when the member is not in the group:

<!-- run: exit 1 -->
```bash
rbac group remove --group payments --member User:bob
```

<!-- output -->
```text
error: no such membership: User:bob in Group:payments
```

## who-can

Names every principal a role or an ACL lets do an operation on a resource,
with the binding or the ACL that lets it, and every principal a deny ACL
stops, with that ACL. It changes nothing.

The examples need two ordinary ACLs, written with the ACL tool: one allows
`User:bob` to read the topic `billing.orders`, and one denies him the topic
`billing.payments`.

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal User:bob --operation Read --topic billing.orders
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --deny-principal User:bob --operation Read --topic billing.payments
```

<!-- run -->
```bash
rbac who-can --operation Read --topic billing.orders
```

<!-- output -->
```text
Read on topic billing.orders:

User:alice  allowed
    allowed by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
    allowed by binding DeveloperRead to User:alice on prefix billing.
User:bob  allowed
    allowed by ACL allow User:bob Read on topic billing.orders

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

A member of a group is named for what the group is given: a role bound to
the group, or an `ALLOW` ACL whose principal is the group. The line says
which group. The group itself is not named, since no client is a group, and
a group nobody is in gives nobody anything.

A principal that a deny ACL stops is named with that ACL and, after
`without that`, with what would have let it:

<!-- run -->
```bash
rbac group add --group payments --member User:bob
rbac who-can --operation Read --topic billing.payments
```

<!-- output -->
```text
Added User:bob to Group:payments
Read on topic billing.payments:

User:alice  allowed
    allowed by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
    allowed by binding DeveloperRead to User:alice on prefix billing.
User:bob  denied
    denied by ACL deny User:bob Read on topic billing.payments
    without that, allowed by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments

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
answer for them: who may create and delete bindings and ACLs, and who may
list them, are in [Delegation](delegation.md), and who may replace a user's
SCRAM credentials in [Roles](roles.md). So this names `User:bob`, a
`ClusterAdmin`, who may not bind a role, and would not name a
`ResourceOwner`, who may, inside what it owns:

<!-- run -->
```bash
rbac who-can --operation Alter --cluster
```

<!-- output -->
```text
Alter on the cluster:

User:bob  allowed
    allowed by binding ClusterAdmin to User:bob on the cluster

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

What it cannot see is the configuration of the brokers:

- principals in `super.users` may do everything and are not named;
- with `allow.everyone.if.no.acl.found=true` the brokers allow what no ACL
  names, and the command does not;
- an ACL may name a host. The question is asked as from an address no ACL
  names, so such ACLs do not apply, and a line says so; `--host` asks as
  from another address.

## what-can

Lists what a principal may do, with the binding or the ACL behind each
line, and what a deny ACL takes away. It changes nothing. `User:bob` holds
`ClusterAdmin` and a binding on a transactional ID, is in `Group:payments`,
and has the two ACLs of the examples above:

<!-- run -->
```bash
rbac what-can --principal User:bob
```

<!-- output -->
```text
User:bob is in Group:payments.
User:bob may:
  topics with prefix billing.             Read, Describe                                                                                     by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
  groups with prefix billing.             Read, Describe                                                                                     by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
  transactional IDs with prefix billing.  Describe                                                                                           by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
  every topic                             Create, Delete, Alter, Describe, DescribeConfigs, AlterConfigs                                     by binding ClusterAdmin to User:bob on the cluster
  the cluster                             Create, Alter, Describe, DescribeConfigs, AlterConfigs                                             by binding ClusterAdmin to User:bob on the cluster
  every subject                           all                                                                                                by binding ClusterAdmin to User:bob on the cluster
  every connector                         Create, Delete, ReadConfig, ReadStatus, ReadActiveTopics, Pause, Resume, Scale, ResetActiveTopics  by binding ClusterAdmin to User:bob on the cluster
  transactional ID billing-1              Write, Describe                                                                                    by binding DeveloperWrite to User:bob on transactional ID billing-1
  topic billing.orders                    Read, Describe                                                                                     by ACL allow User:bob Read on topic billing.orders
but not, whatever allows it:
  topic billing.payments  Read  by ACL deny User:bob Read on topic billing.payments

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

| Option | Meaning |
|---|---|
| `--principal <type:name>` | the principal: `User:alice`; `User:*` is what everyone may do, `Group:payments` what that group gives its members; required |
| `--host <address>` | the address the request comes from, as for `who-can` |

There is one line for each record and kind of resource: what the record
gives on the names its pattern covers. `all` is every operation of the
kind. An operation that Kafka allows along with another is listed with it:
an ACL that allows `Read` shows `Read, Describe`. What the principal gets
as a member of a group is listed with the group, and the first line names
the groups it is in.

A deny ACL beats every role and every allow, so what is under `but not` is
denied on the names of that line whatever the lines above give there.

As for `who-can`, a line is an operation as an ordinary request asks it:
what the principal may bind, and whether it may list the ACLs, is not in
it.

The lines for subjects and connectors say what the role of the binding
grants there ([Roles](roles.md)). A broker never decides those requests:
the extension for Kafka Connect answers for connectors
([Kafka Connect](connect.md)), and nothing in this jar answers for
subjects.

Asked about a group, it lists the members and what the group gives them:

<!-- run -->
```bash
rbac what-can --principal Group:payments
```

<!-- output -->
```text
Group:payments has the members User:alice, User:bob.
A member of Group:payments may:
  topics with prefix billing.             Read, Describe  by binding DeveloperRead to Group:payments on prefix billing.
  groups with prefix billing.             Read, Describe  by binding DeveloperRead to Group:payments on prefix billing.
  transactional IDs with prefix billing.  Describe        by binding DeveloperRead to Group:payments on prefix billing.

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

A `DENY` ACL whose principal is a group cannot be created
([Groups](groups.md#denying)). Where one is in the metadata all the same, a
warning on standard error names it, `who-can` names it as the reason for the
members it stops, and `what-can` lists it apart, under `and not by a role or
a group, only by an ACL that names` the principal: it takes away what roles
and groups give, and nothing an ACL allows the principal by name. This
paragraph is checked by the unit tests of the command, not on a broker.

A principal no record names may do nothing. That is so for a super user as
well, whom the brokers allow everything by their own setting:

<!-- run -->
```bash
rbac what-can --principal User:admin
```

<!-- output -->
```text
User:admin may do nothing, by the roles and the ACLs.

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

### Everyone

`User:*` is everyone, as in an ACL. `who-can` names it, and does not name
again a principal that has only what everyone has; `what-can` lists what
everyone may do with what the principal may:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal 'User:*' --operation Read --topic orders
```

<!-- run -->
```bash
rbac who-can --operation Read --topic orders
```

<!-- output -->
```text
Read on topic orders:

User:*  allowed
    allowed by ACL allow User:* Read on topic orders

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

<!-- run -->
```bash
rbac what-can --principal User:admin
```

<!-- output -->
```text
User:admin may:
  topic orders  Read, Describe  by ACL allow User:* Read on topic orders

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

## apply

Keeps the bindings and the groups in a file: `apply` compares the file with
the cluster, prints the difference and makes the cluster match. It creates
what is in the file and not in the cluster, and deletes the bindings and the
memberships that are in the cluster and not in the file.

<!-- run -->
```bash
rbac apply --file docs/examples/apply.json --dry-run
```

<!-- output -->
```text
+ binding DeveloperWrite to User:bob on name orders
+ binding ClusterAdmin to User:carol on the cluster
- binding DeveloperManage to User:alice on connectors with prefix billing-
- membership of User:bob in Group:payments
- binding DeveloperRead to User:alice on subjects with prefix orders-
- binding DeveloperRead to User:alice on prefix billing.
- binding ClusterAdmin to User:bob on the cluster
- binding DeveloperWrite to User:bob on transactional ID billing-1
Would create 2 and delete 6. Nothing was changed.
```

<!-- run -->
```bash
rbac apply --file docs/examples/apply.json
```

<!-- output -->
```text
+ binding DeveloperWrite to User:bob on name orders
+ binding ClusterAdmin to User:carol on the cluster
- binding DeveloperManage to User:alice on connectors with prefix billing-
- membership of User:bob in Group:payments
- binding DeveloperRead to User:alice on subjects with prefix orders-
- binding DeveloperRead to User:alice on prefix billing.
- binding ClusterAdmin to User:bob on the cluster
- binding DeveloperWrite to User:bob on transactional ID billing-1
Created 2, deleted 6.
```

<!-- run -->
```bash
rbac apply --file docs/examples/apply.json
```

<!-- output -->
```text
Nothing to change: the cluster has what the file has.
```

| Option | Meaning |
|---|---|
| `--file <file>` | the JSON file; required |
| `--dry-run` | print the difference and change nothing |
| `--no-delete` | only create: delete nothing |

### The file

The file is what the command itself prints. It is one JSON object with two
fields, each optional:

| Field | Value |
|---|---|
| `bindings` | the array that `list --output json` prints: one object for each binding, with `role`, `member`, `resourceType`, `name` and `patternType` ([JSON](#json)) |
| `groups` | the array that `group list --output json` prints: one object for each membership, with `group` and `member` ([group list](#group-list)) |

[examples/apply.json](examples/apply.json), the file of the example above,
is:

```json
{
  "bindings": [
    {"role":"DeveloperRead","member":"Group:payments","resourceType":"TOPIC","name":"billing.","patternType":"PREFIXED"},
    {"role":"DeveloperWrite","member":"User:bob","resourceType":"TOPIC","name":"orders","patternType":"LITERAL"},
    {"role":"ClusterAdmin","member":"User:carol","resourceType":"CLUSTER","name":"kafka-cluster","patternType":"LITERAL"}
  ],
  "groups": [
    {"group":"payments","member":"User:alice"}
  ]
}
```

A file of a cluster as it is now is the two listings put together, and
applying it changes nothing:

<!-- run -->
```bash
{
  echo '{"bindings":'
  rbac list --output json
  echo ',"groups":'
  rbac group list --output json
  echo '}'
} > access.json
rbac apply --file access.json
```

<!-- output -->
```text
Nothing to change: the cluster has what the file has.
```

The array of `list --output json` alone is a file too: it is the bindings,
as if it were the object with `bindings` only.

- **A field the file does not have is not touched.** A file with `bindings`
  only creates and deletes bindings and leaves every membership as it is; a
  file with `groups` only does the reverse. An empty array is different: it
  says there are to be none, and all of them are deleted.
- **Ordinary ACLs are never touched**, whatever their principal: not the
  ACLs that name a user, not the `ALLOW` ACLs of a group. Neither is a
  record that looks like a binding or a membership and is not one; `apply`
  names it in a warning, as `list` does.
- **Other fields of an object are ignored**, so a file made from a later
  `list --output json`, which may have gained fields, still applies. Another
  field of the file itself, next to `bindings` and `groups`, is refused.
- **`--no-delete` only adds.** What is in the cluster and not in the file is
  printed as `not in the file, and left`, and stays:

<!-- run -->
```bash
rbac bind --role DeveloperRead --member User:bob --name orders
rbac apply --file docs/examples/apply.json --no-delete
```

<!-- output -->
```text
Bound DeveloperRead to User:bob on name orders
  not in the file, and left: binding DeveloperRead to User:bob on name orders
Nothing to change; 1 not in the file left as it is, as --no-delete says.
```

### What it does to the cluster

`apply` lists the ACLs once, creates everything there is to create in one
request and then deletes everything there is to delete in one request, each
record by a filter that matches it and nothing else. The cluster decides
whether the caller may, as for `bind` and `unbind`: the authorizer applies
a request as a whole or not at all, so a refused creation creates nothing
and nothing is deleted after it, and the status is 1 with the cluster's
reason. Running `apply` again brings the cluster to the file from wherever
it is.

A file that is not valid JSON, lacks a field or has a binding that the
record format does not allow is refused before the cluster is asked
anything, with the place and the field, and the status is 2:

<!-- run: exit 2 -->
```bash
cat > broken.json <<'JSON'
{
  "bindings": [
    {"role":"DeveloperRead","member":"User:alice","resourceType":"TOPIC","name":"billing.","patternType":"PREFIXED"},
    {"role":"DeveloperWrite","member":"User:bob","resourceType":"TOPIC","patternType":"LITERAL"}
  ]
}
JSON
rbac apply --file broken.json
```

<!-- output -->
```text
error: broken.json: line 4, column 5: bindings[1]: "name" is required
```

## import

Writes the bindings of a file that has another shape than the command's
own: a list of bindings of named roles, or a list of roles that are sets of
ACLs. The two shapes, field by field, what each becomes and what is skipped
are in [Bringing bindings over](import.md).

<!-- run -->
```bash
rbac import --file docs/examples/role-acls.json --format role-acls --dry-run
```

<!-- output -->
```text
+ membership of User:alice in Group:orders-readers
+ membership of User:bob in Group:orders-readers
+ ACL allow Group:orders-readers Read on topic orders
+ ACL allow Group:orders-readers Read on groups with prefix orders-
skipped [1] (line 10) role billing-writers: it has a DENY ACL ([1].acls[1] (line 15)), and a group cannot be denied: a DENY ACL names the users; nothing of the role is written
Would write 4 records; skipped 1. Nothing was changed.
```

| Option | Meaning |
|---|---|
| `--file <file>` | the JSON file; required |
| `--format role-bindings\|role-acls` | the shape of the file; required |
| `--cluster-id <id>` | for `role-bindings`: only the bindings for that Kafka cluster, and those that name none |
| `--dry-run` | print what would be written and what is skipped, and change nothing |

What cannot be written as the file has it is skipped and reported, never
guessed. `import` only adds.

## Wrong usage

What is wrong is printed first, then the usage of the verb, on standard
error, and the status is 2. Nothing is sent to the cluster.

<!-- run: exit 2 -->
```bash
rbac bind --role DeveloperRead --member User:alice
```

<!-- output: excerpt -->
```text
DeveloperRead is bound to a name pattern: give --prefix or --name
Usage: kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand bind --bootstrap-server <host:port> [--command-config <file>]
```

## Who may run it

The cluster decides, as it does for the ACL tool.

| Verb | What it asks of the cluster | Who may |
|---|---|---|
| `list`, `group list`, `who-can`, `what-can` | to list the ACLs | whoever may describe the cluster, and a `ResourceOwner` of any pattern |
| `bind`, `unbind` | to create or delete one ACL record | a principal the ACLs allow to alter the cluster and a `SystemAdmin`: any binding; a `ResourceOwner`: bindings inside what it owns |
| `group add`, `group remove` | to create or delete one membership record | a principal the ACLs allow to alter the cluster, and a `SystemAdmin` |
| `apply` | to list the ACLs, and then what `bind`, `unbind`, `group add` and `group remove` ask | by the same rules |
| `import` | to create bindings, or memberships and ordinary ACLs; it lists nothing | as for `bind` and, for `role-acls`, as for `group add` |

The rules are in [Delegation](delegation.md). A refusal comes back in one
of two ways:

- A principal that holds `ResourceOwner`, or that something lets alter the
  cluster, reaches the rules for bindings, and a refusal there is the
  authorizer's, with what was asked, why not and what the caller does own,
  as [above](#bind).
- A principal with neither is stopped before that, by the check the broker
  makes of every request that creates or deletes ACLs, and the message is
  the broker's own. `User:alice` holds two roles here and neither lets her
  bind:

<!-- run: exit 1 -->
```bash
rbac_as alice bind --role DeveloperRead --member User:alice --name orders
```

<!-- output: excerpt -->
```text
needs ALTER permission.
```

  The broker's log has a line for that check
  ([Troubleshooting](troubleshooting.md#creating-a-binding-needs-alter-permission)).
