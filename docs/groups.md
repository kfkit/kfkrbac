# Groups

A group is a name and the principals that membership records put in it. A
role can be bound to a group instead of to every user in it, and an ordinary
`ALLOW` ACL whose principal is a group works for its members. Membership
lives in the metadata of the cluster of Apache Kafka®, next to the ACLs and
the bindings: there is no directory to connect and nothing else to run.

> Status: every command of this page is run by a test of the integration
> suite (`DocsIT`) on a broker of the newest supported release, and the
> output shown is what the commands printed there.

## The cluster of the examples

One node at `localhost:9092` that runs `RbacAuthorizer`
([Installation](install.md)), with three users: `User:admin`, who is in
`super.users`, `User:alice` and `User:bob`, whom no ACL names. Each has a
file of client properties for the tools: `admin.properties`,
`alice.properties` and `bob.properties`. Kafka's `bin` is on the `PATH`.
The cluster has two topics, `orders` and `billing.orders`.

The examples write `rbac` for the command of the jar
([The command](command.md)):

<!-- run: session -->
```bash
rbac() { kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand "$@"; }
```

## Putting a user in a group

<!-- run -->
```bash
rbac group add --bootstrap-server localhost:9092 --command-config admin.properties \
  --group payments --member User:alice
```

<!-- output -->
```text
Added User:alice to Group:payments
```

What it writes is a membership: an ACL record whose principal is
`Member:<member>`, on the literal topic name `Group:<name>`, with the
operation `All`. The ACL tool writes the same record, and adding who is in
the group already changes nothing:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal Member:User:alice --operation All --topic Group:payments
```

So after both there is one record, which the ACL tool lists as the ACL it
is and the command as a membership:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --list --topic Group:payments
```

<!-- output -->
```text
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=Group:payments, patternType=LITERAL)`:
    (principal=Member:User:alice, host=*, operation=ALL, permissionType=ALLOW)
```

<!-- run -->
```bash
rbac group list --bootstrap-server localhost:9092 --command-config admin.properties
```

<!-- output -->
```text
GROUP     MEMBER
payments  User:alice
```

A group is not created or deleted: it exists for as long as a record puts a
member in it. No topic can be called `Group:payments`, a colon is not legal
in a topic name, so the record never stands for a topic.

## A role for a group

Being in a group gives nothing by itself. `User:alice` does not see the
topic `billing.orders`:

<!-- run: exit 1 -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config alice.properties \
  --topic billing.orders
```

<!-- output -->
```text
Error occurred: Could not match any topic-partitions with the specified filters
```

The member of a binding can be a group:

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role DeveloperRead --member Group:payments --prefix billing.
```

<!-- output -->
```text
Bound DeveloperRead to Group:payments on prefix billing.
```

With the ACL tool it is the binding of [Bindings](bindings.md) with the
group as its member:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal DeveloperRead:Group:payments --operation All \
  --topic billing. --resource-pattern-type prefixed
```

Every member of `Group:payments` now holds `DeveloperRead` on the prefix
`billing.`, in both of the broker's questions, and stops holding it when its
membership is deleted. Any role can be bound to a group, the cluster roles
included.

<!-- run -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config alice.properties \
  --topic billing.orders
```

<!-- output -->
```text
billing.orders:0:0
```

`User:bob` is not in the group and still does not see the topic:

<!-- run: exit 1 -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config bob.properties \
  --topic billing.orders
```

<!-- output -->
```text
Error occurred: Could not match any topic-partitions with the specified filters
```

## An ACL for a group

An ordinary `ALLOW` ACL can name a group:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal Group:payments --operation Read --topic orders
```

The broker's own authorizer stores such an ACL and never matches it, because
no client is the principal `Group:payments`. kfkrbac honours it for the
members of the group, by the rules the broker matches any ACL with: literal
and prefixed names and `*`, the operation and what it implies, and the host
when the ACL names one. This is how a set of permissions that no role names
is given to many users at once.

<!-- run -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config alice.properties \
  --topic orders
```

<!-- output -->
```text
orders:0:0
```

Like a role, an ACL of a group only adds access, and only where the ACLs
that name the user did not already decide.

## Who is allowed through a group

`who-can` names the members of a group for what the group is given, with the
group, and `what-can` lists what a principal gets through each of its groups
([The command](command.md)):

<!-- run -->
```bash
rbac who-can --bootstrap-server localhost:9092 --command-config admin.properties \
  --operation Read --topic orders
```

<!-- output -->
```text
Read on topic orders:

User:alice  allowed
    allowed by ACL allow Group:payments Read on topic orders, as a member of Group:payments

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

<!-- run -->
```bash
rbac what-can --bootstrap-server localhost:9092 --command-config admin.properties \
  --principal User:alice
```

<!-- output -->
```text
User:alice is in Group:payments.
User:alice may:
  topics with prefix billing.             Read, Describe  by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
  groups with prefix billing.             Read, Describe  by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
  transactional IDs with prefix billing.  Describe        by binding DeveloperRead to Group:payments on prefix billing., as a member of Group:payments
  topic orders                            Read, Describe  by ACL allow Group:payments Read on topic orders, as a member of Group:payments

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

## Denying

A `DENY` ACL that names the user beats what the user gets through a group,
from a role or from an ACL, as it beats every role:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --deny-principal User:alice --operation Read --topic orders
```

<!-- run -->
```bash
rbac who-can --bootstrap-server localhost:9092 --command-config admin.properties \
  --operation Read --topic orders
```

<!-- output -->
```text
Read on topic orders:

User:alice  denied
    denied by ACL deny User:alice Read on topic orders
    without that, allowed by ACL allow Group:payments Read on topic orders, as a member of Group:payments

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --remove --force --deny-principal User:alice --operation Read --topic orders
```

A `DENY` ACL that names a group cannot be created: the request is refused,
for everyone, and says why. A group only adds access, and such an ACL could
not take away what an ACL allows a member by name. Deny the users by name.

<!-- run: exit 1 -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --deny-principal Group:payments --operation Read --topic orders
```

<!-- output: excerpt -->
```text
Error while executing ACL command: org.apache.kafka.common.errors.InvalidRequestException:
```

A `DENY` ACL of a group that is in the metadata anyway, because it was
created before kfkrbac was installed, is logged at `WARN` and counted in the
gauge `malformed-bindings`
([Bindings](bindings.md#a-record-that-is-not-a-binding) shows how to read
both). It takes away from the members what roles and groups give them, and
nothing that an ACL allows them by name. Delete it. This paragraph is
checked by the unit tests of the authorizer, not on a broker.

## Who decides who is in a group

Whoever creates and deletes any ACL: a principal that the ACLs allow `ALTER`
on the cluster, as they do a super user, or a `SystemAdmin`. Nobody else
creates or deletes a membership record.

A `ResourceOwner` binds roles to a group inside what it owns, as it does to
a user, and does not decide who is in the group
([Delegation](delegation.md)). `User:admin` makes `User:bob` the owner of
`billing.`:

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role ResourceOwner --member User:bob --prefix billing.
```

<!-- output -->
```text
Bound ResourceOwner to User:bob on prefix billing.
```

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config bob.properties \
  --role DeveloperWrite --member Group:payments --prefix billing.
```

<!-- output -->
```text
Bound DeveloperWrite to Group:payments on prefix billing.
```

<!-- run: exit 1 -->
```bash
rbac group add --bootstrap-server localhost:9092 --command-config bob.properties \
  --group payments --member User:bob
```

<!-- output -->
```text
error: ClusterAuthorizationException: User:bob may not create Member:User:bob on the TOPIC name 'Group:payments': a record of group membership is managed only by a principal that the ACLs allow ALTER on the cluster, as they do a super user, or a SystemAdmin
```

An ACL of a group is honoured for requests, not for managing access: an ACL
that allows `Group:payments` to `ALTER` the cluster does not let its members
create ACLs, bindings or memberships, or replace a user's SCRAM credentials.
Bind `SystemAdmin` to the group for that. A member that is refused is told
so after the reason, in the words `an ACL of its group Group:payments
allows it ALTER on the cluster, but an ACL of a group does not make its
members managers of access`. This is checked by the unit tests of the
authorizer, not on a broker.

## Taking a user out of a group

<!-- run -->
```bash
rbac group remove --bootstrap-server localhost:9092 --command-config admin.properties \
  --group payments --member User:alice
```

<!-- output -->
```text
Removed User:alice from Group:payments
```

With the ACL tool it is the membership record, removed like any ACL by
naming it in full: `--remove --force --allow-principal Member:User:alice
--operation All --topic Group:payments`. The bindings and the ACLs of the
group stay, and give nobody anything while nobody is in it:

<!-- run: exit 1 -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config alice.properties \
  --topic billing.orders
```

<!-- output -->
```text
Error occurred: Could not match any topic-partitions with the specified filters
```

<!-- run -->
```bash
rbac who-can --bootstrap-server localhost:9092 --command-config admin.properties \
  --operation Read --topic orders
```

<!-- output -->
```text
Read on topic orders:

Nobody, by the roles and the ACLs.

Not counted: what the brokers' own settings allow (super.users, allow.everyone.if.no.acl.found).
```

## What a group is not

- **Not nested.** A group is not a member of a group, and the cluster
  refuses the record:

<!-- run: exit 1 -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal Member:Group:billing --operation All --topic Group:payments
```

<!-- output: excerpt -->
```text
Error while executing ACL command: org.apache.kafka.common.errors.InvalidRequestException:
```

- **Not a wildcard.** `Member:User:*` puts the one principal called `*` in
  the group, not everyone, as `DeveloperRead:User:*` binds nothing to
  everyone.
- **Not a super user.** A group in `super.users` makes none of its members a
  super user.
- **Not case-insensitive.** `Group:payments` and `Group:Payments` are two
  groups, and `group:payments` is not one.

The last three are checked by the unit tests of the authorizer, not on a
broker.

## A record that is not a membership

A record whose principal type is `Member` and which is not a membership as
above puts nobody in a group: an operation other than `All`, a `Deny`, a
host other than `*`, a member that is not `Type:name` or that is a group, a
resource type other than `Topic`, a prefixed pattern, a name that is not
`Group:<name>`. Creating one is refused, for a super user too, with the
reason, as above. One that is in the metadata anyway, written while the cluster ran
the plain authorizer, is logged at `WARN` by each authorizer when it first
reads it, with the ID of the record, and counted in the gauge
`malformed-bindings` for as long as it exists: the log and the gauge are
read as [Bindings](bindings.md#a-record-that-is-not-a-binding) shows for a
record that is not a binding.

`Group:` in front of a name is reserved for these records: a role binding
whose name starts with it is refused and grants nothing.

## While the records change

A membership, a binding or an ACL that is created or deleted takes effect
on a node when the record reaches it, like any ACL. When a node loads a
whole snapshot of the metadata, a principal is, for that moment, only in the
groups it is in both before and after: no request is allowed that neither
the records before nor the records after allow.

## What it costs

A request that an ACL allows is answered by the ACLs alone, as before. A
principal in no group costs one lookup more on the way to its roles. A
principal in groups has the bindings of each group looked up, and, when no
role grants the request, the ACLs of those of its groups that have any
asked. The numbers are in `./gradlew jmh`, `GroupsBenchmark`.
