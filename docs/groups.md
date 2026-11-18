# Groups

A group is a name and the principals that membership records put in it. A
role can be bound to a group instead of to every user in it, and an ordinary
`ALLOW` ACL whose principal is a group works for its members. Membership
lives in the cluster's metadata, next to the ACLs and the bindings: there is
no directory to connect and nothing else to run.

> Status: the authorizer reads membership records and decides with them in
> unit tests against the real `StandardAuthorizer`; groups have not run on a
> broker yet. The extension for Kafka Connect reads the same records and
> applies a role bound to a group to its members, in real workers too
> ([connect.md](connect.md)). The command that makes the examples shorter
> does not exist.

## Putting a user in a group

A membership is an ACL record whose principal is `Member:<member>`, on the
literal topic name `Group:<name>`, with the operation `All`:

```bash
kafka-acls.sh --bootstrap-server localhost:9092 --add \
  --allow-principal 'Member:User:alice' --operation All \
  --topic 'Group:payments'
```

`User:alice` is now in `Group:payments`. Delete the record and she is not:

```bash
kafka-acls.sh --bootstrap-server localhost:9092 --remove --force \
  --allow-principal 'Member:User:alice' --operation All \
  --topic 'Group:payments'
```

A group is not created or deleted: it exists for as long as a record puts a
member in it. No topic can be called `Group:payments`, a colon is not legal
in a topic name, so the record never stands for a topic.

## A role for a group

The member of a binding can be a group:

```bash
kafka-acls.sh --bootstrap-server localhost:9092 --add \
  --allow-principal 'DeveloperRead:Group:payments' --operation All \
  --topic billing. --resource-pattern-type prefixed
```

Every member of `Group:payments` now holds `DeveloperRead` on the prefix
`billing.`, in both of the broker's questions, and stops holding it when its
membership is deleted. Any role can be bound to a group, the cluster roles
included.

## An ACL for a group

An ordinary `ALLOW` ACL can name a group:

```bash
kafka-acls.sh --bootstrap-server localhost:9092 --add \
  --allow-principal 'Group:payments' --operation Read --topic reports
```

The broker's own authorizer stores such an ACL and never matches it, because
no client is the principal `Group:payments`. kfkrbac honours it for the
members of the group, by the rules the broker matches any ACL with: literal
and prefixed names and `*`, the operation and what it implies, and the host
when the ACL names one. This is how a set of permissions that no role names
is given to many users at once.

Like a role, an ACL of a group only adds access, and only where the ACLs
that name the user did not already decide.

## Denying

- A `DENY` ACL that names the user beats what the user gets through a group,
  from a role or from an ACL, as it beats every role.
- A `DENY` ACL that names a group cannot be created: the request is refused,
  for everyone, and says why. A group only adds access, and such an ACL
  could not take away what an ACL allows a member by name. Deny the users by
  name.
- A `DENY` ACL of a group that is in the metadata anyway, because it was
  created before kfkrbac was installed, is logged at `WARN` and counted in
  the gauge `malformed-bindings`. It takes away from the members what roles
  and groups give them, and nothing that an ACL allows them by name. Delete
  it.

## Who decides who is in a group

Whoever creates and deletes any ACL: a principal that the ACLs allow `ALTER`
on the cluster, as they do a super user, or a `SystemAdmin`. Nobody else
creates or deletes a membership record.

A `ResourceOwner` binds roles to a group inside what it owns, as it does to
a user, and does not decide who is in the group.

An ACL of a group is honoured for requests, not for managing access: an ACL
that allows `Group:operators` to `ALTER` the cluster does not let its
members create ACLs, bindings or memberships, or replace a user's SCRAM
credentials. Bind `SystemAdmin` to the group for that.

## What a group is not

- **Not nested.** A group is not a member of a group: `Member:Group:billing`
  is refused.
- **Not a wildcard.** `Member:User:*` puts the one principal called `*` in
  the group, not everyone, as `DeveloperRead:User:*` binds nothing to
  everyone.
- **Not a super user.** A group in `super.users` makes none of its members a
  super user.
- **Not case-insensitive.** `Group:payments` and `Group:Payments` are two
  groups, and `group:payments` is not one.

## A record that is not a membership

A record whose principal type is `Member` and which is not a membership as
above puts nobody in a group: an operation other than `All`, a `Deny`, a
host other than `*`, a member that is not `Type:name` or that is a group, a
resource type other than `Topic`, a prefixed pattern, a name that is not
`Group:<name>`. Creating one is refused, whoever asks, with the reason. One
that is in the metadata anyway is logged once, at `WARN`, with the ID of the
record, and counted in the gauge `malformed-bindings` for as long as it
exists.

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
