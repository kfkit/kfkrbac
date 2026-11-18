# Delegation

The owner of a prefix hands out roles inside it, and nowhere else. This
page says who may create and delete which role bindings on a cluster of
Apache Kafka® that runs `RbacAuthorizer`, and walks through one example: an
admin makes `User:bob` the owner of `billing.`, and `User:bob` takes it
from there. What a binding is, and the commands that manage bindings, are
in [Bindings](bindings.md).

> Status: every command of this page is run by a test of the integration
> suite (`DocsIT`) on a broker of the newest supported release, and the
> output shown is what the commands printed there.

## Who may bind what

Bindings are ACL records, so they are created and deleted through Kafka's
ACL API, and the cluster decides each request by who sent it. The decision
is made on the controller, when the record is created or deleted, which is
one reason the jar is on the controllers too.

| Who | May create and delete |
|---|---|
| a principal that the ACLs allow to alter the cluster: one in `super.users`, or one that an ACL allows `Alter` on the cluster and none denies | any binding, any group membership and any ACL, as before there were roles |
| a principal that holds `SystemAdmin` | any binding, any group membership and any ACL |
| a principal that holds `ResourceOwner` | bindings of the five roles that are bound to a pattern, `ResourceOwner` among them, to users and to groups, on patterns inside what it owns |
| everyone else, a `ClusterAdmin` included | nothing |

The first row is about the ACLs that name the principal itself, the roles
and the groups left out. A principal that may alter the cluster only
because a role says so is not in it, unless the role is `SystemAdmin`. A
role bound to a group is held by its members here as everywhere; what
groups are, and who decides who is in one, is in [Groups](groups.md).

What a `ResourceOwner` owns is the pattern of its own binding, and "inside"
means:

- The owner of a prefix binds on that prefix, on any longer prefix and on
  any name that starts with it. The owner of `billing.` binds on
  `billing.`, on the prefix `billing.eu.` and on the name `billing.orders`;
  it does not bind on `*`, which is every name.
- The owner of one name binds on that name only. That holds for `*` too:
  the owner of every name binds on `*`, not on a part of it.
- An owner whose binding covers topics, groups and transactional IDs also
  binds roles narrowed to groups or to transactional IDs. An owner whose
  own binding is narrowed binds only the same kind.

A `ResourceOwner` creates no ordinary ACL, no group membership and no
binding of `ClusterAdmin` or `SystemAdmin`, and it deletes only what it
could have created.

A request is decided as a whole. When one record of a request is refused,
none is created; when a filter of a delete matches one record the caller
may not delete, nothing is deleted. The caller gets the reason back.

That reason is this authorizer's only for a caller that reaches these
rules: one that holds `ResourceOwner`, or that something, an ACL, a role or
a group, lets alter the cluster. A caller with neither is stopped before
them, by the check the broker makes of every request that creates or
deletes ACLs, and gets the broker's own message, which ends in `needs ALTER
permission.` The first command of the example below is such a caller, and
[Troubleshooting](troubleshooting.md#creating-a-binding-needs-alter-permission)
has the line the broker logs for it.

## The cluster of the examples

One node at `localhost:9092` that runs `RbacAuthorizer`
([Installation](install.md)), with three users: `User:admin`, who is in
`super.users`, `User:alice` and `User:bob`, whom no ACL names. Each has a
file of client properties for the tools: `admin.properties`,
`alice.properties` and `bob.properties`. Kafka's `bin` is on the `PATH`.
The cluster has no topic and no ACL.

The examples write `rbac` for the command of the jar:

<!-- run: session -->
```bash
rbac() { kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand "$@"; }
```

## An owner and what it hands out

`User:bob` holds nothing yet, and may not bind. The cluster's answer here
is Kafka's own, from before the request reaches the rules above: one long
line about the request that ends as shown.

<!-- run: exit 1 -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config bob.properties \
  --role DeveloperRead --member User:alice --prefix billing.
```

<!-- output: excerpt -->
```text
needs ALTER permission.
```

`User:admin` makes him the owner of everything that starts with `billing.`:

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role ResourceOwner --member User:bob --prefix billing.
```

<!-- output -->
```text
Bound ResourceOwner to User:bob on prefix billing.
```

`ResourceOwner` grants every operation on what the pattern covers, so
`User:bob` creates his own topic:

<!-- run -->
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --command-config bob.properties \
  --create --topic billing.orders
```

<!-- output: excerpt -->
```text
Created topic billing.orders.
```

And he hands out roles inside the prefix, on all of it or on a part:

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config bob.properties \
  --role DeveloperRead --member User:alice --prefix billing.
```

<!-- output -->
```text
Bound DeveloperRead to User:alice on prefix billing.
```

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config bob.properties \
  --role DeveloperWrite --member User:alice --name billing.orders
```

<!-- output -->
```text
Bound DeveloperWrite to User:alice on name billing.orders
```

`User:alice` has what he gave her:

<!-- run -->
```bash
kafka-get-offsets.sh --bootstrap-server localhost:9092 --command-config alice.properties \
  --topic billing.orders
```

<!-- output -->
```text
billing.orders:0:0
```

Outside the prefix he is refused, and the cluster says what he asked for,
why not, and what he does own:

<!-- run: exit 1 -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config bob.properties \
  --role DeveloperWrite --member User:alice --name orders
```

<!-- output -->
```text
error: ClusterAuthorizationException: User:bob may not create DeveloperWrite:User:alice on the TOPIC name 'orders': none of the ResourceOwner bindings of User:bob covers that pattern; it owns the TOPIC prefix 'billing.'
```

The reason is the cluster's, not the command's: the ACL tool gets the same
one, ahead of its stack trace.

<!-- run: exit 1 -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config bob.properties \
  --add --allow-principal DeveloperWrite:User:alice --operation All --topic orders
```

<!-- output: excerpt -->
```text
Error while executing ACL command: org.apache.kafka.common.errors.ClusterAuthorizationException: User:bob may not create DeveloperWrite:User:alice on the TOPIC name 'orders': none of the ResourceOwner bindings of User:bob covers that pattern; it owns the TOPIC prefix 'billing.'
```

A role that is bound to the cluster is not his to give:

<!-- run: exit 1 -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config bob.properties \
  --role ClusterAdmin --member User:alice
```

<!-- output -->
```text
error: ClusterAuthorizationException: User:bob may not create ClusterAdmin:User:alice on the CLUSTER name 'kafka-cluster': ClusterAdmin is bound to the cluster, and a binding to the cluster is managed only by a principal that the ACLs allow ALTER on the cluster, as they do a super user, or a SystemAdmin
```

Nor is an ordinary ACL, even inside the prefix:

<!-- run: exit 1 -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config bob.properties \
  --add --allow-principal User:alice --operation Read --topic billing.orders
```

<!-- output: excerpt -->
```text
Error while executing ACL command: org.apache.kafka.common.errors.ClusterAuthorizationException: User:bob may not create the ACL User:alice ALLOW READ from host * on the TOPIC name 'billing.orders': it is an ordinary ACL, and a ResourceOwner manages role bindings only: an ordinary ACL takes a principal that the ACLs allow ALTER on the cluster, as they do a super user, or a SystemAdmin
```

## What a delegate can see

To manage its bindings an owner has to list them, and Kafka's listing of
ACLs cannot be narrowed to the caller. So a principal that holds
`ResourceOwner`, on any pattern, sees every binding and every ACL of the
cluster, the ones outside what it owns included. `User:admin` binds a role
outside the prefix:

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role DeveloperRead --member User:alice --name orders
```

<!-- output -->
```text
Bound DeveloperRead to User:alice on name orders
```

and `User:bob` sees it next to his own:

<!-- run -->
```bash
rbac list --bootstrap-server localhost:9092 --command-config bob.properties
```

<!-- output -->
```text
ROLE            MEMBER      ON
DeveloperRead   User:alice  prefix billing.
ResourceOwner   User:bob    prefix billing.
DeveloperWrite  User:alice  name billing.orders
DeveloperRead   User:alice  name orders
```

A principal that owns nothing lists nothing. `User:alice` holds two roles
and neither is `ResourceOwner`:

<!-- run: exit 1 -->
```bash
rbac list --bootstrap-server localhost:9092 --command-config alice.properties
```

<!-- output: excerpt -->
```text
needs DESCRIBE permission.
```

## What a delegate can delete

An owner deletes any binding inside what it owns: the ones it created, the
ones somebody else created there, and its own ownership.

<!-- run -->
```bash
rbac unbind --bootstrap-server localhost:9092 --command-config bob.properties \
  --role DeveloperWrite --member User:alice --name billing.orders
```

<!-- output -->
```text
Unbound DeveloperWrite to User:alice on name billing.orders
```

A delete that reaches outside is refused as a whole, with the first record
that is in the way:

<!-- run: exit 1 -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config bob.properties \
  --remove --force --topic orders
```

<!-- output: excerpt -->
```text
Error while executing ACL command: org.apache.kafka.common.errors.ClusterAuthorizationException: User:bob may not delete DeveloperRead:User:alice on the TOPIC name 'orders', which the filter matches: none of the ResourceOwner bindings of User:bob covers that pattern; it owns the TOPIC prefix 'billing.'
```

## What `ClusterAdmin` may not do

`ClusterAdmin` runs the cluster and does not manage access
([Roles](roles.md)). It is allowed to alter the cluster, and that is the
right Kafka asks for before it creates an ACL, but here the right has to
come from the ACLs or from `SystemAdmin`:

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role ClusterAdmin --member User:alice
```

<!-- output -->
```text
Bound ClusterAdmin to User:alice on the cluster
```

<!-- run: exit 1 -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config alice.properties \
  --role DeveloperRead --member User:alice --prefix billing.
```

<!-- output -->
```text
error: ClusterAuthorizationException: User:alice may not create DeveloperRead:User:alice on the TOPIC prefix 'billing.': that takes a principal that the ACLs allow ALTER on the cluster, as they do a super user, or a SystemAdmin, or a ResourceOwner of a pattern that covers the record, and User:alice is none of them
```

The same right guards the request that replaces a user's SCRAM
credentials, the password of a super user included, and `ClusterAdmin` does
not pass there either, so that a role is not a way to become a super user:

<!-- run: exit 1 -->
```bash
kafka-configs.sh --bootstrap-server localhost:9092 --command-config alice.properties \
  --alter --add-config 'SCRAM-SHA-256=[password=alice-secret]' --entity-type users --entity-name alice
```

<!-- output: excerpt -->
```text
needs ALTER permission.
```

`SystemAdmin` is the one role that may do both. `User:admin` binds it, and
`User:alice` then binds a cluster role herself:

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config admin.properties \
  --role SystemAdmin --member User:alice
```

<!-- output -->
```text
Bound SystemAdmin to User:alice on the cluster
```

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config alice.properties \
  --role ClusterAdmin --member User:bob
```

<!-- output -->
```text
Bound ClusterAdmin to User:bob on the cluster
```

## ACLs decide as before

Whoever the ACLs let alter the cluster managed every ACL before there were
roles, and still does: the roles add ways to bind and take none away. An
ordinary ACL makes `User:bob` such a principal:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal User:bob --operation Alter --cluster
```

The ordinary ACL he was refused above now goes through:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config bob.properties \
  --add --allow-principal User:alice --operation Read --topic billing.orders
```

and so does the binding outside his prefix:

<!-- run -->
```bash
rbac bind --bootstrap-server localhost:9092 --command-config bob.properties \
  --role DeveloperWrite --member User:alice --name orders
```

<!-- output -->
```text
Bound DeveloperWrite to User:alice on name orders
```

So an ACL that allows `Alter` on the cluster is as much as `SystemAdmin`
for managing access: give it as carefully.
