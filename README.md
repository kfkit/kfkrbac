# kfkrbac

Role-based access control for Apache Kafka®: an authorizer that grants access
through roles and role bindings, on top of the ACLs the broker already has.
One jar, one line of configuration, free and open source.

> Status: pre-alpha, not released. The authorizer can tell a deny ACL from
> no ACL and lets a role grant only the second, but no roles can be defined
> or bound yet, so it behaves exactly like `StandardAuthorizer`. What is
> described below is the design, not something you can install.

## Why

An ACL binds one principal to one resource. With tens of teams and hundreds
of topics the ACLs multiply, nobody can say what a team is allowed to do, and
bringing one more service on is a list of commands to run. A role names a set
of permissions once; a role binding hands it to a user or a group, for a
topic, a prefix or the whole cluster.

## What it will be

- `dev.kfkit.kfkrbac.RbacAuthorizer`: the broker's `StandardAuthorizer` plus
  roles. The ACLs you have keep working, and nothing changes until the first
  role is bound.
- Roles only add access. What an ACL denies stays denied; a request no ACL
  allows is allowed when a role bound to its principal covers it.
- Roles and bindings live in the cluster itself, not in a file on every
  node, and every node reads them from there.
- Access metrics on request: the per-principal, per-client, per-topic
  metrics of [kfkwho](https://github.com/kfkit/kfkwho), with its exporter
  rules and dashboard unchanged. Off by default.

## How a request is decided

For every resource of every request, the broker asks the authorizer two
questions, and both are answered the same way:

1. The ACLs first, exactly as `StandardAuthorizer` answers them. What they
   allow is allowed, and the roles are not consulted at all: a request an
   ACL allows costs what it costs today.
2. If the ACLs did not allow it and a deny ACL matches it, it is denied. A
   deny ACL beats every role.
3. Otherwise it is allowed when a role bound to the principal covers it,
   and denied when none does or the roles cannot be read.

The second question, "may this principal do this on any resource of the
type", is what an idempotent producer is asked for `WRITE` on topics before
it sends anything. A role answers it with yes when it covers some resource
that no deny ACL of the principal covers entirely.

With `allow.everyone.if.no.acl.found=true` the ACLs already allow whatever
no ACL names, so roles add nothing there. Leave it at its default, `false`,
to use roles.

## Installation, as designed

```bash
cp kfkrbac-<version>.jar "$KAFKA_HOME/libs/"      # on every node, controllers included
echo 'authorizer.class.name=dev.kfkit.kfkrbac.RbacAuthorizer' >> "$KAFKA_HOME/config/server.properties"
```

Then restart, node by node. The jar has no dependencies to install next to
it.

## Starting up

Roles are read from the cluster, so a node that has just started does not
have them yet. Until it has loaded both the ACLs and the roles:

| Principal | What happens |
|---|---|
| in `super.users` | allowed, as always |
| in `kfkrbac.early.users` | decided by the ACLs alone |
| anyone else | "authorizer not ready", which clients retry |

Nobody is let in by mistake while the roles are missing. `kfkrbac.early.users`
is for what must work before the roles are there: the account that reads
them, monitoring, a service whose access is granted by ACLs anyway. It has
the format of `super.users` and is empty by default.

## Compatibility

Compiled against Kafka 4.4, Java 17. Kafka 4.1 and later is the target
(KRaft, `StandardAuthorizer`); nothing is tested on a broker yet.

## Development

```bash
./gradlew check
./gradlew jmh      # the cost of authorize(), in build/results/jmh/
```

Rules for contributors, human or agent, are in [AGENTS.md](AGENTS.md).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

Apache Kafka, Kafka, and the Kafka logo are trademarks of The Apache Software
Foundation. kfkrbac is an independent project, not affiliated with or endorsed
by the ASF.
