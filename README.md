# kfkrbac

Role-based access control for Apache Kafka®: an authorizer that grants access
through roles and role bindings, on top of the ACLs the broker already has.
One jar, one line of configuration, free and open source.

> Status: pre-alpha, not released. The skeleton compiles and behaves exactly
> like `StandardAuthorizer`; roles do not exist yet. What is described below
> is the design, not something you can install.

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
```

Rules for contributors, human or agent, are in [AGENTS.md](AGENTS.md).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

Apache Kafka, Kafka, and the Kafka logo are trademarks of The Apache Software
Foundation. kfkrbac is an independent project, not affiliated with or endorsed
by the ASF.
