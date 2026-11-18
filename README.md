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
- Seven roles, under names operators already use where such a name exists:
  `DeveloperRead`, `DeveloperWrite`, `DeveloperManage`, `ResourceOwner` and
  `ResourceViewer`, bound to a name pattern; `ClusterAdmin` and
  `SystemAdmin`, bound to the cluster. What each grants on topics, groups,
  transactional IDs, the cluster, subjects and connectors is in
  [docs/roles.md](docs/roles.md).
- Roles only add access. What an ACL denies stays denied; a request no ACL
  allows is allowed when a role bound to its principal covers it.
- One binding for one application: a role bound to the prefix `billing.`
  covers the topics and the consumer groups that start with it.
- Nothing else to run. Bindings live in the cluster's own metadata, next to
  the ACLs, and reach every node the way ACLs do: no service, no directory
  server, no topic to create.
- The tools you have keep working. A binding is created and listed with the
  ACL tool, `AdminClient` or whatever manages your ACLs today; a command of
  our own makes it shorter and answers "who can read this topic".
- Delegation: the owner of a prefix hands out roles inside it, and nowhere
  else.
- Schemas and connectors: the same jar plugs into Kafka Connect and a schema
  registry and applies the same bindings there.
- Access metrics on request: the per-principal, per-client, per-topic
  metrics of [kfkwho](https://github.com/kfkit/kfkwho), with its exporter
  rules and dashboard unchanged. Off by default.

## How a request is decided

The broker asks the authorizer about every resource of every request:

1. The ACLs first, exactly as `StandardAuthorizer` answers them. What they
   allow is allowed, and the roles are not consulted at all: a request an
   ACL allows costs what it costs today.
2. If the ACLs did not allow it and a deny ACL matches it, it is denied. A
   deny ACL beats every role.
3. Otherwise it is allowed when a role bound to the principal covers it,
   and denied when none does or the roles cannot be read.

It also asks a second question, "may this principal do this on any
resource of the type": an idempotent producer is asked it for `WRITE` on
topics before it sends anything. That one is answered the same way: the
ACLs first, and then yes when a role covers some resource that no deny ACL
of the principal covers entirely.

With `allow.everyone.if.no.acl.found=true` the ACLs already allow whatever
no ACL names, so roles add nothing there. Leave it at its default, `false`,
to use roles.

## Installation, as designed

```bash
cp kfkrbac-<version>.jar "$KAFKA_HOME/libs/"      # on every node, controllers included
echo 'authorizer.class.name=dev.kfkit.kfkrbac.RbacAuthorizer' >> "$KAFKA_HOME/config/server.properties"
```

Then restart, node by node. The jar has no dependencies to install next to
it. A release carries the jar, its SHA-256 and a keyless signature; how to
verify them is in [docs/provenance](docs/provenance/README.md), and the
steps are in [docs/install.md](docs/install.md).

## A binding, as designed

A role binding is an ACL whose principal names the role and the member:

```bash
kafka-acls.sh --bootstrap-server localhost:9092 --add \
  --allow-principal 'DeveloperRead:User:alice' --operation All \
  --topic billing. --resource-pattern-type prefixed
```

`User:alice` may now read the topics and use the consumer groups whose names
start with `billing.`. No client is ever a principal of type `DeveloperRead`,
so to a broker without kfkrbac the record means nothing.

## Compatibility

Compiled against Kafka 4.4, Java 17. Kafka 4.1 and later is the target
(KRaft, `StandardAuthorizer`); nothing is tested on a broker yet.

## Development

```bash
./gradlew check
./gradlew jmh      # the cost of authorize(), in build/results/jmh/
./gradlew publishToMavenLocal -Pversion=0.0.0-test   # jar, sources, javadoc, POM
```

A release is a signed `vX.Y.Z` tag; the [release workflow](.github/workflows/release.yml)
does the rest.

Rules for contributors, human or agent, are in [AGENTS.md](AGENTS.md).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

Apache Kafka, Kafka, and the Kafka logo are trademarks of The Apache Software
Foundation. kfkrbac is an independent project, not affiliated with or endorsed
by the ASF.
