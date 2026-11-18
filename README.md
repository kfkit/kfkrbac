# kfkrbac

Role-based access control for Apache Kafka®: an authorizer that grants access
through roles and role bindings, on top of the ACLs the broker already has.
One jar, one line of configuration, free and open source.

> Status: pre-alpha, not released. The authorizer reads role bindings from
> ACL records and decides with them, in unit tests against the real
> `StandardAuthorizer`, and as the authorizer of real brokers in the
> integration tests, on Kafka 4.1 to 4.4. Who may bind what is
> decided when an ACL record is created or deleted: whoever the ACLs allow
> to alter the cluster, or a `SystemAdmin`, anything; a `ResourceOwner`
> bindings inside what it owns; everyone else nothing. The command binds,
> unbinds and lists bindings and answers "who can" and "what can"
> ([docs/command.md](docs/command.md)). The extension for Kafka Connect
> decides connector requests from the same bindings, in unit tests and in
> real workers of Kafka 4.1 to 4.4 with basic authentication and with
> mutual TLS in the integration tests ([docs/connect.md](docs/connect.md)).
> Groups are read from membership
> records and honoured by the authorizer and by the extension for Kafka
> Connect ([docs/groups.md](docs/groups.md)); the command does not know
> them yet. The `group` verbs, `apply` and
> `import` do not exist. What
> is described below is the design, not something you can install.

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
  our own makes it shorter and answers "who can read this topic". The
  command is in [docs/command.md](docs/command.md).
- Delegation: the owner of a prefix hands out roles inside it, and nowhere
  else.
- Connectors: the same jar plugs into Kafka Connect with one line,
  `rest.extension.classes=dev.kfkit.kfkrbac.connect.RbacConnectRestExtension`,
  and applies the same bindings there: [docs/connect.md](docs/connect.md).
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
3. Otherwise it is allowed when a role bound to the principal, or to a
   group it is in, covers it, or an `ALLOW` ACL of such a group allows it,
   and denied when none does or the roles cannot be read.

It also asks a second question, "may this principal do this on any
resource of the type": an idempotent producer is asked it for `WRITE` on
topics before it sends anything. That one is answered the same way: the
ACLs first, and then yes when a role covers some resource that no deny ACL
of the principal covers entirely.

With `allow.everyone.if.no.acl.found=true` the ACLs already allow whatever
no ACL names. A binding is an ACL record and names its pattern, so there the
first binding on a pattern takes away what everyone had on it by default.
Leave the setting at its default, `false`, to use roles.

## Installation, as designed

```bash
cp kfkrbac-<version>.jar "$KAFKA_HOME/libs/"      # on every node, controllers included
echo 'authorizer.class.name=dev.kfkit.kfkrbac.RbacAuthorizer' >> "$KAFKA_HOME/config/server.properties"
```

Then restart, node by node. The jar has no dependencies to install next to
it: what it needs of kfkwho is inside. A release carries the jar, its SHA-256 and a keyless signature; how to
verify them is in [docs/provenance](docs/provenance/README.md), and the
steps are in [docs/install.md](docs/install.md).

## A binding, as designed

A role binding is an ACL whose principal names the role and the member:

```bash
kafka-acls.sh --bootstrap-server localhost:9092 --add \
  --allow-principal 'DeveloperRead:User:alice' --operation All \
  --topic billing. --resource-pattern-type prefixed
```

The same with the command:

```bash
kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand bind --bootstrap-server localhost:9092 \
  --role DeveloperRead --member User:alice --prefix billing.
```

`User:alice` may now read the topics and use the consumer groups whose names
start with `billing.`. No client is ever a principal of type `DeveloperRead`,
so to a broker without kfkrbac the record means nothing.

## Documentation

- [Installation](docs/install.md): the jar and the line, what changes on
  the cluster, the `apache/kafka` image, Strimzi, removing it.
- [Roles](docs/roles.md): what each of the seven roles grants.
- [Bindings](docs/bindings.md): what a binding is, with the ACL tool and
  with the command; narrowing; cluster roles; records that are not bindings.
- [Delegation](docs/delegation.md): who may bind what, with a worked
  example.
- [Groups](docs/groups.md): membership records, roles and ACLs for a group.
- [The command](docs/command.md): every verb and option.
- [Kafka Connect](docs/connect.md): the extension for the workers.
- [Provenance](docs/provenance/README.md): verifying a released jar.

## Compatibility

Compiled against Kafka 4.4, Java 17. Kafka 4.1 and later is the target
(KRaft, `StandardAuthorizer`); the integration tests run the jar on brokers
of 4.1.2, 4.2.2, 4.3.1 and 4.4.0.

## Development

```bash
./gradlew check
./gradlew integrationTest                      # the jar on real brokers; needs Docker
./gradlew integrationTest -PbrokerVersion=4.1.2
./gradlew integrationTest --tests '*DocsIT'    # the commands of docs/ against a broker
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
