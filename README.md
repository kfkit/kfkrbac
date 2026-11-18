# kfkrbac

Role-based access control for Apache Kafka®: an authorizer that grants access
through roles and role bindings, on top of the ACLs the broker already has.
One jar, one line of configuration, free and open source.

> Status: 0.1.0, the first release. Everything on this page exists: the
> authorizer for the brokers and controllers, groups, the command, the
> extension for Kafka Connect and the access metrics. The jar runs as the
> authorizer of real brokers of Kafka 4.1 to 4.4 in the integration tests,
> and the commands of the documentation are run there too; nothing has run
> on a production cluster yet. What is not there is said
> [below](#what-is-not-there). The jar, its SHA-256 and its signature are on
> the [releases page](https://github.com/kfkit/kfkrbac/releases). Until 1.0
> a setting, a record or a role may still change, and every such change is
> listed in the [changelog](CHANGELOG.md).

## Why

An ACL binds one principal to one resource. With tens of teams and hundreds
of topics the ACLs multiply, nobody can say what a team is allowed to do, and
bringing one more service on is a list of commands to run. A role names a set
of permissions once; a role binding hands it to a user or a group, for a
topic, a prefix or the whole cluster.

## What you get

- `dev.kfkit.kfkrbac.RbacAuthorizer`: the broker's `StandardAuthorizer` plus
  roles. The ACLs you have keep working, and nothing changes until the first
  role is bound.
- Seven roles: `DeveloperRead`, `DeveloperWrite`, `DeveloperManage`,
  `ResourceOwner` and `ResourceViewer`, bound to a name pattern;
  `ClusterAdmin` and `SystemAdmin`, bound to the cluster. What each grants
  is in [Roles](docs/roles.md).
- Roles only add access. What an ACL denies stays denied; a request no ACL
  allows is allowed when a role bound to its principal covers it.
- One binding for one application: a role bound to the prefix `billing.`
  covers the topics, the consumer groups and the transactional IDs that
  start with it.
- Nothing else to run. Bindings live in the cluster's own metadata, next to
  the ACLs, and reach every node the way ACLs do: no service, no directory
  server, no topic to create.
- The tools you have keep working. A binding is an ACL record, created and
  listed with the ACL tool, the admin client or whatever manages your ACLs
  today. [The command](docs/command.md) of the jar makes it shorter, answers
  "who can read this topic" and "what may this user do", keeps bindings in a
  file and [brings bindings over](docs/import.md) from files of two other
  shapes.
- [Delegation](docs/delegation.md): the owner of a prefix hands out roles
  inside it, and nowhere else.
- [Groups](docs/groups.md): membership is a record in the same metadata. A
  role is bound to a group, and an ordinary ACL can name one.
- A denial says what was missing. The broker's log names who was denied
  what, why, and which roles the principal does hold, so that a binding on
  the wrong prefix shows at once:
  [Troubleshooting](docs/troubleshooting.md).
- Connectors: the same jar plugs into Kafka Connect with one line,
  `rest.extension.classes=dev.kfkit.kfkrbac.connect.RbacConnectRestExtension`,
  and applies the same bindings to the workers' REST interface:
  [Kafka Connect](docs/connect.md).
- Access metrics on request: the per-principal, per-client, per-topic
  metrics of [kfkwho](https://github.com/kfkit/kfkwho), under its names, so
  that its exporter rules work unchanged. Off by default:
  [Metrics](docs/metrics.md).

## Installation

On every node, controllers included: copy the jar to `libs/`, and name the
class where the node's configuration names its authorizer.

```bash
cp kfkrbac-*.jar "$KAFKA_HOME/libs/"
sed -i 's/^authorizer\.class\.name=.*/authorizer.class.name=dev.kfkit.kfkrbac.RbacAuthorizer/' \
  "$KAFKA_HOME/config/server.properties"
```

Then restart, node by node. That is for a cluster that has an authorizer
already, which is where nothing changes until the first role is bound. A
cluster that has none gets the line added instead, and starts enforcing
ACLs the moment it has it: read [Installation](docs/install.md) first. It
also has the `apache/kafka` image, Strimzi, and how to take the jar out
again.

The jar has no dependencies to install next to it. A release carries the
jar, its SHA-256 and a keyless signature; how to verify them is in
[Provenance](docs/provenance/README.md).

## A first binding

A principal that is in `super.users`, here `User:admin`, binds the first
role, with the command of the jar:

```bash
kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand bind \
  --bootstrap-server localhost:9092 --command-config admin.properties \
  --role DeveloperRead --member User:alice --prefix billing.
```

`User:alice` may now read the topics and use the consumer groups whose names
start with `billing.`. What the command wrote is an ACL record whose
principal names the role and the member, and the ACL tool writes the same
one:

```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal DeveloperRead:User:alice --operation All \
  --topic billing. --resource-pattern-type prefixed
```

No client is ever a principal of type `DeveloperRead`, so to a broker
without kfkrbac the record means nothing. [Bindings](docs/bindings.md) goes
on from here.

## How a request is decided

The broker asks the authorizer about every resource of every request:

1. The ACLs first, exactly as `StandardAuthorizer` answers them. What they
   allow is allowed, and no role is asked: a request an ACL allows costs
   what it costs today and a lookup or two, which say whether its
   principal holds a binding or is in a group at all.
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

Who may create and delete bindings is decided when the record is created or
deleted: whoever the ACLs allow to alter the cluster, or a `SystemAdmin`,
anything; a `ResourceOwner` bindings inside what it owns; everyone else
nothing. No role but `SystemAdmin` lets its holder replace a user's SCRAM
credentials.

With `allow.everyone.if.no.acl.found=true` the ACLs already allow whatever
no ACL names. A binding is an ACL record and names its pattern, so there the
first binding on a pattern takes away what everyone had on it by default.
Leave the setting at its default, `false`, to use roles.

## What is not there

- **Nothing for schemas yet.** The roles say what they would grant on the
  subjects of a schema registry, and a binding for subjects can be written,
  but nothing reads it: there is no extension for a schema registry.
- **No dashboard of our own.** The access metrics are kfkwho's, under
  kfkwho's names, for what is built on those. kfkrbac's own metrics are
  three gauges, in [Bindings](docs/bindings.md#a-record-that-is-not-a-binding)
  and [Kafka Connect](docs/connect.md#before-the-bindings-are-read-and-when-they-cannot-be).
- **Not on Maven Central yet.** A release is a signed jar on the releases
  page of this repository; `./gradlew publishToMavenLocal` makes the
  artifacts Maven Central asks for.
- **No roles of your own.** There are the seven. A set of permissions that
  none of them names is given with ordinary ACLs to a
  [group](docs/groups.md#an-acl-for-a-group).
- **No groups from a directory.** A principal is in the groups that
  membership records put it in, and in no other.

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
- [Bringing bindings over](docs/import.md): the two shapes of file that
  `import` reads, field by field, and what each becomes.
- [Kafka Connect](docs/connect.md): the extension for the workers.
- [Metrics](docs/metrics.md): the access metrics of kfkwho, on request.
- [Settings](docs/settings.md): every setting the jar reads.
- [Troubleshooting](docs/troubleshooting.md): reading the line for a
  denied request, and what else goes wrong.
- [Provenance](docs/provenance/README.md): verifying a released jar.
- [Changelog](CHANGELOG.md).

Every shell command in the documents of `docs/` is run against a real
broker by a test, and its output compared with what the page shows, or the
page says next to it that it was not.

## Compatibility

Compiled against Kafka 4.4, Java 17. Kafka 4.1 and later is the target
(KRaft, `StandardAuthorizer`); the integration tests run the jar on brokers
and Connect workers of 4.1.2, 4.2.2, 4.3.1 and 4.4.0.

## Development

<!-- not run: the build of this repository, not commands for a cluster -->
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
