# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/); until v1.0 the
major version is 0.

## Unreleased

### Fixed

- With `kfkrbac.metrics.enabled=true`, a denial the broker asks for without
  logging it is no longer recorded as `DENIED`: a producer that writes by a
  role, or by an ACL on its topics, no longer leaves a growing `DENIED`
  series for `IDEMPOTENT_WRITE` on the cluster, and a topic left out of a
  listing of all topics no longer leaves one for `DESCRIBE`. A producer
  that is refused is still recorded as denied. See
  [docs/metrics.md](docs/metrics.md#what-is-recorded).
- A member of a group that an ACL allows `ALTER` on the cluster, refused
  when it creates or deletes an ACL record, is no longer told only that it
  is not a principal the ACLs allow `ALTER` on the cluster: the reason
  names the group and says that an ACL of a group does not make its
  members managers of access. See [docs/groups.md](docs/groups.md).
- The line of `dev.kfkit.kfkrbac.denied` that says `roles held for
  CLUSTER: none`, or `none` for any other type, now says how many role
  bindings the principal holds for other resource types, so that it does
  not read as "this principal holds no role at all". See
  [docs/troubleshooting.md](docs/troubleshooting.md).
- With `kafka.authorizer.logger` at `TRACE`, the check for a deny ACL after
  a role or a group allowed a request, and the check of the ACLs of groups,
  no longer write lines of their own there, as if they were decisions of
  the broker's authorizer.

## 0.1.0 - 2026-11-18

The first release.

### Added

- `dev.kfkit.kfkrbac.RbacAuthorizer`, an authorizer for the brokers and
  controllers of Kafka 4.1 and later: the broker's `StandardAuthorizer`
  with roles. The ACLs decide first and as before; a request no ACL allows
  is allowed when a role bound to its principal covers it, and never when a
  deny ACL matches it, in `authorize` and in `authorizeByResourceType`. See
  [docs/install.md](docs/install.md).
- Seven roles, `DeveloperRead`, `DeveloperWrite`, `DeveloperManage`,
  `ResourceOwner`, `ResourceViewer`, `ClusterAdmin` and `SystemAdmin`, with
  what each grants on topics, groups, transactional IDs, the cluster,
  subjects and connectors in [docs/roles.md](docs/roles.md). Nothing
  decides with what they grant on subjects: there is no extension for a
  schema registry.
- Role bindings as ACL records: an ACL whose principal is `<Role>:<member>`
  is read as "the member holds the role on that pattern", or on the
  cluster. Bindings are created, listed and deleted through Kafka's ACL
  API, by the ACL tool, the admin client and whatever manages ACLs. A
  binding on a name or a prefix covers the topics, the groups and the
  transactional IDs of that name. A record of a role that is not a
  well-formed binding is refused when it is created; one that is in the
  metadata anyway grants nothing, is logged at `WARN` by each authorizer
  when it first reads it, and is counted in the gauge `malformed-bindings`.
  See [docs/bindings.md](docs/bindings.md).
- Delegation. `RbacAuthorizer` decides who may create and delete ACL
  records: a principal that the ACLs alone allow `ALTER` on the cluster, as
  before, or a `SystemAdmin` anything; a `ResourceOwner` role bindings on
  patterns inside what it owns and nothing else; everyone else nothing. A
  request is applied as a whole or not at all. A refusal by these rules
  goes back to the caller with its reason; a principal that holds no
  `ResourceOwner` and that nothing lets alter the cluster is refused
  before them, by the broker's own check and with the broker's message. See
  [docs/delegation.md](docs/delegation.md).
- No role but `SystemAdmin` lets its holder replace a user's SCRAM
  credentials: for `AlterUserScramCredentials`, `ClusterAdmin` does not
  grant `ALTER` on the cluster, and neither does an ACL of a group.
- Groups. A membership record, an ACL whose principal is `Member:<member>`
  on the literal topic name `Group:<name>`, puts a principal in a group. A
  role bound to a group (`DeveloperRead:Group:payments`) is held by its
  members, and an ordinary `ALLOW` ACL whose principal is a group is
  honoured for them by the broker's own matching rules, in `authorize` and
  in `authorizeByResourceType`; a deny ACL that names the user beats both.
  Membership is created and deleted by whoever creates any ACL: a principal
  that the ACLs allow `ALTER` on the cluster, or a `SystemAdmin`. A
  membership the format does not allow, a group as a member included, is
  refused when it is created; one that is in the metadata anyway puts nobody
  in a group, is logged at `WARN` and counted in the gauge
  `malformed-bindings`. A `DENY` ACL whose principal is a group is refused
  when it is created, for everyone. A binding's name may not start with
  `Group:`. See [docs/groups.md](docs/groups.md).
- A denied request says what was missing: one line on the logger
  `dev.kfkit.kfkrbac.denied` with the principal, the operation, the
  resource, the request and the cause, and, when no ACL and no role allowed
  it, the roles the principal holds for that resource type. The same
  principal, operation and resource is logged at most once in
  `kfkrbac.denied.log.interval.seconds` (60; 0 logs every denial), and the
  next line says how many denials were left out. See
  [docs/troubleshooting.md](docs/troubleshooting.md).
- The broker's authorizer log (`kafka.authorizer.logger`) says what was
  decided in the end. A request a role, or an ACL of a group, allows is
  logged as the broker logs a request an ACL allows, at `DEBUG`, with the
  binding or the group as its rule, and leaves no `Denied` line; a denied
  request is logged by the broker's own line, as without roles.
- The command: `kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand` with the
  verbs `bind`, `unbind`, `list` (`--output json`), `who-can` and
  `what-can`, through the admin client only. `who-can` and `what-can` give
  the authorizer's own answer for the ACL records of the cluster, with the
  binding or ACL behind each line. Exit status 0 done, 1 refused or failed,
  2 wrong usage. See [docs/command.md](docs/command.md).
- Groups in the command: `group add --group payments --member User:alice`,
  `group remove` and `group list` (`--output json`: one object for each
  membership, with `group` and `member`) write and read membership records.
  `who-can` names the members of a group for what the group is given, a role
  or an `ALLOW` ACL, with the group in the reason; `what-can` lists what a
  principal gets through each of its groups, and for `Group:payments` the
  members of the group and what it gives them.
- `apply --file <file>` in the command: reads the JSON that `list --output
  json` and `group list --output json` print, as `{"bindings": [...],
  "groups": [...]}` or as the array of bindings alone, compares it with the
  cluster, prints the difference and makes the cluster match. `--dry-run`
  changes nothing, `--no-delete` only adds, and ordinary ACLs are never
  touched.
- `import --file <file> --format role-bindings|role-acls` in the command:
  writes the bindings of a file that has another shape. A list of bindings
  of named roles becomes bindings, and `--cluster-id` selects those of one
  Kafka cluster; a list of roles that are sets of ACLs becomes, for each
  role, a group with the role's members and `ALLOW` ACLs of the group. What
  cannot be written as the file has it is skipped and reported, never
  guessed; `--dry-run` writes nothing. The two shapes are in
  [docs/import.md](docs/import.md), field by field, with an example file
  each in `docs/examples/`.
- A file that `apply` or `import` cannot read as what it must be, invalid
  JSON or a missing field, fails with the line, the column and the field,
  exit status 2, before the cluster is asked anything.
- The extension for Kafka Connect,
  `rest.extension.classes=dev.kfkit.kfkrbac.connect.RbacConnectRestExtension`:
  it reads the bindings with the worker's own connection settings, every
  `kfkrbac.refresh.interval.ms`, and allows a request to the worker's REST
  interface when a role bound to the caller, or to a group the caller is
  in, grants its operation on the connector; a refusal is a 403 that says
  what was missing, `GET /connectors` lists only what the caller may see, an
  anonymous request gets 401, and before the first read everything but `GET
  /health` gets 503. The caller is the basic-authentication user or the
  certificate's subject; `kfkrbac.super.users` pass. The gauges
  `bindings-age-ms` and `bindings` are in the MBean
  `kfkrbac:type=rest-bindings`. See [docs/connect.md](docs/connect.md).
- Access metrics on request: with `kfkrbac.metrics.enabled=true` the
  authorizer publishes every verdict it gives, after the ACLs and the roles
  have decided, as the metrics of [kfkwho](https://github.com/kfkit/kfkwho),
  under kfkwho's MBean names and `kfkwho.*` settings, so that its exporter
  rules work unchanged. Off by default: without the setting no MBean is
  registered, no thread is started and no `kfkwho.*` setting is read. See
  [docs/metrics.md](docs/metrics.md).
- The jar is self-contained: the classes of kfkwho 0.1.0 that the metrics
  need are inside it, relocated into `dev.kfkit.kfkrbac.shaded.kfkwho`, with
  `META-INF/LICENSE` and `META-INF/NOTICE`. It has no dependency and can be
  in `libs/` next to kfkwho's own jar. The build takes kfkwho from its
  GitHub release and checks its SHA-256.
- Every setting the jar reads, in a node and in a Connect worker, in one
  table with its type, default and meaning: [docs/settings.md](docs/settings.md).
  `./gradlew check` fails when the table and the code disagree.
- Operator documentation: [docs/install.md](docs/install.md),
  [docs/roles.md](docs/roles.md), [docs/bindings.md](docs/bindings.md),
  [docs/delegation.md](docs/delegation.md), [docs/groups.md](docs/groups.md),
  [docs/command.md](docs/command.md), [docs/import.md](docs/import.md),
  [docs/connect.md](docs/connect.md), [docs/metrics.md](docs/metrics.md),
  [docs/settings.md](docs/settings.md) and
  [docs/troubleshooting.md](docs/troubleshooting.md). Their shell commands
  are run against a real broker by `DocsIT`, in the integration suite,
  those of `docs/connect.md` with a Connect worker next to it, and the
  output they show is compared with what the commands print; a command that
  is not run says so.
- `./gradlew check` fails when a relative link in `README.md` or `docs/`
  points at a file that does not exist, and when a block of shell commands
  in `docs/` does not say whether a test runs it.
- A release workflow: a `vX.Y.Z` tag runs the whole check and publishes a
  GitHub Release with the jar, its SHA-256, a keyless cosign signature and a
  build provenance attestation. How to verify a released jar is in
  [docs/provenance](docs/provenance/README.md).
- `./gradlew publishToMavenLocal` produces the jar, sources, javadoc and a
  POM with what Maven Central asks for; the version comes from
  `-Pversion`. Nothing is published to Maven Central yet.
- An integration-test suite that runs the built jar as the authorizer of
  real brokers in containers, on Kafka 4.1.2, 4.2.2, 4.3.1 and 4.4.0 and on a
  cluster of a controller and several brokers: `./gradlew integrationTest`,
  `-PbrokerVersion=4.1.2` for another release. It runs the command inside
  the broker's container with `kafka-run-class.sh`, every verb, and
  `ConnectIT` and `TlsConnectIT` run two Connect workers with the jar as
  their extension against a broker with the jar, with basic authentication
  and with mutual TLS, on each of those releases. It needs Docker
  and is skipped without it; `./gradlew check -PintegrationTest` makes it
  part of `check`.
- JMH benchmarks of `authorize`, `./gradlew jmh`: without and with roles,
  for a principal in 0, 1 and 20 groups (`GroupsBenchmark`), and for the
  lines of the logs.
