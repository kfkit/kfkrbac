# Settings

Every setting the jar reads, in a node of a cluster of Apache Kafka® and in
a worker of Kafka Connect. All of them are optional: with none, the
authorizer decides with ACLs and roles, logs why a request was denied, and
publishes no access metrics.

A unit test (`SettingsDocTest`) fails when this table and the code
disagree: on which settings the classes of the jar name, on a type or on a
default. Change both together.

| Setting | Read by | Type | Default | Meaning |
|---|---|---|---|---|
| `kfkrbac.metrics.enabled` | a node | boolean | `false` | Whether every verdict of the authorizer is published as the access metrics of kfkwho ([Metrics](metrics.md)). `true` or `false`; anything else fails the start of the node with a message that names the setting. Only a node that is a broker records. |
| `kfkrbac.denied.log.interval.seconds` | a node | int | `60` | Seconds before the line for the same denied request, by principal, operation and resource, is written again to the logger `dev.kfkit.kfkrbac.denied` ([Troubleshooting](troubleshooting.md#how-often-a-line-is-written)). The same interval bounds the `ERROR` line `Role lookup failed`. `0` writes a line for every denial, and every failed lookup. A whole number, 0 or more; anything else fails the start of the node with a message that names the setting. |
| `kfkrbac.super.users` | a worker | string | none | Principals that pass every request to the worker's REST interface, separated by `;` as in the broker's `super.users`: `User:admin;User:CN=connect-1,O=example`. Each is `<type>:<name>`; anything else stops the worker at start-up. |
| `kfkrbac.refresh.interval.ms` | a worker | long | `5000` | Milliseconds between two reads of the bindings and the memberships from the cluster. A binding created on the cluster is in force in the worker within this time. More than 0; anything else stops the worker at start-up. |

## In a node

The settings go where `authorizer.class.name` is, the same on every node:
in the properties file the node is started with, or, in the `apache/kafka`
image, as an environment variable, `KAFKA_KFKRBAC_METRICS_ENABLED=true`
([Installation](install.md#the-apachekafka-image)). They are read when the
node starts; changing one takes a restart.

```properties
authorizer.class.name=dev.kfkit.kfkrbac.RbacAuthorizer
kfkrbac.metrics.enabled=true
kfkrbac.denied.log.interval.seconds=300
```

The authorizer also reads settings that are not its own:

- **`super.users` and `allow.everyone.if.no.acl.found`**, the settings of
  the broker's `StandardAuthorizer`, which mean what they mean without
  kfkrbac. A group in `super.users` makes none of its members a super user
  ([Groups](groups.md#what-a-group-is-not)), and with
  `allow.everyone.if.no.acl.found=true` a binding closes what it names to
  everyone else ([Installation](install.md#a-cluster-that-ran-without-an-authorizer)).
- **The `kfkwho.*` settings**, and `process.roles`, only with
  `kfkrbac.metrics.enabled=true`: they are kfkwho's, of the version inside
  the jar, and are in
  [its reference](https://github.com/kfkit/kfkwho/blob/v0.1.0/docs/config.md).
  A wrong value then fails the start of the node with a message that names
  the setting. Without `kfkrbac.metrics.enabled=true` none of them is read.

There is no setting for where the bindings are: they are ACL records in the
cluster's metadata, and reach the authorizer the way ACLs do.

## In a worker of Kafka Connect

The settings go in the worker's properties file, next to
`rest.extension.classes` ([Kafka Connect](connect.md)), the same on every
worker of a Connect cluster. They are read when the worker starts.

```properties
rest.extension.classes=dev.kfkit.kfkrbac.connect.RbacConnectRestExtension
kfkrbac.super.users=User:admin
kfkrbac.refresh.interval.ms=5000
```

The extension reads the bindings with an admin client of its own, and gives
it the worker's own connection settings: every top-level setting of the
worker that is a setting of Kafka's admin client, `bootstrap.servers`,
`security.protocol`, `sasl.*` and `ssl.*` among them. Its `client.id` is
`kfkrbac-connect`, whatever the worker's is. Settings with the prefixes
`producer.`, `consumer.` and `admin.` are not read.

## The command

The command reads no setting of its own. `--command-config` names a file of
properties for its admin client, as for Kafka's own tools
([The command](command.md#options-of-every-verb)).
