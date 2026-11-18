# Kafka Connect

The same jar applies the same role bindings to the REST interface of Kafka
Connect, the part of Apache Kafka® that runs connectors: a request is
allowed when a role bound to the caller covers the connector, and refused
otherwise, saying which operation on which connector was missing. A broker
never sees those requests, so the workers take the jar as an extension of
their own.

> Status: the commands of this page are run by a test of the integration
> suite (`DocsIT`), on a broker and a Connect worker of the newest supported
> release, with HTTP basic authentication, and the output shown is what
> they printed there. Two more tests of that suite run two workers of every
> supported release, forwarding between them included: `ConnectIT` with
> basic authentication and a role held through a group, `TlsConnectIT` with
> mutual TLS.

## The cluster of the examples

One node at `localhost:9092` that runs `RbacAuthorizer`
([Installation](install.md)), with the users `User:admin`, who is in
`super.users`, `User:alice` and `User:bob`, whom no ACL names, a file of
client properties `admin.properties`, and a topic `billing.orders`. Kafka's
`bin` is on the `PATH`.

Next to it is one Connect worker, not started yet. Its settings are in
`worker.properties` in the current directory: it connects to the cluster as
`User:admin`, listens at `http://localhost:8083`, and authenticates the
callers of its REST interface with HTTP basic authentication, the extension
that ships with Kafka, for the users `admin`, `alice` and `bob`:

```properties
rest.extension.classes=org.apache.kafka.connect.rest.basic.auth.extension.BasicAuthSecurityRestExtension
```

The requests are sent with `curl`, and `rbac` is the command of the jar
([The command](command.md)) with the connection of `User:admin`:

<!-- run: session -->
```bash
rbac() {
  kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand "$@" \
    --bootstrap-server localhost:9092 --command-config admin.properties
}
```

## Installation

Put the jar on the class path of every worker of the Connect cluster, in
`libs/` or in `CLASSPATH`, as for a broker ([Installation](install.md)). In
the examples it is where the broker has it. The jar brings nothing the
worker already has; the worker provides the REST classes it uses.

Then name the extension in the worker's settings:

```properties
rest.extension.classes=dev.kfkit.kfkrbac.connect.RbacConnectRestExtension
```

The setting is a list. A worker that has an extension already, as the one
of the examples has, keeps it and gets this one after it. The second line
makes `User:admin` a super user of the extension, so that somebody can
create connectors before the first binding:

<!-- run -->
```bash
sed -i 's/^rest\.extension\.classes=.*/&,dev.kfkit.kfkrbac.connect.RbacConnectRestExtension/' worker.properties
echo 'kfkrbac.super.users=User:admin' >> worker.properties
grep -e '^rest.extension.classes' -e '^kfkrbac' worker.properties
```

<!-- output -->
```text
rest.extension.classes=org.apache.kafka.connect.rest.basic.auth.extension.BasicAuthSecurityRestExtension,dev.kfkit.kfkrbac.connect.RbacConnectRestExtension
kfkrbac.super.users=User:admin
```

Start the worker, or restart it if it runs:

<!-- run -->
```bash
connect-distributed.sh -daemon worker.properties
```

It answers once it has started and the extension has read the bindings:

<!-- run: within 180 s -->
```bash
curl -s -o /dev/null -w '%{http_code}\n' -u admin:admin-secret http://localhost:8083/
```

<!-- output -->
```text
200
```

The extension reads the bindings from the Kafka cluster the worker uses,
with the worker's own connection settings (`bootstrap.servers`,
`security.protocol`, `sasl.*`, `ssl.*`). The principal the worker connects
with must be allowed to list ACLs, which is `Describe` on the cluster:

<!-- not run: the worker of the examples connects as a super user, which needs no ACL -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal User:connect --operation Describe --cluster
```

This command has not been run by the tests, and that the ACL is all the
extension needs is not verified in a worker: the workers of the tests
connect as a super user. That listing ACLs takes `Describe` on the cluster
is shown in [Delegation](delegation.md#what-a-delegate-can-see).

The worker must use `connect.protocol=sessioned`, its default: see
[Requests between workers](#requests-between-workers).

The two settings of the extension, `kfkrbac.super.users` and
`kfkrbac.refresh.interval.ms`, are in the table of
[Settings](settings.md#in-a-worker-of-kafka-connect). One that cannot be
read stops the worker at start-up.

## Who is calling

Whoever the worker authenticated, as a principal of type `User`:

- the user of HTTP basic authentication (`User:alice`), when the worker
  authenticates with it;
- otherwise the subject of the client certificate of a mutual TLS
  connection, as the broker writes it by default: `User:CN=alice,O=example`.

Authenticating callers is the worker's job; the extension only reads what
the worker established. A request with neither is anonymous, and the
extension refuses it with 401. In a worker with basic authentication, as
here, that extension refuses it first:

<!-- run -->
```bash
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8083/connectors
```

<!-- output -->
```text
401
```

## A binding for connectors

A connector is the resource `Connector:<name>`: a binding on the prefix
`Connector:billing-` covers the connectors whose names start with
`billing-`, and a cluster role covers every connector. What each role grants
on connectors is in [Roles](roles.md).

`User:alice` wants a connector that writes the topic `billing.orders` to a
file:

<!-- run -->
```bash
cat > billing-sink.json <<'JSON'
{"name": "billing-sink",
 "config": {"connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
            "tasks.max": "1", "topics": "billing.orders", "file": "/tmp/billing-sink.txt"}}
JSON
```

No binding names her, and the refusal is a 403 whose body says what was
missing:

<!-- run -->
```bash
curl -s -w '\n%{http_code}\n' -u alice:alice-secret -H 'Content-Type: application/json' \
  --data @billing-sink.json http://localhost:8083/connectors
```

<!-- output -->
```text
{"error_code":403,"message":"kfkrbac: User:alice may not Create connector billing-sink: no role bound to it grants Create on that connector"}
403
```

`User:admin` binds `DeveloperManage` to her on the connectors that start
with `billing-`:

<!-- run -->
```bash
rbac bind --role DeveloperManage --member User:alice --prefix billing- --connector
```

<!-- output -->
```text
Bound DeveloperManage to User:alice on connectors with prefix billing-
```

With the ACL tool it is the same record as any binding
([Bindings](bindings.md)), with `Connector:` in front of the name:

<!-- run -->
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --command-config admin.properties \
  --add --allow-principal DeveloperManage:User:alice --operation All \
  --topic Connector:billing- --resource-pattern-type prefixed
```

The worker reads the bindings again every five seconds, so a binding is in
force there within that time, and her request goes through:

<!-- run: within 30 s -->
```bash
curl -s -o /dev/null -w '%{http_code}\n' -u alice:alice-secret -H 'Content-Type: application/json' \
  --data @billing-sink.json http://localhost:8083/connectors
```

<!-- output -->
```text
201
```

She sees it, and `User:bob`, who holds nothing, sees an empty list and may
not ask about it by name:

<!-- run -->
```bash
curl -s -w '\n' -u alice:alice-secret http://localhost:8083/connectors
curl -s -w '\n' -u bob:bob-secret http://localhost:8083/connectors
curl -s -w '\n%{http_code}\n' -u bob:bob-secret http://localhost:8083/connectors/billing-sink/status
```

<!-- output -->
```text
["billing-sink"]
[]
{"error_code":403,"message":"kfkrbac: User:bob may not ReadStatus connector billing-sink: no role bound to it grants ReadStatus on that connector"}
403
```

`DeveloperManage` grants `Pause` and not `Delete`, so she pauses her
connector and does not delete it:

<!-- run -->
```bash
curl -s -o /dev/null -w '%{http_code}\n' -u alice:alice-secret -X PUT \
  http://localhost:8083/connectors/billing-sink/pause
curl -s -w '\n%{http_code}\n' -u alice:alice-secret -X DELETE \
  http://localhost:8083/connectors/billing-sink
```

<!-- output -->
```text
202
{"error_code":403,"message":"kfkrbac: User:alice may not Delete connector billing-sink: no role bound to it grants Delete on that connector"}
403
```

`User:admin`, a super user of the extension, does:

<!-- run -->
```bash
curl -s -o /dev/null -w '%{http_code}\n' -u admin:admin-secret -X DELETE \
  http://localhost:8083/connectors/billing-sink
```

<!-- output -->
```text
204
```

## What each request takes

| Request | Operation |
|---|---|
| `GET /connectors` | lists only the connectors the caller may `ReadStatus` |
| `POST /connectors`, `PUT /connectors/{name}/config` when it does not exist | `Create` |
| `PUT /connectors/{name}/config` when it exists, `PATCH .../config` | `Configure` |
| `GET /connectors/{name}`, `GET .../config`, `GET .../tasks` | `ReadConfig` |
| `GET .../status`, `GET .../tasks/{id}/status` | `ReadStatus` |
| `GET .../topics` | `ReadActiveTopics` |
| `PUT .../topics/reset` | `ResetActiveTopics` |
| `PUT .../pause`, `PUT .../stop` | `Pause` |
| `PUT .../resume`, `POST .../restart`, `POST .../tasks/{id}/restart` | `Resume` |
| `PATCH .../offsets`, `DELETE .../offsets` | `Configure` |
| `GET .../offsets` | `ReadStatus` |
| `DELETE /connectors/{name}` | `Delete` |
| `GET /`, `GET /connector-plugins`, `GET /health` | allowed to any authenticated caller |
| anything else, `/admin` and `/connector-plugins/*/config/validate` among it | `ClusterAdmin` or `SystemAdmin` |

- `HEAD` takes what `GET` takes.
- `POST /connectors` names the connector in its body; the name is read
  from there, trimmed as the worker trims it.
- `GET /connectors?expand=status` and `?expand=info` list the same
  connectors; a connector's configuration (`info`) only where the caller
  may also `ReadConfig`.
- A principal in `kfkrbac.super.users` passes every request.
- A role bound to a group (`DeveloperManage:Group:payments`) applies to the
  group's members: the extension reads the membership records together with
  the bindings, and a membership is in force when the binding would be.
  Groups are in [Groups](groups.md).
- An ordinary ACL, of a user or of a group, means nothing to the extension:
  for connectors only roles grant, and a deny ACL takes nothing away.

The table is checked request by request by the unit tests of the
extension; the examples above are the part of it that is run in a worker.

## Before the bindings are read, and when they cannot be

Until the first read of the bindings succeeds, every request but
`GET /health` is refused with 503. When a later read fails, the bindings
read last stay in force, and the gauges in the MBean
`kfkrbac:type=rest-bindings` say so:

| Gauge | What |
|---|---|
| `bindings-age-ms` | How old the bindings in force are: since the last read that succeeded, or since the start while none has. It grows while reads fail. |
| `bindings` | How many role bindings are in force. |

The gauges are checked by the unit tests; they have not been read from a
running worker.

## Requests between workers

Two requests are sent by one worker to another: `POST
/connectors/{name}/tasks` and `PUT /connectors/{name}/fence`. They pass the
extension, as they pass the worker's own authentication, and the worker
verifies them itself with the session key of `connect.protocol=sessioned`.
With an older protocol nothing would verify them: use `sessioned`, the
default.

A worker that is not the leader forwards some requests to the leader. With
basic authentication the user's credentials go along, and the leader decides
the request as the user again. With mutual TLS the leader sees the
certificate of the worker that forwarded the request: the request was
decided where it arrived, and the workers' own principal has to be in
`kfkrbac.super.users` for the leader to let it through. Otherwise the
leader refuses it, naming the worker:

```json
{"error_code":403,"message":"kfkrbac: User:CN=connect-1,O=example may not Create connector billing-sink: no role bound to it grants Create on that connector"}
```

A worker presents to the leader the certificate of its own key store,
`listeners.https.ssl.keystore.*`. A sketch of a worker's settings for mutual TLS, with every worker's subject
a super user:

```properties
listeners=https://0.0.0.0:8443
rest.advertised.listener=https
listeners.https.ssl.keystore.location=/etc/connect/connect-1.p12
listeners.https.ssl.keystore.password=<password>
listeners.https.ssl.truststore.location=/etc/connect/truststore.p12
listeners.https.ssl.truststore.password=<password>
listeners.https.ssl.client.auth=required
rest.extension.classes=dev.kfkit.kfkrbac.connect.RbacConnectRestExtension
kfkrbac.super.users=User:CN=connect-1,O=example;User:CN=connect-2,O=example
```

With `ssl.client.auth=requested` instead of `required`, a caller without a
certificate reaches the extension and is refused with 401.

## What it does not cover

- **What connectors do on the broker.** A connector's tasks read and write
  topics as the principal the worker connects with; that is decided by the
  broker, by that principal's ACLs and roles, not by the caller's.
- **A separate admin listener.** With `admin.listeners` set, the worker
  does not register extensions on that listener in 4.1.x, 4.2.0, 4.2.1,
  4.3.0 and 4.3.1: requests to `/admin/loggers` there are outside what the
  extension decides. This was read in the source of those releases, not
  run.
- **Authentication and tokens.** The worker authenticates callers; the
  extension does not.
- **Subjects.** There is no extension for a schema registry: a binding for
  subjects can be written, and nothing reads it.
