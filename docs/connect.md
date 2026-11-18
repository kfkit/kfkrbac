# Kafka Connect

The same jar applies the same role bindings to the REST interface of Kafka
Connect: a request is allowed when a role bound to the caller covers the
connector, and refused otherwise, saying which operation on which connector
was missing.

> Status: the extension is built, its filter is tested request by request,
> and the integration tests run it in two Connect workers of Kafka 4.1.2,
> 4.2.2, 4.3.1 and 4.4.0 with basic authentication, forwarding between
> them included. Mutual TLS has not run in a real worker yet.

## Installation

Put the jar on the worker's class path, on every worker of the Connect
cluster, and add one line to the worker's settings:

```properties
rest.extension.classes=dev.kfkit.kfkrbac.connect.RbacConnectRestExtension
```

```bash
cp kfkrbac-<version>.jar "$KAFKA_HOME/libs/"
```

The jar brings nothing the worker already has; the worker provides the
REST classes it uses.

The extension reads the bindings from the Kafka cluster the worker uses,
with the worker's own connection settings (`bootstrap.servers`,
`security.protocol`, `sasl.*`, `ssl.*`). The principal the worker connects
with must be allowed to list ACLs: `Describe` on the cluster.

```bash
kafka-acls.sh --bootstrap-server localhost:9092 --add \
  --allow-principal User:connect --operation Describe --cluster
```

The worker must use `connect.protocol=sessioned`, its default: see
[Requests between workers](#requests-between-workers).

## Settings

In the worker's settings, next to the line above.

| Setting | Default | What |
|---|---|---|
| `kfkrbac.super.users` | none | Principals that pass every request, separated by `;`: `User:admin;User:CN=connect-1,O=example`. |
| `kfkrbac.refresh.interval.ms` | `5000` | How often the bindings are read again. A binding created on the cluster is in force within this time. |

A setting that cannot be read stops the worker at start-up.

## Who is calling

Whoever the worker authenticated, as a principal of type `User`:

- the user of HTTP basic authentication (`User:alice`), when the worker
  authenticates with it;
- otherwise the subject of the client certificate of a mutual TLS
  connection, as the broker writes it by default: `User:CN=alice,O=example`.

An anonymous request is refused with 401. Authenticating callers is the
worker's job; the extension only reads what the worker established.

## What each request takes

A connector is the resource `Connector:<name>`: a binding on the prefix
`Connector:billing-` covers the connectors whose names start with
`billing-`, and a cluster role covers every connector. What each role grants
on connectors is in [roles.md](roles.md).

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
  Groups are in [groups.md](groups.md).
- A refusal is a 403 whose body says what was missing:

  ```json
  {"error_code":403,"message":"kfkrbac: User:alice may not Create connector payments-sink: no role bound to it grants Create on that connector"}
  ```

A binding for connectors, with the ACL tool and with the command:

```bash
kafka-acls.sh --bootstrap-server localhost:9092 --add \
  --allow-principal 'DeveloperManage:User:alice' --operation All \
  --topic Connector:billing- --resource-pattern-type prefixed

kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand bind --bootstrap-server localhost:9092 \
  --role DeveloperManage --member User:alice --prefix billing- --connector
```

## Before the bindings are read, and when they cannot be

Until the first read of the bindings succeeds, every request but
`GET /health` is refused with 503. When a later read fails, the bindings
read last stay in force, and the gauges in the MBean
`kfkrbac:type=rest-bindings` say so:

| Gauge | What |
|---|---|
| `bindings-age-ms` | How old the bindings in force are: since the last read that succeeded, or since the start while none has. It grows while reads fail. |
| `bindings` | How many role bindings are in force. |

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
`kfkrbac.super.users` for the leader to let it through.

## What it does not cover

- **What connectors do on the broker.** A connector's tasks read and write
  topics as the principal the worker connects with; that is decided by the
  broker, by that principal's ACLs and roles, not by the caller's.
- **A separate admin listener.** With `admin.listeners` set, the worker
  does not register extensions on that listener in 4.1.x, 4.2.0, 4.2.1,
  4.3.0 and 4.3.1: requests to `/admin/loggers` there are outside what the
  extension decides.
- **Authentication and tokens.** The worker authenticates callers; the
  extension does not.
