# Metrics

kfkrbac can publish who accessed what on a cluster of Apache Kafka®, and
with which verdict, as the access
metrics of [kfkwho](https://github.com/kfkit/kfkwho): the same MBeans, the
same tags and the same `kfkwho.*` settings. kfkwho's exporter rules, and the
dashboards and alerts built on them, work with kfkrbac as they are. What
differs is whose verdict is counted: kfkwho counts what the ACLs decided,
kfkrbac what the ACLs and the roles decided.

The metrics are off by default, and off means off: no MBean, no thread, and
no `kfkwho.*` setting is read.

> Status: the commands of this page are run by a test of the integration
> suite (`DocsIT`) on a broker of the newest supported release, and the
> output shown is what they printed there. The exporter rules and the
> series they make are read by another test of that suite
> (`MetricsBrokerIT`), on every supported release.

## Turning them on

One line next to `authorizer.class.name`, the same on every node:

```properties
authorizer.class.name=dev.kfkit.kfkrbac.RbacAuthorizer
kfkrbac.metrics.enabled=true
```

Then restart, one node at a time. In the `apache/kafka` image the line is
the environment variable `KAFKA_KFKRBAC_METRICS_ENABLED=true`
([Installation](install.md#the-apachekafka-image)).

There is nothing to install next to the jar: it carries kfkwho 0.1.1 inside.
The setting is in the table of [Settings](settings.md): a value that is
neither `true` nor `false` fails the start of the node with a message that
names the key.

### On a node

The node of the example is the one of [Installation](install.md): started
from the distribution, at `localhost:9092`, with `RbacAuthorizer`, the users
`User:admin`, who is in `super.users`, `User:alice` and `User:bob`, their
files of client properties, and a topic `billing.orders`. Its JMX port is
9999. `User:bob` holds `DeveloperWrite` on the prefix `billing.`:

<!-- run -->
```bash
kafka-run-class.sh dev.kfkit.kfkrbac.RbacCommand bind \
  --bootstrap-server localhost:9092 --command-config admin.properties \
  --role DeveloperWrite --member User:bob --prefix billing.
```

<!-- output -->
```text
Bound DeveloperWrite to User:bob on prefix billing.
```

Without the setting the node has no MBean of the `kfkwho` domain, and
Kafka's JMX tool finds none and fails:

<!-- run: exit 1 -->
```bash
kafka-jmx.sh --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi \
  --object-name 'kfkwho:*' --one-time true --report-format properties
```

<!-- output: excerpt -->
```text
No matched attributes for the queried objects [kfkwho:*].
```

Add the line and restart:

<!-- run -->
```bash
echo 'kfkrbac.metrics.enabled=true' >> "$KAFKA_HOME/config/server.properties"
```

<!-- restart -->

`User:bob` writes a record, which his role allows, and `User:alice`, who
holds nothing, tries to:

<!-- run -->
```bash
echo 'first order' | kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --command-config bob.properties --topic billing.orders
```

<!-- prints nothing -->

<!-- run -->
```bash
echo 'not written' | kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --command-config alice.properties --topic billing.orders
```

<!-- output: excerpt -->
```text
org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [billing.orders]
```

On Kafka 4.1 the console producer takes the client properties as
`--producer.config`, not as `--command-config`.

Both are in the MBeans of the type `access`, each with the verdict the
client got. What `User:bob` produced is one series, with its count:

<!-- run -->
```bash
kafka-jmx.sh --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi \
  --object-name 'kfkwho:type=access,api=PRODUCE,*' --attributes request-total \
  --one-time true --report-format properties
```

<!-- output: excerpt -->
```text
kfkwho:type=access,principal="User:bob",client-id=console-producer,resource-type=TOPIC,resource=billing.orders,operation=WRITE,api=PRODUCE,result=ALLOWED:request-total=1.0
```

And the series of `User:alice`, here without their counts, are what she was
denied:

<!-- run -->
```bash
kafka-jmx.sh --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi \
  --object-name 'kfkwho:type=access,principal="User:alice",*' --attributes request-total \
  --one-time true --report-format properties | grep '^kfkwho' | sed 's/:request-total=.*//' | sort
```

<!-- output: excerpt -->
```text
kfkwho:type=access,principal="User:alice",client-id=console-producer,resource-type=CLUSTER,resource=kafka-cluster,operation=IDEMPOTENT_WRITE,api=INIT_PRODUCER_ID,result=DENIED
kfkwho:type=access,principal="User:alice",client-id=console-producer,resource-type=TOPIC,resource=billing.orders,operation=DESCRIBE,api=METADATA,result=DENIED
```

Only brokers record. On a node that is only a controller the setting creates
nothing, because the requests a controller authorizes come from brokers; see
[Brokers only](https://github.com/kfkit/kfkwho/blob/v0.1.1/docs/config.md#brokers-only).

## What is recorded

Every verdict the authorizer gives the broker for an action of a request,
once it is final:

| The request | `result` |
|---|---|
| an ACL allows it | `ALLOWED` |
| no ACL allows it, a role bound to the principal covers it and no deny ACL matches | `ALLOWED` |
| a deny ACL matches it, whatever the roles say | `DENIED` |
| nothing allows it | `DENIED` |

The verdict recorded is the one the client got. What is not counted is what
does not refuse a request:

- A denial the broker asks for without logging it. The broker asks some
  questions only to find out, and logs no denial for them in its own
  authorizer log: whether to list a topic in a reply that lists all topics,
  or whether a producer may write idempotently to the whole cluster before
  it is given a producer ID. Such a denial is not counted; the same
  question allowed is, as `ALLOWED`. A `DENIED` series is therefore a
  request, or a part of one, that was refused.
- The broker's second question, "may this principal do this on any resource
  of the type", which an idempotent producer is asked before it sends
  anything. The broker asks it only after `IDEMPOTENT_WRITE` on the cluster
  was denied, without logging. When a role or an ACL lets the producer write
  to some topic, the producer is not refused and nothing denied is counted:
  a healthy producer that writes by a role leaves no `DENIED` series. When
  nothing does, the producer is refused, and the denial of
  `IDEMPOTENT_WRITE` on `kafka-cluster` is counted, with the request type
  `INIT_PRODUCER_ID`, as for `User:alice` above. The second question's own
  answer names no resource and is not a series; the produce requests that
  follow are counted, topic by topic.
- A refusal to create or delete a role binding. The request of a resource
  owner is let through to where that is decided, and is counted as `ALLOWED`
  `ALTER` on the cluster even when what it asks for is then refused; the
  refusal and its reason go back to the caller and to the log of the
  controller.

kfkwho's own authorizer, of the version in the jar, counts every denial,
those the broker does not log included; with kfkrbac the series of a
producer that writes by an ordinary ACL change in the same way.

## Everything else is kfkwho's

The settings, the MBeans and the exporter rules are kfkwho's own, of the
version in the jar, and are documented there:

- [Configuration](https://github.com/kfkit/kfkwho/blob/v0.1.1/docs/config.md):
  the `kfkwho.*` settings, such as how long an idle series lives, how many
  series may exist and how client ids are rewritten. They go into the same
  configuration, are read only when `kfkrbac.metrics.enabled` is `true`, and
  a wrong value then fails the start of the node with a message that names
  the key.
- [Metrics](https://github.com/kfkit/kfkwho/blob/v0.1.1/docs/metrics.md):
  the MBeans of the `kfkwho` domain, their tags and attributes, and what a
  series is.
- [The exporter rules](https://github.com/kfkit/kfkwho/blob/v0.1.1/jmx-exporter/kfkwho.yml)
  for the JMX Exporter, which turn the MBeans into series such as

  ```
  kfkwho_access_requests_total{principal="User:alice",client_id="billing",resource_type="TOPIC",resource="billing.orders",operation="WRITE",api="PRODUCE",result="ALLOWED"} 10.0
  ```

- [Performance](https://github.com/kfkit/kfkwho/blob/v0.1.1/docs/performance.md):
  what recording costs and the budget it is held to.

## Next to kfkwho's own jar

The classes of kfkwho inside the kfkrbac jar are in a package of their own,
`dev.kfkit.kfkrbac.shaded.kfkwho`. A kfkwho jar that is still in `libs/` is
not in the way and is not used: a broker has one authorizer, the one
`authorizer.class.name` names.

To move a cluster from kfkwho's authorizer to kfkrbac, change
`authorizer.class.name` and add `kfkrbac.metrics.enabled=true`; the
`kfkwho.*` settings stay as they are, and so do the names of the series.

## The authorizer's own metric

`malformed-bindings`, a gauge among the plugin metrics of the authorizer,
does not depend on this setting:
[Bindings](bindings.md#a-record-that-is-not-a-binding) shows how to read
it. The extension for Kafka Connect has two gauges of its own, in
[Kafka Connect](connect.md#before-the-bindings-are-read-and-when-they-cannot-be).
