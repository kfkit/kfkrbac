# Metrics

kfkrbac can publish who accessed what, and with which verdict, as the access
metrics of [kfkwho](https://github.com/kfkit/kfkwho): the same MBeans, the
same tags and the same `kfkwho.*` settings. kfkwho's exporter rules, and the
dashboards and alerts built on them, work with kfkrbac as they are. What
differs is whose verdict is counted: kfkwho counts what the ACLs decided,
kfkrbac what the ACLs and the roles decided.

The metrics are off by default, and off means off: no MBean, no thread, and
no `kfkwho.*` setting is read.

## Turning them on

One line next to `authorizer.class.name`, the same on every node:

```properties
authorizer.class.name=dev.kfkit.kfkrbac.RbacAuthorizer
kfkrbac.metrics.enabled=true
```

Then restart, one node at a time. In the `apache/kafka` image the line is
the environment variable `KAFKA_KFKRBAC_METRICS_ENABLED=true`.

There is nothing to install next to the jar: it carries kfkwho 0.1.0 inside.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `kfkrbac.metrics.enabled` | boolean | `false` | Whether the verdicts are published as kfkwho's metrics. A value that is neither `true` nor `false` fails the start of the node with a message that names the key. |

Only brokers record. On a node that is only a controller the setting creates
nothing, because the requests a controller authorizes come from brokers; see
[Brokers only](https://github.com/kfkit/kfkwho/blob/v0.1.0/docs/config.md#brokers-only).

## What is recorded

Every verdict the authorizer gives the broker for an action of a request,
once it is final:

| The request | `result` |
|---|---|
| an ACL allows it | `ALLOWED` |
| no ACL allows it, a role bound to the principal covers it and no deny ACL matches | `ALLOWED` |
| a deny ACL matches it, whatever the roles say | `DENIED` |
| nothing allows it | `DENIED` |

The verdict recorded is the one the client got. Two things are not counted,
because they are not verdicts on a resource:

- The broker's second question, "may this principal do this on any resource
  of the type", which an idempotent producer is asked before it sends
  anything. Its answer names no resource. The produce requests that follow
  are counted, topic by topic. The broker asks that question only after
  `IDEMPOTENT_WRITE` on the cluster was denied, and that denial is a verdict
  and is counted: a producer that writes by a role, or by an ACL on its
  topics, shows one `DENIED` `IDEMPOTENT_WRITE` on `kafka-cluster` with the
  request type `INIT_PRODUCER_ID` each time it starts.
- A refusal to create or delete a role binding. The request of a resource
  owner is let through to where that is decided, and is counted as `ALLOWED`
  `ALTER` on the cluster even when what it asks for is then refused; the
  refusal and its reason go back to the caller and to the log of the
  controller.

## Everything else is kfkwho's

The settings, the MBeans and the exporter rules are kfkwho's own, of the
version in the jar, and are documented there:

- [Configuration](https://github.com/kfkit/kfkwho/blob/v0.1.0/docs/config.md):
  the `kfkwho.*` settings, such as how long an idle series lives, how many
  series may exist and how client ids are rewritten. They go into the same
  configuration, are read only when `kfkrbac.metrics.enabled` is `true`, and
  a wrong value then fails the start of the node with a message that names
  the key.
- [Metrics](https://github.com/kfkit/kfkwho/blob/v0.1.0/docs/metrics.md):
  the MBeans of the `kfkwho` domain, their tags and attributes, and what a
  series is.
- [The exporter rules](https://github.com/kfkit/kfkwho/blob/v0.1.0/jmx-exporter/kfkwho.yml)
  for the JMX Exporter, which turn the MBeans into series such as

  ```
  kfkwho_access_requests_total{principal="User:alice",client_id="billing",resource_type="TOPIC",resource="billing.orders",operation="WRITE",api="PRODUCE",result="ALLOWED"} 10.0
  ```

- [Performance](https://github.com/kfkit/kfkwho/blob/v0.1.0/docs/performance.md):
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
does not depend on this setting; see [Roles](roles.md).
