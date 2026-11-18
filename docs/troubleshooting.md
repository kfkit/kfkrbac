# Troubleshooting

## A client is "not authorized"

The client's error names a topic or a group and nothing more. The broker's
log has the rest: one line for the denied request that says who asked, for
what, and what was missing. This one is from a broker of the integration
tests, where `User:alice` holds `DeveloperRead` on the prefix `billing.` and
consumes in the group `payments.app`:

```
[2026-11-18 11:05:15,256] INFO Denied User:alice DESCRIBE on GROUP 'payments.app' (request FindCoordinator): no ACL and no role allows it; roles held for GROUP: DeveloperRead on prefix billing. (dev.kfkit.kfkrbac.denied)
```

Read it left to right:

| Part | Meaning |
|---|---|
| `User:alice` | the principal that asked |
| `DESCRIBE on GROUP 'payments.app'` | the operation, the resource type and the name of the resource, as the broker asked the authorizer |
| `(request FindCoordinator)` | the request the client sent |
| `no ACL and no role allows it` | the cause, one of the three below |
| `roles held for GROUP: ...` | what the principal does hold for that resource type |

Here the role is right and the pattern is not: the binding is on `billing.`
and the group is `payments.app`. Call the group `billing.app`, or bind the
role on the group as well ([roles.md](roles.md), "A producer and a
consumer"). [`what-can` and `who-can`](command.md) answer the same
questions before a client fails.

### The cause

| The line says | What happened | What to do |
|---|---|---|
| `no ACL and no role allows it` | no ACL allows the request and no role bound to the principal grants it | read the roles the line names, below |
| `a deny ACL matches` | a deny ACL matches the request; a deny ACL beats every role and every allow ACL | find the ACL in the broker's authorizer log, see "Where each line goes"; no binding helps until it is deleted |
| `the roles could not be consulted, see "Role lookup failed" in the broker's log` | the role lookup failed, and nothing is granted on failure | the `ERROR` line `Role lookup failed` before it has the exception |

For `ALTER` on the cluster with the request `AlterUserScramCredentials` the
first cause reads `no ACL allows it, and for this request no role but
SystemAdmin does`: see [roles.md](roles.md), `ClusterAdmin`.

### The roles the line names

After `no ACL and no role allows it` the line lists every binding of the
principal that names resources of that type, the cluster roles among them:

```
Denied User:bob READ on TOPIC 'biling.invoices' (request Fetch): no ACL and no role allows it; roles held for TOPIC: DeveloperRead on prefix billing.
Denied User:bob WRITE on TOPIC 'billing.invoices' (request Fetch): no ACL and no role allows it; roles held for TOPIC: DeveloperRead on prefix billing. (does not grant WRITE on TOPIC)
Denied User:dave READ on TOPIC 'billing.invoices' (request Fetch): no ACL and no role allows it; roles held for TOPIC: none
```

- A binding with nothing after it grants the operation, on a pattern that
  does not cover the resource: the first line is a topic name that was
  mistyped, `biling.` for `billing.`.
- `(does not grant WRITE on TOPIC)`: the role is the wrong one for the
  operation, whatever its pattern.
- `none`: the principal holds no role for that resource type. It may hold
  roles for others: a binding on groups only is not listed for a topic.
- `as Group:payments` after a binding: the principal holds it as a member
  of that [group](groups.md).

A binding is written as [the command](command.md) writes it: `prefix
billing.`, `name orders`, `every name`, `groups with prefix billing.`,
`the cluster`. At most 20 are named, and the line says how many more there
are.

### "on any TOPIC"

The broker also asks whether a principal may do something on any resource
of a type: an idempotent producer is asked it for `WRITE` on topics before
it sends anything. When that is denied there is no resource to name:

```
Denied User:bob WRITE on any TOPIC (request InitProducerId): no ACL and no role allows it; roles held for TOPIC: DeveloperRead on prefix billing. (does not grant WRITE on TOPIC)
Denied User:alice WRITE on any TOPIC (request InitProducerId): deny ACLs cover everything a role or a group allows it on; roles held for TOPIC: DeveloperWrite on prefix orders
```

The second cause is the one a deny ACL has here: a role of the principal
grants the operation, or an ACL of one of its [groups](groups.md) allows
it, and deny ACLs cover every resource it is granted on. An allow ACL that a deny ACL cancels is not told apart for this
question: it reads `no ACL and no role allows it`.

### How often a line is written

A client retries a denied request many times a second. The line for the
same principal, operation and resource is written at most once in 60
seconds, and the line that follows says what was left out:

```
Denied User:bob READ on TOPIC 'orders' (request Fetch): no ACL and no role allows it; roles held for TOPIC: none; denied 42 more times since this was last logged
```

| Setting | Default | Meaning |
|---|---|---|
| `kfkrbac.denied.log.interval.seconds` | `60` | seconds before the same denial is logged again; `0` logs every denial. A value that is not a whole number, 0 or more, fails the authorizer's configuration, and so the broker's start, with the name of the setting. |

Only denials the broker itself wants logged are written: a request that
only asks what a client may see, such as a listing of all topics, is not a
failure and leaves no line.

The authorizer remembers at most 10,000 different denied requests at a
time, each by a principal and a resource name of at most 256 characters; a
longer name is shown cut, and told apart from others by its first 256
characters. When more are denied, the ones not denied for an interval are
forgotten first, and then all of them. A line says so, and a request that
was forgotten is logged again the next time it is denied. This one is from
a test, where the limit is 16:

```
Forgot 16 of the denied requests remembered so as not to log them again, to stay within 16; 3 denials that were not logged will not be counted in a later line
```

## Where each line goes

| Logger | Level | What |
|---|---|---|
| `dev.kfkit.kfkrbac.denied` | `INFO` | the line above: a request that was denied, and why |
| `kafka.authorizer.logger` | `INFO` | the broker's own line for a denied request, unchanged |
| `kafka.authorizer.logger` | `DEBUG` | the broker's own line for a request an ACL allows, and the same line for a request a role or an ACL of a group allows |
| `dev.kfkit.kfkrbac.RbacAuthorizer` | `ERROR` | `Role lookup failed`, with the exception |

`dev.kfkit.kfkrbac.denied` is a logger of its own so that these lines can
be sent to a file of their own or silenced without touching anything else:
give the logger of that name an appender, or the level `OFF`, in the
broker's logging configuration. With the configuration a broker ships it
goes where the broker's other messages go.
The line names the principal, the operation, the resource, the request, the
cause and the roles of that principal: no host, no client ID, and nothing
about any other principal.

`kafka.authorizer.logger` is the broker's log of what its authorizer
decided, `kafka-authorizer.log` in the configuration a broker ships. It says
what was decided in the end, once for each resource of a request:

```
Principal = User:bob is Denied operation = READ from host = 127.0.0.1 on resource = Topic:LITERAL:orders for request = Fetch with resourceRefCount = 1 based on rule DefaultDeny
Principal = User:bob is Allowed operation = READ from host = 127.0.0.1 on resource = Topic:LITERAL:billing.invoices for request = Fetch with resourceRefCount = 1 based on rule RoleBinding(role=DeveloperRead, member=User:bob, resourceType=TOPIC, resourceName=billing., patternType=PREFIXED)
```

The first is the broker's own line, at `INFO`, as it is without roles; a
deny ACL that matched is named in it, as `MatchingAcl(...)`. The second is
written at `DEBUG`, where the broker writes the requests an ACL allows: a
request a role allows reads the same up to the rule, which is the binding,
with the resource type, the name and the pattern type of its ACL record. A
request that an ACL of a group allows its member names the group instead:
`based on rule GroupAcl(group=Group:payments)`. Neither ever leaves a
`Denied` line there.
