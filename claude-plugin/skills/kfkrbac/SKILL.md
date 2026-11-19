---
name: kfkrbac
description: Adopt and operate kfkrbac, role-based access control for Apache Kafka, an authorizer that adds roles and role bindings on top of the ACLs the broker already has. Use when installing its jar on brokers and controllers (the Kafka distribution, the apache/kafka image, Strimzi), deciding what happens to a cluster that ran without an authorizer, binding roles to users or groups, handing a prefix to its owner, keeping bindings in a file or bringing them over from a file of another shape, adding the extension for Kafka Connect, turning the access metrics on, or working out why a client is not authorized, why bindings disappeared, or why a binding took access away from everyone else.
---

# Adopting kfkrbac

kfkrbac is role-based access control for Apache Kafka®. `RbacAuthorizer` is the broker's
`StandardAuthorizer` with roles added: seven roles, and role bindings that give one to a user or a
group on a name, a prefix or the cluster. A binding is an ACL record in the cluster's own metadata,
so there is one jar, one line of configuration, and nothing else to run.

The project's documents are the source of truth; this skill is the order to do things in and the
mistakes to avoid. Read the section a step names before acting on it, and quote it to the user
rather than this file when the two differ:

- What it is, how a request is decided: https://github.com/kfkit/kfkrbac/blob/main/README.md
- Installation: https://github.com/kfkit/kfkrbac/blob/main/docs/install.md
- Provenance: https://github.com/kfkit/kfkrbac/blob/main/docs/provenance/README.md
- Roles: https://github.com/kfkit/kfkrbac/blob/main/docs/roles.md
- Bindings: https://github.com/kfkit/kfkrbac/blob/main/docs/bindings.md
- Delegation: https://github.com/kfkit/kfkrbac/blob/main/docs/delegation.md
- Groups: https://github.com/kfkit/kfkrbac/blob/main/docs/groups.md
- The command: https://github.com/kfkit/kfkrbac/blob/main/docs/command.md
- Bringing bindings over: https://github.com/kfkit/kfkrbac/blob/main/docs/import.md
- Kafka Connect: https://github.com/kfkit/kfkrbac/blob/main/docs/connect.md
- Metrics: https://github.com/kfkit/kfkrbac/blob/main/docs/metrics.md
- Settings: https://github.com/kfkit/kfkrbac/blob/main/docs/settings.md
- Troubleshooting: https://github.com/kfkit/kfkrbac/blob/main/docs/troubleshooting.md
- What changed between releases: https://github.com/kfkit/kfkrbac/blob/main/CHANGELOG.md

## Say this first

**What the line does depends on what the cluster runs today.** On a cluster that already runs
`StandardAuthorizer`, nothing changes until the first role is bound. A cluster that ran without an
authorizer starts enforcing ACLs the moment it gets the line: every request of a principal that is
not a super user is denied, the brokers' own requests to each other included. Ask which it is
before anything else.

**Roles only add access.** The ACLs are asked first and keep working; a deny ACL beats every role.
Do not promise roles of the user's own, groups from a directory or anything for schemas: read
"What is not there" in the README to the user.

## Before installing, find out

1. **Whether the cluster has an authorizer today** ("What changes when the line is set").
2. **The release of Kafka and its mode.** Kafka 4.1 or later, in KRaft mode ("Before you start").
3. **How the nodes are run**: the Kafka distribution, the `apache/kafka` image or Strimzi. The
   Strimzi steps have not been run; their section lists what is not verified.
4. **Who is in `super.users`**, or is allowed by an ACL to alter the cluster: one of them binds the
   first role. On a cluster without an authorizer the nodes' own principals go there too.
5. **The value of `allow.everyone.if.no.acl.found`.** With `true`, the first binding on a pattern
   closes what it names to everyone else. To use roles it stays at its default, `false`.
6. **Whether a tool keeps the ACLs equal to a list in a file.** A binding that is not in its list
   is an ACL it deletes ("Tools that make the ACLs equal to a list" in Bindings).
7. **What names the authorizer's class, and what makes the principals.** A dashboard or an
   exporter rule that names the class `StandardAuthorizer` has to follow. A principal builder of
   the user's own must not produce the names of the roles as principal types ("Reserved names").
8. **How an application's resources are named.** A binding on a prefix covers the topics, the
   consumer groups and the transactional IDs that start with it; a name outside the prefix needs a
   binding of its own ("Narrowing a binding").
9. **Whether Kafka Connect is in scope**, and how its workers authenticate callers: HTTP basic
   authentication or mutual TLS ("Who is calling").
10. **Whether there are role bindings to bring over**, and in which shape of file.

## Install

1. Take the jar from the releases page and verify it before it goes on a node ("A released jar" in
   Provenance).
2. On a cluster without an authorizer, decide first what happens to the clients it has, then
   follow "A cluster that ran without an authorizer: the steps".
3. On every node, controllers included: the jar first, then `authorizer.class.name`, from "A node
   from the Kafka distribution", "The `apache/kafka` image" or "Strimzi". In the image the two
   variables go next to a full configuration, not instead of one.
4. Restart the nodes one at a time. Check each: its log says which authorizer it runs (the `grep`
   in the distribution's section), and a client that an existing ACL allows still works.
5. When every node is back, and not before, bind the first role ("The first binding").

Kafka Connect has its own "Installation": the jar on the class path of every worker, the extension
added to `rest.extension.classes` after any the worker already has, and `kfkrbac.super.users` so
that somebody can create connectors before the first binding. To take kfkrbac out, follow
"Removing it": the bindings stay in the metadata and mean nothing to the plain authorizer.

## Binding roles

- **Choose the role** from "What each role grants" in Roles. A consumer takes `DeveloperRead` and
  a producer `DeveloperWrite`, each on the prefix of its topics.
- **Bind** with the command of the jar or with the ACL tool; both write the same record ("One
  binding for an application"). `who-can` and `what-can` answer before a client fails; neither
  counts what `super.users` and `allow.everyone.if.no.acl.found` allow.
- **Delegate** with `ResourceOwner`: the owner of a prefix hands out roles inside it, and nowhere
  else ("Who may bind what" in Delegation).
- **Groups**: membership is a record in the same metadata, managed by whoever manages every ACL.
  A set of permissions that no role names goes to a group as ordinary ACLs ("An ACL for a group").
- **In a file**: `apply` creates what is in the file and deletes the bindings and the memberships
  that are not; an empty array deletes all of them. `--dry-run` changes nothing, `--no-delete`
  only adds ("apply" in The command).
- **Bringing over**: `import` only adds, and skips and reports what it cannot write exactly. Run
  it with `--dry-run` first ("Nothing is guessed").

## The settings, and what they trade

The table is in Settings. A setting is read at start; a wrong value fails the start of the node
with a message that names the setting, or stops the worker at start-up.

- **`allow.everyone.if.no.acl.found=true`**, the broker's own, keeps every client working while a
  cluster that had no authorizer is closed step by step. While no ACL names the cluster, every
  principal may create and delete ACLs and bindings. It is a way in, not a way to run with roles.
- **`kfkrbac.metrics.enabled`** (`false`). `true` publishes every verdict as the access metrics of
  kfkwho; only brokers record. What recording costs is linked from "Everything else is kfkwho's".
- **`kfkrbac.denied.log.interval.seconds`** (`60`). The line for the same denied request is
  written at most once in that time. `0` logs every denial, and clients retry many times a second.
- **`kfkrbac.super.users`**, in a worker: these principals pass every request. With mutual TLS the
  workers' own principals have to be there, or the leader refuses what a worker forwards.
- **`ResourceOwner`** lets the owner of a prefix bind inside it. Its holder also sees every ACL
  and binding of the cluster ("What a delegate can see").
- **`ClusterAdmin`** does not manage access, but it can change the brokers' configuration, how
  clients authenticate included: bind it only to those trusted with the cluster. An ACL that
  allows `Alter` on the cluster is as much as `SystemAdmin` for managing access.

## Do not

- **Do not set the line on a node that does not have the jar.** The node does not start:
  `java.lang.ClassNotFoundException: dev.kfkit.kfkrbac.RbacAuthorizer`.
- **Do not leave a node on the plain authorizer, and do not bind before every node is back.** A
  broker that runs it ignores bindings; a controller applies none of the rules for creating them.
- **Do not bind the first role on a pattern while `allow.everyone.if.no.acl.found=true`** before
  everyone who uses what the pattern covers is bound or allowed: the binding closes it to them.
  When removing kfkrbac with that setting, delete the binding records too.
- **Do not leave a tool that keeps the ACLs equal to a list unprepared, or give the ACL tool a
  wide `--remove` filter.** The bindings are deleted, and nothing of kfkrbac logs it.
- **Do not write a binding with an operation other than `All`, a `Deny` or a host other than
  `*`.** The cluster refuses it; one written before the jar was installed grants nothing ("A
  record that is not a binding").
- **Do not try to deny with a role or a group.** A `DENY` ACL that names a group is refused. Deny
  the users by name, with ordinary ACLs.
- **Do not take a group for more than it is** ("What a group is not"): it is not nested, and a
  group in `super.users` makes none of its members a super user.
- **Do not expect a binding for subjects to grant anything.** It can be written; nothing reads it.
- **Do not move the workers off `connect.protocol=sessioned`**, the default: with an older
  protocol nothing would verify the requests between workers.

## When something is off

Go to Troubleshooting with the symptom. Start with the broker's log: the logger
`dev.kfkit.kfkrbac.denied` has one line for a denied request, with who asked, for what, the cause
and the roles that principal holds ("A client is "not authorized""). The ones people hit first:

- A consumer gets `GroupAuthorizationException`: the binding is on the prefix of the topics and
  the group's name does not start with it.
- A producer gets `Cluster authorization failed`: it may write no topic at all.
- Binding ends in `needs ALTER permission.`: the caller holds neither `ResourceOwner` nor a right
  to alter the cluster.
- Bindings are gone, or a binding closed a topic to everyone else: "Bindings disappear" and "A
  binding took access away from everyone else".
- A worker answers 503 with `the role bindings have not been read from the cluster yet`: the
  extension has not read them for the first time ("The authorizer is "not ready"").

Worth watching: the gauge `malformed-bindings` is 0 on a healthy cluster, and in a worker
`bindings-age-ms` grows while reads of the bindings fail.
