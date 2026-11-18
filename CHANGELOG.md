# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/); until v1.0 the
major version is 0.

## Unreleased

### Added

- `RbacAuthorizer` tells a deny ACL from no ACL: a role can allow a request
  no ACL allows, never one a deny ACL denies, in `authorize` and in
  `authorizeByResourceType`.
- A JMH benchmark of `authorize`: `./gradlew jmh`.
- The seven roles in code and in [docs/roles.md](docs/roles.md), with what
  each grants on topics, groups, transactional IDs, the cluster, subjects and
  connectors.
- `RbacAuthorizer` reads role bindings from the ACL records the broker
  delivers and decides with them: a principal that holds a role on a pattern,
  or on the cluster, is allowed what the role grants there. A record of a
  role that is not a well-formed binding grants nothing, is logged once and
  counted in the gauge `malformed-bindings`. Who may create a binding is not
  checked yet, and group membership is not honoured.
