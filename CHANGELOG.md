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
  counted in the gauge `malformed-bindings`. Group membership is not
  honoured yet.
- `RbacAuthorizer` decides who may create and delete ACL records: a
  principal that the ACLs alone allow `ALTER` on the cluster, as before, or
  a `SystemAdmin` anything; a `ResourceOwner` role bindings on patterns
  inside what it owns and nothing else; everyone else nothing. A request is
  applied as a whole or not at all, and a refusal goes back to the caller
  with its reason. A record of a role that is not a well-formed binding is
  refused when it is created, and a binding's name may not start with
  `Group:`.
- No role but `SystemAdmin` lets its holder replace a user's SCRAM
  credentials: for `AlterUserScramCredentials`, `ClusterAdmin` does not
  grant `ALTER` on the cluster.
- A release workflow: a `vX.Y.Z` tag runs the whole check and publishes a
  GitHub Release with the jar, its SHA-256, a keyless cosign signature and a
  build provenance attestation. How to verify a released jar is in
  [docs/provenance](docs/provenance/README.md).
- `./gradlew publishToMavenLocal` produces the jar, sources, javadoc and a
  POM with what Maven Central asks for; the version comes from
  `-Pversion`.
