# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/); until v1.0 the
major version is 0.

## Unreleased

### Added

- `RbacAuthorizer` tells a deny ACL from no ACL: a role can allow a request
  no ACL allows, never one a deny ACL denies, in `authorize` and in
  `authorizeByResourceType`. No roles can be defined yet, so the verdict is
  still the ACLs'.
- A JMH benchmark of `authorize`: `./gradlew jmh`.
- The seven roles in code and in [docs/roles.md](docs/roles.md), with what
  each grants on topics, groups, transactional IDs, the cluster, subjects and
  connectors. Nothing reads bindings yet, so no role grants anything on a
  broker.
- A release workflow: a `vX.Y.Z` tag runs the whole check and publishes a
  GitHub Release with the jar, its SHA-256, a keyless cosign signature and a
  build provenance attestation. How to verify a released jar is in
  [docs/provenance](docs/provenance/README.md).
- `./gradlew publishToMavenLocal` produces the jar, sources, javadoc and a
  POM with what Maven Central asks for; the version comes from
  `-Pversion`.
