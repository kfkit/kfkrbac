# Installation

> There is no release yet, and the authorizer has not run on a broker; see
> the status in the [README](../README.md). This page says how the jar is
> installed once it is released.

kfkrbac is one jar and one line of configuration, on every node of the
cluster, controllers included:

```bash
cp kfkrbac-<version>.jar "$KAFKA_HOME/libs/"
echo 'authorizer.class.name=dev.kfkit.kfkrbac.RbacAuthorizer' >> "$KAFKA_HOME/config/server.properties"
```

Then restart, one node at a time.

## Before you start

- **Kafka 4.1 or later, KRaft.** See [Compatibility](../README.md#compatibility).
- **The jar.** Each release on
  [GitHub Releases](https://github.com/kfkit/kfkrbac/releases) carries the
  jar, its SHA-256 and a signature. Verify it before you copy it to a
  broker: [Provenance](provenance/README.md) has the commands. To build it
  yourself, `./gradlew jar` writes `build/libs/kfkrbac-<version>.jar`.
