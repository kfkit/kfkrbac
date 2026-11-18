# Provenance

How to check that a kfkrbac jar is the one this repository built, and what
is kept here as evidence of when each release existed.

There is no release yet, so the commands of this page have not been run on
one.

## A released jar

Every release `vX.Y.Z` is built by the [release workflow](../../.github/workflows/release.yml)
from the tag, after the whole check has passed, and carries three files:

| File | What |
|---|---|
| `kfkrbac-X.Y.Z.jar` | the jar to copy to `libs/` |
| `kfkrbac-X.Y.Z.jar.sha256` | its SHA-256 |
| `kfkrbac-X.Y.Z.jar.sigstore.json` | a [Sigstore](https://www.sigstore.dev) bundle: the signature, the short-lived certificate GitHub's OIDC token was exchanged for, and the transparency log entry |

The workflow also records a build provenance attestation for the jar with
GitHub.

Download the three files into one directory and check them, from the
weakest proof to the strongest:

<!-- not run: it needs a release, and there is none yet -->
```bash
X=X.Y.Z

# The download is complete.
sha256sum -c "kfkrbac-$X.jar.sha256"

# The jar was signed by a workflow of this repository, and the signature
# is in the public transparency log. Needs cosign 2 or later.
cosign verify-blob "kfkrbac-$X.jar" \
  --bundle "kfkrbac-$X.jar.sigstore.json" \
  --certificate-identity-regexp '^https://github.com/kfkit/kfkrbac/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

# The jar was built by this repository's release workflow from the tag.
gh attestation verify "kfkrbac-$X.jar" --repo kfkit/kfkrbac
```

`cosign verify-blob` prints `Verified OK`. The certificate in the bundle
names the workflow and the tag it ran for; to pin both, use
`--certificate-identity "https://github.com/kfkit/kfkrbac/.github/workflows/release.yml@refs/tags/vX.Y.Z"`
instead of the regular expression.

The jar's manifest names its version:

<!-- not run: it needs a release, and there is none yet -->
```bash
unzip -p "kfkrbac-$X.jar" META-INF/MANIFEST.MF | grep Implementation-Version
```

## Release hashes

`release-hashes.txt` lists the signed tags of the first and the current
release with their commit and source-archive hashes, for timestamping.
There is no release yet, so it lists none. Once a release exists, the file
gets its line and an [OpenTimestamps](https://opentimestamps.org) proof,
`release-hashes.txt.ots`, that the file, and so the code it names, existed
no later than the Bitcoin block the proof is attached to. The proof depends
neither on GitHub nor on the signatures above.

Check that a line still matches its tag:

<!-- not run: it needs a release, and there is none yet -->
```bash
git rev-parse vX.Y.Z vX.Y.Z^{commit}
git archive --format=tar vX.Y.Z | shasum -a 256
```

and, once the proof exists, read it:

<!-- not run: it needs a release, and there is none yet -->
```bash
uvx --from opentimestamps-client ots info docs/provenance/release-hashes.txt.ots
```
