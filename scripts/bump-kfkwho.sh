#!/usr/bin/env bash
# Copyright 2026 the kfkrbac authors
# SPDX-License-Identifier: Apache-2.0
#
# Moves the kfkwho bundled in the jar to a release of kfkwho: the version and
# both SHA-256 in build.gradle.kts, and the links of docs/ that point at
# kfkwho's documents of the version bundled. The changelog entry, the build
# and the pull request are left to whoever runs it (AGENTS.md, "A new
# kfkwho").
#
#   scripts/bump-kfkwho.sh            the latest release of kfkwho
#   scripts/bump-kfkwho.sh 0.2.0      that release
#
# It prints the version it moved to, or nothing when the bundled one is
# already that version. It changes no file unless every check passed.
set -euo pipefail

repo=https://github.com/kfkit/kfkwho
root=$(git rev-parse --show-toplevel)
build="$root/build.gradle.kts"

sha256() {
    if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1
}

current=$(sed -n 's/^val kfkwhoVersion = "\(.*\)"$/\1/p' "$build")
[[ -n $current ]] || { echo "no kfkwhoVersion in $build" >&2; exit 1; }

if [[ $# -gt 0 ]]; then
    version=${1#v}
else
    # The redirect of /releases/latest names the tag; no API, no token.
    location=$(curl -fsSI "$repo/releases/latest" | tr -d '\r' | sed -n 's/^[Ll]ocation: //p')
    version=${location##*/tag/v}
fi
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "not a release version: '$version'" >&2; exit 1; }
[[ $version == "$current" ]] && exit 0
# Only forward: an older release is never bundled again by this script.
[[ $(printf '%s\n%s\n' "$current" "$version" | sort -V | tail -1) == "$version" ]] \
    || { echo "kfkwho $version is older than the bundled $current" >&2; exit 1; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

curl -fsSL -o "$work/kfkwho.jar" "$repo/releases/download/v$version/kfkwho-$version.jar"
curl -fsSL -o "$work/kfkwho.jar.sha256" "$repo/releases/download/v$version/kfkwho-$version.jar.sha256"
curl -fsSL -o "$work/kfkwho.yml" "$repo/raw/v$version/jmx-exporter/kfkwho.yml"

jar=$(sha256 "$work/kfkwho.jar")
published=$(cut -d' ' -f1 "$work/kfkwho.jar.sha256")
[[ $jar == "$published" ]] || { echo "the jar's SHA-256 $jar is not the one the release states, $published" >&2; exit 1; }
# The release's jar is the one on Maven Central, byte for byte; a release
# before 0.1.1 is not there, and that is not an error.
central="https://repo1.maven.org/maven2/dev/kfkit/kfkwho/$version/kfkwho-$version.jar"
if curl -fsSL -o "$work/central.jar" "$central" 2>/dev/null; then
    [[ $(sha256 "$work/central.jar") == "$jar" ]] || { echo "the jar on Maven Central is not the release's jar" >&2; exit 1; }
fi
rules=$(sha256 "$work/kfkwho.yml")

sed -i.bak \
    -e "s/^val kfkwhoVersion = \".*\"$/val kfkwhoVersion = \"$version\"/" \
    -e "s/^val kfkwhoSha256 = \".*\"$/val kfkwhoSha256 = \"$jar\"/" \
    -e "s/^val kfkwhoRulesSha256 = \".*\"$/val kfkwhoRulesSha256 = \"$rules\"/" \
    "$build"
rm "$build.bak"
for doc in "$root"/docs/*.md; do
    sed -i.bak \
        -e "s#kfkit/kfkwho/blob/v$current/#kfkit/kfkwho/blob/v$version/#g" \
        -e "s/carries kfkwho $current inside/carries kfkwho $version inside/" \
        "$doc"
    rm "$doc.bak"
done
echo "$version"
