#!/bin/bash
# Build the release tarball for one version: the static linux/amd64 binary plus the files the
# installer puts next to it. Deterministic (same Go version, same flags, normalised tar), so
# anyone can rebuild a tag and get the published sha256.
#
#   scripts/release.sh v1.0.0 dist
set -euo pipefail
version=${1:?usage: release.sh vX.Y.Z OUTDIR}
out=${2:?usage: release.sh vX.Y.Z OUTDIR}
[[ $version =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "version must look like v1.2.3" >&2; exit 2; }
want_go=$(cat .go-version)
have_go=$(go env GOVERSION)
[ "$have_go" = "go$want_go" ] || { echo "release builds need go$want_go, this is $have_go" >&2; exit 1; }

name="emak-jobs-$version-linux-amd64"
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
mkdir -p "$stage/$name" "$out"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false \
	-ldflags "-s -w -X main.version=$version" -o "$stage/$name/emak-jobs" .
install -m 0644 deploy/emak-jobs.service README.md "$stage/$name/"
install -m 0755 deploy/emak-jobs-replay "$stage/$name/"
chmod 0755 "$stage/$name/emak-jobs"
tar --sort=name --mtime='2026-01-01 00:00:00Z' --owner=0 --group=0 --numeric-owner \
	-C "$stage" -cf - "$name" | gzip -9n > "$out/$name.tar.gz"
(cd "$out" && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256" && cat "$name.tar.gz.sha256")
