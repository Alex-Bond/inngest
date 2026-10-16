#!/usr/bin/env bash
# Builds trustis/inngest:<VERSION> for linux/amd64 from the current checkout.
# Run from anywhere; outputs (UI dist, binary) go to $OUT, never into .trustis-build/.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
HERE="$REPO/.trustis-build"
VERSION="${VERSION:-1.46.0-trustis.1}"
TAG="${TAG:-trustis/inngest:v$VERSION}"
OUT="${OUT:-$(mktemp -d)}"
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.26.4}"   # go.mod's version; Go 1.27 can't build vendored cockroachdb/swiss

cd "$REPO"
git submodule update --init internal/embeddocs/website   # docs are go:embed'ed, pinned by the tag

# 1. Dashboard UI (arch-independent static assets).
mkdir -p "$OUT/ui-ctx"
rsync -a --delete --exclude node_modules --exclude dist ui "$OUT/ui-ctx/"
find pkg/coreapi -name '*.graphql' -exec rsync -R {} "$OUT/ui-ctx/" \;
docker build -f "$HERE/Dockerfile.ui" --output "type=local,dest=$OUT/ui-dist" "$OUT/ui-ctx"

# 2. Embed it where pkg/devserver/ui.go's `//go:embed all:static` reads it (gitignored).
rm -rf pkg/devserver/static/client pkg/devserver/static/server
cp -R "$OUT/ui-dist/client" "$OUT/ui-dist/server" pkg/devserver/static/

# 3. Binary with goreleaser-equivalent flags (.goreleaser.yml: CGO_ENABLED=0, -s -w, version ldflags).
mkdir -p "$OUT/image"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOFLAGS=-mod=vendor go build -trimpath \
  -ldflags "-s -w -X github.com/inngest/inngest/pkg/inngest/version.Version=$VERSION -X github.com/inngest/inngest/pkg/inngest/version.Hash=$(git rev-parse --short=9 HEAD)" \
  -o "$OUT/image/inngest" ./cmd/

# 4. Flattened image on the official runtime.
docker pull --platform linux/amd64 inngest/inngest:v1.46.0
docker build --platform linux/amd64 -f "$HERE/Dockerfile" -t "$TAG" "$OUT/image"

echo "built $TAG (artifacts in $OUT)"
