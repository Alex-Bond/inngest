# trustis inngest build

Patched engine image `trustis/inngest:v1.46.0-trustis.1`: inngest v1.46.0 plus the
Connect no-healthy-connection halt (branch `fix/connect-no-worker-halt`, refs
inngest/inngest#5022). Local-only build tooling; excluded via `.git/info/exclude`.

## Build

```sh
.trustis-build/build.sh                                   # defaults below
VERSION=1.46.0-trustis.2 OUT=/tmp/inngest-out .trustis-build/build.sh
```

Requires Docker (buildx; amd64 emulation only needed to *run* the image on arm64),
rsync, git, and Go (`GOTOOLCHAIN=go1.26.4` is fetched automatically; Go 1.27 cannot
build the vendored `cockroachdb/swiss`).

## What it does (mirrors the official goreleaser release)

1. **UI**: `Dockerfile.ui` runs `pnpm install --frozen-lockfile && pnpm build` in
   `ui/apps/dev-server-ui` on node 22.16.0 / pnpm 10.18.2 (same as `make build-ui`).
   `NODE_OPTIONS=--dns-result-order=ipv4first` is required: the TanStack prerender
   step fetches `127.0.0.1` and fails if the preview server binds `::1`.
2. **Embed**: copy `dist/{client,server}` into `pkg/devserver/static/` (gitignored),
   read by `//go:embed all:static` in `pkg/devserver/ui.go`. The docs submodule
   `internal/embeddocs/website` is also embedded; it must be initialized.
3. **Binary**: `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOFLAGS=-mod=vendor go build
   -trimpath -ldflags "-s -w -X .../version.Version=$VERSION -X .../version.Hash=<HEAD short9>" ./cmd/`
   (`inngest version` prints `$VERSION-<hash>`; official prints `1.46.0-879bc8e75`).
4. **Image**: `Dockerfile` copies the official `inngest/inngest:v1.46.0` root
   filesystem minus `/bin/inngest` into `scratch`, adds our binary, restores `PATH`
   and `CMD ["inngest"]`. Same runtime as the official image, ~206 MB.

## Verify against the official image

```sh
SK=$(openssl rand -hex 32)
docker run -d --name inn-off --platform linux/amd64 -p 18288:8288 inngest/inngest:v1.46.0 inngest start --signing-key $SK --event-key localtest
docker run -d --name inn-pat --platform linux/amd64 -p 28288:8288 trustis/inngest:v1.46.0-trustis.1 inngest start --signing-key $SK --event-key localtest
curl -s localhost:28288/health                      # {"status":200,"message":"OK"}
curl -s -X POST localhost:28288/v0/gql -H 'content-type: application/json' -d '{"query":"{ apps { id name } }"}'
# Compare / and every /assets/* it references between ports 18288 and 28288: only the
# prerender timestamp in the inline TanStack script should differ.
docker rm -f inn-off inn-pat
```
