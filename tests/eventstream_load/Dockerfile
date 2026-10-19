# Builds the server from this checkout for the load test; the build context is
# the repository root.
FROM golang:1.26.4-alpine AS build
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    GOFLAGS=-mod=vendor CGO_ENABLED=0 go build -o /go/bin/inngest ./cmd

FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /go/bin/inngest /bin/inngest
CMD ["inngest"]
