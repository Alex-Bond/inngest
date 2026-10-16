# Official v1.46.0 runtime filesystem (debian:stable-slim + ca-certs/tzdata) with only
# /bin/inngest replaced by our build (dashboard UI embedded). Flattened so the official
# binary is not carried as a dead layer underneath.
# Context: a directory containing the linux/amd64 `inngest` binary.
ARG BASE=inngest/inngest:v1.46.0
FROM ${BASE} AS official
RUN rm /bin/inngest

FROM scratch
COPY --from=official / /
COPY --chmod=0755 inngest /bin/inngest
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
LABEL org.opencontainers.image.version="1.46.0-trustis.1" \
      org.opencontainers.image.base.name="docker.io/inngest/inngest:v1.46.0" \
      org.opencontainers.image.description="inngest v1.46.0 + Connect no-healthy-connection halt patch (refs inngest/inngest#5022)"
CMD ["inngest"]
