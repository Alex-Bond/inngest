#!/usr/bin/env bash
# Durable event stream load test: sends EVENTS (50000) in BATCH (500) requests,
# SIGKILLs then SIGTERMs the server past KILL_BACKLOG (10000); KEEP=1 keeps the stack.
set -euo pipefail

cd "$(dirname "$0")"
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.26.4}"

EVENTS="${EVENTS:-50000}"
BATCH="${BATCH:-500}"
KILL_BACKLOG="${KILL_BACKLOG:-10000}"

mkdir -p bin
go build -o bin/eventstream_load .

cleanup() {
  [[ -n "${APP_PID:-}" ]] && kill "$APP_PID" 2>/dev/null || true
  if [[ "${KEEP:-}" != "1" ]]; then
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build

for _ in $(seq 1 120); do
  curl -fs http://localhost:18288/health/ready >/dev/null && break
  sleep 1
done
curl -fs http://localhost:18288/health/ready >/dev/null

bin/eventstream_load app &
APP_PID=$!
sleep 1
bin/eventstream_load register

for signal in SIGKILL SIGTERM; do
  phase="$(echo "$signal" | tr '[:upper:]' '[:lower:]')"
  echo "== phase $phase: $EVENTS events, restart with $signal once the stream holds $KILL_BACKLOG entries"
  bin/eventstream_load drive -phase "$phase" -signal "$signal" \
    -events "$EVENTS" -batch "$BATCH" -kill-backlog "$KILL_BACKLOG"
done
