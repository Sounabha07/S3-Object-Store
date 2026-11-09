#!/usr/bin/env bash
# Runs the S3 smoke test with each metadata node stopped in turn (one of them
# is the Raft leader), then checks that with two of three stopped the gateway
# refuses writes (503) instead of acknowledging them. Needs the
# docker-compose cluster running.
# Usage: scripts/meta-failover-smoke.sh [endpoint]
set -euo pipefail

EP="${1:-http://localhost:9000}"
DC="docker compose -f deploy/docker-compose.yml"
here="$(cd "$(dirname "$0")" && pwd)"

for m in meta-1 meta-2 meta-3; do
  echo "--- $m stopped"
  $DC stop "$m" >/dev/null
  bash "$here/smoke.sh" "$EP"
  $DC start "$m" >/dev/null
  sleep 3
done

echo "--- meta-1 and meta-2 stopped (no quorum)"
$DC stop meta-1 meta-2 >/dev/null
code="$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$EP/noquorum-$RANDOM")"
$DC start meta-1 meta-2 >/dev/null
if [ "$code" != 503 ]; then
  echo "FAIL: write without quorum returned HTTP $code, want 503" >&2
  exit 1
fi
sleep 3
bash "$here/smoke.sh" "$EP"
echo "metadata failover smoke test passed"
