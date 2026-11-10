#!/usr/bin/env bash
# Runs the S3 smoke test with each metadata node and then each storage node
# stopped in turn, and checks that with two of three metadata nodes stopped
# the gateway refuses writes (503) instead of acknowledging them. Needs the
# docker-compose cluster running.
# Usage: scripts/failover-smoke.sh [endpoint]
set -euo pipefail

EP="${1:-http://localhost:9000}"
DC="docker compose -f deploy/docker-compose.yml"
here="$(cd "$(dirname "$0")" && pwd)"

for n in meta-1 meta-2 meta-3 storage-1 storage-2 storage-3 storage-4 storage-5 storage-6; do
  echo "--- $n stopped"
  $DC stop "$n" >/dev/null
  bash "$here/smoke.sh" "$EP"
  $DC start "$n" >/dev/null
  sleep 3
done

echo "--- meta-1 and meta-2 stopped (no quorum)"
$DC stop meta-1 meta-2 >/dev/null
code="$(curl --aws-sigv4 "aws:amz:us-east-1:s3" --user "${AWS_ACCESS_KEY_ID:-test-access-key}:${AWS_SECRET_ACCESS_KEY:-test-secret-key}"   -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" -s -o /dev/null -w '%{http_code}' -X PUT "$EP/noquorum-$RANDOM")"
$DC start meta-1 meta-2 >/dev/null
if [ "$code" != 503 ]; then
  echo "FAIL: write without quorum returned HTTP $code, want 503" >&2
  exit 1
fi
sleep 3
bash "$here/smoke.sh" "$EP"
echo "failover smoke test passed"
