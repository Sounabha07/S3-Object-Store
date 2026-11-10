#!/usr/bin/env bash
# Scripted, repeatable chaos run against a fresh docker-compose cluster:
# kill storage nodes, kill and partition the Raft leader, partition a
# storage node, corrupt shard files. While writers and a reader run, every
# acknowledged write is recorded with its SHA-256; at the end all of them
# are read back and compared. Writes the report JSON to bench/results/.
#
# Usage: scripts/chaos.sh [runs]   (default 1)
set -euo pipefail
cd "$(dirname "$0")/.."
RUNS="${1:-1}"
DC="docker compose -f deploy/docker-compose.yml"
mkdir -p bench/results/chaos

for r in $(seq "$RUNS"); do
  out="bench/results/chaos/chaos-$(date -u +%Y%m%dT%H%M%SZ).json"
  echo "=== chaos run $r -> $out"
  $DC down -v >/dev/null 2>&1 || true
  $DC up -d --build --wait --wait-timeout 180 >/dev/null
  S3OS_CHAOS=1 S3OS_CHAOS_OUT="$PWD/$out" go test -count=1 -v -timeout 30m -run TestChaos ./test/chaos 2>&1 | tee "${out%.json}.log" | grep -E "phase:|recovery|^(--- |ok|FAIL)"
  $DC logs > "${out%.json}.cluster.log" 2>&1 || true
done
$DC down -v >/dev/null 2>&1 || true
