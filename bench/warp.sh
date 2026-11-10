#!/usr/bin/env bash
# Cross-checks the custom load generator with MinIO's warp
# (go install github.com/minio/warp@latest). Each benchmark runs RUNS times
# on a fresh cluster; warp's text reports are saved under bench/results/.
# warp reports MiB/s (2^20 bytes), loadgen reports MB/s (10^6 bytes).
set -euo pipefail
cd "$(dirname "$0")/.."
RUNS="${RUNS:-3}"
DC="docker compose -f deploy/docker-compose.yml"
OUT="bench/results/${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}-warp"
mkdir -p "$OUT"
$DC build >/dev/null
{ echo "warp: $(warp --version 2>&1 | head -1)"; echo "git_commit: $(git rev-parse HEAD)"; } > "$OUT/versions.txt"

common=(--host localhost:9000 --access-key test-access-key --secret-key test-secret-key
        --obj.size 1MiB --concurrent 8 --duration 30s --noclear)
for bench in put get mixed; do
  for r in $(seq "$RUNS"); do
    $DC down -v >/dev/null 2>&1 || true
    $DC up -d --wait --wait-timeout 180 >/dev/null
    echo "[$(date -u +%H:%M:%S)] warp $bench run $r"
    warp "$bench" "${common[@]}" --benchdata "$OUT/warp-$bench-r$r" > "$OUT/warp-$bench-r$r.txt" 2>&1
  done
done
$DC down -v >/dev/null 2>&1 || true
echo "results in $OUT"
