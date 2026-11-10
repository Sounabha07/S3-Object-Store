#!/usr/bin/env bash
# Runs every benchmark scenario against the docker-compose cluster on this
# machine, RUNS times each (default 3), and writes raw results under
# bench/results/<timestamp>/. bench/summarize.sh turns them into tables.
#
# Usage: bench/run.sh [scenario ...]   (default: all of s1..s7)
#   RUNS=3 DURATION=15s bench/run.sh s1 s2
set -euo pipefail
cd "$(dirname "$0")/.."

RUNS="${RUNS:-3}"
DURATION="${DURATION:-15s}"
EP=http://localhost:9000
DC="docker compose -f deploy/docker-compose.yml"
OUT="bench/results/${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT"
LG=./bin/loadgen
go build -o "$LG" ./bench/loadgen

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUT/run.log"; }

# fresh recreates the cluster with empty volumes. ERASURE=false makes the
# gateway replicate every object (scenario 7).
fresh() {
  $DC down -v >/dev/null 2>&1 || true
  S3OS_ERASURE="${ERASURE:-true}" $DC up -d --wait --wait-timeout 180 >/dev/null
  sleep 2
}

lg() { "$LG" run -endpoint "$EP" "$@"; }

leader() {
  for p in 7101 7102 7103; do
    if curl -fsS "http://127.0.0.1:$p/v1/status" 2>/dev/null | grep -q '"state":"Leader"'; then
      echo "meta-$((p - 7100))"; return
    fi
  done
  echo "none"
}

# volume_bytes sums the shard bytes stored on the given storage volumes.
volume_bytes() {
  local total=0 n
  for v in "$@"; do
    n=$(docker run --rm -v "s3os_$v:/data" golang:1.27 sh -c 'du -sb /data/shards 2>/dev/null | cut -f1')
    total=$((total + n))
  done
  echo "$total"
}

machine_info() {
  {
    echo "date_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_commit: $(git rev-parse HEAD)"
    echo "go: $(go version)"
    powershell -NoProfile -Command '
      $c = Get-CimInstance Win32_Processor
      "cpu: $($c.Name) ($($c.NumberOfCores) cores, $($c.NumberOfLogicalProcessors) threads)"
      "ram_gb: $([math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory/1GB,1))"
      "os: $((Get-CimInstance Win32_OperatingSystem).Caption) $((Get-CimInstance Win32_OperatingSystem).Version)"
      Get-PhysicalDisk | ForEach-Object { "disk: $($_.FriendlyName) $($_.MediaType) $($_.BusType) $([math]::Round($_.Size/1GB))GB" }' 2>/dev/null | tr -d '\r'
    echo "docker: $(docker version --format '{{.Server.Version}}')"
    echo "docker_vm_cpus: $(docker info --format '{{.NCPU}}')"
    echo "docker_vm_mem_gb: $(docker info --format '{{.MemTotal}}' | awk '{printf "%.1f", $1/1073741824}')"
    echo "docker_os: $(docker info --format '{{.OperatingSystem}} / kernel {{.KernelVersion}}')"
  } > "$OUT/machine.txt"
}

s1() { # PUT and GET at 4 KiB, 1 MiB, 64 MiB (8 clients)
  for size in 4KiB 1MiB 64MiB; do
    fresh
    local pre=32
    [ "$size" = 64MiB ] && pre=8
    for r in $(seq "$RUNS"); do
      log "s1 put $size run $r"
      lg -op put -size "$size" -concurrency 8 -duration "$DURATION" -label "s1 PUT $size" -out "$OUT/s1-put-$size-r$r.json"
      log "s1 get $size run $r"
      lg -op get -size "$size" -concurrency 8 -duration "$DURATION" -prefill "$pre" -label "s1 GET $size" -out "$OUT/s1-get-$size-r$r.json"
    done
  done
}

s2() { # concurrency sweep, 1 MiB objects
  fresh
  for c in 1 8 32 128; do
    for r in $(seq "$RUNS"); do
      log "s2 put c$c run $r"
      lg -op put -size 1MiB -concurrency "$c" -duration "$DURATION" -label "s2 PUT 1MiB c$c" -out "$OUT/s2-put-c$c-r$r.json"
      log "s2 get c$c run $r"
      lg -op get -size 1MiB -concurrency "$c" -duration "$DURATION" -label "s2 GET 1MiB c$c" -out "$OUT/s2-get-c$c-r$r.json"
    done
  done
}

s3() { # mixed 70% GET / 25% PUT / 5% DELETE, 1 MiB, 32 clients
  fresh
  for r in $(seq "$RUNS"); do
    log "s3 mixed run $r"
    lg -op mixed -size 1MiB -concurrency 32 -duration 30s -prefill 256 -label "s3 mixed 1MiB c32" -out "$OUT/s3-mixed-r$r.json"
  done
}

s4() { # GET latency, healthy vs. 2 of 6 storage nodes down (1 MiB, erasure coded)
  for r in $(seq "$RUNS"); do
    fresh
    lg -op put -size 1MiB -concurrency 8 -count 256 -label prefill -keys-out "$OUT/s4-keys-r$r.txt" -out "$OUT/s4-prefill-r$r.json"
    log "s4 healthy run $r"
    lg -op get -size 1MiB -concurrency 8 -duration "$DURATION" -keys-in "$OUT/s4-keys-r$r.txt" -label "s4 GET 1MiB healthy" -out "$OUT/s4-healthy-r$r.json"
    $DC stop storage-1 storage-2 >/dev/null
    sleep 3
    log "s4 degraded run $r"
    lg -op get -size 1MiB -concurrency 8 -duration "$DURATION" -keys-in "$OUT/s4-keys-r$r.txt" -label "s4 GET 1MiB 2 nodes down" -out "$OUT/s4-degraded-r$r.json"
    $DC start storage-1 storage-2 >/dev/null
  done
}

s5() { # repair throughput after wiping one node
  for r in $(seq "$RUNS"); do
    fresh
    log "s5 write run $r"
    lg -op put -size 4MiB -concurrency 8 -count 512 -label "s5 write" -out "$OUT/s5-write-r$r.json"
    local held
    held=$(volume_bytes storage-3)
    $DC stop storage-3 >/dev/null
    docker run --rm -v s3os_storage-3:/data golang:1.27 sh -c 'rm -rf /data/shards/* /data/tmp/*'
    $DC start storage-3 >/dev/null
    sleep 3
    log "s5 repair run $r (storage-3 held $held bytes)"
    local t0 t1
    t0=$(date +%s%N)
    curl -fsS -X POST -H "Authorization: Bearer dev-admin-token" "$EP/_admin/repair" > "$OUT/s5-repair-r$r.json"
    t1=$(date +%s%N)
    echo "{\"wiped_node_bytes\": $held, \"repair_wall_ns\": $((t1 - t0))}" > "$OUT/s5-timing-r$r.json"
  done
}

s6() { # Raft leader failover under write load
  for r in $(seq "$RUNS"); do
    fresh
    log "s6 run $r"
    lg -op put -size 4KiB -concurrency 4 -duration 40s -label "s6 failover" -events "$OUT/s6-events-r$r.csv" -out "$OUT/s6-load-r$r.json" &
    local pid=$!
    sleep 15
    local l t
    l=$(leader)
    t=$(date +%s%N)
    $DC kill "$l" >/dev/null
    echo "{\"killed\": \"$l\", \"kill_unix_ns\": $t}" > "$OUT/s6-kill-r$r.json"
    wait "$pid"
    $DC start "$l" >/dev/null
    "$LG" gap -events "$OUT/s6-events-r$r.csv" -after "$t" > "$OUT/s6-gap-r$r.json"
    log "s6 run $r: $(cat "$OUT/s6-gap-r$r.json")"
  done
}

s7() { # erasure coding vs 3x replication: overhead and write throughput
  for mode in true false; do
    for r in $(seq "$RUNS"); do
      ERASURE=$mode fresh
      local label="ec"
      [ "$mode" = false ] && label="replication"
      log "s7 $label run $r"
      lg -op put -size 4MiB -concurrency 8 -count 256 -label "s7 PUT 4MiB $label" -out "$OUT/s7-$label-r$r.json"
      echo "{\"disk_bytes\": $(volume_bytes storage-1 storage-2 storage-3 storage-4 storage-5 storage-6), \"logical_bytes\": $((256 * 4 * 1024 * 1024))}" > "$OUT/s7-$label-disk-r$r.json"
    done
  done
}

machine_info
scenarios=("$@")
[ ${#scenarios[@]} -eq 0 ] && scenarios=(s1 s2 s3 s4 s5 s6 s7)
for s in "${scenarios[@]}"; do
  log "=== $s"
  "$s"
done
$DC down -v >/dev/null 2>&1 || true
log "results in $OUT"
