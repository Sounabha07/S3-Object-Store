# Benchmarks

Every number below comes from runs on the machine described here, using
the commands shown. The raw output of each run is committed under
[`bench/results/`](bench/results/). Each scenario was run 3 times, and
tables report the **median of the 3 runs** for every column. Nothing here is
estimated or extrapolated.

## Environment

| | |
|---|---|
| CPU | 13th Gen Intel Core i7-13620H (10 cores, 16 threads), laptop |
| RAM | 15.7 GB (the Docker VM gets 9.7 GB) |
| Disks | NVMe WD PC SN740 512 GB; NVMe SSSTC CL4-8D512 512 GB |
| OS | Windows 11 Home 10.0.26300 |
| Containers | Docker Desktop 29.8.0, WSL2 VM (kernel 6.6.87.2), 16 vCPUs |
| Go | 1.27.3 |

From [`bench/results/20261110-main/machine.txt`](bench/results/20261110-main/machine.txt).

**Topology.** The `deploy/docker-compose.yml` cluster:

- 1 gateway with SigV4 authentication on;
- 3 Raft metadata nodes;
- 6 storage nodes.

Objects of 1 MiB or more are erasure coded RS(4,2) in chunks of up to 4 MiB;
smaller objects are replicated 3x. Every storage node fsyncs each shard and
its directory before acknowledging, and every metadata write is a Raft commit
(fsync on a majority of the 3 nodes).

**Important: everything shares one machine.** All 10 containers, the load
generator and Docker's VM run on the same laptop CPU, and all "storage
nodes" write to the **same virtual disk** (the Docker VM's ext4 image on one
NVMe drive). A real deployment would spread these over separate machines,
disks and NICs. These numbers measure this implementation on this laptop;
they are not a capacity estimate for a real cluster. See
[What limits these numbers](#what-limits-these-numbers).

## Tools and commands

- **`bench/loadgen`**: this project's load generator, using plain
  `net/http` and the repository's SigV4 signer. Every PUT body is fresh
  pseudo-random data (a ChaCha8 stream); the store deduplicates identical
  content, so reusing a buffer would measure deduplication instead of
  writes. Latencies are per request (first byte sent to last byte read),
  and percentiles are nearest-rank over all requests in a run.
  **MB/s = 10^6 bytes per second.**
- **`bench/run.sh`**: runs scenarios 1–7 against a freshly created
  cluster (`docker compose down -v`, then `up`).
- **MinIO `warp`**, used as an independent cross-check (`bench/warp.sh`). It
  reports **MiB/s (2^20 bytes)**.

```bash
RUNS=3 bash bench/run.sh                     # all scenarios -> bench/results/<stamp>/
go run ./bench/loadgen report -dir bench/results/<stamp>   # the tables below
bash scripts/chaos.sh 3                      # chaos + durability runs
RUNS=3 bash bench/warp.sh                    # warp cross-check
```

Each table names the commit it measured. Scenarios 1–3, 5 and 7 ran at
`137fd4c`. Scenarios 4 and 6 were re-run at `f9b021c`, after the first run
exposed two latency bugs ([see below](#bugs-found-by-the-benchmarks)). Those
fixes only change degraded reads and metadata-leader failover, so the other
scenarios' code paths are identical at both commits.

## 1. PUT and GET by object size (8 clients, 15 s per run)

Raw data: `bench/results/20261110-main/s1-*.json`, commit `137fd4c`.

| scenario | MB/s | ops/s | p50 ms | p95 ms | p99 ms | errors |
|---|---|---|---|---|---|---|
| PUT 4 KiB (3x replication) | 0.4 | 102.5 | 72.70 | 115.70 | 139.31 | 0 |
| GET 4 KiB | 2.6 | 634.9 | 9.52 | 23.32 | 30.39 | 0 |
| PUT 1 MiB (RS 4+2) | 44.5 | 42.4 | 174.50 | 306.52 | 907.53 | 0 |
| GET 1 MiB | 408.4 | 389.5 | 18.47 | 34.89 | 49.15 | 0 |
| PUT 64 MiB (RS 4+2) | 101.3 | 1.5 | 4085.35 | 7288.32 | 7969.62 | 0 |
| GET 64 MiB | 324.0 | 4.8 | 1613.53 | 2128.50 | 2203.14 | 0 |

## 2. Concurrency sweep (1 MiB objects, 15 s per run)

Raw data: `bench/results/20261110-main/s2-*.json`, commit `137fd4c`.

| clients | PUT MB/s | PUT p50 / p99 ms | GET MB/s | GET p50 / p99 ms | errors |
|---|---|---|---|---|---|
| 1 | 16.9 | 55.99 / 137.24 | 78.0 | 11.64 / 36.67 | 0 |
| 8 | 54.4 | 145.73 / 308.20 | 253.9 | 28.51 / 97.46 | 0 |
| 32 | 36.2 | 788.82 / 2370.77 | 314.3 | 88.81 / 360.45 | 0 |
| 128 | 37.2 | 3216.14 / 5421.00 | 223.6 | 404.13 / 7408.74 | 0 |

PUT throughput peaks around 8 clients; beyond that, extra clients only add
queueing delay. GET throughput peaks around 32 clients.

## 3. Mixed workload (70% GET / 25% PUT / 5% DELETE, 1 MiB, 32 clients, 30 s)

Raw data: `bench/results/20261110-main/s3-mixed-*.json`, commit `137fd4c`.

| operation | MB/s | ops/s | p50 ms | p95 ms | p99 ms | errors |
|---|---|---|---|---|---|---|
| all | 122.3 | 122.4 | 204.61 | 612.83 | 738.89 | 0 |
| GET | 89.0 | 84.9 | 176.14 | 364.36 | 592.14 | 0 |
| PUT | 33.3 | 31.7 | 453.30 | 719.91 | 877.66 | 0 |
| DELETE | – | 5.7 | 115.10 | 277.47 | 455.88 | 0 |

## 4. GET with 2 of 6 storage nodes down (1 MiB, erasure coded, 8 clients)

The objects are written with all nodes up. Two storage containers are then
stopped (`docker compose stop storage-1 storage-2`), so every chunk with a
data shard on one of them must be reconstructed from parity.

Raw data: `bench/results/20261110-fixes/s4-*.json`, commit `f9b021c`.

| state | MB/s | ops/s | p50 ms | p95 ms | p99 ms | errors |
|---|---|---|---|---|---|---|
| all 6 nodes healthy | 250.7 | 239.1 | 29.31 | 59.70 | 93.77 | 0 |
| 2 nodes down (degraded reads) | 227.5 | 216.9 | 32.99 | 66.20 | 107.57 | 0 |

Degraded reads cost about 9% of throughput and 3.7 ms at the median. Before
the fix described below, the same scenario measured **2.9 MB/s with a 2.5 s
median** (`bench/results/20261110-main/s4-degraded-*.json`, commit
`137fd4c`).

## 5. Repair after wiping one storage node

Write 2 GiB (512 x 4 MiB objects, erasure coded). Stop `storage-3`, delete
everything in its volume, restart it, then time one repair pass
(`POST /_admin/repair`) until it returns.

Raw data: `bench/results/20261110-main/s5-*.json`, commit `137fd4c`.

| run | data on wiped node (MB) | shards rebuilt | bytes rebuilt (MB) | repair time (s) | rebuilt MB/s |
|---|---|---|---|---|---|
| r1 | 537 | 512 | 537 | 16.15 | 33.2 |
| r2 | 537 | 512 | 537 | 16.32 | 32.9 |
| r3 | 537 | 512 | 537 | 18.29 | 29.4 |
| **median** | | | | **16.32** | **32.9** |

Each of the 512 lost shards was rebuilt by reading 4 surviving shards,
verifying their SHA-256s, running Reed-Solomon reconstruction and writing
(fsync) the result. That reads about 4x the rebuilt bytes over the internal
API. Repair processes one chunk at a time; it is not parallelised.

## 6. Raft leader failover under write load

4 clients PUT 4 KiB objects continuously for 40 s. After 15 s the Raft
leader container is killed (`docker compose kill`). The measured value is
the **longest interval between two consecutive successful PUTs that ends
after the kill**, i.e. how long writes were stalled. Ops already in flight
are retried by the gateway, so no PUT failed.

Raw data: `bench/results/20261110-fixes/s6-*`, commit `f9b021c`.

| run | killed | longest gap between successful PUTs (ms) | typical gap before the kill (ms) | failed PUTs |
|---|---|---|---|---|
| r1 | meta-1 | 8907 | 9.07 | 0 |
| r2 | meta-2 | 3663 | 9.98 | 0 |
| r3 | meta-1 | 3674 | 12.09 | 0 |
| **median** | | **3674** | | |

The earlier run at `137fd4c` measured a median of 7696 ms
(`bench/results/20261110-main/s6-*`). The fix below brought the median down
to 3.7 s, but run r1 still stalled for 8.9 s. I have not isolated that
remaining slow path. One plausible cause is followers handing out the dead
leader's address until the election completes, sending the client back to
it; this is a hypothesis, not a measured result. Part of the stall is
inherent: Raft must notice the leader is gone and elect a new one, using
hashicorp/raft's default 1 s heartbeat and election timeouts.

## 7. Erasure coding vs 3x replication

Write 1 GiB (256 x 4 MiB objects, 8 clients) on a fresh cluster with each
scheme (`-erasure=true` or `false` on the gateway), then sum the shard
bytes in all six storage volumes (`du -sb`).

Raw data: `bench/results/20261110-main/s7-*.json`, commit `137fd4c`.

| scheme | bytes on disk / logical bytes | PUT MB/s | PUT p99 ms |
|---|---|---|---|
| RS(4,2) erasure coding | 1.500 | 104.4 | 495.44 |
| 3x replication | 3.000 | 52.2 | 1031.33 |

Erasure coding writes half as many bytes as replication, and on this setup
(where every write competes for one disk) that roughly doubles write
throughput. It tolerates any 2 node failures, the same as 3x replication.

## Chaos and durability

`scripts/chaos.sh` runs [`test/chaos`](test/chaos/chaos_test.go) against a
fresh cluster. Four writers PUT objects of 4 KiB to 5 MiB, recording the
SHA-256 of every acknowledged write, while one reader re-reads acknowledged
objects. Six faults are injected in turn, each held for about 8 s under load:

1. kill a storage node;
2. kill two storage nodes at once;
3. kill the Raft leader;
4. disconnect a storage node from the Docker network;
5. disconnect the Raft leader from the Docker network;
6. corrupt 40 shard files inside a storage node's volume.

At the end, every acknowledged object is read back and compared.
**Recovery** is the time from injecting a fault to the first write that
started after the fault and succeeded.

Raw data: `bench/results/chaos/` (report JSON, test log and full cluster
logs). Run 1 used the working tree that became commit `d0a070a`; runs 2–3
used `d0a070a` itself. Both predate the two fixes below.

| | run 1 | run 2 | run 3 | **median** |
|---|---|---|---|---|
| kill one storage node: recovery ms | 153 | 106 | 375 | **153** |
| kill two storage nodes: recovery ms | 195 | 233 | 72 | **195** |
| kill Raft leader: recovery ms | 3573 | 3611 | 6308 | **3611** |
| partition a storage node: recovery ms | 2223 | 9629 | 9734 | **9629** |
| partition Raft leader: recovery ms | 3013 | 2823 | 2917 | **2917** |
| corrupt 40 shard files: recovery ms | 170 | 493 | 70 | **170** |

| durability | run 1 | run 2 | run 3 | total |
|---|---|---|---|---|
| acknowledged writes | 2158 | 1775 | 2047 | **5980** |
| verified intact (SHA-256 match) | 2158 | 1775 | 2047 | **5980** |
| **mismatches** | 0 | 0 | 0 | **0** |
| unreadable | 0 | 0 | 0 | **0** |
| corrupt shard reads detected and recovered | 24 | 26 | 24 | 74 |
| reads during faults: OK / failed / wrong data | 988 / 1 / 0 | 937 / 1 / 0 | 1137 / 2 / 0 | 3062 / 4 / **0** |
| PUTs rejected during faults (not acknowledged) | 124 | 96 | 115 | 335 |

No acknowledged write was lost or corrupted, and no read ever returned wrong
data. The few failed reads and rejected writes all happened while a fault was
in progress. They were refused cleanly (503), never silently wrong.

**Partitioning a storage node is the slowest fault: writes stalled about
9.6 s** in 2 of 3 runs. A partitioned node silently drops packets instead of
refusing connections, so a request already sent to it waits for the client's
10 s response-header timeout before the gateway gives up on that node. A
killed container, by contrast, fails fast. Lowering that timeout trades
faster failover against false failures under heavy load; I left it at 10 s
and report the cost here.

## MinIO warp cross-check (1 MiB, 8 clients, 30 s per run)

Raw output: `bench/results/20261110-warp/`.

`warp` (built from `github.com/minio/warp@latest` on 2026-11-10; it reports
its version as `dev`) ran unmodified against the gateway with SigV4,
commit `f9b021c`. Command: `warp <put|get|mixed> --host localhost:9000
--obj.size 1MiB --concurrent 8 --duration 30s --noclear`. `--noclear` is
needed because warp's cleanup uses DeleteObjects, which this gateway does not
implement.

| benchmark | MiB/s | obj/s | p50 ms | p99 ms |
|---|---|---|---|---|
| PUT | 39.66 | 39.66 | 209.1 | 471.1 |
| GET | 212.42 | 212.42 | 33.5 | 110.5 |
| mixed: GET (45%) | 50.39 | 50.39 | 47.7 | 331.7 |
| mixed: STAT (30%) | – | 33.67 | 35.7 | 303.3 |
| mixed: PUT (15%) | 16.85 | 16.85 | 283.5 | 700.5 |
| mixed: DELETE (10%) | – | 11.31 | 30.9 | 279.9 |

Medians of 3 runs per row. Each run was on a fresh cluster.

**Comparison with `loadgen`.** Same object size and client count (scenario
2, 8 clients, converted to MiB/s):

| | loadgen | warp | warp vs loadgen |
|---|---|---|---|
| PUT | 51.9 MiB/s | 39.66 MiB/s | 24% lower |
| GET | 242.1 MiB/s | 212.42 MiB/s | 12% lower |

The two tools agree on the shape: GET is about 5x PUT. They differ in
absolute throughput, and I have not isolated why. The runs used different
commits (`137fd4c` vs `f9b021c`, though the healthy PUT/GET paths are the
same code), different client libraries and timing windows, and for PUT,
different payload generation.

## Bugs found by the benchmarks

The first full benchmark run exposed two latency bugs. Each was fixed with a
regression test that fails on the old code, and the affected scenarios
were re-run (commit `f9b021c`):

1. **Degraded erasure-coded reads waited on dead nodes** (scenario 4: 2.9
   MB/s, 2.5 s median). The read path asked every data shard's node first,
   including nodes the health checker already knew were down, and paid the
   connect timeout on each read before falling back to parity. Reads now
   skip known-down nodes and go to parity straight away (227.5 MB/s, 33 ms
   median). Test: `TestDegradedReadSkipsKnownDownNodes`.
2. **The metadata client could retry the same dead leader twice**
   (scenario 6: 7.7 s median stall). After a failed attempt it picked the
   next endpoint by attempt number, which could land on the node that had
   just failed. It now moves to the endpoint after the failed one (3.7 s
   median). Test: `TestClientDoesNotRetryDeadNodeImmediately`.

My first re-run also had a measurement mistake: the benchmark driver did
not rebuild the Docker image, so it measured the old code. That run was
discarded. `bench/run.sh` now builds the image from the current checkout
before any scenario.

## What limits these numbers

- **One shared virtual disk.** Every storage node writes to the same
  Docker VM disk image on one NVMe drive, and every write is fsynced.
  - An erasure-coded 1 MiB PUT does 6 shard writes, each with a file fsync
    and a directory fsync, plus a Raft commit that fsyncs on at least 2 of
    the 3 metadata nodes.
  - All of that contends for one device behind the WSL2 virtualisation
    layer.

  This is the most likely reason PUT throughput (16.9–104.4 MB/s) is far
  below GET (78–408 MB/s for 1 MiB and larger objects): reads need no fsync,
  and probably come largely from the VM's page cache, though I have not
  measured cache hit rates. It is also the likely reason PUT throughput stops
  scaling past 8 clients.
- **One CPU for everything.** Gateway, 9 server containers, Docker networking
  and the load generator share 16 hardware threads. GET at 128 clients
  (223.6 MB/s) is lower than at 32 (314.3 MB/s), and its p99 rises to 7.4 s:
  the machine is saturated and requests queue.
- **Small objects are bound by per-request overhead.** Each 4 KiB PUT pays
  for 3 replica writes with fsync, a Raft commit, and a separate linearizable
  read (a Raft barrier) to check the bucket exists. Hence 102.5 PUT/s and a
  72.7 ms median. Each GET pays one Raft barrier plus a shard read.
- **The gateway buffers one chunk per request** (up to 4 MiB, or 6 MiB of
  shards with erasure coding), so the sustained 64 MiB PUT rate (101.3 MB/s)
  is limited by chunks moving through one gateway sequentially per request.
- **Not measured:** multiple gateways, networks slower than loopback, data
  sets larger than memory, and runs longer than 40 s. None of these numbers
  should be read as applying to those cases.
