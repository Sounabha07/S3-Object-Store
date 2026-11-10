# Resume bullets

Each bullet uses only numbers measured in [BENCHMARKS.md](BENCHMARKS.md) or
reported by the test suite, and cites where. Hardware context for every
number: one laptop (i7-13620H, 16 GB, NVMe) running all 10 containers.

- **Built a distributed, S3-compatible object store in Go** (3-node Raft
  metadata on hashicorp/raft, 6 storage nodes, SigV4 auth) that works
  unmodified with aws-sdk-go-v2, the aws CLI and MinIO warp. It serves
  1 MiB GETs at **408.4 MB/s with 49 ms p99** using 8 clients.
  *(BENCHMARKS.md §1; warp cross-check)*

- **Implemented Reed-Solomon 4+2 erasure coding with per-shard SHA-256
  verification**, cutting bytes on disk from **3.00x to 1.50x** of logical
  data versus 3x replication and **doubling write throughput (52.2 → 104.4
  MB/s)**. With 2 of 6 storage nodes down, reads keep **91% of healthy
  throughput (227.5 vs 250.7 MB/s)**.
  *(BENCHMARKS.md §7, §4)*

- **Wrote scripted chaos tests** (storage-node kills, Raft leader kill and
  network partition, on-disk shard corruption) that verified **5,980
  acknowledged writes by SHA-256 with 0 mismatches** across 3 runs. Built a
  background repair loop that rebuilt a wiped node's **537 MB of shards in
  16.3 s**.
  *(BENCHMARKS.md "Chaos and durability", §5)*

- **Used benchmarks to find and fix two latency bugs**, raising degraded-read
  throughput **78x (2.9 → 227.5 MB/s)** and halving the median write stall
  after a Raft leader crash **(7.7 s → 3.7 s)**, each with a regression
  test. The suite has **187 tests and 4 fuzz targets** (314 runs including
  subtests) and **83.8% statement coverage**, and passes `go test -race`.
  *(BENCHMARKS.md "Bugs found by the benchmarks"; `go test ./...` output)*
