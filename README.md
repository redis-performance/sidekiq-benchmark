# sidekiq-benchmark

[![CI](https://github.com/redis-performance/sidekiq-benchmark/actions/workflows/ci.yml/badge.svg)](https://github.com/redis-performance/sidekiq-benchmark/actions/workflows/ci.yml)
[![Docker Pulls](https://img.shields.io/docker/pulls/redis/sidekiq-benchmark)](https://hub.docker.com/r/redis/sidekiq-benchmark)
[![Docker Image Size](https://img.shields.io/docker/image-size/redis/sidekiq-benchmark/latest)](https://hub.docker.com/r/redis/sidekiq-benchmark)
[![Docker Platforms](https://img.shields.io/badge/platform-linux%2Famd64%20%7C%20linux%2Farm64-blue)](https://hub.docker.com/r/redis/sidekiq-benchmark)

A Sidekiq protocol load benchmark written in Rust. Measures job throughput and
full latency spectrum (p50→p99.99) against any Redis endpoint.

## Why Rust?

| | Ruby `bin/sidekiqload` | This tool |
|---|---|---|
| GIL | Yes — limits true concurrency | No — tokio async tasks, no GIL |
| Throughput ceiling | Peaks at ~5 threads | Scales to 200+ workers |
| Latency recording | None | HDRHistogram per job (p50→p99.99) |
| Per-second time series | None | throughput + latency percentiles + errors |
| Multi-queue | No | `--num-queues N` (round-robin distribution) |
| Dependency | Sidekiq gem + Ruby | Single static binary |

## Protocol compatibility

Uses a [forked rusty-sidekiq](https://github.com/redis-performance/sidekiq-rs)
for the full Sidekiq worker protocol. Workers dequeue jobs via **BRPOP** (same
as production OSS Sidekiq). Job JSON format matches Ruby Sidekiq exactly.
Process heartbeat entries in the `processes` set are cleaned up on graceful
shutdown.

## Quick start

### Docker Hub

> **Memory:** the default run pre-fills 500,000 jobs (~300 B each) → **~143 MB**
> peak Redis memory before workers drain the queue. Use `--jobs 50000` (~14 MB)
> for a quick local smoke test.

```bash
# Run against local Redis (default: db 13, 500k jobs, workers 10/50/100/200)
docker run --rm --network host redis/sidekiq-benchmark

# Lighter local run (~14 MB Redis memory)
docker run --rm --network host redis/sidekiq-benchmark \
  --workers 10,50 --jobs 50000

# Custom settings
docker run --rm --network host redis/sidekiq-benchmark \
  --url redis://127.0.0.1:6379/0 \
  --workers 10,50,100 \
  --jobs 100000 \
  --num-queues 4

# Point at a remote Redis
docker run --rm redis/sidekiq-benchmark \
  --url redis://myhost:6379/0 \
  --workers 50,100,200 \
  --jobs 500000 \
  --output -
```

### docker compose (Redis included)

```bash
# Start Redis + run benchmark
docker compose run --rm bench

# Use a different Redis image
REDIS_IMAGE=redis:7.4 docker compose run --rm bench  # override to a different version

# Point at an external Redis
REDIS_URL=redis://myhost:6379/0 docker compose run --rm bench
```

### Pre-built binary

```bash
sidekiq-bench --workers 10,50,100,200 --jobs 500000
```

### From source

```bash
cargo build --release
./target/release/sidekiq-bench --workers 5 --jobs 10000
```

## CLI flags

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--url` | `REDIS_URL` | `redis://127.0.0.1:6379/13` | Full Redis URL |
| `--host` | — | — | Override host component of URL |
| `--port` | — | — | Override port component of URL |
| `--password` | `REDIS_PASSWORD` | — | Auth (prefer env var — CLI exposes it in `ps`) |
| `--tls` | `REDIS_TLS` | false | Enable TLS (`rediss://`) |
| `--db` | — | `13` | Database number (matches Ruby sidekiqload safety default) |
| `--workers` | — | `10,50,100,200` | Comma-separated concurrency levels — one trial each |
| `--jobs` | — | `500000` | Total jobs per trial |
| `--warmup-jobs` | — | `0` | Warmup pass before each trial (0 = skip) |
| `--queue` | — | `default` | Base Sidekiq queue name |
| `--num-queues` | — | `1` | Number of queues (jobs distributed round-robin); names are `<queue>_0…<queue>_{N-1}` when N > 1 |
| `--payload-size` | — | `6` | ASCII filler bytes in each job's `args[0]`. Default `6` matches the historical `"string"` placeholder length (~200 B serialized job). Envelope is ~200 B, so use `~800` for ~1 KB jobs; `~3800` for ~4 KB. |
| `--latency-percentiles` | — | `p50,p90,p99,p999,max` | Per-second latency series to record; supports `p50`, `p75`, `p90`, `p95`, `p99`, `p999`, `p9999`, `max`, `mean` |
| `--tag` | — | from Redis `INFO` | Label for output filename and JSON |
| `--output` | — | `sidekiq_bench_<tag>.json` | JSON output path; `-` for stdout |
| `--timeout` | — | `300` | Per-trial timeout in seconds |
| `--quiet` | — | false | Suppress per-second progress dots |
| `--allow-flushdb` | `SIDEKIQ_BENCH_ALLOW_FLUSHDB` | false | FLUSHDB before each trial (default: DEL only the queue keys — safe on shared Redis) |
| `--track-stats` | — | false | Emit the per-job stats writes Ruby Sidekiq makes when `Sidekiq[:track_stats] = true` (its default): `HSET <identity>:work <tid> <work_json>` on start, then `HDEL` + `INCR stat:processed` + `INCR stat:processed:<date>` on completion. Adds 4 Redis commands per job — significant cost at small payloads. Off by default to keep historical (Phase 2) wire shape. |
| `--duration-secs` | — | — | Opt-in steady-state mode: producer + consumer run concurrently for N seconds. Producer LPUSHes one job at a time (with per-call HDR latency), consumer's BRPOP drains in parallel. `--jobs` is ignored in this mode; `--warmup-jobs` is skipped. When unset (default), keeps the burst-then-drain behavior (pre-fill via bulk pipeline, then drain). |
| `--target-queue-depth` | — | `1000` | Steady-state only — soft cap on in-flight jobs (`produced − completed`). Producer yields when at the cap so the queue doesn't grow unbounded. ~5 ms of work at 200K jobs/s. |

Equivalent to Ruby's `THREADS=N ITER=500 COUNT=1000 bin/sidekiqload`:
```bash
sidekiq-bench --workers N --jobs 500000
```

### Multi-queue mode

Dragonfly's `--shard_round_robin_prefix` + `--singlehop_blocking=true` flags
unlock parallel BRPOP dispatch across multiple queues. To replicate the
Dragonfly blog benchmark scenario:

```bash
# Single queue — Redis ceiling ~140k jobs/s
sidekiq-bench --workers 100 --jobs 500000 --num-queues 1

# 8 queues — shows Dragonfly scaling to 488k jobs/s vs Redis staying flat
sidekiq-bench --workers 100 --jobs 500000 --num-queues 8
```

## Output

**Console:**
```
=== sidekiq-bench — redis-8.0 ===
    redis://127.0.0.1:6379/13  jobs=500,000  queues=default

  [  10 workers] ........  11,062 jobs/s  p50=450 µs  p99=2.3 ms  p99.9=5.6 ms  max=45 ms
  [  50 workers] ........  18,341 jobs/s  p50=2.1 ms  p99=8.4 ms  p99.9=12 ms   max=34 ms

--- Summary ---
+---------+--------+--------+--------+---------+---------+--------+
| Workers | jobs/s | p50    | p99    | p99.9   | max     | errors |
+---------+--------+--------+--------+---------+---------+--------+
|      10 | 11,062 | 450 µs | 2.3 ms | 5.6 ms  | 45 ms   | 0      |
|      50 | 18,341 | 2.1 ms | 8.4 ms | 12 ms   | 34 ms   | 0      |
+---------+--------+--------+--------+---------+---------+--------+
Results saved → sidekiq_bench_redis-8.0.json
```

Progress shows `.` per second, or `[e:N]` when errors occur in that window so
nothing is silently swallowed.

**JSON** (`sidekiq_bench_<tag>.json`):

```json
{
  "tag": "redis-8.0",
  "timestamp": "2026-05-22T01:30:00Z",
  "config": {
    "url": "redis://127.0.0.1:6379/13",
    "workers": [10, 50, 100, 200],
    "jobs_per_trial": 500000,
    "queues": ["default"],
    "warmup_jobs": 0
  },
  "results": [{
    "workers": 10,
    "total_jobs": 500000,
    "duration_s": 45.21,
    "jobs_per_sec": 11062.3,
    "timed_out": false,
    "throughput_per_sec": [11200, 11050, 10980],
    "errors_per_sec":     [0, 0, 0],
    "latency_per_sec_us": {
      "p50":  [440, 455, 448],
      "p90":  [820, 835, 810],
      "p99":  [2280, 2350, 2290],
      "p999": [5500, 5700, 5600],
      "max":  [44000, 46000, 43000]
    },
    "latency_us": {
      "p50": 450, "p75": 620, "p90": 810,
      "p95": 850, "p99": 2300, "p99_9": 5600,
      "p99_99": 12000, "max": 45000,
      "mean": 512.3, "total_count": 500000
    },
    "errors": 0
  }]
}
```

All latency values are in **microseconds**. `latency_per_sec_us` contains one
value per elapsed second of the trial, making it easy to plot latency stability
over time or spot degradation as the queue drains.

> **Note on latency:** the benchmark pre-fills the queue then starts workers.
> Latency = time a job spends in the queue until dequeued (wall-clock, same
> host as producer). Workers dequeue via **BRPOP** (OSS Sidekiq protocol).

> **Password safety:** passwords passed via `--password` are visible in `ps aux`.
> Prefer the `REDIS_PASSWORD` environment variable. Passwords are redacted
> (`****`) in all output and JSON.

## Safety notes

### Default database: 13

The default Redis database is **13**, matching Ruby's `bin/sidekiqload`. This
avoids colliding with application data (which typically lives in db 0) and
makes `--allow-flushdb` safe by default. Always confirm the target db before
running against a shared Redis.

### Shared / production Redis

Do **not** run this benchmark against a production Redis instance. The
benchmark pre-fills the queue with hundreds of thousands of jobs and
(optionally) flushes the entire database. Use a dedicated benchmark instance
or an isolated database number.

### Intentionally omitted Sidekiq metric keys

Ruby Sidekiq 8 middleware writes housekeeping keys (`stat:processed`,
`stat:failed`, `j|*` job detail hashes, `h|*` history hashes) as a side-effect
of normal operation. This benchmark measures **queue mechanics in isolation** —
enqueue throughput and BRPOP latency — so those keys are intentionally omitted.

## Building

Requires Rust stable (1.75+).

```bash
cargo build --release
cargo test
cargo clippy -- -D warnings
```

## Docker image

Multi-platform image (`linux/amd64`, `linux/arm64`) published to
[`redis/sidekiq-benchmark`](https://hub.docker.com/r/redis/sidekiq-benchmark)
on every push to `main`. Tagged `latest` on main; semver tags (`1.0.0`,
`1.0`) on `v*` git tags.

```bash
# Pull and run
docker pull redis/sidekiq-benchmark
docker run --rm --network host redis/sidekiq-benchmark --workers 10 --jobs 50000

# Build locally
docker build -t sidekiq-bench .
docker run --rm sidekiq-bench --url redis://host:6379/0 --workers 10 --jobs 50000
```

## License

Apache-2.0
