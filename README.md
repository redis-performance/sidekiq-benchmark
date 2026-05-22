# sidekiq-benchmark

A Sidekiq protocol load benchmark written in Rust. Measures job throughput and
full latency spectrum (p50→p99.99) against any Redis endpoint.

## Why Rust?

| | Ruby `bin/sidekiqload` | This tool |
|---|---|---|
| GIL | Yes — limits true concurrency | No — tokio async tasks, no GIL |
| Throughput ceiling | Peaks at ~5 threads | Scales to 200+ workers |
| Latency recording | None | HDRHistogram per job (p50→p99.99) |
| Dependency | Sidekiq gem + Ruby | Single static binary |

## Protocol compatibility

Uses [rusty-sidekiq](https://github.com/film42/sidekiq-rs) for the full
Sidekiq worker protocol. Workers dequeue jobs via **BRPOP** (same as
production OSS Sidekiq). Job JSON format matches Ruby Sidekiq exactly.

## Quick start

### Pre-built binary

```bash
# Run against local Redis
sidekiq-bench --url redis://127.0.0.1:6379/0

# Custom worker counts and job volume
sidekiq-bench --workers 10,50,100,200 --jobs 500000
```

### Docker

```bash
# Start Redis + run benchmark
docker compose run --rm bench

# Use a different Redis image
REDIS_IMAGE=redis:7.4 docker compose run --rm bench

# Point at an external Redis
REDIS_URL=redis://myhost:6379/0 docker compose run --rm bench
```

### From source

```bash
cargo build --release
./target/release/sidekiq-bench --workers 5 --jobs 10000
```

## CLI flags

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--url` | `REDIS_URL` | `redis://127.0.0.1:6379/13` | Full URL |
| `--host` | — | — | Override host component |
| `--port` | — | — | Override port component |
| `--password` | `REDIS_PASSWORD` | — | Auth |
| `--tls` | `REDIS_TLS` | false | Enable TLS (`rediss://`) |
| `--db` | — | `13` | Database number (matches Ruby sidekiqload safety default) |
| `--workers` | — | `10,50,100,200` | Comma-separated concurrency levels |
| `--jobs` | — | `500000` | Total jobs per trial |
| `--warmup-jobs` | — | `0` | Warmup pass jobs (0 = skip) |
| `--queue` | — | `default` | Sidekiq queue name |
| `--tag` | — | from Redis INFO | Label for output filename |
| `--output` | — | `sidekiq_bench_<tag>.json` | JSON output path; `-` for stdout |
| `--timeout` | — | `300` | Per-trial timeout in seconds |
| `--quiet` | — | false | Suppress progress dots |
| `--allow-flushdb` | `SIDEKIQ_BENCH_ALLOW_FLUSHDB` | false | FLUSHDB before each trial (default: only DELetes the queue key — safe on shared Redis) |

Equivalent to `THREADS=N ITER=500 COUNT=1000 bin/sidekiqload` in Ruby:
```bash
sidekiq-bench --workers N --jobs 500000
```

## Output

**Console:**
```
=== sidekiq-bench — redis-8.0 ===
    redis://127.0.0.1:6379/0  jobs=500,000

  [  10 workers] ........  11,062 jobs/s  p50=450.1 ms p99=891.3 ms p99.9=898.0 ms max=899.1 ms
  [  50 workers] ........  18,341 jobs/s  p50=2.2 s    p99=4.4 s    p99.9=4.4 s   max=4.4 s

--- Summary ---
+---------+--------+----------+----------+----------+----------+--------+
| Workers | jobs/s | p50      | p99      | p99.9    | max      | errors |
+===========================================================================+
|      10 | 11,062 | 450.1 ms | 891.3 ms | 898.0 ms | 899.1 ms | 0      |
|      50 | 18,341 |    2.2 s |    4.4 s |    4.4 s |    4.4 s | 0      |
+---------+--------+----------+----------+----------+----------+--------+
```

> **Note on latency:** the benchmark pre-fills the queue then starts workers.
> Latency = time a job sits in the queue until dequeued (wall-clock, same host).
> At 10 workers × 500k jobs the p50 reflects the average queue-drain wait, not
> a Redis round-trip. Workers dequeue via **BRPOP** (OSS Sidekiq protocol).

> **Password safety:** passwords passed via `--password` are visible in `ps aux`.
> Prefer the `REDIS_PASSWORD` environment variable. Passwords are redacted
> (`****`) in console output and JSON.

**JSON** (saved to `sidekiq_bench_<tag>.json`):

```json
{
  "tag": "redis-8.0",
  "timestamp": "2026-05-22T01:30:00Z",
  "config": {
    "url": "redis://127.0.0.1:6379/13",
    "workers": [10, 50, 100, 200],
    "jobs_per_trial": 500000,
    "queue": "default",
    "warmup_jobs": 0
  },
  "results": [{
    "workers": 10,
    "total_jobs": 500000,
    "duration_s": 45.21,
    "jobs_per_sec": 11062.3,
    "timed_out": false,
    "throughput_per_sec": [11200, 11050, 10980],
    "latency_us": {
      "p50": 450100, "p75": 620000, "p90": 810000,
      "p95": 850000, "p99": 891300, "p99_9": 898000,
      "p99_99": 899000, "max": 899100,
      "mean": 460000.0, "total_count": 500000
    },
    "errors": 0
  }]
}
```

Latency values are in **microseconds**.

## Safety notes

### Default database: 13

The default Redis database is **13**, matching Ruby's `bin/sidekiqload`. This avoids
colliding with application data (which typically lives in db 0) and makes `--allow-flushdb`
safe by default. Always confirm the target db before running against a shared Redis.

### Shared / production Redis

Do **not** run this benchmark against a production Redis instance. The benchmark
pre-fills the queue with hundreds of thousands of jobs and (optionally) flushes the
entire database. Use a dedicated benchmark instance or an isolated database number.

### Intentionally omitted Sidekiq metric keys

Ruby Sidekiq 8 middleware writes housekeeping keys (`stat:processed`, `stat:failed`,
`j|*` job detail hashes, `h|*` history hashes) as a side-effect of normal operation.
This benchmark measures **queue mechanics in isolation** — enqueue throughput and BRPOP
latency — so those keys are intentionally omitted. They would add per-job Redis writes
that obscure the metric we care about. The `queues` set and `queue:default` list are
managed as in production; `processes` heartbeat entries are cleaned up after each trial.

### `processes` set cleanup

`rusty-sidekiq` 0.14 does not remove process heartbeat entries from the `processes` set
on shutdown (a known upstream gap). This tool works around it by snapshotting the set
before each trial and removing any new entries afterwards.

## Building

Requires Rust stable (1.75+).

```bash
cargo build --release
cargo test
cargo clippy -- -D warnings
```

## Docker image

Multi-stage build produces a ~10 MB static musl binary on Alpine:

```bash
docker build -t sidekiq-bench .
docker run --rm sidekiq-bench --url redis://host:6379/0 --workers 10 --jobs 50000
```

## License

Apache-2.0
