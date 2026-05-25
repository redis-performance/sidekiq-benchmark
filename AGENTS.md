# Agent guidelines

Instructions for AI coding agents (Claude Code, Copilot, Cursor, etc.) working in this repo.

## Project overview

`sidekiq-benchmark` is a Sidekiq protocol load benchmark written in Rust. It measures job throughput (jobs/second) and full latency spectrum (p50 → p99.99) against any Redis endpoint. Workers dequeue jobs via BRPOP — the same protocol used by production OSS Sidekiq — and latency is recorded per-job using HDRHistogram. The tool supports multiple concurrency levels in a single run, multi-queue round-robin distribution, per-second time-series output, and emits results as both a formatted console table and a JSON file. It is published as a Docker image (`redis/sidekiq-benchmark`) and as a single static binary.

## Local setup

Requires Rust stable (1.75+). The Sidekiq worker implementation lives in `sidekiq-rs/` as a git submodule, so clone with `--recurse-submodules`.

```bash
git clone --recurse-submodules git@github.com:redis-performance/sidekiq-benchmark.git
cd sidekiq-benchmark
cargo build --release
```

Verify the build:

```bash
# Requires a running Redis on 127.0.0.1:6379
./target/release/sidekiq-bench \
  --url redis://127.0.0.1:6379/0 \
  --workers 2 \
  --jobs 500 \
  --output -
```

## Branch naming

Same as human contributors: `<type>/<short-description>` (e.g. `fix/off-by-one-in-pipeline`).

## Coding standards

- Match the style already in the file you are editing.
- Prefer clear, minimal changes over large refactors unless explicitly asked.
- Do not add comments that describe *what* the code does — only add comments when the *why* is non-obvious.
- Do not introduce new dependencies without checking with the maintainer.

## Running tests

Run the full suite before declaring a task complete:

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

For a full end-to-end smoke test (requires Redis on `127.0.0.1:6379`):

```bash
cargo run --release -- \
  --url redis://127.0.0.1:6379/0 \
  --workers 2 \
  --jobs 500 \
  --timeout 60 \
  --output /tmp/smoke.json \
  --quiet \
  --tag smoke
```

Always run tests before declaring a task complete.

## How to submit changes

1. Create a branch: `git checkout -b <type>/<description>`.
2. Commit with a clear message focused on *why*, not *what*.
3. Open a pull request against `main`.
4. Do **not** push directly to `main`.

## What to avoid

- Do not reformat files unrelated to your change.
- Do not remove error handling or tests.
- Do not commit secrets, credentials, or large binary files.
- Do not amend published commits.
- Do not run the benchmark against a production Redis instance — it pre-fills hundreds of thousands of jobs and can optionally flush the entire database.
- Do not modify the `sidekiq-rs/` submodule directory directly; changes to the submodule must go through the upstream fork at `https://github.com/redis-performance/sidekiq-rs`.
