mod job;
mod metrics;
mod producer;
mod report;
mod worker;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use anyhow::{Context, Result};
use clap::Parser;
use hdrhistogram::Histogram;
use metrics::{LatencyStats, Metrics, TrialResult};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "sidekiq-bench",
    version,
    about = "Sidekiq protocol load benchmark — measures job throughput and latency against any Redis endpoint"
)]
struct Cli {
    /// Redis URL (takes precedence over --host/--port).
    /// Defaults to db 13 — the same safety default as Ruby's bin/sidekiqload — to avoid
    /// colliding with application data and to make --allow-flushdb safe by default.
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379/13")]
    url: String,

    /// Override host in the Redis URL
    #[arg(long)]
    host: Option<String>,

    /// Override port in the Redis URL
    #[arg(long)]
    port: Option<u16>,

    /// Redis password — prefer REDIS_PASSWORD env var; passing on CLI exposes it in process list
    #[arg(long, env = "REDIS_PASSWORD")]
    password: Option<String>,

    /// Enable TLS (upgrades scheme to rediss://)
    #[arg(long, env = "REDIS_TLS")]
    tls: bool,

    /// Redis database number (default 13 matches Ruby sidekiqload's safety default)
    #[arg(long, default_value = "13")]
    db: u8,

    /// Comma-separated concurrency levels — each becomes a separate trial
    #[arg(long, default_value = "10,50,100,200", value_delimiter = ',')]
    workers: Vec<usize>,

    /// Total jobs per trial
    #[arg(long, default_value = "500000")]
    jobs: u64,

    /// Jobs for warmup run before each trial (0 = skip)
    #[arg(long, default_value = "0")]
    warmup_jobs: u64,

    /// Base Sidekiq queue name
    #[arg(long, default_value = "default")]
    queue: String,

    /// Number of queues to distribute jobs across (1 = single queue, matching bin/sidekiqload;
    /// 2–8 matches common production patterns and shows Dragonfly's multi-queue advantage).
    /// Queue names are generated as <queue>_0, <queue>_1, … when > 1.
    #[arg(long, default_value = "1")]
    num_queues: usize,

    /// Per-second latency percentiles to record (comma-separated).
    /// Supported values: p50, p75, p90, p95, p99, p999, p9999, max, mean.
    #[arg(long, default_value = "p50,p90,p99,p999,max", value_delimiter = ',')]
    latency_percentiles: Vec<String>,

    /// Label for output (defaults to redis_version from INFO)
    #[arg(long)]
    tag: Option<String>,

    /// Output file path, or '-' for stdout
    #[arg(long)]
    output: Option<String>,

    /// Per-trial timeout in seconds
    #[arg(long, default_value = "300")]
    timeout: u64,

    /// Suppress per-second progress output
    #[arg(long)]
    quiet: bool,

    /// Allow FLUSHDB before each trial (clears the entire database).
    /// Default: only deletes the specific queue key, which is safe on shared Redis.
    #[arg(long, env = "SIDEKIQ_BENCH_ALLOW_FLUSHDB")]
    allow_flushdb: bool,

    /// ASCII filler bytes in each job's args[0]. Default 6 matches the
    /// historical `"string"` placeholder length (same wire size as pre-flag).
    /// Envelope is ~200 B, so 800 → ~1 KB total job; 3800 → ~4 KB.
    #[arg(long, default_value = "6")]
    payload_size: usize,

    /// Enable Sidekiq stats tracking — matches Ruby Sidekiq's `Sidekiq[:track_stats]`
    /// (default `true` upstream). When set, every processed job adds four
    /// extra Redis commands (`HSET <identity>:work <tid> <work_json>` on start,
    /// `HDEL` + `INCR stat:processed` + `INCR stat:processed:<date>` on
    /// completion). Default off so the tool keeps its historical lean output
    /// shape for Phase 2 reproductions.
    #[arg(long)]
    track_stats: bool,

    /// Run in sustained steady-state mode: producer + consumer execute
    /// concurrently for this many seconds, producer LPUSHes one job at a time
    /// (with per-call HDR latency captured), consumer's BRPOP drains in
    /// parallel. Soft-caps in-flight at `--target-queue-depth`. `--jobs` is
    /// ignored in this mode. When unset (default), the tool keeps the
    /// existing burst-then-drain behavior: pre-fill via bulk pipeline, then
    /// run workers until the queue empties.
    #[arg(long)]
    duration_secs: Option<u64>,

    /// Steady-state mode only: soft cap on `produced − completed`. When the
    /// gap reaches this, the producer yields (100 µs sleeps) until the
    /// consumer catches up. Stops the queue from growing unbounded if the
    /// consumer can't keep up. Default 1000 ≈ 5 ms of work at ~200K jobs/s.
    #[arg(long, default_value = "1000")]
    target_queue_depth: u64,

    /// Skip the per-trial DEL of `queue:<name>` — preserves any pre-existing
    /// backlog at trial start. Required by Phase 3 Experiment 3 (latency-vs-fill),
    /// where the test runs a steady-state workload against a 25 / 100 / 240 GB
    /// pre-filled list. Only meaningful with `--duration-secs`; ignored in
    /// burst-then-drain mode (which always pre-fills its own backlog).
    #[arg(long)]
    no_clear: bool,
}

// ── Redis URL helpers ─────────────────────────────────────────────────────────

fn build_redis_url(cli: &Cli) -> Result<String> {
    let mut u =
        url::Url::parse(&cli.url).with_context(|| format!("invalid Redis URL: {}", cli.url))?;

    if let Some(host) = &cli.host {
        u.set_host(Some(host))
            .map_err(|_| anyhow::anyhow!("invalid --host: {host}"))?;
    }
    if let Some(port) = cli.port {
        u.set_port(Some(port))
            .map_err(|_| anyhow::anyhow!("cannot set port on URL: {}", cli.url))?;
    }
    if cli.tls && u.scheme() == "redis" {
        u.set_scheme("rediss")
            .map_err(|_| anyhow::anyhow!("cannot upgrade scheme to rediss"))?;
    }
    if let Some(password) = &cli.password {
        // url::Url::set_password percent-encodes special characters (e.g. '@', '/', ':')
        u.set_password(Some(password))
            .map_err(|_| anyhow::anyhow!("cannot set password on URL: {}", cli.url))?;
    }
    // Ensure db path is present
    if u.path().trim_matches('/').is_empty() {
        u.set_path(&format!("/{}", cli.db));
    }

    Ok(u.to_string())
}

/// Return the URL with the password replaced by **** for logging and JSON output.
fn redact_url(raw: &str) -> String {
    match url::Url::parse(raw) {
        Ok(mut u) => {
            if u.password().is_some() {
                let _ = u.set_password(Some("****"));
            }
            u.to_string()
        }
        Err(_) => raw.to_string(),
    }
}

/// Sanitize a tag string to characters safe for use in filenames.
fn sanitize_tag(tag: &str) -> String {
    let s: String = tag
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if s.is_empty() {
        "unknown".to_string()
    } else {
        s
    }
}

/// Reject output paths containing '..' to prevent path traversal.
fn validate_output_path(path: &str) -> Result<()> {
    if path == "-" {
        return Ok(());
    }
    for component in std::path::Path::new(path).components() {
        if component == std::path::Component::ParentDir {
            anyhow::bail!("--output must not contain '..' segments: {path}");
        }
    }
    Ok(())
}

// ── Per-second latency percentile specs ──────────────────────────────────────

#[derive(Clone)]
enum PercentileSpec {
    Quantile { name: String, q: f64 },
    Max,
    Mean,
}

impl PercentileSpec {
    fn name(&self) -> &str {
        match self {
            Self::Quantile { name, .. } => name,
            Self::Max => "max",
            Self::Mean => "mean",
        }
    }

    fn value(&self, hist: &Histogram<u64>) -> u64 {
        if hist.is_empty() {
            return 0;
        }
        match self {
            Self::Quantile { q, .. } => hist.value_at_quantile(*q),
            Self::Max => hist.max(),
            Self::Mean => hist.mean() as u64,
        }
    }
}

/// Parse a percentile spec string: "p50" → 0.50, "p999" → 0.999, "max", "mean".
fn parse_percentile_spec(s: &str) -> Result<PercentileSpec> {
    match s {
        "max" => Ok(PercentileSpec::Max),
        "mean" => Ok(PercentileSpec::Mean),
        s if s.starts_with('p') => {
            let digits = &s[1..];
            anyhow::ensure!(!digits.is_empty(), "invalid percentile spec: '{s}'");
            let n: u64 = digits
                .parse()
                .with_context(|| format!("invalid percentile spec: '{s}'"))?;
            let divisor = 10u64.pow(digits.len() as u32);
            let q = n as f64 / divisor as f64;
            anyhow::ensure!(q > 0.0 && q <= 1.0, "percentile out of range (0, 1]: '{s}'");
            Ok(PercentileSpec::Quantile {
                name: s.to_string(),
                q,
            })
        }
        _ => anyhow::bail!("unknown percentile spec '{s}' — use p50, p99, p999, max, mean"),
    }
}

/// Generate queue names from a base name and count.
/// With n=1 returns `["default"]`; with n=4 returns `["default_0".."default_3"]`.
fn make_queue_names(base: &str, n: usize) -> Vec<String> {
    if n <= 1 {
        vec![base.to_string()]
    } else {
        (0..n).map(|i| format!("{base}_{i}")).collect()
    }
}

async fn fetch_tag(url: &str) -> String {
    let client = match redis::Client::open(url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("warning: could not build Redis client for tag lookup: {e}");
            return "unknown".to_string();
        }
    };
    match client.get_multiplexed_async_connection().await {
        Ok(mut conn) => {
            match redis::cmd("INFO")
                .arg("server")
                .query_async::<String>(&mut conn)
                .await
            {
                Ok(info) => {
                    for line in info.lines() {
                        if let Some(v) = line.strip_prefix("redis_version:") {
                            return format!("redis-{}", v.trim());
                        }
                    }
                    "unknown".to_string()
                }
                Err(e) => {
                    eprintln!("warning: could not fetch Redis INFO for tag: {e}");
                    "unknown".to_string()
                }
            }
        }
        Err(e) => {
            eprintln!("warning: could not connect to Redis for tag lookup: {e}");
            "unknown".to_string()
        }
    }
}

// ── Trial execution ───────────────────────────────────────────────────────────

struct TrialConfig<'a> {
    url: &'a str,
    queues: &'a [String],
    jobs: u64,
    timeout_secs: u64,
    quiet: bool,
    percentile_specs: &'a [PercentileSpec],
    track_stats: bool,
    /// `None` = burst-then-drain (pre-fill happens before run_trial).
    /// `Some(_)` = steady-state — producer runs inside run_trial for the
    /// configured duration; the consumer ends when the producer signals stop.
    steady_state: Option<SteadyStateCfg>,
    payload_size: usize,
}

#[derive(Clone, Copy)]
struct SteadyStateCfg {
    duration_secs: u64,
    target_queue_depth: u64,
}

fn empty_histogram() -> Histogram<u64> {
    // Re-export of metrics::empty_histogram so the local module name resolves
    // without an extra `use` line in every call site.
    metrics::empty_histogram()
}

async fn run_trial(cfg: &TrialConfig<'_>, n_workers: usize) -> Result<TrialResult> {
    let metrics = Arc::new(Metrics::new());
    let (done_tx, mut done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let (latency_tx, latency_rx) = mpsc::unbounded_channel::<u64>();
    let (brpop_tx, brpop_rx) = mpsc::unbounded_channel::<u64>();
    let (lpush_tx, lpush_rx) = mpsc::unbounded_channel::<u64>();

    // Histogram collector — drains the latency channel and builds a HDR histogram.
    // Channel closes when all latency_tx clones are dropped (main sentinel + worker clones).
    let collector = tokio::spawn(async move {
        let mut hist = empty_histogram();
        let mut rx = latency_rx;
        while let Some(us) = rx.recv().await {
            // Lower bound is 0; clamp to 1 so every sample falls within [1, max_value]
            let _ = hist.record(us.max(1));
        }
        hist
    });

    // BRPOP histogram collector — drains per-call latencies from rusty-sidekiq's
    // fetcher. Closes when the processor and its clones drop the sender.
    let brpop_collector = tokio::spawn(async move {
        let mut hist = empty_histogram();
        let mut rx = brpop_rx;
        while let Some(us) = rx.recv().await {
            let _ = hist.record(us.max(1));
        }
        hist
    });

    // LPUSH histogram collector — populated only in steady-state mode (the
    // burst-then-drain path uses bulk pipelines where per-LPUSH timing is
    // meaningless). Stays empty otherwise.
    let lpush_collector = tokio::spawn(async move {
        let mut hist = empty_histogram();
        let mut rx = lpush_rx;
        while let Some(us) = rx.recv().await {
            let _ = hist.record(us.max(1));
        }
        hist
    });

    // Build the rusty-sidekiq Processor with a bb8 connection pool
    let manager = sidekiq::RedisConnectionManager::new(cfg.url)
        .context("invalid Redis URL for Sidekiq pool")?;
    let redis_pool = bb8::Pool::builder()
        .max_size(n_workers as u32 + 4) // +4 for internal sidekiq bookkeeping tasks
        .connection_timeout(Duration::from_secs(10))
        .test_on_check_out(false) // avoid a PING per job dequeue — connections are always live
        .build(manager)
        .await
        .context("failed to build Redis connection pool")?;

    let w = worker::LoadWorker {
        metrics: metrics.clone(),
        latency_tx: latency_tx.clone(), // worker holds a clone; main keeps the sentinel
        done_tx: done_tx.clone(),
        target_jobs: cfg.jobs,
    };

    let mut processor = sidekiq::Processor::new(redis_pool, cfg.queues.to_vec()).with_config(
        sidekiq::ProcessorConfig::default()
            .num_workers(n_workers)
            .track_stats(cfg.track_stats)
            .brpop_latency_tx(brpop_tx.clone()),
    );
    processor.register(w);

    let token = processor.get_cancellation_token();
    // Keep abort_handle so we can abort after timeout even after consuming proc_handle
    let mut proc_handle = tokio::spawn(async move { processor.run().await });
    let abort_handle = proc_handle.abort_handle();

    // Steady-state producer (only in steady-state mode). Runs concurrently
    // with the consumer, LPUSHing one job at a time and recording per-call
    // latency. Soft-caps in-flight at target_queue_depth. Token cancellation
    // ends the producer cleanly at duration_secs.
    let producer_token = CancellationToken::new();
    let producer_handle = if let Some(ss) = cfg.steady_state {
        let producer_url = cfg.url.to_string();
        let producer_queues = cfg.queues.to_vec();
        let producer_metrics = metrics.clone();
        let producer_tx = lpush_tx.clone();
        let producer_cancel = producer_token.clone();
        let payload_size = cfg.payload_size;
        Some(tokio::spawn(async move {
            let client = redis::Client::open(producer_url.as_str())
                .context("invalid Redis URL for producer")?;
            let mut conn = client
                .get_multiplexed_async_connection()
                .await
                .context("producer failed to connect to Redis")?;
            producer::stream_enqueue(
                &mut conn,
                &producer_queues,
                payload_size,
                ss.target_queue_depth,
                producer_metrics,
                producer_tx,
                producer_cancel,
            )
            .await
        }))
    } else {
        None
    };

    // Per-second samples collected by the monitor task
    let throughput_samples: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let errors_samples: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let latency_sec_samples: Arc<Mutex<HashMap<String, Vec<u64>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let tput_for_monitor = throughput_samples.clone();
    let err_for_monitor = errors_samples.clone();
    let lat_for_monitor = latency_sec_samples.clone();
    let metrics_mon = metrics.clone();
    let specs_for_monitor = cfg.percentile_specs.to_vec();
    let quiet = cfg.quiet;

    let monitor = tokio::spawn(async move {
        let mut prev_completed = 0u64;
        let mut prev_errors = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let cur = metrics_mon.get_completed();
            let tput_delta = cur - prev_completed;
            prev_completed = cur;
            if let Ok(mut v) = tput_for_monitor.lock() {
                v.push(tput_delta);
            }

            let cur_err = metrics_mon.get_errors();
            let err_delta = cur_err - prev_errors;
            prev_errors = cur_err;
            if let Ok(mut v) = err_for_monitor.lock() {
                v.push(err_delta);
            }

            let snap = metrics_mon.drain_per_sec();
            if let Ok(mut map) = lat_for_monitor.lock() {
                for spec in &specs_for_monitor {
                    map.entry(spec.name().to_string())
                        .or_default()
                        .push(spec.value(&snap));
                }
            }

            if !quiet {
                if err_delta > 0 {
                    print!("[e:{err_delta}]");
                } else {
                    print!(".");
                }
                use std::io::Write;
                let _ = std::io::stdout().flush();
            }
        }
    });

    let start = Instant::now();
    let mut timed_out = false;
    let mut proc_exited_early = false;

    // Wait condition depends on mode:
    //  - Burst (default): wait until consumer drains target_jobs OR timeout.
    //  - Steady-state: run for the configured duration. The producer runs
    //    concurrently and the trial ends when the duration elapses; the
    //    consumer is then drained as part of the shutdown sequence below.
    tokio::select! {
        _ = done_rx.wait_for(|v| *v), if cfg.steady_state.is_none() => {},
        _ = tokio::time::sleep(Duration::from_secs(
            cfg.steady_state.map(|s| s.duration_secs).unwrap_or(cfg.timeout_secs)
        )) => {
            if cfg.steady_state.is_none() {
                if !cfg.quiet { eprintln!(); }
                eprintln!("  [timeout after {}s]", cfg.timeout_secs);
                timed_out = true;
            }
        }
        res = &mut proc_handle => {
            if !cfg.quiet { eprintln!(); }
            match res {
                Err(e) if e.is_panic() => eprintln!("  [processor task panicked: {e}]"),
                _ => eprintln!("  [processor exited unexpectedly — check Redis connection]"),
            }
            proc_exited_early = true;
            timed_out = true;
        }
    }

    // Snapshot the trial window NOW, before producer/consumer shutdown.
    // In steady-state mode the consumer keeps draining the queue during the
    // ~5 s shutdown timeout; if we read completed at the end, jobs/s would be
    // diluted by drain work that didn't happen at sustained-load conditions.
    let window_duration = start.elapsed();
    let window_completed = metrics.get_completed();
    let window_errors = metrics.get_errors();

    if !cfg.quiet && !timed_out {
        println!();
    }

    monitor.abort();

    // Stop the producer first (so no more LPUSHes hit Redis) before draining
    // the consumer. In burst mode this is a no-op (no producer task).
    producer_token.cancel();
    if let Some(handle) = producer_handle {
        // Producer's drop closes its lpush_tx clone; we still hold the main
        // sentinel below.
        let _ = handle.await;
    }

    if proc_exited_early {
        // Processor::run() waits for in-flight workers before returning, so all worker
        // latency_tx clones are already dropped. Drop the sentinel and the channel closes.
        drop(latency_tx);
        drop(brpop_tx);
        drop(lpush_tx);
    } else {
        // Signal graceful shutdown and give workers up to 5 s to finish current jobs.
        // Workers drop their latency_tx clones when they exit, closing the channel.
        token.cancel();
        drop(latency_tx); // drop sentinel before waiting so channel can close
        drop(brpop_tx); // same — fetcher's clones close once Processor::run returns
        drop(lpush_tx); // producer already exited above; main sentinel is the last clone
        let timed_shutdown = tokio::time::timeout(Duration::from_secs(5), proc_handle).await;
        if timed_shutdown.is_err() {
            // Workers are stuck; force-abort and give the runtime a tick to clean up
            abort_handle.abort();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Channels are now closed — collectors drain buffered values and return histograms
    let hist = collector.await.unwrap_or_else(|_| empty_histogram());
    let brpop_hist = brpop_collector.await.unwrap_or_else(|_| empty_histogram());
    let lpush_hist = lpush_collector.await.unwrap_or_else(|_| empty_histogram());

    // Use the window snapshots (taken before shutdown drain) for the
    // headline numbers. The per-second monitor samples capture progression
    // during the window naturally; readings after window_duration are
    // discarded below.
    let total_jobs = window_completed;
    let errors = window_errors;
    let duration = window_duration;
    let throughput_per_sec = throughput_samples
        .lock()
        .map(|v| v.clone())
        .unwrap_or_default();
    let errors_per_sec = errors_samples.lock().map(|v| v.clone()).unwrap_or_default();
    let latency_per_sec = latency_sec_samples
        .lock()
        .map(|v| v.clone())
        .unwrap_or_default();

    let jobs_per_sec = if duration.as_secs_f64() > 0.0 {
        total_jobs as f64 / duration.as_secs_f64()
    } else {
        0.0
    };

    Ok(TrialResult {
        workers: n_workers,
        total_jobs,
        duration_s: duration.as_secs_f64(),
        jobs_per_sec,
        throughput_per_sec,
        errors_per_sec,
        latency_per_sec,
        latency: LatencyStats::from_histogram(&hist),
        brpop_latency: LatencyStats::from_histogram(&brpop_hist),
        lpush_latency: LatencyStats::from_histogram(&lpush_hist),
        errors,
        timed_out,
    })
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    anyhow::ensure!(cli.jobs > 0, "--jobs must be > 0");
    anyhow::ensure!(cli.num_queues > 0, "--num-queues must be > 0");

    let url = build_redis_url(&cli)?;
    let display_url = redact_url(&url);

    // Warn loudly if FLUSHDB is enabled on db 0 — application data lives there by default.
    if cli.allow_flushdb {
        let db_in_url = url::Url::parse(&url)
            .ok()
            .and_then(|u| u.path().trim_matches('/').parse::<u8>().ok())
            .unwrap_or(0);
        if db_in_url == 0 {
            eprintln!(
                "warning: --allow-flushdb is set on db 0 — this will destroy ALL keys in the \
                 database. Use --db 13 (or any non-zero db) to isolate benchmark data."
            );
        }
    }

    if let Some(out) = &cli.output {
        validate_output_path(out)?;
    }

    let tag = match &cli.tag {
        Some(t) => sanitize_tag(t),
        None => sanitize_tag(&fetch_tag(&url).await),
    };

    let output = cli
        .output
        .clone()
        .unwrap_or_else(|| format!("sidekiq_bench_{tag}.json"));

    let queue_names = make_queue_names(&cli.queue, cli.num_queues);
    let queues_label = if queue_names.len() == 1 {
        queue_names[0].clone()
    } else {
        format!(
            "{} queues ({}…{})",
            queue_names.len(),
            queue_names[0],
            queue_names[queue_names.len() - 1]
        )
    };

    println!("\n=== sidekiq-bench — {tag} ===");
    let workload_label = match cli.duration_secs {
        Some(d) => format!(
            "steady-state {d}s (target depth {})",
            report::format_n(cli.target_queue_depth)
        ),
        None => format!("jobs={}", report::format_n(cli.jobs)),
    };
    println!(
        "    {}  {}  queues={}",
        display_url, workload_label, queues_label
    );
    println!();

    let client = redis::Client::open(url.as_str()).context("invalid Redis URL")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to connect to Redis")?;

    let percentile_specs: Vec<PercentileSpec> = cli
        .latency_percentiles
        .iter()
        .map(|s| parse_percentile_spec(s))
        .collect::<Result<Vec<_>>>()?;

    let cfg = TrialConfig {
        url: &url,
        queues: &queue_names,
        jobs: cli.jobs,
        timeout_secs: cli.timeout,
        quiet: cli.quiet,
        percentile_specs: &percentile_specs,
        track_stats: cli.track_stats,
        steady_state: cli.duration_secs.map(|d| SteadyStateCfg {
            duration_secs: d,
            target_queue_depth: cli.target_queue_depth,
        }),
        payload_size: cli.payload_size,
    };
    // Warmup uses the same settings but targets warmup_jobs completions
    let warmup_cfg = TrialConfig {
        jobs: cli.warmup_jobs,
        ..cfg
    };

    let workers_list = cli.workers.clone();
    let mut results: Vec<TrialResult> = Vec::new();
    let mut any_timeout = false;

    // Warn if the burst-mode pre-fill will likely use significant Redis
    // memory. ~300 B per job (class, jid, args array, queue, retry,
    // timestamps). Steady-state mode keeps in_flight bounded by
    // target_queue_depth, so the memory ceiling is small and the warning
    // would be misleading.
    if cli.duration_secs.is_none() {
        let estimated_mb = cli.jobs as f64 * 300.0 / (1024.0 * 1024.0);
        if estimated_mb > 100.0 {
            eprintln!(
                "warning: estimated peak Redis memory ~{:.0} MB ({} jobs × ~300 B/job)",
                estimated_mb,
                report::format_n(cli.jobs)
            );
        }
    }

    for &n_workers in &workers_list {
        // Warmup + pre-fill only apply to burst-then-drain. In steady-state
        // mode the producer runs concurrently inside run_trial and the
        // backlog starts empty (Phase 3 handles 240 GB pre-fill via a
        // separate one-shot script, not through this tool's CLI).
        if cli.duration_secs.is_none() && cli.warmup_jobs > 0 {
            pre_trial_clear(&mut conn, &queue_names, cli.allow_flushdb).await?;
            producer::bulk_enqueue(&mut conn, &queue_names, cli.warmup_jobs, cli.payload_size)
                .await?;
            if !cli.quiet {
                print!("  [{n_workers:>4} workers] warmup … ");
            }
            run_trial(&warmup_cfg, n_workers).await?;
        }

        if cli.duration_secs.is_none() {
            pre_trial_clear(&mut conn, &queue_names, cli.allow_flushdb).await?;
            producer::bulk_enqueue(&mut conn, &queue_names, cli.jobs, cli.payload_size).await?;
        } else if !cli.no_clear {
            // Steady-state: still clear stale backlog so the trial starts at
            // a known empty state. Producer fills as the consumer drains.
            pre_trial_clear(&mut conn, &queue_names, cli.allow_flushdb).await?;
        }
        // else: --no-clear with --duration-secs — Phase 3 Experiment 3 path.
        // Pre-existing backlog stays; the consumer drains it from the tail
        // while the producer LPUSHes new jobs at the head. Per-command HDR
        // (LPUSH + BRPOP) is the meaningful measurement in this mode; the
        // e2e histogram will skew very high (pre-fill jobs sat in queue for
        // the full pre-fill duration before being dequeued).

        if !cli.quiet {
            print!("  [{n_workers:>4} workers] ");
        }

        let result = run_trial(&cfg, n_workers).await?;

        if result.timed_out {
            any_timeout = true;
        }
        report::print_trial_line(&result);
        results.push(result);
    }

    report::print_summary(&results);

    report::write_json(
        &results,
        &tag,
        &display_url,
        &workers_list,
        cli.jobs,
        &queue_names,
        cli.warmup_jobs,
        &output,
    )?;

    if any_timeout {
        eprintln!("warning: one or more trials timed out — results are incomplete");
        std::process::exit(1);
    }

    Ok(())
}

/// Clear queues before a trial. Uses DEL by default; FLUSHDB only when explicitly allowed.
async fn pre_trial_clear(
    conn: &mut redis::aio::MultiplexedConnection,
    queues: &[String],
    allow_flushdb: bool,
) -> Result<()> {
    if allow_flushdb {
        producer::flushdb(conn).await
    } else {
        producer::clear_queue(conn, queues).await
    }
}

// ── TrialConfig Copy impl ─────────────────────────────────────────────────────

impl<'a> Copy for TrialConfig<'a> {}
impl<'a> Clone for TrialConfig<'a> {
    fn clone(&self) -> Self {
        *self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_size_default_matches_legacy_string_length() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["sidekiq-bench"]).unwrap();
        assert_eq!(cli.payload_size, 6);
    }

    #[test]
    fn track_stats_default_is_off() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["sidekiq-bench"]).unwrap();
        assert!(!cli.track_stats);
    }

    #[test]
    fn track_stats_flag_is_presence_based() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["sidekiq-bench", "--track-stats"]).unwrap();
        assert!(cli.track_stats);
    }

    #[test]
    fn duration_secs_default_is_none_burst_mode() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["sidekiq-bench"]).unwrap();
        assert!(cli.duration_secs.is_none());
        assert_eq!(cli.target_queue_depth, 1000);
    }

    #[test]
    fn duration_secs_opts_into_steady_state() {
        use clap::Parser;
        let cli = Cli::try_parse_from([
            "sidekiq-bench",
            "--duration-secs",
            "30",
            "--target-queue-depth",
            "5000",
        ])
        .unwrap();
        assert_eq!(cli.duration_secs, Some(30));
        assert_eq!(cli.target_queue_depth, 5000);
    }

    #[test]
    fn sanitize_tag_strips_unsafe_chars() {
        assert_eq!(sanitize_tag("redis-8.0"), "redis-8.0"); // dots and dashes kept
        assert_eq!(sanitize_tag("redis/8.0"), "redis-8.0"); // slash → dash
                                                            // dots are valid in filenames; path-traversal is caught by validate_output_path
        assert_eq!(sanitize_tag("../evil"), "..-evil");
        assert_eq!(sanitize_tag("foo bar"), "foo-bar"); // space → dash
        assert_eq!(sanitize_tag(""), "unknown");
    }

    #[test]
    fn validate_output_path_rejects_traversal() {
        assert!(validate_output_path("../evil.json").is_err());
        assert!(validate_output_path("foo/../bar.json").is_err());
        assert!(validate_output_path("results/out.json").is_ok());
        assert!(validate_output_path("-").is_ok());
        assert!(validate_output_path("out.json").is_ok());
    }

    #[test]
    fn redact_url_hides_password() {
        let raw = "redis://:hunter2@127.0.0.1:6379/0";
        let redacted = redact_url(raw);
        assert!(
            !redacted.contains("hunter2"),
            "password still visible: {redacted}"
        );
        assert!(redacted.contains("****"), "no redaction marker: {redacted}");
    }

    #[test]
    fn redact_url_leaves_no_password_url_unchanged() {
        let raw = "redis://127.0.0.1:6379/0";
        assert_eq!(redact_url(raw), raw);
    }

    #[test]
    fn build_redis_url_encodes_special_chars_in_password() {
        // Password containing '@' must be percent-encoded so the URL is parsed correctly.
        // url::Url::password() returns the raw percent-encoded form; host_str() must still
        // be 127.0.0.1 (proving '@' wasn't treated as a userinfo/host separator).
        let cli = Cli {
            url: "redis://127.0.0.1:6379/0".into(),
            host: None,
            port: None,
            password: Some("p@ss/word".into()),
            tls: false,
            db: 0,
            workers: vec![10],
            jobs: 1000,
            warmup_jobs: 0,
            queue: "default".into(),
            num_queues: 1,
            latency_percentiles: vec![],
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
            payload_size: 0,
            track_stats: false,
            duration_secs: None,
            target_queue_depth: 1000,
            no_clear: false,
        };
        let url = build_redis_url(&cli).unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        // host must be 127.0.0.1 — not a fragment of the password mistaken for userinfo
        assert_eq!(parsed.host_str().unwrap(), "127.0.0.1");
        // url::Url::password() returns the raw percent-encoded form; '@' → %40, '/' → %2F
        let raw_pw = parsed.password().unwrap();
        assert!(raw_pw.contains("%40"), "@ not percent-encoded: {raw_pw}");
        assert!(!url.contains(":p@ss"), "raw '@' leaked into URL: {url}");
    }

    #[test]
    fn build_redis_url_upgrades_scheme_with_tls() {
        let cli = Cli {
            url: "redis://127.0.0.1:6379/0".into(),
            host: None,
            port: None,
            password: None,
            tls: true,
            db: 0,
            workers: vec![10],
            jobs: 1000,
            warmup_jobs: 0,
            queue: "default".into(),
            num_queues: 1,
            latency_percentiles: vec![],
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
            payload_size: 0,
            track_stats: false,
            duration_secs: None,
            target_queue_depth: 1000,
            no_clear: false,
        };
        let url = build_redis_url(&cli).unwrap();
        assert!(url.starts_with("rediss://"), "expected rediss:// got {url}");
    }

    #[test]
    fn build_redis_url_host_port_override() {
        let cli = Cli {
            url: "redis://127.0.0.1:6379/0".into(),
            host: Some("10.0.0.1".into()),
            port: Some(6380),
            password: None,
            tls: false,
            db: 0,
            workers: vec![10],
            jobs: 1000,
            warmup_jobs: 0,
            queue: "default".into(),
            num_queues: 1,
            latency_percentiles: vec![],
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
            payload_size: 0,
            track_stats: false,
            duration_secs: None,
            target_queue_depth: 1000,
            no_clear: false,
        };
        let url = build_redis_url(&cli).unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.host_str().unwrap(), "10.0.0.1");
        assert_eq!(parsed.port().unwrap(), 6380);
    }

    #[test]
    fn parse_percentile_spec_valid() {
        let cases: &[(&str, f64)] = &[
            ("p50", 0.50),
            ("p90", 0.90),
            ("p99", 0.99),
            ("p999", 0.999),
            ("p9999", 0.9999),
            ("p75", 0.75),
        ];
        for &(s, expected_q) in cases {
            match parse_percentile_spec(s).unwrap() {
                PercentileSpec::Quantile { q, name } => {
                    assert!((q - expected_q).abs() < 1e-9, "{s}: got {q}");
                    assert_eq!(name, s);
                }
                other => panic!("{s} parsed as non-quantile: {}", other.name()),
            }
        }
        assert!(matches!(
            parse_percentile_spec("max").unwrap(),
            PercentileSpec::Max
        ));
        assert!(matches!(
            parse_percentile_spec("mean").unwrap(),
            PercentileSpec::Mean
        ));
    }

    #[test]
    fn parse_percentile_spec_invalid() {
        assert!(parse_percentile_spec("p0").is_err()); // 0/10 = 0.0 out of range
        assert!(parse_percentile_spec("p").is_err());
        assert!(parse_percentile_spec("pxyz").is_err());
        assert!(parse_percentile_spec("99").is_err());
        assert!(parse_percentile_spec("").is_err());
    }

    #[test]
    fn make_queue_names_single_and_multi() {
        assert_eq!(make_queue_names("default", 1), vec!["default"]);
        assert_eq!(make_queue_names("q", 3), vec!["q_0", "q_1", "q_2"]);
    }
}
