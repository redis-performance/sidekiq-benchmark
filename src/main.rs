mod job;
mod metrics;
mod producer;
mod report;
mod worker;

use anyhow::{Context, Result};
use clap::Parser;
use hdrhistogram::Histogram;
use metrics::{LatencyStats, Metrics, TrialResult};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};

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

    /// Sidekiq queue name
    #[arg(long, default_value = "default")]
    queue: String,

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
    queue: &'a str,
    jobs: u64,
    timeout_secs: u64,
    quiet: bool,
}

fn empty_histogram() -> Histogram<u64> {
    // HDRHistogram requires low >= 1; values are clamped to .max(1) before recording
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("valid histogram bounds")
}

async fn run_trial(cfg: &TrialConfig<'_>, n_workers: usize) -> Result<TrialResult> {
    let metrics = Arc::new(Metrics::new());
    let (done_tx, mut done_rx) = watch::channel(false);
    let done_tx = Arc::new(done_tx);
    let (latency_tx, latency_rx) = mpsc::unbounded_channel::<u64>();

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

    // Build the rusty-sidekiq Processor with a bb8 connection pool
    let manager = sidekiq::RedisConnectionManager::new(cfg.url)
        .context("invalid Redis URL for Sidekiq pool")?;
    let redis_pool = bb8::Pool::builder()
        .max_size(n_workers as u32 + 4) // +4 for internal sidekiq bookkeeping tasks
        .connection_timeout(Duration::from_secs(10))
        .build(manager)
        .await
        .context("failed to build Redis connection pool")?;

    let w = worker::LoadWorker {
        metrics: metrics.clone(),
        latency_tx: latency_tx.clone(), // worker holds a clone; main keeps the sentinel
        done_tx: done_tx.clone(),
        target_jobs: cfg.jobs,
    };

    let mut processor = sidekiq::Processor::new(redis_pool, vec![cfg.queue.to_string()])
        .with_config(sidekiq::ProcessorConfig::default().num_workers(n_workers));
    processor.register(w);

    let token = processor.get_cancellation_token();
    // Keep abort_handle so we can abort after timeout even after consuming proc_handle
    let mut proc_handle = tokio::spawn(async move { processor.run().await });
    let abort_handle = proc_handle.abort_handle();

    // Per-second throughput samples collected by the monitor task
    let throughput_samples: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let samples_for_monitor = throughput_samples.clone();
    let metrics_mon = metrics.clone();
    let quiet = cfg.quiet;
    let monitor = tokio::spawn(async move {
        let mut prev = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let cur = metrics_mon.get_completed();
            let delta = cur - prev;
            prev = cur;
            if let Ok(mut v) = samples_for_monitor.lock() {
                v.push(delta);
            }
            if !quiet {
                print!(".");
                use std::io::Write;
                let _ = std::io::stdout().flush();
            }
        }
    });

    let start = Instant::now();
    let mut timed_out = false;
    let mut proc_exited_early = false;

    // Wait for all jobs to complete, timeout, or processor failure
    tokio::select! {
        _ = done_rx.wait_for(|v| *v) => {},
        _ = tokio::time::sleep(Duration::from_secs(cfg.timeout_secs)) => {
            if !cfg.quiet { eprintln!(); }
            eprintln!("  [timeout after {}s]", cfg.timeout_secs);
            timed_out = true;
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

    let duration = start.elapsed();
    if !cfg.quiet && !timed_out {
        println!();
    }

    monitor.abort();

    if proc_exited_early {
        // Processor::run() waits for in-flight workers before returning, so all worker
        // latency_tx clones are already dropped. Drop the sentinel and the channel closes.
        drop(latency_tx);
    } else {
        // Signal graceful shutdown and give workers up to 5 s to finish current jobs.
        // Workers drop their latency_tx clones when they exit, closing the channel.
        token.cancel();
        drop(latency_tx); // drop sentinel before waiting so channel can close
        let timed_shutdown = tokio::time::timeout(Duration::from_secs(5), proc_handle).await;
        if timed_shutdown.is_err() {
            // Workers are stuck; force-abort and give the runtime a tick to clean up
            abort_handle.abort();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Channel is now closed — collector drains buffered values and returns the histogram
    let hist = collector.await.unwrap_or_else(|_| empty_histogram());

    let total_jobs = metrics.get_completed();
    let errors = metrics.errors.load(std::sync::atomic::Ordering::Relaxed);
    let throughput_per_sec = throughput_samples
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
        latency: LatencyStats::from_histogram(&hist),
        errors,
        timed_out,
    })
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    anyhow::ensure!(cli.jobs > 0, "--jobs must be > 0");

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

    println!("\n=== sidekiq-bench — {tag} ===");
    println!("    {}  jobs={}", display_url, report::format_n(cli.jobs));
    println!();

    let client = redis::Client::open(url.as_str()).context("invalid Redis URL")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("failed to connect to Redis")?;

    let cfg = TrialConfig {
        url: &url,
        queue: &cli.queue,
        jobs: cli.jobs,
        timeout_secs: cli.timeout,
        quiet: cli.quiet,
    };
    // Warmup uses the same settings but targets warmup_jobs completions
    let warmup_cfg = TrialConfig {
        jobs: cli.warmup_jobs,
        ..cfg
    };

    let workers_list = cli.workers.clone();
    let mut results: Vec<TrialResult> = Vec::new();
    let mut any_timeout = false;

    // Warn if the queue fill will likely use significant Redis memory.
    // ~300 B per job (class, jid, args array, queue, retry, timestamps).
    let estimated_mb = cli.jobs as f64 * 300.0 / (1024.0 * 1024.0);
    if estimated_mb > 100.0 {
        eprintln!(
            "warning: estimated peak Redis memory ~{:.0} MB ({} jobs × ~300 B/job)",
            estimated_mb,
            report::format_n(cli.jobs)
        );
    }

    for &n_workers in &workers_list {
        if cli.warmup_jobs > 0 {
            pre_trial_clear(&mut conn, &cli.queue, cli.allow_flushdb).await?;
            producer::bulk_enqueue(&mut conn, &cli.queue, cli.warmup_jobs).await?;
            if !cli.quiet {
                print!("  [{n_workers:>4} workers] warmup … ");
            }
            run_trial(&warmup_cfg, n_workers).await?;
        }

        pre_trial_clear(&mut conn, &cli.queue, cli.allow_flushdb).await?;
        producer::bulk_enqueue(&mut conn, &cli.queue, cli.jobs).await?;

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
        &cli.queue,
        cli.warmup_jobs,
        &output,
    )?;

    if any_timeout {
        eprintln!("warning: one or more trials timed out — results are incomplete");
        std::process::exit(1);
    }

    Ok(())
}

/// Clear the queue before a trial. Uses DEL by default; FLUSHDB only when explicitly allowed.
async fn pre_trial_clear(
    conn: &mut redis::aio::MultiplexedConnection,
    queue: &str,
    allow_flushdb: bool,
) -> Result<()> {
    if allow_flushdb {
        producer::flushdb(conn).await
    } else {
        producer::clear_queue(conn, queue).await
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
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
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
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
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
            tag: None,
            output: None,
            timeout: 300,
            quiet: false,
            allow_flushdb: false,
        };
        let url = build_redis_url(&cli).unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        assert_eq!(parsed.host_str().unwrap(), "10.0.0.1");
        assert_eq!(parsed.port().unwrap(), 6380);
    }
}
