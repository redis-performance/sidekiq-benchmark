use crate::job::SidekiqJob;
use crate::metrics::Metrics;
use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const BATCH_SIZE: usize = 1000;

/// Delete all benchmark queue keys and remove them from the `queues` set.
/// This is the default pre-trial cleanup — safe to use on shared Redis.
pub async fn clear_queue(
    conn: &mut redis::aio::MultiplexedConnection,
    queues: &[String],
) -> Result<()> {
    let mut pipe = redis::pipe();
    for queue in queues {
        pipe.cmd("DEL").arg(format!("queue:{queue}")).ignore();
        pipe.cmd("SREM").arg("queues").arg(queue).ignore();
    }
    pipe.query_async::<()>(conn).await?;
    Ok(())
}

/// Flush the entire database. Only called when --allow-flushdb is explicitly set.
pub async fn flushdb(conn: &mut redis::aio::MultiplexedConnection) -> Result<()> {
    redis::cmd("FLUSHDB").query_async::<()>(conn).await?;
    Ok(())
}

/// Bulk-enqueue `n_jobs` Sidekiq jobs distributed round-robin across `queues`.
/// Also registers every queue in the `queues` set for Sidekiq-web visibility.
/// `payload_size` sets each job's `args[0]` byte length (0 = empty).
pub async fn bulk_enqueue(
    conn: &mut redis::aio::MultiplexedConnection,
    queues: &[String],
    n_jobs: u64,
    payload_size: usize,
) -> Result<()> {
    // Register all queues for Sidekiq-web and monitoring tools
    let mut sadd_pipe = redis::pipe();
    for queue in queues {
        sadd_pipe.cmd("SADD").arg("queues").arg(queue).ignore();
    }
    sadd_pipe.query_async::<()>(conn).await?;

    let arg0 = SidekiqJob::build_arg0(payload_size);
    let n_queues = queues.len() as u64;
    let mut idx = 0u64;
    let mut remaining = n_jobs;

    while remaining > 0 {
        let batch = remaining.min(BATCH_SIZE as u64) as usize;
        let mut pipe = redis::pipe();

        for j in 0..batch {
            let queue = &queues[((idx + j as u64) % n_queues) as usize];
            let job = SidekiqJob::new(queue, idx + j as u64, &arg0);
            let payload = serde_json::to_string(&job)?;
            pipe.lpush(format!("queue:{queue}"), payload).ignore();
        }

        pipe.query_async::<()>(conn).await?;
        idx += batch as u64;
        remaining -= batch as u64;
    }

    Ok(())
}

/// What the producer emits per pushed job.
///
/// `Raw` is the historical Phase 1/2 wire shape: a single bare `LPUSH` per
/// job, with a one-shot upfront `SADD queues <queue>` for every queue. Use
/// this when cross-comparing against pre-Phase-3 results.
///
/// `Sidekiq` mirrors Ruby Sidekiq's `Sidekiq::Client.push` — every job's
/// enqueue emits two commands in a single pipelined round trip:
///
///   SADD queues <queue>
///   LPUSH queue:<queue> <job_json>
///
/// matching the on-the-wire shape an application using `Worker.perform_async`
/// would generate. This is the right mode for benchmarks meant to reflect
/// what a customer's Sidekiq client puts on Redis. Default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProducerMode {
    Raw,
    Sidekiq,
}

/// Steady-state producer: fan out `parallelism` concurrent push tasks, each
/// owning a clone of the multiplexed Redis connection, and let them push
/// one job per iteration with per-call HDR latency recording until the
/// cancellation token fires.
///
/// Soft-caps in-flight at `target_queue_depth` (= produced − completed) so
/// the consumer keeps up; when at the cap, each task yields briefly instead
/// of busy-spinning. Returns the total job count pushed across all tasks
/// (useful for diagnostics + sanity checks against the consumer's completed
/// count).
///
/// The wire shape per push is controlled by `mode`:
///   - `ProducerMode::Raw`     → 1 LPUSH per push (Phase 1/2 baseline).
///   - `ProducerMode::Sidekiq` → 1 SADD + 1 LPUSH pipelined per push
///     (Ruby `Sidekiq::Client.push` wire shape). Adds ~1 cmd/job of
///     producer-side Redis work.
///
/// Recorded latency (`lpush_latency_tx`) in Sidekiq mode covers the full
/// SADD+LPUSH pipeline round trip — the per-push wire-level cost as a real
/// Sidekiq client would see it.
///
/// Parallelism rationale (Phase 3 pre-flight, 2026-06-02): a single
/// sequential push cycles at ~290 µs RTT to a peered RS endpoint — about
/// 3.4K jobs/s. Phase 3's 240 GB scenario needs the producer to sustain
/// ~200K jobs/s to match a saturated single-shard consumer; fanning out N
/// concurrent in-flight pushes through the same multiplexed connection
/// scales the producer to ~3.4K × N jobs/s. Default 64 is comfortable
/// headroom for the 240 GB single-shard scenario.
#[allow(clippy::too_many_arguments)]
pub async fn stream_enqueue(
    conn: redis::aio::MultiplexedConnection,
    queues: &[String],
    payload_size: usize,
    target_queue_depth: u64,
    parallelism: usize,
    mode: ProducerMode,
    metrics: Arc<Metrics>,
    lpush_latency_tx: mpsc::UnboundedSender<u64>,
    cancel: CancellationToken,
) -> Result<u64> {
    anyhow::ensure!(parallelism >= 1, "producer parallelism must be >= 1");

    // Register all queues once for Sidekiq-web / monitoring tools. In
    // Sidekiq mode this primes the `queues` set so the per-push SADD is
    // always a no-op fast path on a hot member.
    {
        let mut sadd_conn = conn.clone();
        let mut sadd_pipe = redis::pipe();
        for queue in queues {
            sadd_pipe.cmd("SADD").arg("queues").arg(queue).ignore();
        }
        sadd_pipe.query_async::<()>(&mut sadd_conn).await?;
    }

    let arg0 = Arc::new(SidekiqJob::build_arg0(payload_size));
    let total_pushed = Arc::new(AtomicU64::new(0));
    let queues: Arc<[String]> = Arc::from(queues.to_vec());
    let n_queues = queues.len() as u64;

    let mut handles = Vec::with_capacity(parallelism);
    for _ in 0..parallelism {
        let mut c = conn.clone();
        let queues = queues.clone();
        let arg0 = arg0.clone();
        let metrics = metrics.clone();
        let lpush_tx = lpush_latency_tx.clone();
        let cancel = cancel.clone();
        let total_pushed = total_pushed.clone();

        handles.push(tokio::spawn(async move {
            // Each task soft-caps its share of the in-flight window so the
            // sum across all tasks respects the global target depth.
            let per_task_cap = target_queue_depth;
            while !cancel.is_cancelled() {
                let done = metrics.get_completed();
                let pushed_now = total_pushed.load(Ordering::Acquire);
                let in_flight = pushed_now.saturating_sub(done);
                if in_flight >= per_task_cap {
                    tokio::time::sleep(std::time::Duration::from_micros(100)).await;
                    continue;
                }

                let idx = total_pushed.fetch_add(1, Ordering::AcqRel);
                let queue = &queues[(idx % n_queues) as usize];
                let job = SidekiqJob::new(queue, idx, arg0.as_str());
                let payload = match serde_json::to_string(&job) {
                    Ok(p) => p,
                    Err(_) => continue,
                };

                let start = Instant::now();
                let res: redis::RedisResult<()> = match mode {
                    ProducerMode::Raw => {
                        // Phase 1/2 baseline: bare LPUSH, no per-push SADD.
                        redis::cmd("LPUSH")
                            .arg(format!("queue:{queue}"))
                            .arg(&payload)
                            .query_async::<i64>(&mut c)
                            .await
                            .map(|_| ())
                    }
                    ProducerMode::Sidekiq => {
                        // Ruby `Sidekiq::Client.push` wire shape: SADD + LPUSH
                        // pipelined into a single round trip per job.
                        let mut pipe = redis::pipe();
                        pipe.cmd("SADD").arg("queues").arg(queue).ignore();
                        pipe.cmd("LPUSH")
                            .arg(format!("queue:{queue}"))
                            .arg(&payload)
                            .ignore();
                        pipe.query_async::<()>(&mut c).await
                    }
                };
                if res.is_err() {
                    // Producer connection died — most likely target Redis went
                    // away. Bail this task; the consumer will detect EOF and
                    // the trial will time out gracefully.
                    break;
                }
                let _ = lpush_tx.send(start.elapsed().as_micros().max(1) as u64);
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    Ok(total_pushed.load(Ordering::Acquire))
}
