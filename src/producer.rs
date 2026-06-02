use crate::job::SidekiqJob;
use crate::metrics::Metrics;
use anyhow::Result;
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

/// Steady-state producer: loops LPUSHing one job at a time, recording per-call
/// latency, until the cancellation token fires. Soft-caps in-flight at
/// `target_queue_depth` (= produced − completed) so the consumer keeps up;
/// when at the cap, yields briefly instead of busy-spinning. Returns the
/// total job count it managed to push (useful for diagnostics + sanity
/// checks against the consumer's completed count).
///
/// Used by Phase 3's sustained-load trials, where the goal is latency
/// stability under continuous push+pop — not burst throughput.
#[allow(clippy::too_many_arguments)]
pub async fn stream_enqueue(
    conn: &mut redis::aio::MultiplexedConnection,
    queues: &[String],
    payload_size: usize,
    target_queue_depth: u64,
    metrics: Arc<Metrics>,
    lpush_latency_tx: mpsc::UnboundedSender<u64>,
    cancel: CancellationToken,
) -> Result<u64> {
    // Register all queues once for Sidekiq-web / monitoring tools.
    let mut sadd_pipe = redis::pipe();
    for queue in queues {
        sadd_pipe.cmd("SADD").arg("queues").arg(queue).ignore();
    }
    sadd_pipe.query_async::<()>(conn).await?;

    let arg0 = SidekiqJob::build_arg0(payload_size);
    let n_queues = queues.len() as u64;
    let mut idx = 0u64;

    while !cancel.is_cancelled() {
        let done = metrics.get_completed();
        let in_flight = idx.saturating_sub(done);
        if in_flight >= target_queue_depth {
            // Consumer hasn't caught up — back off a tick so we don't pile
            // backlog and burn the producer's CPU on a spin loop.
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
            continue;
        }

        let queue = &queues[(idx % n_queues) as usize];
        let job = SidekiqJob::new(queue, idx, &arg0);
        let payload = serde_json::to_string(&job)?;
        let key = format!("queue:{queue}");

        let start = Instant::now();
        redis::cmd("LPUSH")
            .arg(&key)
            .arg(payload)
            .query_async::<i64>(conn)
            .await?;
        let _ = lpush_latency_tx.send(start.elapsed().as_micros().max(1) as u64);

        idx += 1;
    }

    Ok(idx)
}
