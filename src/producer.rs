use crate::job::SidekiqJob;
use anyhow::Result;

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
/// `payload_size` sets each job's `args[0]` byte length (0 = default "string").
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

    let n_queues = queues.len() as u64;
    let mut idx = 0u64;
    let mut remaining = n_jobs;

    while remaining > 0 {
        let batch = remaining.min(BATCH_SIZE as u64) as usize;
        let mut pipe = redis::pipe();

        for j in 0..batch {
            let queue = &queues[((idx + j as u64) % n_queues) as usize];
            let job = SidekiqJob::new(queue, idx + j as u64, payload_size);
            let payload = serde_json::to_string(&job)?;
            pipe.lpush(format!("queue:{queue}"), payload).ignore();
        }

        pipe.query_async::<()>(conn).await?;
        idx += batch as u64;
        remaining -= batch as u64;
    }

    Ok(())
}
