use crate::job::SidekiqJob;
use anyhow::Result;

const BATCH_SIZE: usize = 1000;

/// Delete only the benchmark queue key — safe to use on shared Redis.
/// This is the default pre-trial cleanup.
pub async fn clear_queue(conn: &mut redis::aio::MultiplexedConnection, queue: &str) -> Result<()> {
    redis::cmd("DEL")
        .arg(format!("queue:{queue}"))
        .query_async::<()>(conn)
        .await?;
    Ok(())
}

/// Flush the entire database. Only called when --allow-flushdb is explicitly set.
pub async fn flushdb(conn: &mut redis::aio::MultiplexedConnection) -> Result<()> {
    redis::cmd("FLUSHDB").query_async::<()>(conn).await?;
    Ok(())
}

/// Bulk-enqueue `n_jobs` Sidekiq jobs into `queue:{queue}` using pipelined LPUSH.
/// Also registers the queue in the `queues` set for Sidekiq-web visibility.
pub async fn bulk_enqueue(
    conn: &mut redis::aio::MultiplexedConnection,
    queue: &str,
    n_jobs: u64,
) -> Result<()> {
    // Register queue for Sidekiq-web and monitoring tools
    redis::cmd("SADD")
        .arg("queues")
        .arg(queue)
        .query_async::<()>(conn)
        .await?;

    let redis_key = format!("queue:{queue}");
    let mut idx = 0u64;
    let mut remaining = n_jobs;

    while remaining > 0 {
        let batch = remaining.min(BATCH_SIZE as u64) as usize;
        let mut pipe = redis::pipe();

        for _ in 0..batch {
            let job = SidekiqJob::new(queue, idx);
            let payload = serde_json::to_string(&job)?;
            pipe.lpush(&redis_key, payload).ignore();
            idx += 1;
        }

        pipe.query_async::<()>(conn).await?;
        remaining -= batch as u64;
    }

    Ok(())
}
