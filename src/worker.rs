use crate::metrics::Metrics;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};

/// Sidekiq worker that records job latency and counts completions.
/// rusty-sidekiq wraps this in Arc<LoadWorker> shared by all worker tasks.
#[derive(Clone)]
pub struct LoadWorker {
    pub metrics: Arc<Metrics>,
    /// Sends latency_us values to the histogram collector task.
    pub latency_tx: mpsc::UnboundedSender<u64>,
    /// Signals the trial orchestrator when all jobs are done.
    pub done_tx: Arc<watch::Sender<bool>>,
    pub target_jobs: u64,
}

#[async_trait]
impl sidekiq::Worker<serde_json::Value> for LoadWorker {
    /// Called by rusty-sidekiq after BRPOP dequeues a job.
    /// `args` is the JSON array from the job payload: ["string", idx, {"mike":"bob"}, enqueued_at_ns]
    async fn perform(&self, args: serde_json::Value) -> sidekiq::Result<()> {
        // args[3] carries enqueued_at_ns (u64 nanoseconds since epoch)
        if let Some(enqueued_at_ns) = args
            .as_array()
            .and_then(|a| a.get(3))
            .and_then(|v| v.as_u64())
        {
            let now_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is before UNIX_EPOCH")
                .as_nanos() as u64;

            let latency_us = if now_ns >= enqueued_at_ns {
                (now_ns - enqueued_at_ns) / 1_000
            } else {
                // Clock skew: producer clock is ahead of worker clock — record 1 µs to avoid
                // saturating_sub giving 0 which would be silently discarded by HDR lower bound
                1
            };
            // Clamp to 1 µs minimum so the value is always within the histogram's lower bound
            let clamped = latency_us.max(1);
            // The collector task in main.rs owns both the trial-long and the
            // per-second histograms (no shared mutex on the worker hot path).
            let _ = self.latency_tx.send(clamped);
        } else {
            self.metrics.inc_error();
        }

        let done = self.metrics.inc_completed();
        if done >= self.target_jobs {
            let _ = self.done_tx.send(true);
        }
        Ok(())
    }
}
