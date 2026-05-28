use hdrhistogram::Histogram;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Shared counters updated atomically by worker tasks.
///
/// Note: per-second latency windowing is intentionally NOT stored here. It used
/// to be a `Mutex<Histogram>` on which every worker contended for every job;
/// at 100+ workers that lock became the dominant throughput bottleneck (~4×
/// degradation at saturation). It now lives inside the collector task in
/// `main.rs`, which is the sole writer for both the trial-long and the
/// per-second histograms — no mutex on the worker hot path.
pub struct Metrics {
    pub completed: AtomicU64,
    pub errors: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            completed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    /// Increment completion counter and return the new value.
    /// AcqRel ensures the increment is visible to other threads before done_tx.send().
    pub fn inc_completed(&self) -> u64 {
        self.completed.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn inc_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Acquire ordering guarantees we see all inc_completed writes from other threads.
    pub fn get_completed(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }

    pub fn get_errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregated results from one trial.
#[derive(Debug, Clone)]
pub struct TrialResult {
    pub workers: usize,
    pub total_jobs: u64,
    pub duration_s: f64,
    pub jobs_per_sec: f64,
    pub throughput_per_sec: Vec<u64>,
    pub errors_per_sec: Vec<u64>,
    pub latency_per_sec: HashMap<String, Vec<u64>>,
    pub latency: LatencyStats,
    pub errors: u64,
    pub timed_out: bool,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct LatencyStats {
    pub p50: u64,
    pub p75: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
    pub p99_9: u64,
    pub p99_99: u64,
    pub max: u64,
    pub mean: f64,
    pub total_count: u64,
}

impl LatencyStats {
    pub fn from_histogram(hist: &Histogram<u64>) -> Self {
        if hist.is_empty() {
            return Self::default();
        }
        Self {
            p50: hist.value_at_quantile(0.50),
            p75: hist.value_at_quantile(0.75),
            p90: hist.value_at_quantile(0.90),
            p95: hist.value_at_quantile(0.95),
            p99: hist.value_at_quantile(0.99),
            p99_9: hist.value_at_quantile(0.999),
            p99_99: hist.value_at_quantile(0.9999),
            max: hist.max(),
            mean: hist.mean(),
            total_count: hist.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_histogram_returns_zero_stats() {
        let hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        let stats = LatencyStats::from_histogram(&hist);
        assert_eq!(stats.total_count, 0);
        assert_eq!(stats.p99, 0);
        assert_eq!(stats.max, 0);
        assert_eq!(stats.mean, 0.0);
    }

    #[test]
    fn histogram_records_values_correctly() {
        let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        for i in 1..=1000u64 {
            let _ = hist.record(i * 1_000);
        }
        let stats = LatencyStats::from_histogram(&hist);
        assert_eq!(stats.total_count, 1000);
        assert!(stats.p50 > 0);
        assert!(stats.p99 > stats.p50);
        assert!(stats.mean > 0.0);
    }

    #[test]
    fn inc_completed_returns_new_value() {
        let m = Metrics::new();
        assert_eq!(m.inc_completed(), 1);
        assert_eq!(m.inc_completed(), 2);
        assert_eq!(m.get_completed(), 2);
    }
}
