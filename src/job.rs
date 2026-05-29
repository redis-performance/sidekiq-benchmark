use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Sidekiq-wire-compatible job payload.
/// Matches the JSON format that rusty-sidekiq's Job struct expects on BRPOP.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SidekiqJob {
    pub class: String,
    pub jid: String,
    /// args[3] carries enqueued_at_ns (u64 nanoseconds since epoch) for latency measurement.
    /// Full layout: ["string", idx, {"mike":"bob"}, enqueued_at_ns]
    pub args: Vec<serde_json::Value>,
    pub queue: String,
    pub retry: serde_json::Value, // matches Ruby retry: 1 (retry once on failure)
    pub created_at: f64,          // required by rusty-sidekiq Job struct
    pub enqueued_at: f64,         // Unix seconds — standard Sidekiq field
}

impl SidekiqJob {
    /// `payload_size` controls the byte length of `args[0]`:
    ///   * 0  → keeps the literal `"string"` placeholder (~6 bytes; backwards-compatible default)
    ///   * N  → replaces with `N` ASCII filler bytes (use ~700 for total serialized job ~1 KB)
    pub fn new(queue: &str, idx: u64, payload_size: usize) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch");
        let enqueued_at_ns = now.as_nanos() as u64;
        let enqueued_at_secs = now.as_secs_f64();

        let arg0 = if payload_size == 0 {
            "string".to_string()
        } else {
            "a".repeat(payload_size)
        };

        SidekiqJob {
            class: "LoadWorker".to_string(),
            jid: new_jid(),
            args: vec![
                serde_json::Value::String(arg0),
                serde_json::Value::Number(idx.into()),
                serde_json::json!({"mike": "bob"}),
                serde_json::Value::Number(enqueued_at_ns.into()),
            ],
            queue: queue.to_string(),
            retry: serde_json::json!(1), // Ruby sidekiqload default: retry: 1
            created_at: enqueued_at_secs,
            enqueued_at: enqueued_at_secs,
        }
    }

    /// Extract the enqueue timestamp embedded in args[3] (nanoseconds since epoch).
    #[allow(dead_code)]
    pub fn enqueued_at_ns(args: &serde_json::Value) -> Option<u64> {
        args.as_array()?.get(3)?.as_u64()
    }
}

/// Sidekiq jid: 24 hex characters (12 random bytes), matching Ruby SecureRandom.hex(12).
pub fn new_jid() -> String {
    let mut bytes = [0u8; 12];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jid_is_24_hex_chars() {
        let jid = new_jid();
        assert_eq!(jid.len(), 24);
        assert!(jid.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn job_args_roundtrip() {
        let job = SidekiqJob::new("default", 42, 0);
        let json = serde_json::to_string(&job).unwrap();
        let back: SidekiqJob = serde_json::from_str(&json).unwrap();
        assert_eq!(back.jid.len(), 24);
        assert_eq!(back.args[0].as_str().unwrap(), "string");
        assert_eq!(back.args[1].as_u64().unwrap(), 42);
        assert!(SidekiqJob::enqueued_at_ns(&serde_json::Value::Array(back.args)).is_some());
    }

    #[test]
    fn payload_size_grows_args0() {
        let job = SidekiqJob::new("default", 0, 700);
        assert_eq!(job.args[0].as_str().unwrap().len(), 700);
        let json = serde_json::to_string(&job).unwrap();
        // Serialized job should be roughly payload_size + ~200 bytes of envelope.
        assert!(json.len() > 700);
        assert!(json.len() < 1100);
    }
}
