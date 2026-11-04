//! Shared runtime for every ported job: the event model, dedup and checkpoint/restore.
//! In production events come from Kafka; in this repo they come from fixture files.

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

/// One telemetry event (same fields as legacy/Events.scala).
#[derive(Debug, Clone, Deserialize)]
pub struct Event {
    pub event_id: String,
    pub vin: String,
    pub seq: i64,
    pub ts_ms: i64,
    /// Type-specific payload: status, soc_pct, kwh, lat, lon, ...
    #[serde(flatten)]
    pub fields: Map<String, Value>,
}

impl Event {
    pub fn str(&self, key: &str) -> Option<&str> {
        self.fields.get(key).and_then(Value::as_str)
    }
    pub fn f64(&self, key: &str) -> Option<f64> {
        self.fields.get(key).and_then(Value::as_f64)
    }
    pub fn i64(&self, key: &str) -> Option<i64> {
        self.fields.get(key).and_then(Value::as_i64)
    }
}

/// A ported job: folds events into its state and emits one JSON record per output key.
/// The state must not depend on arrival order (events can arrive out of order).
pub trait Job: Default + Serialize + DeserializeOwned {
    fn apply(&mut self, event: &Event);
    fn output(&self) -> Vec<Value>;
}

/// Runs a job so each event takes effect exactly once: duplicates are dropped by
/// `event_id`, and the dedup set is saved in the checkpoint, so events Kafka redelivers
/// after a restart are dropped too.
#[derive(Default, Serialize, Deserialize)]
pub struct Runner<J> {
    pub job: J,
    /// Event ids already applied. (Production would expire ids older than the watermark.)
    seen: BTreeSet<String>,
    /// Source events consumed so far (the Kafka offset in production).
    pub offset: usize,
}

impl<J: Job> Runner<J> {
    pub fn process(&mut self, event: &Event) {
        self.offset += 1;
        let first_time = self.seen.insert(event.event_id.clone());
        if first_time {
            self.job.apply(event);
        }
    }

    pub fn checkpoint(&self) -> String {
        serde_json::to_string(self).expect("runner state serializes")
    }

    pub fn restore(checkpoint: &str) -> Self {
        serde_json::from_str(checkpoint).expect("checkpoint is valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Minimal job for testing the runner: counts applied events.
    #[derive(Default, Serialize, Deserialize)]
    struct Count(u64);
    impl Job for Count {
        fn apply(&mut self, _: &Event) {
            self.0 += 1;
        }
        fn output(&self) -> Vec<Value> {
            vec![json!(self.0)]
        }
    }

    fn event(id: &str) -> Event {
        serde_json::from_value(json!({"event_id": id, "vin": "V", "seq": 1, "ts_ms": 0})).unwrap()
    }

    #[test]
    fn duplicates_are_applied_once() {
        let mut runner = Runner::<Count>::default();
        for id in ["a", "b", "a"] {
            runner.process(&event(id));
        }
        assert_eq!(runner.job.0, 2);
    }

    #[test]
    fn redelivery_after_restart_is_applied_once() {
        let mut runner = Runner::<Count>::default();
        runner.process(&event("a"));
        let mut restored = Runner::<Count>::restore(&runner.checkpoint());
        for id in ["a", "b"] {
            restored.process(&event(id)); // "a" is redelivered
        }
        assert_eq!(restored.job.0, 2);
    }
}
