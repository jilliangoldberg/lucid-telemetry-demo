//! Port of `OtaStatusJob` (legacy/Jobs.scala): latest OTA status per (vin, ota_version),
//! plus how many status updates arrived.

use crate::skeleton::{Event, Job};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Default, Serialize, Deserialize)]
pub struct OtaStatus {
    /// Keyed by "vin|ota_version".
    latest: BTreeMap<String, Latest>,
}

#[derive(Serialize, Deserialize)]
struct Latest {
    vin: String,
    ota_version: String,
    seq: i64,
    status: String,
    ts_ms: i64,
    updates: i64,
}

impl Job for OtaStatus {
    fn apply(&mut self, e: &Event) {
        let (Some(version), Some(status)) = (e.str("ota_version"), e.str("status")) else {
            return;
        };
        let key = format!("{}|{}", e.vin, version);
        let latest = self.latest.entry(key).or_insert_with(|| Latest {
            vin: e.vin.clone(),
            ota_version: version.to_string(),
            seq: i64::MIN,
            status: String::new(),
            ts_ms: 0,
            updates: 0,
        });
        latest.updates += 1;
        // Highest seq wins, not the last event to arrive.
        if e.seq > latest.seq {
            latest.seq = e.seq;
            latest.status = status.to_string();
            latest.ts_ms = e.ts_ms;
        }
    }

    fn output(&self) -> Vec<Value> {
        self.latest
            .values()
            .map(|l| {
                json!({
                    "vin": l.vin,
                    "ota_version": l.ota_version,
                    "status": l.status,
                    "last_seq": l.seq,
                    "updated_ts_ms": l.ts_ms,
                    "updates": l.updates,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ota(seq: i64, status: &str) -> Event {
        serde_json::from_value(json!({
            "event_id": format!("e{seq}"), "vin": "V", "seq": seq, "ts_ms": seq * 1000,
            "ota_version": "2.5.0", "status": status,
        }))
        .unwrap()
    }

    #[test]
    fn highest_seq_wins_even_when_it_arrives_first() {
        let mut job = OtaStatus::default();
        job.apply(&ota(2, "succeeded"));
        job.apply(&ota(1, "installing")); // late arrival of an older event
        assert_eq!(job.output()[0]["status"], "succeeded");
    }
}
