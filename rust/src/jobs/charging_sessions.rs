//! Port of `ChargingSessionsJob` (legacy/Jobs.scala): one record per (vin, session_id) with
//! start/end time, energy, and the charger and coarse location of the session's start event.

use crate::location;
use crate::skeleton::{Event, Job};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Default, Serialize, Deserialize)]
pub struct ChargingSessions {
    /// Keyed by "vin|session_id".
    sessions: BTreeMap<String, Session>,
}

#[derive(Serialize, Deserialize)]
struct Session {
    vin: String,
    session_id: String,
    /// Spark's `min` over start events only (it ignores nulls), so `None` until a start arrives.
    charger_id: Option<String>,
    /// Only the cell is kept, never raw lat/lon, so checkpoints don't hold precise location.
    location_cell: Option<String>,
    start_ts_ms: i64,
    end_ts_ms: i64,
    /// (seq, kwh) of every event. Summed in seq order at output, so the float sum doesn't
    /// depend on arrival order.
    kwh: Vec<(i64, f64)>,
}

/// Keeps the smaller of two optional strings, like Spark's `min` (nulls ignored).
fn keep_min(slot: &mut Option<String>, value: String) {
    if slot.as_ref().map_or(true, |current| value < *current) {
        *slot = Some(value);
    }
}

impl Job for ChargingSessions {
    fn apply(&mut self, e: &Event) {
        let Some(session_id) = e.str("session_id") else {
            return;
        };
        let key = format!("{}|{}", e.vin, session_id);
        let s = self.sessions.entry(key).or_insert_with(|| Session {
            vin: e.vin.clone(),
            session_id: session_id.to_string(),
            charger_id: None,
            location_cell: None,
            start_ts_ms: i64::MAX,
            end_ts_ms: i64::MIN,
            kwh: Vec::new(),
        });
        // Session span is min/max event time over all its events, not first/last arrival.
        s.start_ts_ms = s.start_ts_ms.min(e.ts_ms);
        s.end_ts_ms = s.end_ts_ms.max(e.ts_ms);
        // `coalesce(kwh, 0.0)`: events without kwh count as 0.
        s.kwh.push((e.seq, e.f64("kwh").unwrap_or(0.0)));
        if e.str("phase") == Some("start") {
            if let Some(charger) = e.str("charger_id") {
                keep_min(&mut s.charger_id, charger.to_string());
            }
            if let (Some(lat), Some(lon)) = (e.f64("lat"), e.f64("lon")) {
                keep_min(&mut s.location_cell, location::cell(lat, lon));
            }
        }
    }

    fn output(&self) -> Vec<Value> {
        self.sessions
            .values()
            .map(|s| {
                let mut kwh = s.kwh.clone();
                kwh.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
                let energy_kwh: f64 = kwh.iter().map(|(_, k)| k).sum();
                json!({
                    "vin": s.vin,
                    "session_id": s.session_id,
                    "charger_id": s.charger_id,
                    "start_ts_ms": s.start_ts_ms,
                    "end_ts_ms": s.end_ts_ms,
                    "energy_kwh": energy_kwh,
                    "location_cell": s.location_cell,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn charging(seq: i64, phase: &str, kwh: f64) -> Event {
        let mut e = json!({
            "event_id": format!("e{seq}"), "vin": "V", "seq": seq, "ts_ms": 1000 + seq,
            "session_id": "V-S1", "phase": phase, "kwh": kwh,
            "lat": 37.774_93 + seq as f64, "lon": -122.419_42,
        });
        if phase == "start" {
            e["charger_id"] = json!("CHG-1");
        }
        serde_json::from_value(e).unwrap()
    }

    fn session() -> Vec<Event> {
        vec![
            charging(1, "start", 0.0),
            charging(2, "progress", 0.1),
            charging(3, "progress", 0.2),
            charging(4, "end", 0.3),
        ]
    }

    fn run(events: &[Event]) -> Value {
        let mut job = ChargingSessions::default();
        events.iter().for_each(|e| job.apply(e));
        job.output().remove(0)
    }

    #[test]
    fn start_event_fields_and_span_dont_depend_on_arrival_order() {
        let in_order = run(&session());
        let mut reversed = session();
        reversed.reverse(); // the start event arrives last
        assert_eq!(run(&reversed), in_order);
        assert_eq!(in_order["charger_id"], "CHG-1");
        assert_eq!(in_order["location_cell"], "38.77,-122.42"); // the start event's cell
        assert_eq!(in_order["start_ts_ms"], 1001);
        assert_eq!(in_order["end_ts_ms"], 1004);
    }

    #[test]
    fn energy_sum_is_bit_identical_in_any_order() {
        // 0.1 + 0.2 + 0.3 != 0.3 + 0.2 + 0.1 in floating point, so arrival order would leak.
        let mut events = session();
        let expected = run(&events)["energy_kwh"].as_f64().unwrap();
        events.swap(1, 3);
        assert_eq!(
            run(&events)["energy_kwh"].as_f64().unwrap().to_bits(),
            expected.to_bits()
        );
    }

    #[test]
    fn missing_kwh_counts_as_zero_and_no_start_gives_null_charger() {
        let e: Event = serde_json::from_value(json!({
            "event_id": "x", "vin": "V", "seq": 1, "ts_ms": 5,
            "session_id": "V-S1", "phase": "progress", "lat": 1.0, "lon": 2.0,
        }))
        .unwrap();
        let out = run(&[e]);
        assert_eq!(out["energy_kwh"], 0.0);
        assert!(out["charger_id"].is_null() && out["location_cell"].is_null());
    }
}
