//! Port of `ChargingSessionsJob` (legacy/Jobs.scala): one record per (vin, session_id) with
//! duration, energy, charger and coarse location.

use crate::location;
use crate::skeleton::{Event, Job};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Default, Serialize, Deserialize)]
pub struct ChargingSessions {
    /// Keyed by `[vin, session_id]` as a JSON array.
    sessions: BTreeMap<String, Session>,
}

#[derive(Serialize, Deserialize)]
struct Session {
    vin: String,
    session_id: Option<String>,
    /// min over "start" events, like Spark's `min(when(isStart, ...))`; null if none.
    charger_id: Option<String>,
    location_cell: Option<String>,
    start_ts_ms: i64,
    end_ts_ms: i64,
    /// (seq, kwh) per event, summed in seq order so the float sum ignores arrival order.
    kwh: Vec<(i64, f64)>,
}

/// Spark `min` skips nulls.
fn min_opt(current: &mut Option<String>, candidate: Option<String>) {
    if let Some(c) = candidate {
        if current.as_ref().map_or(true, |cur| c < *cur) {
            *current = Some(c);
        }
    }
}

impl Job for ChargingSessions {
    fn apply(&mut self, e: &Event) {
        // Runner feeds every event; Spark only sees `type == "charging"`.
        if e.str("type") != Some("charging") {
            return;
        }
        let session_id = e.str("session_id").map(str::to_string);
        let key = json!([e.vin, session_id]).to_string();
        let s = self.sessions.entry(key).or_insert_with(|| Session {
            vin: e.vin.clone(),
            session_id,
            charger_id: None,
            location_cell: None,
            start_ts_ms: e.ts_ms,
            end_ts_ms: e.ts_ms,
            kwh: Vec::new(),
        });
        s.start_ts_ms = s.start_ts_ms.min(e.ts_ms);
        s.end_ts_ms = s.end_ts_ms.max(e.ts_ms);
        s.kwh.push((e.seq, e.f64("kwh").unwrap_or(0.0))); // coalesce(kwh, 0.0)
        if e.str("phase") == Some("start") {
            min_opt(&mut s.charger_id, e.str("charger_id").map(str::to_string));
            // Location only as a ~1 km cell; no cell if a coordinate is missing.
            let cell = e.f64("lat").zip(e.f64("lon"));
            min_opt(
                &mut s.location_cell,
                cell.map(|(lat, lon)| location::cell(lat, lon)),
            );
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
    use crate::skeleton::Runner;

    fn ev(seq: i64, phase: &str, extra: Value) -> Event {
        let mut v = json!({
            "event_id": format!("e{seq}"), "vin": "V", "seq": seq, "ts_ms": seq * 1000,
            "type": "charging", "session_id": "V-S1", "phase": phase,
        });
        v.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(v).unwrap()
    }

    fn session() -> Vec<Event> {
        vec![
            ev(
                1,
                "start",
                json!({"charger_id": "CHG-1", "kwh": 0.0, "lat": 37.774_93, "lon": -122.419_42}),
            ),
            ev(
                2,
                "progress",
                json!({"kwh": 0.1, "lat": 37.8, "lon": -122.5}),
            ),
            ev(3, "progress", json!({"kwh": 0.2})),
            ev(4, "end", json!({"kwh": 0.7})),
        ]
    }

    fn run(events: &[Event]) -> Vec<Value> {
        let mut job = ChargingSessions::default();
        events.iter().for_each(|e| job.apply(e));
        job.output()
    }

    #[test]
    fn computes_one_session() {
        let out = run(&session());
        assert_eq!(out.len(), 1);
        let r = &out[0];
        assert_eq!(r["charger_id"], "CHG-1");
        assert_eq!(r["location_cell"], "37.77,-122.42"); // from the start event only
        assert_eq!(r["start_ts_ms"], 1000);
        assert_eq!(r["end_ts_ms"], 4000);
        assert_eq!(r["energy_kwh"], 0.0 + 0.1 + 0.2 + 0.7);
    }

    #[test]
    fn out_of_order_gives_identical_output() {
        let mut reversed = session();
        reversed.reverse();
        assert_eq!(run(&reversed), run(&session()));
    }

    #[test]
    fn duplicates_are_counted_once_via_runner() {
        let mut runner = Runner::<ChargingSessions>::default();
        let events = session();
        events.iter().chain(&events).for_each(|e| runner.process(e));
        assert_eq!(runner.job.output(), run(&session()));
    }

    #[test]
    fn nulls_follow_spark() {
        // No start event: charger and cell are null. Null kwh counts as 0.
        let out = run(&[
            ev(2, "progress", json!({"kwh": null})),
            ev(3, "end", json!({"kwh": 1.5})),
        ]);
        assert_eq!(out[0]["charger_id"], Value::Null);
        assert_eq!(out[0]["location_cell"], Value::Null);
        assert_eq!(out[0]["energy_kwh"], 1.5);
        // Start event without coordinates: no cell, charger still set.
        let out = run(&[ev(1, "start", json!({"charger_id": "CHG-1", "lat": null}))]);
        assert_eq!(out[0]["charger_id"], "CHG-1");
        assert_eq!(out[0]["location_cell"], Value::Null);
    }

    #[test]
    fn ignores_other_event_types() {
        let mut other = ev(5, "end", json!({"kwh": 9.0}));
        other.fields.insert("type".into(), json!("battery"));
        let mut events = session();
        events.push(other);
        assert_eq!(run(&events), run(&session()));
    }
}
