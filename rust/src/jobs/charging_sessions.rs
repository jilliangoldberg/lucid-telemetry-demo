//! Port of `ChargingSessionsJob` (legacy/Jobs.scala): one row per (vin, session_id) with
//! duration, energy, and the charger and coarse location of the session's "start" event.

use crate::location;
use crate::skeleton::{Event, Job};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Default, Serialize, Deserialize)]
pub struct ChargingSessions {
    /// Keyed by `json!([vin, session_id])`.
    sessions: BTreeMap<String, Session>,
}

#[derive(Serialize, Deserialize)]
struct Session {
    vin: String,
    session_id: String,
    /// min over "start" events; nulls skipped like Spark `min`.
    charger_id: Option<String>,
    start_ts_ms: i64,
    end_ts_ms: i64,
    /// (seq, event_id, kwh) of every event, summed in seq order so the float sum doesn't
    /// depend on arrival order.
    kwh: Vec<(i64, String, f64)>,
    /// min over "start" events of the ~1 km cell; None if no start event had lat and lon.
    location_cell: Option<String>,
}

fn min_opt(cur: &mut Option<String>, v: Option<String>) {
    if let Some(v) = v {
        if cur.as_ref().map_or(true, |c| v < *c) {
            *cur = Some(v);
        }
    }
}

impl Job for ChargingSessions {
    fn apply(&mut self, e: &Event) {
        if e.str("type") != Some("charging") {
            return;
        }
        // Without a session id there is no output key (contract: session_id is a string).
        let Some(session_id) = e.str("session_id") else {
            return;
        };
        let key = json!([e.vin, session_id]).to_string();
        let s = self.sessions.entry(key).or_insert_with(|| Session {
            vin: e.vin.clone(),
            session_id: session_id.to_string(),
            charger_id: None,
            start_ts_ms: e.ts_ms,
            end_ts_ms: e.ts_ms,
            kwh: Vec::new(),
            location_cell: None,
        });
        s.start_ts_ms = s.start_ts_ms.min(e.ts_ms);
        s.end_ts_ms = s.end_ts_ms.max(e.ts_ms);
        s.kwh
            .push((e.seq, e.event_id.clone(), e.f64("kwh").unwrap_or(0.0)));
        if e.str("phase") == Some("start") {
            min_opt(&mut s.charger_id, e.str("charger_id").map(str::to_string));
            // Spark would format a missing coordinate as "null" (e.g. "33.88,null"); a
            // malformed cell is never emitted here, the cell is null instead.
            let cell = match (e.f64("lat"), e.f64("lon")) {
                (Some(lat), Some(lon)) => Some(location::cell(lat, lon)),
                _ => None,
            };
            min_opt(&mut s.location_cell, cell);
        }
    }

    fn output(&self) -> Vec<Value> {
        self.sessions
            .values()
            .map(|s| {
                let mut kwh: Vec<_> = s.kwh.iter().collect();
                kwh.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
                let energy_kwh: f64 = kwh.iter().map(|k| k.2).sum();
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
                json!({"charger_id": "CHG-1", "kwh": 0.0, "lat": 33.884, "lon": -118.391}),
            ),
            ev(
                2,
                "progress",
                json!({"kwh": 0.1, "lat": 33.9, "lon": -118.4}),
            ),
            ev(
                3,
                "progress",
                json!({"kwh": 0.2, "lat": 33.9, "lon": -118.4}),
            ),
            ev(4, "end", json!({"kwh": 0.3, "lat": 33.9, "lon": -118.4})),
        ]
    }

    fn run(events: &[Event]) -> Vec<Value> {
        let mut job = ChargingSessions::default();
        events.iter().for_each(|e| job.apply(e));
        job.output()
    }

    #[test]
    fn aggregates_one_session() {
        let out = run(&session());
        assert_eq!(out.len(), 1);
        let r = &out[0];
        assert_eq!(r["charger_id"], "CHG-1");
        assert_eq!(r["start_ts_ms"], 1000);
        assert_eq!(r["end_ts_ms"], 4000);
        assert_eq!(r["location_cell"], "33.88,-118.39");
        assert_eq!(r["energy_kwh"], ((0.0 + 0.1) + 0.2) + 0.3);
    }

    #[test]
    fn out_of_order_gives_identical_output() {
        let mut reversed = session();
        reversed.reverse();
        assert_eq!(run(&reversed), run(&session()));
    }

    #[test]
    fn duplicates_via_runner_are_applied_once() {
        let mut runner = Runner::<ChargingSessions>::default();
        for e in session().iter().chain(session().iter()) {
            runner.process(e);
        }
        assert_eq!(runner.job.output(), run(&session()));
    }

    #[test]
    fn restart_with_replay_gives_identical_output() {
        let events = session();
        let mut runner = Runner::<ChargingSessions>::default();
        events[..3].iter().for_each(|e| runner.process(e));
        let mut runner = Runner::<ChargingSessions>::restore(&runner.checkpoint());
        events[1..].iter().for_each(|e| runner.process(e));
        assert_eq!(runner.job.output(), run(&events));
    }

    #[test]
    fn null_kwh_counts_as_zero() {
        let mut events = session();
        events[2] = ev(3, "progress", json!({"kwh": null}));
        assert_eq!(run(&events)[0]["energy_kwh"], 0.0 + 0.1 + 0.3);
    }

    #[test]
    fn null_coordinate_gives_null_cell_not_malformed() {
        let events = vec![ev(
            1,
            "start",
            json!({"charger_id": "CHG-1", "lat": 33.88, "lon": null}),
        )];
        let r = &run(&events)[0];
        assert!(r["location_cell"].is_null());
        assert_eq!(r["charger_id"], "CHG-1");
    }

    #[test]
    fn charger_and_cell_come_only_from_start_events() {
        let events = vec![
            ev(
                1,
                "progress",
                json!({"charger_id": "CHG-0", "lat": 1.0, "lon": 2.0}),
            ),
            ev(
                2,
                "start",
                json!({"charger_id": null, "lat": null, "lon": null}),
            ),
        ];
        let r = &run(&events)[0];
        assert!(r["charger_id"].is_null());
        assert!(r["location_cell"].is_null());
    }

    #[test]
    fn ignores_other_event_types_and_missing_session_id() {
        let mut other = ev(1, "start", json!({}));
        other.fields.insert("type".into(), json!("battery"));
        let mut no_session = ev(2, "start", json!({}));
        no_session.fields.insert("session_id".into(), Value::Null);
        assert!(run(&[other, no_session]).is_empty());
    }
}
