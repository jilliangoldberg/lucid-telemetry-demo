//! Port of `BatteryHealthJob` (legacy/Jobs.scala): battery health per VIN per 5-minute
//! tumbling window, with an alert flag for service.

use crate::skeleton::{Event, Job};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

const WINDOW_MS: i64 = 5 * 60 * 1000;
const ALERT_TEMP_C: f64 = 45.0;
const ALERT_CELL_DELTA_MV: i64 = 50;

#[derive(Default, Serialize, Deserialize)]
pub struct BatteryHealth {
    /// Keyed by `["vin", window_start_ms]` as JSON.
    windows: BTreeMap<String, Window>,
}

#[derive(Serialize, Deserialize)]
struct Window {
    vin: String,
    window_start_ms: i64,
    events: i64,
    /// `(seq, soc_pct)`, summed in seq order so the float average ignores arrival order.
    soc: Vec<(i64, f64)>,
    max_temp_c: Option<f64>,
    max_cell_delta_mv: Option<i64>,
}

/// Spark's `floor(ts_ms / WindowMs) * WindowMs`, including for negative times.
fn window_start(ts_ms: i64) -> i64 {
    ts_ms.div_euclid(WINDOW_MS) * WINDOW_MS
}

/// SQL three-valued OR: true wins, otherwise a null side makes the result null.
fn sql_or(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

impl Job for BatteryHealth {
    fn apply(&mut self, e: &Event) {
        if e.str("type") != Some("battery") {
            return;
        }
        let start = window_start(e.ts_ms);
        let w = self
            .windows
            .entry(json!([e.vin, start]).to_string())
            .or_insert_with(|| Window {
                vin: e.vin.clone(),
                window_start_ms: start,
                events: 0,
                soc: Vec::new(),
                max_temp_c: None,
                max_cell_delta_mv: None,
            });
        // count(lit(1)) counts every event; avg/max skip missing values.
        w.events += 1;
        if let Some(soc) = e.f64("soc_pct") {
            w.soc.push((e.seq, soc));
        }
        if let Some(t) = e.f64("temp_c") {
            w.max_temp_c = Some(w.max_temp_c.map_or(t, |m| m.max(t)));
        }
        if let Some(d) = e.i64("cell_delta_mv") {
            w.max_cell_delta_mv = Some(w.max_cell_delta_mv.map_or(d, |m| m.max(d)));
        }
    }

    fn output(&self) -> Vec<Value> {
        self.windows
            .values()
            .map(|w| {
                let mut soc = w.soc.clone();
                soc.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
                let avg_soc_pct = (!soc.is_empty())
                    .then(|| soc.iter().map(|s| s.1).sum::<f64>() / soc.len() as f64);
                let alert = sql_or(
                    w.max_temp_c.map(|t| t > ALERT_TEMP_C),
                    w.max_cell_delta_mv.map(|d| d > ALERT_CELL_DELTA_MV),
                );
                json!({
                    "vin": w.vin,
                    "window_start_ms": w.window_start_ms,
                    "events": w.events,
                    "avg_soc_pct": avg_soc_pct,
                    "max_temp_c": w.max_temp_c,
                    "max_cell_delta_mv": w.max_cell_delta_mv,
                    "alert": alert,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn battery(seq: i64, ts_ms: i64, fields: Value) -> Event {
        let mut e = json!({
            "event_id": format!("e{seq}"), "vin": "V", "seq": seq, "ts_ms": ts_ms,
            "type": "battery",
        });
        e.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        serde_json::from_value(e).unwrap()
    }

    fn run(events: &[Event]) -> Vec<Value> {
        let mut job = BatteryHealth::default();
        events.iter().for_each(|e| job.apply(e));
        job.output()
    }

    #[test]
    fn windows_include_start_and_exclude_end() {
        assert_eq!(window_start(0), 0);
        assert_eq!(window_start(WINDOW_MS - 1), 0);
        assert_eq!(window_start(WINDOW_MS), WINDOW_MS);
        assert_eq!(window_start(-1), -WINDOW_MS); // floor, not truncation
    }

    #[test]
    fn average_is_identical_in_any_arrival_order() {
        let socs = [0.1, 0.2, 0.3, 73.12, 45.64, 90.05];
        let events: Vec<Event> = socs
            .iter()
            .enumerate()
            .map(|(i, s)| battery(i as i64, 1000, json!({"soc_pct": s})))
            .collect();
        let forward = run(&events);
        let reversed: Vec<Event> = events.iter().rev().cloned().collect();
        assert_eq!(forward, run(&reversed));
    }

    #[test]
    fn alert_thresholds_are_strict() {
        let at = run(&[battery(1, 0, json!({"temp_c": 45.0, "cell_delta_mv": 50}))]);
        assert_eq!(at[0]["alert"], false);
        let hot = run(&[battery(1, 0, json!({"temp_c": 45.01, "cell_delta_mv": 50}))]);
        assert_eq!(hot[0]["alert"], true);
        let delta = run(&[battery(1, 0, json!({"temp_c": 20.0, "cell_delta_mv": 51}))]);
        assert_eq!(delta[0]["alert"], true);
    }

    #[test]
    fn missing_metrics_follow_spark_null_semantics() {
        let out = run(&[battery(1, 0, json!({"cell_delta_mv": 10}))]);
        assert_eq!(out[0]["events"], 1);
        assert_eq!(out[0]["avg_soc_pct"], Value::Null);
        assert_eq!(out[0]["max_temp_c"], Value::Null);
        assert_eq!(out[0]["alert"], Value::Null); // null OR false
        let hot = run(&[battery(1, 0, json!({"cell_delta_mv": 90}))]);
        assert_eq!(hot[0]["alert"], true); // null OR true
    }

    #[test]
    fn other_event_types_are_ignored() {
        let mut e = battery(1, 0, json!({"soc_pct": 50.0}));
        e.fields.insert("type".into(), json!("charging"));
        assert!(run(&[e]).is_empty());
    }
}
