//! Port of `BatteryHealthJob` (legacy/Jobs.scala): battery health per VIN per 5-minute
//! tumbling window, with an alert flag for service.

use crate::skeleton::{Event, Job};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

const WINDOW_MS: i64 = 5 * 60 * 1000;

#[derive(Default, Serialize, Deserialize)]
pub struct BatteryHealth {
    /// Keyed by json!([vin, window_start_ms]).
    windows: BTreeMap<String, Window>,
}

#[derive(Serialize, Deserialize)]
struct Window {
    vin: String,
    window_start_ms: i64,
    events: i64,
    /// (seq, soc_pct) pairs; summed in seq order so the average ignores arrival order.
    soc: Vec<(i64, f64)>,
    max_temp_c: Option<f64>,
    max_cell_delta_mv: Option<i64>,
}

impl Job for BatteryHealth {
    fn apply(&mut self, e: &Event) {
        // Spark filters `type === eventType` upstream; Runner feeds every event.
        if e.str("type") != Some("battery") {
            return;
        }
        let window_start_ms = e.ts_ms.div_euclid(WINDOW_MS) * WINDOW_MS; // floor, also for negatives
        let key = json!([e.vin, window_start_ms]).to_string();
        let w = self.windows.entry(key).or_insert_with(|| Window {
            vin: e.vin.clone(),
            window_start_ms,
            events: 0,
            soc: Vec::new(),
            max_temp_c: None,
            max_cell_delta_mv: None,
        });
        w.events += 1;
        // Like Spark's avg/max, nulls (missing fields) are skipped.
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
                json!({
                    "vin": w.vin,
                    "window_start_ms": w.window_start_ms,
                    "events": w.events,
                    "avg_soc_pct": w.avg_soc_pct(),
                    "max_temp_c": w.max_temp_c,
                    "max_cell_delta_mv": w.max_cell_delta_mv,
                    "alert": w.alert(),
                })
            })
            .collect()
    }
}

impl Window {
    fn avg_soc_pct(&self) -> Option<f64> {
        if self.soc.is_empty() {
            return None;
        }
        let mut soc = self.soc.clone();
        soc.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
        Some(soc.iter().map(|s| s.1).sum::<f64>() / soc.len() as f64)
    }

    /// SQL three-valued OR: true if either side is true, null if unknown and none is true.
    fn alert(&self) -> Option<bool> {
        let hot = self.max_temp_c.map(|t| t > 45.0);
        let imbalanced = self.max_cell_delta_mv.map(|d| d > 50);
        match (hot, imbalanced) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skeleton::Runner;

    fn battery(seq: i64, ts_ms: i64, soc: Value, temp: Value, delta: Value) -> Event {
        serde_json::from_value(json!({
            "event_id": format!("e{seq}"), "vin": "V", "seq": seq, "ts_ms": ts_ms,
            "type": "battery", "soc_pct": soc, "temp_c": temp, "cell_delta_mv": delta,
        }))
        .unwrap()
    }

    fn run(events: &[Event]) -> Vec<Value> {
        let mut runner = Runner::<BatteryHealth>::default();
        events.iter().for_each(|e| runner.process(e));
        runner.job.output()
    }

    #[test]
    fn out_of_order_events_give_the_same_output() {
        let events = vec![
            battery(1, 1_000, json!(0.1), json!(30.0), json!(10)),
            battery(2, 2_000, json!(0.2), json!(46.0), json!(20)),
            battery(3, 3_000, json!(0.3), json!(31.0), json!(30)),
            battery(4, WINDOW_MS + 5, json!(50.0), json!(20.0), json!(5)),
        ];
        let mut reversed = events.clone();
        reversed.reverse();
        let out = run(&events);
        assert_eq!(out, run(&reversed));
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["events"], 3);
        assert_eq!(out[0]["max_temp_c"], 46.0);
        assert_eq!(out[0]["alert"], true);
        assert_eq!(out[1]["window_start_ms"], WINDOW_MS);
        assert_eq!(out[1]["alert"], false);
    }

    #[test]
    fn duplicates_count_once() {
        let e = battery(1, 0, json!(80.0), json!(30.0), json!(60));
        let out = run(&[e.clone(), e]);
        assert_eq!(out[0]["events"], 1);
        assert_eq!(out[0]["avg_soc_pct"], 80.0);
    }

    #[test]
    fn nulls_are_skipped_and_alert_is_three_valued() {
        let out = run(&[
            battery(1, 0, json!(null), json!(null), json!(40)),
            battery(2, 1, json!(60.0), json!(null), json!(null)),
        ]);
        assert_eq!(out[0]["events"], 2);
        assert_eq!(out[0]["avg_soc_pct"], 60.0);
        assert_eq!(out[0]["max_temp_c"], Value::Null);
        assert_eq!(out[0]["max_cell_delta_mv"], 40);
        assert_eq!(out[0]["alert"], Value::Null); // null OR false

        let hot_unknown = run(&[battery(1, 0, json!(null), json!(50.0), json!(null))]);
        assert_eq!(hot_unknown[0]["avg_soc_pct"], Value::Null);
        assert_eq!(hot_unknown[0]["alert"], true); // true OR null
    }

    #[test]
    fn other_event_types_are_ignored() {
        let mut e = battery(1, 0, json!(80.0), json!(30.0), json!(10));
        e.fields.insert("type".into(), json!("charging"));
        assert!(run(&[e]).is_empty());
    }

    #[test]
    fn negative_timestamps_floor_to_the_previous_window() {
        let out = run(&[battery(1, -1, json!(1.0), json!(1.0), json!(1))]);
        assert_eq!(out[0]["window_start_ms"], -WINDOW_MS);
    }
}
