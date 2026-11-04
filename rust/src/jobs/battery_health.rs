//! Port of `BatteryHealthJob` (legacy/Jobs.scala): battery health per VIN per 5-minute
//! tumbling window, with an alert flag for service.

use crate::skeleton::{Event, Job};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

const EVENT_TYPE: &str = "battery";
const WINDOW_MS: i64 = 5 * 60 * 1000;
const ALERT_TEMP_C: f64 = 45.0;
const ALERT_CELL_DELTA_MV: i64 = 50;

#[derive(Default, Serialize, Deserialize)]
pub struct BatteryHealth {
    /// Keyed by `[vin, window_start_ms]` as a JSON array string.
    windows: BTreeMap<String, Window>,
}

#[derive(Serialize, Deserialize)]
struct Window {
    vin: String,
    window_start_ms: i64,
    events: i64,
    /// Non-null `soc_pct` readings kept sorted by (seq, event_id), so the average is
    /// summed in the same order whatever order the events arrived in.
    soc_readings: Vec<(i64, String, f64)>,
    max_temp_c: Option<f64>,
    max_cell_delta_mv: Option<i64>,
}

impl Window {
    fn avg_soc_pct(&self) -> Option<f64> {
        if self.soc_readings.is_empty() {
            return None; // Spark avg of only nulls is null
        }
        let sum: f64 = self.soc_readings.iter().map(|r| r.2).sum();
        Some(sum / self.soc_readings.len() as f64)
    }

    /// Spark `max_temp_c > 45.0 || max_cell_delta_mv > 50` with SQL three-valued logic:
    /// true wins over null, null wins over false.
    fn alert(&self) -> Option<bool> {
        let hot = self.max_temp_c.map(|t| t > ALERT_TEMP_C);
        let imbalanced = self.max_cell_delta_mv.map(|d| d > ALERT_CELL_DELTA_MV);
        match (hot, imbalanced) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        }
    }
}

/// Spark `max` skips nulls.
fn max_opt<T: PartialOrd>(cur: Option<T>, new: Option<T>) -> Option<T> {
    match (cur, new) {
        (Some(c), Some(n)) => Some(if n > c { n } else { c }),
        (c, n) => c.or(n),
    }
}

impl Job for BatteryHealth {
    fn apply(&mut self, e: &Event) {
        if e.str("type") != Some(EVENT_TYPE) {
            return;
        }
        // Spark floor(ts_ms / WindowMs) * WindowMs, also correct for negative timestamps.
        let window_start_ms = e.ts_ms.div_euclid(WINDOW_MS) * WINDOW_MS;
        let key = json!([e.vin, window_start_ms]).to_string();
        let w = self.windows.entry(key).or_insert_with(|| Window {
            vin: e.vin.clone(),
            window_start_ms,
            events: 0,
            soc_readings: Vec::new(),
            max_temp_c: None,
            max_cell_delta_mv: None,
        });
        w.events += 1;
        if let Some(soc) = e.f64("soc_pct") {
            let reading = (e.seq, e.event_id.clone(), soc);
            let pos = w
                .soc_readings
                .partition_point(|r| (r.0, &r.1) < (reading.0, &reading.1));
            w.soc_readings.insert(pos, reading);
        }
        w.max_temp_c = max_opt(w.max_temp_c, e.f64("temp_c"));
        w.max_cell_delta_mv = max_opt(w.max_cell_delta_mv, e.i64("cell_delta_mv"));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skeleton::Runner;

    const T0: i64 = 1_767_225_600_000; // a window boundary

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
    fn groups_into_five_minute_tumbling_windows() {
        let out = run(&[
            battery(1, T0, json!(80.0), json!(30.0), json!(10)),
            battery(2, T0 + WINDOW_MS - 1, json!(70.0), json!(31.0), json!(12)),
            battery(3, T0 + WINDOW_MS, json!(60.0), json!(32.0), json!(14)),
        ]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["window_start_ms"], T0);
        assert_eq!(out[0]["events"], 2);
        assert_eq!(out[0]["avg_soc_pct"], 75.0);
        assert_eq!(out[1]["window_start_ms"], T0 + WINDOW_MS);
    }

    #[test]
    fn negative_timestamps_floor_like_spark() {
        let out = run(&[battery(1, -1, json!(50.0), json!(20.0), json!(5))]);
        assert_eq!(out[0]["window_start_ms"], -WINDOW_MS);
    }

    #[test]
    fn out_of_order_arrival_gives_identical_output() {
        // Values chosen so a different summation order changes the float result.
        let socs = [0.1, 0.2, 0.3, 1e16, -1e16, 0.7];
        let events: Vec<Event> = socs
            .iter()
            .enumerate()
            .map(|(i, s)| battery(i as i64, T0 + i as i64, json!(s), json!(40.0), json!(20)))
            .collect();
        let mut reversed = events.clone();
        reversed.reverse();
        let mut rotated = events.clone();
        rotated.rotate_left(2);
        let expected = run(&events);
        assert_eq!(run(&reversed), expected);
        assert_eq!(run(&rotated), expected);
    }

    #[test]
    fn duplicates_are_counted_once() {
        let e = battery(1, T0, json!(80.0), json!(50.0), json!(10));
        let out = run(&[
            e.clone(),
            e.clone(),
            battery(2, T0, json!(60.0), json!(20.0), json!(10)),
            e,
        ]);
        assert_eq!(out[0]["events"], 2);
        assert_eq!(out[0]["avg_soc_pct"], 70.0);
    }

    #[test]
    fn restart_with_replay_gives_identical_output() {
        let events: Vec<Event> = (0..10)
            .map(|i| {
                battery(
                    i,
                    T0 + i * 1000,
                    json!(50.0 + i as f64 * 0.1),
                    json!(30.0),
                    json!(i),
                )
            })
            .collect();
        let mut runner = Runner::<BatteryHealth>::default();
        events[..6].iter().for_each(|e| runner.process(e));
        let mut restored = Runner::<BatteryHealth>::restore(&runner.checkpoint());
        events[3..].iter().for_each(|e| restored.process(e)); // replay 3 events
        assert_eq!(restored.job.output(), run(&events));
    }

    #[test]
    fn other_event_types_are_ignored() {
        let mut job = BatteryHealth::default();
        let ota: Event = serde_json::from_value(json!({
            "event_id": "o1", "vin": "V", "seq": 1, "ts_ms": T0,
            "type": "ota_status", "ota_version": "2.5.0", "status": "installing",
        }))
        .unwrap();
        job.apply(&ota);
        assert!(job.output().is_empty());
    }

    #[test]
    fn nulls_are_skipped_by_avg_and_max_but_counted() {
        let out = run(&[
            battery(1, T0, json!(80.0), Value::Null, json!(10)),
            battery(2, T0 + 1, Value::Null, json!(30.0), Value::Null),
            battery(3, T0 + 2, json!(60.0), json!(20.0), json!(40)),
        ]);
        assert_eq!(out[0]["events"], 3);
        assert_eq!(out[0]["avg_soc_pct"], 70.0);
        assert_eq!(out[0]["max_temp_c"], 30.0);
        assert_eq!(out[0]["max_cell_delta_mv"], 40);
        assert_eq!(out[0]["alert"], false);
    }

    #[test]
    fn all_null_values_give_null_aggregates() {
        let out = run(&[battery(1, T0, Value::Null, Value::Null, Value::Null)]);
        assert_eq!(out[0]["events"], 1);
        assert!(out[0]["avg_soc_pct"].is_null());
        assert!(out[0]["max_temp_c"].is_null());
        assert!(out[0]["max_cell_delta_mv"].is_null());
        assert!(out[0]["alert"].is_null());
    }

    #[test]
    fn alert_follows_sql_three_valued_or() {
        let alert = |temp: Value, delta: Value| {
            run(&[battery(1, T0, json!(50.0), temp, delta)])[0]["alert"].clone()
        };
        assert_eq!(alert(json!(45.1), Value::Null), true); // true OR null = true
        assert_eq!(alert(Value::Null, json!(51)), true); // null OR true = true
        assert!(alert(json!(45.0), Value::Null).is_null()); // false OR null = null
        assert!(alert(Value::Null, json!(50)).is_null()); // null OR false = null
        assert_eq!(alert(json!(45.0), json!(50)), false); // thresholds are strict
        assert_eq!(alert(json!(45.01), json!(50)), true);
        assert_eq!(alert(json!(45.0), json!(51)), true);
    }
}
