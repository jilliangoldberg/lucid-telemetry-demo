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
    /// Keyed by `["vin",window_start_ms]` as JSON, so VINs containing any character can't collide.
    windows: BTreeMap<String, Window>,
}

#[derive(Serialize, Deserialize)]
struct Window {
    vin: String,
    window_start_ms: i64,
    events: i64,
    /// (seq, soc_pct) of every event that has one. Summed in seq order at output, so the
    /// float average doesn't depend on arrival order.
    soc_pct: Vec<(i64, f64)>,
    /// Spark's `max` ignores nulls, so `None` until an event with a value arrives.
    max_temp_c: Option<f64>,
    max_cell_delta_mv: Option<i64>,
}

/// Spark's `floor(ts_ms / WindowMs) * WindowMs`: rounds down, also for negative times.
fn window_start(ts_ms: i64) -> i64 {
    ts_ms.div_euclid(WINDOW_MS) * WINDOW_MS
}

/// SQL `a OR b` with nulls: true if either is true, else null if either is null.
fn sql_or(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

impl Job for BatteryHealth {
    fn apply(&mut self, e: &Event) {
        // StreamingMain/GenerateExpected feed the job only events of its type.
        if e.str("type") != Some("battery") {
            return;
        }
        let start = window_start(e.ts_ms);
        let key = json!([e.vin, start]).to_string();
        let w = self.windows.entry(key).or_insert_with(|| Window {
            vin: e.vin.clone(),
            window_start_ms: start,
            events: 0,
            soc_pct: Vec::new(),
            max_temp_c: None,
            max_cell_delta_mv: None,
        });
        // `count(lit(1))` counts every event, even ones missing a metric.
        w.events += 1;
        if let Some(soc) = e.f64("soc_pct") {
            w.soc_pct.push((e.seq, soc));
        }
        if let Some(temp) = e.f64("temp_c") {
            w.max_temp_c = Some(w.max_temp_c.map_or(temp, |m| m.max(temp)));
        }
        if let Some(delta) = e.i64("cell_delta_mv") {
            w.max_cell_delta_mv = Some(w.max_cell_delta_mv.map_or(delta, |m| m.max(delta)));
        }
    }

    fn output(&self) -> Vec<Value> {
        self.windows
            .values()
            .map(|w| {
                let mut soc = w.soc_pct.clone();
                soc.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
                // `avg` ignores nulls and is null when there are no values.
                let avg_soc_pct = (!soc.is_empty())
                    .then(|| soc.iter().map(|(_, s)| s).sum::<f64>() / soc.len() as f64);
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

    const T0: i64 = 1_767_225_600_000; // a window boundary

    fn battery(seq: i64, ts_ms: i64, soc: f64, temp: f64, delta: i64) -> Event {
        serde_json::from_value(json!({
            "event_id": format!("e{seq}"), "vin": "V", "seq": seq, "ts_ms": ts_ms,
            "type": "battery", "soc_pct": soc, "temp_c": temp, "cell_delta_mv": delta,
            "lat": 37.774_93, "lon": -122.419_42,
        }))
        .unwrap()
    }

    fn run(events: &[Event]) -> Vec<Value> {
        let mut job = BatteryHealth::default();
        events.iter().for_each(|e| job.apply(e));
        job.output()
    }

    #[test]
    fn windows_are_half_open_five_minute_buckets() {
        let out = run(&[
            battery(1, T0, 50.0, 20.0, 10),
            battery(2, T0 + WINDOW_MS - 1, 60.0, 20.0, 10),
            battery(3, T0 + WINDOW_MS, 70.0, 20.0, 10), // first ms of the next window
        ]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["window_start_ms"], T0);
        assert_eq!(out[0]["events"], 2);
        assert_eq!(out[0]["avg_soc_pct"], 55.0);
        assert_eq!(out[1]["window_start_ms"], T0 + WINDOW_MS);
        assert_eq!(window_start(-1), -WINDOW_MS); // floor, not truncation
    }

    #[test]
    fn average_is_bit_identical_in_any_order() {
        // 0.1 + 0.2 + 0.3 != 0.3 + 0.2 + 0.1 in floating point, so arrival order would leak.
        let mut events = vec![
            battery(1, T0, 0.1, 20.0, 10),
            battery(2, T0 + 1, 0.2, 20.0, 10),
            battery(3, T0 + 2, 0.3, 20.0, 10),
        ];
        let expected = run(&events)[0]["avg_soc_pct"].as_f64().unwrap();
        events.reverse();
        let reversed = run(&events)[0]["avg_soc_pct"].as_f64().unwrap();
        assert_eq!(reversed.to_bits(), expected.to_bits());
    }

    #[test]
    fn alert_thresholds_are_strictly_greater_than() {
        let alert = |temp, delta| run(&[battery(1, T0, 50.0, temp, delta)])[0]["alert"].clone();
        assert_eq!(alert(45.0, 50), false);
        assert_eq!(alert(45.01, 50), true);
        assert_eq!(alert(45.0, 51), true);
    }

    #[test]
    fn missing_metrics_count_as_events_but_not_in_aggregates() {
        let partial: Event = serde_json::from_value(json!({
            "event_id": "x", "vin": "V", "seq": 9, "ts_ms": T0, "type": "battery",
            "temp_c": 46.0,
        }))
        .unwrap();
        let out = run(&[battery(1, T0, 80.0, 20.0, 10), partial.clone()]);
        assert_eq!(out[0]["events"], 2);
        assert_eq!(out[0]["avg_soc_pct"], 80.0);
        assert_eq!(out[0]["max_cell_delta_mv"], 10);
        assert_eq!(out[0]["alert"], true);

        // Only a temperature below threshold and no cell delta: Spark's `false OR null` is null.
        let mut cool = partial;
        cool.fields.insert("temp_c".into(), json!(30.0));
        let out = run(&[cool]);
        assert!(out[0]["avg_soc_pct"].is_null() && out[0]["max_cell_delta_mv"].is_null());
        assert!(out[0]["alert"].is_null());
    }

    #[test]
    fn other_event_types_are_ignored() {
        let ota: Event = serde_json::from_value(json!({
            "event_id": "o", "vin": "V", "seq": 1, "ts_ms": T0, "type": "ota_status",
            "ota_version": "2.5.0", "status": "installing",
        }))
        .unwrap();
        assert!(run(&[ota]).is_empty());
    }
}
