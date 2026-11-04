# Migration status

| Job | Legacy (Scala) | Rust port | Gates | Notes |
|---|---|---|---|---|
| `ota_status` | `OtaStatusJob` | `rust/src/jobs/ota_status.rs` | passing | reference port |
| `battery_health` | `BatteryHealthJob` | `rust/src/jobs/battery_health.rs` | passing | 5-min tumbling window per VIN; soc average summed in seq order |
| `charging_sessions` | `ChargingSessionsJob` | not started | | stateful sessions + location cell |
