# Migration status

| Job | Legacy (Scala) | Rust port | Gates | Notes |
|---|---|---|---|---|
| `ota_status` | `OtaStatusJob` | `rust/src/jobs/ota_status.rs` | passing | reference port |
| `battery_health` | `BatteryHealthJob` | not started | | 5-min tumbling window per VIN |
| `charging_sessions` | `ChargingSessionsJob` | not started | | stateful sessions + location cell |
