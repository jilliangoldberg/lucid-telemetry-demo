# Migration status

| Job | Legacy (Scala) | Rust port | Gates | Notes |
|---|---|---|---|---|
| `ota_status` | `OtaStatusJob` | `rust/src/jobs/ota_status.rs` | passing | reference port |
| `battery_health` | `BatteryHealthJob` | not started | | 5-min tumbling window per VIN |
| `charging_sessions` | `ChargingSessionsJob` | `rust/src/jobs/charging_sessions.rs` | passing | stateful sessions + location cell; energy summed in seq order |
