# Location data policy (approved pattern)

Every telemetry event carries GPS `lat`/`lon` tied to a VIN, which makes it personal data.

**Rule:** a job may output location only as a **location cell**: lat/lon rounded to 2 decimals (about 1 km), built with the shared helper:
- Rust: `telemetry::location::cell(lat, lon)` (rust/src/location.rs)
- Scala: `Privacy.locationCell(lat, lon)` (legacy/Privacy.scala)

**Never:** output raw `lat`/`lon`, put coordinates inside another field, or add new VIN-linked output fields without privacy review.

**How it's enforced:** eval gate G5 (evals/run_evals.py) fails any port whose output contains a raw input coordinate, a raw location field, or a malformed cell. The privacy team reviews this pattern once; ports that pass G5 follow it.

*Synthetic policy; not Lucid's actual policy.*
