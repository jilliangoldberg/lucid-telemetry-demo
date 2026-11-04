# Telemetry stream processors: Spark Streaming → Rust

Vehicle telemetry jobs are moving from Scala/Spark Streaming to Rust stream processors on Kubernetes. This repo holds the legacy jobs, the Rust ports, and the eval gates every port must pass before review, so jobs can move one at a time without disrupting downstream consumers or needing a new privacy review per job.

> All jobs, fields, fixtures and policies in this repo are synthetic.

## Correctness gates
"Correct" is defined before a job is ported. Every port is graded by the same gates, and a port PR can't change them.

| Gate | Checks | Protects |
|---|---|---|
| G1 build | Rust builds, unit tests pass | basics |
| G2 contract | output fields/types match `contracts/<job>.json` | downstream consumers |
| G3 parity | output equals the legacy Spark job's output on the same events | downstream consumers |
| G4 faults | same output with duplicates, out-of-order events, and a mid-stream restart with redelivery | ordering and exactly-once |
| G5 privacy | no raw lat/lon in output; location only as a ~1 km cell (`privacy/POLICY.md`) | one privacy review for the pattern, not per job |
| G6 integrity | a port PR can't also change contracts, evals, fixtures, legacy code or CI | trust in the gates |

All six run in `evals/run_evals.py`. CI runs it on every PR and posts the gate report as a PR comment.

**Testing the gates:** `evals/mutants/` holds 6 deliberately broken versions of the reference port (no dedup, dedup lost on restart, last-arrival-wins, renamed field, leaked raw and rounded coordinates). `evals/run_mutants.py` checks that each one is caught by its gate. A gate that misses its mutant has a hole.

## Layout
```
legacy/        Scala/Spark jobs (Jobs.scala has all the business logic)
rust/          Rust runtime (dedup, checkpoints) + ported jobs (rust/src/jobs/)
contracts/     output schema per job
fixtures/      synthetic events (generate.py) + expected.jsonl from Spark
privacy/       approved location pattern
evals/         gates (run_evals.py) and mutants (run_mutants.py)
docs/          porting playbook, migration status
```

## Jobs
| Job | What it computes | Status |
|---|---|---|
| `ota_status` | latest software-update status per VIN and version | ported |
| `battery_health` | battery health per VIN per 5-minute window, with a service alert flag | not started |
| `charging_sessions` | one row per charging session: duration, energy, charger, coarse location | not started |

Current status lives in [`docs/MIGRATION_STATUS.md`](docs/MIGRATION_STATUS.md).

## Porting a job
Follow [`docs/PORTING.md`](docs/PORTING.md): read the legacy job, port it under `rust/src/jobs/`, run the gates, and open a PR with the gate report. A port PR can't change `contracts/`, `evals/`, `fixtures/`, `legacy/`, `privacy/` or `.github/` (G6).

## Running locally
Requires Python 3 and a Rust toolchain. Regenerating the parity baseline also needs scala-cli and Java 17.
```bash
python3 evals/run_evals.py                          # all gates, all jobs
python3 evals/run_evals.py --jobs battery_health    # one job
python3 evals/run_mutants.py                        # check the gates catch known bugs
scala-cli run legacy --main-class lucid.telemetry.GenerateExpected   # regenerate expected output
```

## Path to production
CI checks logic on a small fixed replay that covers the cases that break streaming jobs. It does not cover production exactly-once with Kafka transactions, or behaviour at real volume; later stages do.

| Stage | Data | How it compares | Catches |
|---|---|---|---|
| 1. CI fixtures (this repo) | a few hundred synthetic events per job | row-level diff vs Spark, every PR | logic bugs, in minutes |
| 2. Historical replay | a slice of recorded Kafka (hours, or a set of VINs) | both jobs run as batch on the same input, row-level diff | real-data edge cases the fixtures miss |
| 3. Shadow mode | live traffic, Rust writes to a shadow sink | per window and partition: counts and an aggregate checksum, row diffs only where they disagree or on a sample | behaviour at volume: lag, memory, late data |
| 4. Cutover | one consumer at a time | mismatch rate under an agreed threshold for N days, every mismatch explained | safe switch, easy rollback |

In streaming, output timing differs too: a late event can update a window after it was first emitted. Shadow mode compares a window only after the watermark has passed, and the bar is an explained mismatch rate (float rounding, late data), not a literal 1:1 match.
