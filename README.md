# Telemetry migration demo: Spark Streaming → Rust, with eval gates

**Synthetic demo for a simulated Lucid Motors meeting.** Not Lucid code or data. The scenario comes from the onsite guide: telemetry jobs move from Scala/Spark Streaming to Rust stream processors, ordering and exactly-once must hold, location is tied to a VIN, one small job is already ported, and the rest must move "without disrupting downstream consumers or triggering a lengthy privacy review for each one." Everything else here (job names, fields, fixtures, gates) is made up to illustrate the approach.

## The idea
Before Devin ports anything, freeze what "correct" means. Every port is graded by the same gates, and the agent can't change them.

| Gate | Checks | Protects |
|---|---|---|
| G1 build | Rust builds, unit tests pass | basics |
| G2 contract | output fields/types match `contracts/<job>.json` | downstream consumers |
| G3 parity | output equals the legacy Spark job's output on the same events | downstream consumers |
| G4 faults | same output with duplicates, out-of-order events, and a mid-stream restart with redelivery | ordering and exactly-once |
| G5 privacy | no raw lat/lon in output; location only as a ~1 km cell (`privacy/POLICY.md`) | one privacy review for the pattern, not per job |
| G6 integrity | a port PR can't also change contracts, evals, fixtures, legacy code or CI | trust in the gates |

**Evaluating the evals:** `evals/mutants/` holds 6 deliberately broken versions of the reference port (no dedup, dedup lost on restart, last-arrival-wins, renamed field, leaked raw and rounded coordinates). `run_mutants.py` checks each one is caught by its gate. A gate that misses its mutant has a hole.

## Layout
```
legacy/        Scala/Spark jobs (Jobs.scala has all the business logic)
rust/          Rust skeleton (dedup, checkpoints) + ported jobs (rust/src/jobs/)
contracts/     output schema per job
fixtures/      synthetic events (generate.py) + expected.jsonl from Spark
privacy/       approved location pattern
evals/         gates (run_evals.py) and mutants (run_mutants.py)
docs/          porting playbook, migration status
```

## Run it
```bash
python3 evals/run_evals.py      # all gates, all jobs
python3 evals/run_mutants.py    # prove the gates catch known bugs
# Regenerate the parity baseline from the legacy Spark jobs (needs scala-cli + Java 17):
scala-cli run legacy --main-class lucid.telemetry.GenerateExpected
```

## What this does and doesn't show
- Shows: a port is checked against the legacy output, the consumer contract, delivery faults and the privacy rule before a human reviews it.
- Doesn't show: production exactly-once with Kafka transactions, or behaviour at real volume. After CI, a port would still run in shadow mode against live traffic, with output reconciliation and gradual consumer cutover.

## At production scale
G3 checks logic, not volume: a small fixed replay covers the cases that break streaming jobs (duplicates, late and out-of-order events, restarts). Volume is checked in later stages.

| Stage | Data | How it compares | Catches |
|---|---|---|---|
| 1. CI fixtures (this repo) | a few hundred synthetic events per job | row-level diff vs Spark, every PR | logic bugs, in minutes |
| 2. Historical replay | a slice of recorded Kafka (hours, or a set of VINs) | both jobs run as batch on the same input, row-level diff | real-data edge cases the fixtures miss |
| 3. Shadow mode | live traffic, Rust writes to a shadow sink | per window and partition: counts and an aggregate checksum, row diffs only where they disagree or on a sample | behaviour at volume: lag, memory, late data |
| 4. Cutover | one consumer at a time | mismatch rate under an agreed threshold for N days, every mismatch explained | safe switch, easy rollback |

In streaming, output timing differs too: a late event can update a window after it was first emitted. Shadow mode compares a window only after the watermark has passed, and the bar is an explained mismatch rate (float rounding, late data), not a literal 1:1 match.
