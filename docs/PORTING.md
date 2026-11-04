# Playbook: Port a Spark Streaming job to Rust

Use for each remaining job in `legacy/Jobs.scala`. The reference port is `ota_status` (`rust/src/jobs/ota_status.rs`).

## Steps
1. Read the job's `transform` in `legacy/Jobs.scala`. Write down its inputs, output key, aggregations, dedup and any ordering assumptions.
2. Read `contracts/<job>.json`: the output must have exactly these fields and types.
3. Create `rust/src/jobs/<job>.rs` following `ota_status.rs`: a state struct implementing `Job`, where `apply` folds one event and `output` returns one record per key.
   - The state must not depend on arrival order. Use `seq`/`ts_ms` (e.g. min/max), never "last event seen".
   - Dedup and checkpoints come from `Runner`; don't reimplement them.
   - Location: only via `location::cell` (see `privacy/POLICY.md`).
4. Register the module in `rust/src/jobs/mod.rs` and in the `match` in `rust/src/bin/run_job.rs`.
5. Add unit tests for the job's edge cases (e.g. out-of-order events).
6. Run `python3 evals/run_evals.py --jobs <job>` until every gate passes.
7. Open a PR titled `Port <job> to Rust`. Paste the eval report and list any decision that matters for semantics (dedup, ordering, float handling).

## Rules
- Don't modify `contracts/`, `evals/`, `fixtures/`, `legacy/`, `privacy/` or `.github/`. If a gate looks wrong, stop and ask; don't change the gate (CI gate G6 fails the PR otherwise).
- Keep the port small and readable: one file per job, short comments explaining intent.

## Done when
- CI is green: G1 build, G2 contract, G3 parity (zero diffs vs Spark), G4 faults, G5 privacy, G6 integrity.
- The PR description includes the eval report and the decisions made.
