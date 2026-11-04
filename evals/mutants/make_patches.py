#!/usr/bin/env python3
"""Regenerates the mutant patches from the reference port. Rerun after editing rust/src.

Each mutant is a small, deliberate bug plus the gate that must catch it.
Usage: python3 evals/mutants/make_patches.py
"""
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
RUST = HERE.parent.parent / "rust"
OTA = "src/jobs/ota_status.rs"

# name: (what it breaks, gate that must fail, [(file, original text, mutated text)])
MUTANTS = {
    "no_dedup": ("Removes duplicate dropping", "G4", [
        ("src/skeleton.rs", "let first_time = self.seen.insert(event.event_id.clone());",
         "let first_time = true; // MUTANT: dedup removed")]),
    "checkpoint_drops_dedup": ("Dedup state not saved in the checkpoint", "G4", [
        ("src/skeleton.rs", "    seen: BTreeSet<String>,",
         "    #[serde(skip)] // MUTANT: dedup state lost on restart\n    seen: BTreeSet<String>,")]),
    "last_arrival_wins": ("Uses arrival order instead of seq", "G4", [
        (OTA, "if e.seq > latest.seq {", "if true { // MUTANT: last event to arrive wins")]),
    "renamed_field": ("Renames an output field", "G2", [
        (OTA, '"updated_ts_ms": l.ts_ms,', '"updated_at_ms": l.ts_ms, // MUTANT: renamed')]),
    "raw_location_leak": ("Adds raw lat/lon to an existing field", "G5", [
        (OTA, "latest.status = status.to_string();",
         'latest.status = format!("{status}@{},{}", e.f64("lat").unwrap_or(0.0), e.f64("lon").unwrap_or(0.0)); // MUTANT')]),
}


def main():
    index = {}
    for name, (description, gate, edits) in MUTANTS.items():
        tmp = Path(tempfile.mkdtemp())
        for side in ("a", "b"):
            shutil.copytree(RUST / "src", tmp / side / "src")
        for file, old, new in edits:
            path = tmp / "b" / file
            text = path.read_text()
            assert text.count(old) == 1, f"{name}: can't find {old!r} in {file}"
            path.write_text(text.replace(old, new))
        diff = subprocess.run(["diff", "-ru", "a", "b"], cwd=tmp, capture_output=True, text=True).stdout
        (HERE / f"{name}.patch").write_text(diff)
        index[name] = {"description": description, "must_fail": gate}
        shutil.rmtree(tmp)
    (HERE / "mutants.json").write_text(json.dumps(index, indent=2) + "\n")
    print(f"wrote {len(index)} mutant patches")


if __name__ == "__main__":
    main()
