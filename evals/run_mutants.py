#!/usr/bin/env python3
"""Proves the eval gates catch real bugs ("evaluating the evals").

Each patch in evals/mutants/ breaks the reference port (rust/) in one specific way.
For each one: copy rust/, apply the patch, run the evals on ota_status, and check that
the gate listed in mutants.json fails. A mutant that passes means the evals have a hole.
Usage: python3 evals/run_mutants.py   (exits 1 if any mutant survives)
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MUTANTS = ROOT / "evals" / "mutants"


def main():
    mutants = json.loads((MUTANTS / "mutants.json").read_text())
    # Shared target dir: dependencies compile once, only the patched crate rebuilds.
    env = {**os.environ, "CARGO_TARGET_DIR": str(ROOT / "rust" / "target" / "mutants")}
    rows, survived = [], 0
    for name, info in mutants.items():
        work = Path(tempfile.mkdtemp())
        shutil.copytree(ROOT / "rust", work / "rust", ignore=shutil.ignore_patterns("target"))
        subprocess.run(["patch", "-s", "-p1", "-d", str(work / "rust"), "-i", str(MUTANTS / f"{name}.patch")], check=True)
        results = work / "results.json"
        subprocess.run([sys.executable, str(ROOT / "evals" / "run_evals.py"), "--jobs", "ota_status",
                        "--rust-dir", str(work / "rust"), "--skip-unit-tests", "--json", str(results)],
                       env=env, capture_output=True)
        job = json.loads(results.read_text())["jobs"]["ota_status"]
        failed = [g for g in ("G2", "G3", "G4", "G5") if job.get(g, [None])[0] is False]
        caught = info["must_fail"] in failed
        survived += not caught
        rows.append(f"| `{name}` | {info['description']} | {info['must_fail']} | "
                    f"{', '.join(failed) or 'none'} | {'✅ caught' if caught else '❌ SURVIVED'} |")
        shutil.rmtree(work)
    print("## Mutant check: do the evals catch real bugs?\n")
    print("| Mutant | What it breaks | Must fail | Failed gates | Result |\n|---|---|---|---|---|")
    print("\n".join(rows))
    sys.exit(1 if survived else 0)


if __name__ == "__main__":
    main()
