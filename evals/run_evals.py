#!/usr/bin/env python3
"""Eval gates every ported job must pass before a human reviews it.

  G1 build      the Rust code builds (and its unit tests pass, unless --skip-unit-tests)
  G2 contract   output fields and types match contracts/<job>.json (downstream consumers)
  G3 parity     output equals the legacy Spark job's output (fixtures/<job>/expected.jsonl)
  G4 faults     output is unchanged by duplicates, reordering and a restart with redelivery
  G5 privacy    no raw lat/lon in the output; location only as a 2-decimal cell
  G6 integrity  a PR that changes a port doesn't also change what it's graded against

Usage: python3 evals/run_evals.py [--jobs a,b] [--base origin/main] [--report report.md]
                                  [--json results.json] [--rust-dir rust] [--skip-unit-tests]
Exits 1 if any gate fails.
"""
import argparse
import json
import math
import os
import random
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JOBS = ["ota_status", "battery_health", "charging_sessions"]
# Paths a port PR must not touch: the evals and everything they compare against.
PROTECTED = ("contracts/", "evals/", "fixtures/", "legacy/", "privacy/", ".github/")
NOT_PORTED = 3  # run_job's exit code for a job without a Rust port
LOCATION_CELL = re.compile(r"^-?\d{1,3}\.\d{2},-?\d{1,3}\.\d{2}$")
RAW_LOCATION_FIELDS = {"lat", "lon", "latitude", "longitude", "gps"}


def load_jsonl(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def run(cmd, cwd=None, env=None):
    return subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, text=True)


# ---------- G1: build ----------

def build(rust_dir, skip_unit_tests):
    env = {**os.environ, "CARGO_TERM_COLOR": "never"}
    steps = [["cargo", "build", "--release", "--quiet"]]
    if not skip_unit_tests:
        steps.append(["cargo", "test", "--release", "--quiet"])
    for cmd in steps:
        result = run(cmd, cwd=rust_dir, env=env)
        if result.returncode != 0:
            return False, f"`{' '.join(cmd)}` failed:\n{result.stderr[-1500:]}"
    return True, "builds" + ("" if skip_unit_tests else ", unit tests pass")


def run_job(binary, job, events, restart=None):
    """Runs the Rust job over `events`. Returns output records, or None if not ported."""
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
        f.write("".join(json.dumps(e) + "\n" for e in events))
    cmd = [str(binary), job, f.name]
    if restart:
        cmd += ["--restart-at", str(restart[0]), "--replay", str(restart[1])]
    result = run(cmd)
    os.unlink(f.name)
    if result.returncode == NOT_PORTED:
        return None
    if result.returncode != 0:
        raise RuntimeError(f"run_job {job} crashed: {result.stderr[-500:]}")
    return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]


# ---------- comparing outputs (G3, G4) ----------

def same_value(a, b):
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        # Float sums depend on summation order, so allow tiny differences.
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-6)
    return a == b


def compare(actual, expected, key):
    """Matches records by the contract key and diffs them field by field."""
    index = lambda rows: {tuple(r.get(k) for k in key): r for r in rows}
    got, want = index(actual), index(expected)
    missing = [k for k in want if k not in got]
    extra = [k for k in got if k not in want]
    diffs = []
    for k in want.keys() & got.keys():
        for field in want[k].keys() | got[k].keys():
            if not same_value(got[k].get(field), want[k].get(field)):
                diffs.append(f"{k} {field}: got {got[k].get(field)!r}, want {want[k].get(field)!r}")
    ok = not (missing or extra or diffs)
    detail = f"{len(want)} records, {len(missing)} missing, {len(extra)} extra, {len(diffs)} diffs"
    if not ok:
        detail += "; e.g. " + "; ".join(
            [f"missing {k}" for k in missing[:2]] + [f"extra {k}" for k in extra[:2]] + diffs[:3])
    return ok, detail


# ---------- G2: contract ----------

TYPES = {
    "string": lambda v: isinstance(v, str),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
}


def check_contract(output, contract):
    fields = contract["fields"]
    problems = []
    for record in output:
        if set(record) != set(fields):
            problems.append(f"fields {sorted(set(record) ^ set(fields))} differ from contract")
        problems += [f"{f} is {type(record[f]).__name__}, contract says {t}"
                     for f, t in fields.items() if f in record and not TYPES[t](record[f])]
        if problems:
            break
    keys = [tuple(r.get(k) for k in contract["key"]) for r in output]
    if len(keys) != len(set(keys)):
        problems.append(f"duplicate keys {contract['key']}")
    return not problems, problems[0] if problems else f"{len(output)} records match the contract"


# ---------- G4: fault injection ----------

def fault_scenarios(events):
    rng = random.Random(42)
    duplicated = list(events)
    for e in rng.sample(events, max(1, len(events) // 10)):  # redeliver 10% of events
        duplicated.insert(rng.randrange(len(duplicated) + 1), e)
    shuffled = list(events)
    rng.shuffle(shuffled)  # out of order within and across VINs
    return [
        ("duplicates", duplicated, None),
        ("out-of-order", shuffled, None),
        ("restart", events, (len(events) // 2, 25)),  # crash mid-stream, replay 25 events
    ]


def check_faults(binary, job, events, clean_output, key):
    failures = []
    for name, scenario, restart in fault_scenarios(events):
        ok, detail = compare(run_job(binary, job, scenario, restart), clean_output, key)
        if not ok:
            failures.append(f"{name}: {detail}")
    return not failures, "; ".join(failures) or "duplicates, out-of-order, restart: output unchanged"


# ---------- G5: privacy ----------

def check_privacy(output, events):
    raw = {e[k] for e in events for k in ("lat", "lon") if k in e}
    raw_text = {repr(v) for v in raw}
    for record in output:
        for field, value in record.items():
            if field.lower() in RAW_LOCATION_FIELDS:
                return False, f"raw location field `{field}` in output"
            if isinstance(value, float) and value in raw:
                return False, f"`{field}` holds a raw coordinate ({value})"
            if isinstance(value, str) and any(t in value for t in raw_text):
                return False, f"`{field}` contains a raw coordinate ({value})"
            if field == "location_cell" and not LOCATION_CELL.match(value):
                return False, f"location_cell `{value}` is not a 2-decimal cell"
    return True, "no raw coordinates in output"


# ---------- G6: integrity ----------

def check_integrity(base):
    if not base:
        return None, "skipped (no --base; runs on pull requests)"
    result = run(["git", "diff", "--name-only", f"{base}...HEAD"], cwd=ROOT)
    if result.returncode != 0:
        return False, f"git diff failed: {result.stderr.strip()}"
    changed = result.stdout.split()
    port_changes = [p for p in changed if p.startswith("rust/")]
    eval_changes = [p for p in changed if p.startswith(PROTECTED)]
    if port_changes and eval_changes:
        return False, f"port PR also changes graded-against files: {', '.join(eval_changes[:5])}"
    return True, "port and eval files not changed together"


# ---------- report ----------

def mark(ok):
    return {True: "✅ pass", False: "❌ FAIL", None: "➖ skip"}[ok]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", default=",".join(JOBS))
    ap.add_argument("--base", help="git base ref; enables G6 (e.g. origin/main)")
    ap.add_argument("--report", help="write the markdown report here")
    ap.add_argument("--json", help="write machine-readable results here")
    ap.add_argument("--rust-dir", default=str(ROOT / "rust"))
    ap.add_argument("--skip-unit-tests", action="store_true",
                    help="G1 only builds (unit tests belong to the port, so evals don't rely on them)")
    args = ap.parse_args()

    rust_dir = Path(args.rust_dir).resolve()
    target = Path(os.environ.get("CARGO_TARGET_DIR", rust_dir / "target"))
    binary = target / "release" / "run_job"
    built, build_detail = build(rust_dir, args.skip_unit_tests)
    integrity = check_integrity(args.base)
    results = {"G1": [built, build_detail], "G6": list(integrity), "jobs": {}}

    for job in args.jobs.split(","):
        contract = json.loads((ROOT / "contracts" / f"{job}.json").read_text())
        events = load_jsonl(ROOT / "fixtures" / job / "input.jsonl")
        output = run_job(binary, job, events) if built else None
        if output is None:
            results["jobs"][job] = {"ported": False}
            continue
        expected_path = ROOT / "fixtures" / job / "expected.jsonl"
        parity = (compare(output, load_jsonl(expected_path), contract["key"]) if expected_path.exists()
                  else (None, "skipped: no expected.jsonl yet (run legacy/GenerateExpected)"))
        results["jobs"][job] = {
            "ported": True,
            "G2": list(check_contract(output, contract)),
            "G3": list(parity),
            "G4": list(check_faults(binary, job, events, output, contract["key"])),
            "G5": list(check_privacy(output, events)),
        }

    gates = ["G2", "G3", "G4", "G5"]
    lines = ["## Migration eval report", "",
             "| Job | Ported | G2 contract | G3 parity | G4 faults | G5 privacy |",
             "|---|---|---|---|---|---|"]
    for job, r in results["jobs"].items():
        if r["ported"]:
            lines.append(f"| `{job}` | yes | " + " | ".join(mark(r[g][0]) for g in gates) + " |")
        else:
            lines.append(f"| `{job}` | not yet | | | | |")
    lines += ["", f"**G1 build:** {mark(built)} ({build_detail.splitlines()[0]})  ",
              f"**G6 integrity:** {mark(integrity[0])} ({integrity[1]})", "", "<details><summary>Details</summary>", ""]
    for job, r in results["jobs"].items():
        for g in gates if r["ported"] else []:
            lines.append(f"- `{job}` {g}: {r[g][1]}")
    lines += ["", "</details>", ""]
    report = "\n".join(lines)
    print(report)
    if args.report:
        Path(args.report).write_text(report)
    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=2))

    failed = built is False or integrity[0] is False or any(
        r["ported"] and r[g][0] is False for r in results["jobs"].values() for g in gates)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
