#!/usr/bin/env python3
"""Generates the synthetic input events for each job: fixtures/<job>/input.jsonl.

All data is fake (made-up VINs, random coordinates). Deterministic: same seed, same files.
Events are in arrival order: interleaved across VINs and in seq order per VIN. The evals
add duplicates, reordering and restarts on top of this clean input.
Usage: python3 fixtures/generate.py
"""
import json
import random
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
rng = random.Random(7)
VINS = [f"LDEMO{n:012d}" for n in range(1, 26)]  # 17 chars, like a real VIN
START_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z
CITIES = [(37.77, -122.42), (34.05, -118.24), (33.45, -112.07), (40.71, -74.01)]


def coord(base):
    """A coordinate near `base`, away from x.xx5 so 2-decimal rounding is unambiguous."""
    while True:
        value = round(base + rng.uniform(-0.2, 0.2), 5)
        if abs((abs(value) * 100) % 1 - 0.5) > 0.1:
            return value


class Vehicle:
    def __init__(self, vin):
        self.vin, self.seq = vin, 0
        self.home = rng.choice(CITIES)

    def event(self, ts_ms, type_, **fields):
        self.seq += 1
        return {
            "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "vin": self.vin, "seq": self.seq, "ts_ms": ts_ms, "type": type_,
            **fields,
            "lat": coord(self.home[0]), "lon": coord(self.home[1]),
        }


def ota_status(vehicle):
    ts = START_MS + rng.randint(0, 600_000)
    for version in rng.sample(["2.4.1", "2.5.0", "2.6.0"], rng.randint(1, 2)):
        for status in ["downloading", "installing", rng.choice(["succeeded", "succeeded", "failed"])]:
            ts += rng.randint(60_000, 300_000)
            yield vehicle.event(ts, "ota_status", ota_version=version, status=status)


def battery(vehicle):
    soc = rng.uniform(40, 95)
    for i in range(120):  # one reading every 30 s for an hour
        soc = max(5.0, soc - rng.uniform(0, 0.3))
        yield vehicle.event(
            START_MS + i * 30_000 + rng.randint(0, 5_000), "battery",
            soc_pct=round(soc, 2), temp_c=round(rng.uniform(20, 48), 2),
            cell_delta_mv=rng.randint(5, 60))


def charging(vehicle):
    ts = START_MS + rng.randint(0, 900_000)
    for n in range(1, 3):  # two sessions per vehicle
        session = f"{vehicle.vin}-S{n}"
        yield vehicle.event(ts, "charging", session_id=session, phase="start",
                            charger_id=f"CHG-{rng.randint(100, 999)}", kwh=0.0)
        for _ in range(rng.randint(6, 10)):
            ts += rng.randint(60_000, 180_000)
            yield vehicle.event(ts, "charging", session_id=session, phase="progress",
                                kwh=round(rng.uniform(0.5, 3.0), 2))
        ts += 60_000
        yield vehicle.event(ts, "charging", session_id=session, phase="end", kwh=0.0)
        ts += rng.randint(3_600_000, 7_200_000)


def main():
    for job, generate in [("ota_status", ota_status), ("battery_health", battery),
                          ("charging_sessions", charging)]:
        events = [e for vin in VINS for e in generate(Vehicle(vin))]
        events.sort(key=lambda e: (e["ts_ms"], e["vin"]))  # arrival order
        out = ROOT / job / "input.jsonl"
        out.parent.mkdir(exist_ok=True)
        out.write_text("".join(json.dumps(e) + "\n" for e in events))
        print(f"{job}: {len(events)} events -> {out.relative_to(ROOT.parent)}")


if __name__ == "__main__":
    main()
