import argparse
import json
import random
import time
import uuid
from dataclasses import dataclass, field

import boto3

from telemetry import POINTS

# Setpoint, noise and the value an excursion drifts towards, per point.
PROFILES = {
    "supply_air_temp": (21.0, 0.3, 30.0),
    "relative_humidity": (45.0, 1.5, 88.0),
    "chw_supply_temp": (10.0, 0.2, 17.0),
}
EQUIPMENT_POINTS = {"CRAH": ("supply_air_temp", "relative_humidity"), "CHL": ("chw_supply_temp",)}
SITES = {"SIN1": "AB", "SIN2": "ABC", "JKT1": "A"}
MAX_PUT_RECORDS = 500


def sensors(n: int, seed: int = 0) -> list[tuple[str, str, str]]:
    """n (site, equipment_tag, point) triples, deterministic for a given seed."""
    rng = random.Random(seed)
    out: list[tuple[str, str, str]] = []
    while len(out) < n:
        site = rng.choice(list(SITES))
        prefix = rng.choice(list(EQUIPMENT_POINTS))
        tag = f"{prefix}-{site}-{rng.choice(SITES[site])}-{rng.randint(1, 12):02d}"
        out += [(site, tag, p) for p in EQUIPMENT_POINTS[prefix] if (site, tag, p) not in out]
    return out[:n]


@dataclass
class Excursion:
    sensor: tuple[str, str, str]
    remaining: int
    out_of_spec_emitted: int = 0


@dataclass
class Simulator:
    """Emits one reading per sensor per tick: mean-reverting noise, plus injected excursions, bad records
    and duplicates, keeping ground truth so a test can check what the consumer should have produced."""

    sensors: list[tuple[str, str, str]]
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    excursion_rate: float = 0.002
    excursion_length: tuple[int, int] = (8, 15)
    bad_rate: float = 0.0
    dup_rate: float = 0.0
    seed: int = 0
    values: dict = field(default_factory=dict)
    active: dict = field(default_factory=dict)
    excursions: list[Excursion] = field(default_factory=list)
    counts: dict = field(default_factory=lambda: {"valid": 0, "bad": 0, "duplicates": 0})
    _n: int = 0
    _last: dict | None = None

    def __post_init__(self):
        self.rng = random.Random(self.seed)
        self.values = {s: PROFILES[s[2]][0] for s in self.sensors}

    def tick(self, ts_ms: int) -> list[tuple[str, bytes]]:
        """(partition key, payload) pairs for one tick."""
        out = []
        for s in self.sensors:
            site, tag, point = s
            setpoint, noise, excursion_target = PROFILES[point]
            exc = self.active.get(s)
            # Only start from in-spec so back-to-back excursions never merge into one run.
            in_band = _in_spec({"point": point, "value": self.values[s]})
            if exc is None and in_band and self.rng.random() < self.excursion_rate:
                exc = self.active[s] = Excursion(s, self.rng.randint(*self.excursion_length))
                self.excursions.append(exc)
            target = excursion_target if exc else setpoint
            v = self.values[s] + 0.6 * (target - self.values[s]) + self.rng.gauss(0, noise)
            self.values[s] = v
            self._n += 1
            reading = {
                "reading_id": f"{self.run_id}-{self._n}",
                "site": site,
                "equipment_tag": tag,
                "point": point,
                "value": round(v, 2),
                "ts": ts_ms,
            }
            if self.rng.random() < self.bad_rate:
                out.append((tag, self._corrupt(reading)))
                self.counts["bad"] += 1
            else:
                out.append((tag, json.dumps(reading).encode()))
                self.counts["valid"] += 1
                if exc and not _in_spec(reading):
                    exc.out_of_spec_emitted += 1
                self._last = reading
            if exc:
                exc.remaining -= 1
                if exc.remaining <= 0:
                    del self.active[s]
            if self._last and self.rng.random() < self.dup_rate:
                out.append((self._last["equipment_tag"], json.dumps(self._last).encode()))
                self.counts["duplicates"] += 1
        return out

    def _corrupt(self, r: dict) -> bytes:
        match self.rng.randrange(5):
            case 0:
                return b'{"reading_id": "' + r["reading_id"].encode() + b'", "value": '
            case 1:
                r = {k: v for k, v in r.items() if k != "ts"}
            case 2:
                r["value"] = "n/a"
            case 3:
                r["point"] = "discharge_pressure"
            case 4:
                r["value"] = 999.0
        return json.dumps(r).encode()


def _in_spec(reading: dict) -> bool:
    low, high = POINTS[reading["point"]]["spec"]
    return low <= reading["value"] <= high


def put_with_retry(client, stream: str, items: list[tuple[str, bytes]], max_attempts: int = 6) -> dict:
    """PutRecords in chunks of 500, resending only the entries Kinesis rejected (usually throttling)."""
    stats = {"sent": 0, "throttled": 0, "failed": 0}
    for i in range(0, len(items), MAX_PUT_RECORDS):
        pending = [{"PartitionKey": pk, "Data": data} for pk, data in items[i : i + MAX_PUT_RECORDS]]
        for attempt in range(max_attempts):
            resp = client.put_records(StreamName=stream, Records=pending)
            if not resp["FailedRecordCount"]:
                stats["sent"] += len(pending)
                break
            retry = [rec for rec, res in zip(pending, resp["Records"], strict=True) if "ErrorCode" in res]
            stats["sent"] += len(pending) - len(retry)
            stats["throttled"] += len(retry)
            pending = retry
            time.sleep(min(2.0, 0.05 * 2**attempt) * random.random())
        else:
            stats["failed"] += len(pending)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream synthetic BMS telemetry into Kinesis")
    parser.add_argument("--stream", required=True)
    parser.add_argument("--sensors", type=int, default=20)
    parser.add_argument("--hz", type=float, default=1.0, help="readings per sensor per second")
    parser.add_argument("--duration", type=int, default=60, help="seconds")
    parser.add_argument("--bad-rate", type=float, default=0.01)
    parser.add_argument("--dup-rate", type=float, default=0.005)
    parser.add_argument("--excursion-rate", type=float, default=0.002)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    sim = Simulator(
        sensors(args.sensors, args.seed),
        excursion_rate=args.excursion_rate,
        bad_rate=args.bad_rate,
        dup_rate=args.dup_rate,
        seed=args.seed,
    )
    run(sim, boto3.client("kinesis"), args.stream, args.hz, args.duration)


def run(sim: Simulator, client, stream: str, hz: float, duration: float) -> dict:
    totals = {"sent": 0, "throttled": 0, "failed": 0}
    interval, start = 1 / hz, time.time()
    next_tick = start
    while time.time() - start < duration:
        stats = put_with_retry(client, stream, sim.tick(int(time.time() * 1000)))
        totals = {k: totals[k] + stats[k] for k in totals}
        next_tick += interval
        time.sleep(max(0.0, next_tick - time.time()))
    elapsed = time.time() - start
    print(f"run {sim.run_id}: {totals['sent']} records in {elapsed:.0f}s ({totals['sent'] / elapsed:.0f}/s), {totals}")
    return totals | {"elapsed": elapsed}


if __name__ == "__main__":
    main()
