import base64
import csv
import hashlib
import io
import json
import math
import os
import random
import re
import time
from datetime import UTC, datetime
from decimal import Decimal
from functools import cache

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Spec band: readings outside it are excursions. Plausible range: outside it the sensor is faulty, so quarantine.
POINTS = {
    "supply_air_temp": {"unit": "degC", "spec": (16.0, 27.0), "plausible": (-20.0, 80.0), "equipment": "CRAH"},
    "relative_humidity": {"unit": "%RH", "spec": (20.0, 80.0), "plausible": (0.0, 100.0), "equipment": "CRAH"},
    "chw_supply_temp": {"unit": "degC", "spec": (6.0, 14.0), "plausible": (-10.0, 40.0), "equipment": "chiller"},
}
FIELDS = ("reading_id", "site", "equipment_tag", "point", "value", "ts")
TAG = re.compile(r"^(CRAH|CHL)-([A-Z]{3}\d)-([A-Z])-\d{2}$")
EQUIPMENT_TYPES = {"CRAH": "CRAH", "CHL": "chiller"}
ISSUE_FIELDS = (
    "issue_id",
    "site",
    "building",
    "equipment_type",
    "equipment_tag",
    "manufacturer",
    "contractor",
    "cx_level",
    "severity",
    "status",
    "opened_at",
    "closed_at",
    "notes",
)

BUCKET = os.environ.get("BUCKET", "")
STATE_TABLE = os.environ.get("STATE_TABLE", "")
TELEMETRY_PREFIX = os.environ.get("TELEMETRY_PREFIX", "telemetry/")
QUARANTINE_PREFIX = os.environ.get("TELEMETRY_QUARANTINE_PREFIX", "telemetry-quarantine/")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/")
EXCURSION_READINGS = int(os.environ.get("EXCURSION_READINGS", "5"))


class TransientError(Exception):
    pass


@cache
def _s3():
    return boto3.client("s3")


class StateStore:
    """Per-sensor excursion state in DynamoDB, written only when it changes."""

    def __init__(self, table_name: str):
        self.table = boto3.resource("dynamodb").Table(table_name)

    def load(self, sensors: list[str]) -> dict[str, dict]:
        out = {}
        for i in range(0, len(sensors), 100):
            keys = [{"sensor": s} for s in sensors[i : i + 100]]
            resp = self.table.meta.client.batch_get_item(RequestItems={self.table.name: {"Keys": keys}})
            if resp.get("UnprocessedKeys"):
                raise TransientError("state read throttled")
            for item in resp["Responses"][self.table.name]:
                out[item.pop("sensor")] = {k: int(v) if isinstance(v, Decimal) else v for k, v in item.items()}
        return out

    def save(self, sensor: str, state: dict, prev_ts: int | None) -> None:
        # Optimistic concurrency: only overwrite the state this invocation read.
        cond = "attribute_not_exists(sensor)" if prev_ts is None else "last_ts = :prev"
        values = {} if prev_ts is None else {":prev": prev_ts}
        try:
            self.table.put_item(
                Item={"sensor": sensor, **state},
                ConditionExpression=cond,
                **({"ExpressionAttributeValues": values} if values else {}),
            )
        except ClientError as e:
            raise TransientError(str(e)) from e


@cache
def _store() -> StateStore:
    return StateStore(STATE_TABLE)


def _chaos() -> None:
    # Fault injection for load tests: fail a share of external writes to exercise retries.
    if random.random() < float(os.environ.get("FAIL_RATE", "0")):
        raise TransientError("injected failure")


def parse(data: bytes) -> tuple[dict | None, list[str]]:
    try:
        r = json.loads(data)
    except (ValueError, UnicodeDecodeError):
        return None, ["malformed:json"]
    if not isinstance(r, dict):
        return None, ["malformed:not_object"]

    errors = [f"missing:{k}" for k in FIELDS if r.get(k) in (None, "")]
    spec = POINTS.get(r.get("point"))
    if r.get("point") and spec is None:
        errors.append("invalid:point")
    if r.get("equipment_tag") and not TAG.match(str(r["equipment_tag"])):
        errors.append("invalid:equipment_tag")
    v = r.get("value")
    if v not in (None, ""):
        if isinstance(v, bool) or not isinstance(v, int | float) or not math.isfinite(v):
            errors.append("invalid:value")
        elif spec and not spec["plausible"][0] <= v <= spec["plausible"][1]:
            errors.append("implausible:value")
    ts = r.get("ts")
    if ts not in (None, "") and (isinstance(ts, bool) or not isinstance(ts, int) or ts <= 0):
        errors.append("invalid:ts")
    return (None if errors else r), errors


def _seq(rec: dict) -> int:
    return int(rec["sequence_number"])


def _issue(reading: dict, state: dict) -> dict:
    prefix, site, building = TAG.match(reading["equipment_tag"]).groups()
    point = POINTS[reading["point"]]
    low, high = point["spec"]
    started = datetime.fromtimestamp(state["run_started"] / 1000, UTC)
    digest = hashlib.sha256(f"{reading['equipment_tag']}|{reading['point']}|{state['run_started']}".encode())
    return {
        "issue_id": f"CX-{900000 + int(digest.hexdigest(), 16) % 100000:06d}",
        "site": site,
        "building": building,
        "equipment_type": EQUIPMENT_TYPES[prefix],
        "equipment_tag": reading["equipment_tag"],
        "manufacturer": "TBC",
        "contractor": "TBC",
        "cx_level": "L4",
        "severity": "high",
        "status": "open",
        "opened_at": started.date().isoformat(),
        "closed_at": "",
        "notes": (
            f"BMS trend: {reading['point'].replace('_', ' ')} on {reading['equipment_tag']} outside "
            f"{low:g}-{high:g} {point['unit']} for {state['run']} consecutive readings from {started:%H:%M:%S} UTC, "
            f"last {reading['value']:g} {point['unit']}"
        ),
    }


def advance(state: dict | None, readings: list[dict]) -> tuple[dict | None, list[dict]]:
    """Fold a sensor's readings into its excursion state. Returns (new state or None if unchanged, issues).

    Readings at or before the stored watermark are replays or producer duplicates and are skipped, so
    reprocessing a batch never double counts or raises a second issue.
    """
    new = dict(state or {"last_ts": 0, "run": 0, "run_started": 0, "issue_open": False})
    changed, issues = False, []
    for r in sorted(readings, key=lambda r: r["ts"]):
        if r["ts"] <= new["last_ts"]:
            continue
        new["last_ts"] = r["ts"]
        if r["in_spec"]:
            if new["run"] or new["issue_open"]:
                new |= {"run": 0, "run_started": 0, "issue_open": False}
                changed = True
            continue
        if not new["run"]:
            new["run_started"] = r["ts"]
        new["run"] += 1
        changed = True
        if new["run"] >= EXCURSION_READINGS and not new["issue_open"]:
            issues.append(_issue(r, new))
            new["issue_open"] = True
    return (new if changed else None), issues


def _write_issue(issue: dict) -> None:
    _chaos()
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=ISSUE_FIELDS)
    writer.writeheader()
    writer.writerow(issue)
    _put(f"{RAW_PREFIX}telemetry-{issue['issue_id']}.csv", buf.getvalue().encode(), "text/csv")


def _put(key: str, body: bytes, content_type: str) -> None:
    try:
        _s3().put_object(Bucket=BUCKET, Key=key, Body=body, ContentType=content_type)
    except (ClientError, BotoCoreError) as e:
        raise TransientError(str(e)) from e


def _put_jsonl(key: str, items: list[dict]) -> None:
    _chaos()
    _put(key, "\n".join(json.dumps(i) for i in items).encode(), "application/x-ndjson")


def handler(event, _context, store=None):
    records = event["Records"]
    if not records:
        return {"batchItemFailures": []}
    store = store or _store()
    now_ms = int(time.time() * 1000)
    good, bad, seen, duplicates = [], [], set(), 0

    for r in records:
        k = r["kinesis"]
        raw = base64.b64decode(k["data"])
        rec, errors = parse(raw)
        if errors:
            bad.append(
                {
                    "sequence_number": k["sequenceNumber"],
                    "errors": errors,
                    "data": raw[:500].decode("utf-8", "replace"),
                }
            )
            continue
        if rec["reading_id"] in seen:
            duplicates += 1
            continue
        seen.add(rec["reading_id"])
        low, high = POINTS[rec["point"]]["spec"]
        good.append(
            {
                **{f: rec[f] for f in FIELDS},
                "unit": POINTS[rec["point"]]["unit"],
                "in_spec": low <= rec["value"] <= high,
                "arrived_at": int(float(k["approximateArrivalTimestamp"]) * 1000),
                "sequence_number": k["sequenceNumber"],
            }
        )

    # Lambda retries from the lowest sequence number reported, so everything at or after it must be
    # safe to see again and everything before it must be fully committed.
    failed: int | None = None
    by_sensor: dict[str, list[dict]] = {}
    for g in good:
        by_sensor.setdefault(f"{g['equipment_tag']}#{g['point']}", []).append(g)

    issues_raised = 0
    try:
        states = store.load(list(by_sensor))
    except TransientError:
        failed = int(records[0]["kinesis"]["sequenceNumber"])
    else:
        for sensor, readings in by_sensor.items():
            prev = states.get(sensor)
            new, issues = advance(prev, readings)
            if new is None:
                continue
            try:
                for issue in issues:  # before the state write, so a failed save re-raises the same issue key
                    _write_issue(issue)
                _chaos()
                store.save(sensor, new, prev["last_ts"] if prev else None)
                issues_raised += len(issues)
            except TransientError:
                first = min(_seq(r) for r in readings)
                failed = first if failed is None else min(failed, first)

    first_seq = records[0]["kinesis"]["sequenceNumber"]
    shard = records[0]["eventID"].split(":", 1)[0]
    keep = [g for g in good if failed is None or _seq(g) < failed]
    keep_bad = [b for b in bad if failed is None or _seq(b) < failed]
    partition = f"ingest_date={datetime.now(UTC):%Y-%m-%d}"
    stem = f"{shard}-{first_seq}"  # deterministic, so a retried batch overwrites rather than duplicates
    try:
        if keep:
            _put_jsonl(f"{TELEMETRY_PREFIX}{partition}/{stem}.jsonl", keep)
        if keep_bad:
            _put_jsonl(f"{QUARANTINE_PREFIX}{partition}/{stem}.jsonl", keep_bad)
    except TransientError:
        failed, keep, keep_bad = int(first_seq), [], []

    retried = sum(1 for r in records if failed is not None and int(r["kinesis"]["sequenceNumber"]) >= failed)
    _emit_metrics(
        {
            "readings": len(keep),
            "quarantined": len(keep_bad),
            "duplicates": duplicates,
            "issues_raised": issues_raised,
            "retried": retried,
        },
        [now_ms - g["ts"] for g in keep][:100],
    )
    return {"batchItemFailures": [] if failed is None else [{"itemIdentifier": str(failed)}]}


def _emit_metrics(counts: dict[str, int], latencies: list[int]) -> None:
    metrics = [{"Name": k, "Unit": "Count"} for k in counts]
    values: dict = dict(counts)
    if latencies:
        metrics.append({"Name": "LatencyMs", "Unit": "Milliseconds"})
        values["LatencyMs"] = latencies
    print(
        json.dumps(
            {
                "_aws": {
                    "Timestamp": int(time.time() * 1000),
                    "CloudWatchMetrics": [{"Namespace": "CxPipeline", "Dimensions": [["Stage"]], "Metrics": metrics}],
                },
                "Stage": "telemetry",
                **values,
            }
        )
    )
