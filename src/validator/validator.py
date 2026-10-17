import csv
import io
import json
import os
import re
import time
from datetime import UTC, date, datetime
from functools import cache
from urllib.parse import unquote_plus

import boto3

FIELDS = (
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
REQUIRED = tuple(f for f in FIELDS if f != "closed_at")
ENUMS = {
    "equipment_type": {"UPS", "generator", "chiller", "CRAH", "switchgear", "PDU", "transformer", "BMS"},
    "cx_level": {"L1", "L2", "L3", "L4", "L5"},
    "severity": {"low", "medium", "high", "critical"},
    "status": {"open", "closed"},
}
ISSUE_ID = re.compile(r"^CX-\d{6}$")
MAX_NOTES = 2000

CLEAN_PREFIX = os.environ.get("CLEAN_PREFIX", "clean/")
QUARANTINE_PREFIX = os.environ.get("QUARANTINE_PREFIX", "quarantine/")


@cache
def _s3():
    return boto3.client("s3")


def validate_row(row: dict) -> tuple[dict, list[str]]:
    rec = {k: (row.get(k) or "").strip() for k in FIELDS}
    errors = [f"missing:{k}" for k in REQUIRED if not rec[k]]

    if rec["issue_id"] and not ISSUE_ID.match(rec["issue_id"]):
        errors.append("invalid:issue_id")
    errors += [f"invalid:{k}" for k, allowed in ENUMS.items() if rec[k] and rec[k] not in allowed]

    dates = {}
    for k in ("opened_at", "closed_at"):
        if rec[k]:
            try:
                dates[k] = date.fromisoformat(rec[k])
            except ValueError:
                errors.append(f"invalid:{k}")
    if len(dates) == 2 and dates["closed_at"] < dates["opened_at"]:
        errors.append("order:closed_before_opened")

    if rec["status"] == "closed" and not rec["closed_at"]:
        errors.append("missing:closed_at")
    if rec["status"] == "open" and rec["closed_at"]:
        errors.append("conflict:open_with_closed_at")
    if len(rec["notes"]) > MAX_NOTES:
        errors.append("invalid:notes_too_long")

    rec["closed_at"] = rec["closed_at"] or None
    return rec, errors


def validate_rows(rows) -> tuple[list[dict], list[dict]]:
    clean, bad, seen = [], [], set()
    for line_no, row in enumerate(rows, start=2):
        rec, errors = validate_row(row)
        if rec["issue_id"]:
            if rec["issue_id"] in seen:
                errors.append("duplicate:issue_id")
            seen.add(rec["issue_id"])
        if errors:
            bad.append({"line_no": line_no, "record": rec, "errors": errors})
        else:
            clean.append(rec)
    return clean, bad


def process(bucket: str, key: str) -> dict:
    body = _s3().get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(body))
    missing = sorted(set(FIELDS) - set(reader.fieldnames or []))

    if missing:
        clean = []
        bad = [{"line_no": 1, "record": {}, "errors": [f"schema:missing_column:{c}" for c in missing]}]
    else:
        clean, bad = validate_rows(reader)

    for item in clean + bad:
        item["source_key"] = key

    stem = key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    partition = f"ingest_date={datetime.now(UTC):%Y-%m-%d}"
    if clean:
        _put_jsonl(bucket, f"{CLEAN_PREFIX}{partition}/{stem}.jsonl", clean)
    if bad:
        _put_jsonl(bucket, f"{QUARANTINE_PREFIX}{partition}/{stem}.jsonl", bad)

    _emit_metrics(valid=len(clean), quarantined=len(bad))
    return {"key": key, "valid": len(clean), "quarantined": len(bad)}


def handler(event, _context):
    return [process(r["s3"]["bucket"]["name"], unquote_plus(r["s3"]["object"]["key"])) for r in event["Records"]]


def _put_jsonl(bucket: str, key: str, items: list[dict]) -> None:
    body = "\n".join(json.dumps(i, default=str) for i in items).encode()
    _s3().put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson")


def _emit_metrics(**counts: int) -> None:
    print(
        json.dumps(
            {
                "_aws": {
                    "Timestamp": int(time.time() * 1000),
                    "CloudWatchMetrics": [
                        {
                            "Namespace": "CxPipeline",
                            "Dimensions": [["Stage"]],
                            "Metrics": [{"Name": k, "Unit": "Count"} for k in counts],
                        }
                    ],
                },
                "Stage": "validator",
                **counts,
            }
        )
    )
