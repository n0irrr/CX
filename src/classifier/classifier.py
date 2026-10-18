import json
import os
import re
import time
from functools import cache
from urllib.parse import unquote_plus

import boto3
from botocore.config import Config

CATEGORIES = (
    "equipment_defect",
    "installation_workmanship",
    "design_issue",
    "controls_integration",
    "documentation",
)
RULES = {
    "equipment_defect": r"\b(faulty|failed|failure|defective|doa|burnt|leak(ing)?|bearing noise)\b",
    "installation_workmanship": (
        r"\b(loose|termination|mislabell?ed|cable routing|workmanship|incorrectly installed|unsealed)\b"
    ),
    "design_issue": r"\b(undersized|oversized|clearance|design|spec(ification)? mismatch|rating)\b",
    "controls_integration": (r"\b(bms|plc|scada|setpoint|sequence of operation|point mapping|modbus|bacnet)\b"),
    "documentation": r"\b(o&m|submittal|as-built|datasheet|test certificate|drawing)\b",
}
COMPILED = {k: re.compile(v, re.IGNORECASE) for k, v in RULES.items()}

SYSTEM_PROMPT = (
    "You classify data centre commissioning issues by root cause. "
    f"Allowed categories: {', '.join(CATEGORIES)}. "
    "Input is a JSON array of {id, notes}. Return only a JSON array of "
    '{"id": <id>, "category": <category>} with exactly one category per issue.'
)

MODEL_ID = os.environ.get("MODEL_ID", "global.amazon.nova-2-lite-v1:0")
LLM_ENABLED = os.environ.get("LLM_ENABLED", "true").lower() == "true"
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "20"))
CLEAN_PREFIX = os.environ.get("CLEAN_PREFIX", "clean/")
ENRICHED_PREFIX = os.environ.get("ENRICHED_PREFIX", "enriched/")


@cache
def _s3():
    return boto3.client("s3")


@cache
def _bedrock():
    return boto3.client("bedrock-runtime", config=Config(retries={"max_attempts": 6, "mode": "adaptive"}))


def rule_classify(notes: str) -> str | None:
    hits = [c for c, pattern in COMPILED.items() if pattern.search(notes)]
    return hits[0] if len(hits) == 1 else None


def _parse_labels(text: str) -> dict[str, str]:
    start, end = text.find("["), text.rfind("]")
    try:
        items = json.loads(text[start : end + 1])
    except (ValueError, TypeError):
        return {}
    return {
        str(i["id"]): i["category"]
        for i in items
        if isinstance(i, dict) and "id" in i and i.get("category") in CATEGORIES
    }


def _model_fields(model_id: str) -> dict:
    # Effort is a Claude 5 setting; other models reject it.
    if re.search(r"claude-[a-z]+-5", model_id):
        return {"additionalModelRequestFields": {"output_config": {"effort": "low"}}}
    return {}


def llm_classify(records: list[dict], client=None) -> dict[str, str]:
    client = client or _bedrock()
    labels = {}
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        payload = json.dumps([{"id": r["issue_id"], "notes": r["notes"]} for r in batch])
        resp = client.converse(
            modelId=MODEL_ID,
            system=[{"text": SYSTEM_PROMPT}],
            messages=[{"role": "user", "content": [{"text": payload}]}],
            inferenceConfig={"maxTokens": 4096},
            **_model_fields(MODEL_ID),
        )
        text = "".join(block.get("text", "") for block in resp["output"]["message"]["content"])
        labels |= _parse_labels(text)
    return labels


def classify(records: list[dict], client=None) -> list[dict]:
    pending = []
    for r in records:
        if category := rule_classify(r["notes"]):
            r |= {"category": category, "classified_by": "rules"}
        else:
            pending.append(r)

    if pending:
        labels = llm_classify(pending, client) if LLM_ENABLED else {}
        for r in pending:
            category = labels.get(r["issue_id"])
            r |= {"category": category or "unclassified", "classified_by": "llm" if category else "fallback"}
    return records


def process(bucket: str, key: str) -> dict:
    body = _s3().get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    records = classify([json.loads(line) for line in body.splitlines() if line.strip()])

    out_key = ENRICHED_PREFIX + key.removeprefix(CLEAN_PREFIX)
    _s3().put_object(
        Bucket=bucket,
        Key=out_key,
        Body="\n".join(json.dumps(r) for r in records).encode(),
        ContentType="application/x-ndjson",
    )

    counts = {m: sum(r["classified_by"] == m for r in records) for m in ("rules", "llm", "fallback")}
    _emit_metrics(counts)
    return {"key": out_key, **counts}


def handler(event, _context):
    return [process(r["s3"]["bucket"]["name"], unquote_plus(r["s3"]["object"]["key"])) for r in event["Records"]]


def _emit_metrics(counts: dict[str, int]) -> None:
    names = {f"ClassifiedBy_{k}": v for k, v in counts.items()}
    print(
        json.dumps(
            {
                "_aws": {
                    "Timestamp": int(time.time() * 1000),
                    "CloudWatchMetrics": [
                        {
                            "Namespace": "CxPipeline",
                            "Dimensions": [["Stage"]],
                            "Metrics": [{"Name": k, "Unit": "Count"} for k in names],
                        }
                    ],
                },
                "Stage": "classifier",
                **names,
            }
        )
    )
