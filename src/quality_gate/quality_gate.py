import json
import os
from functools import cache
from pathlib import Path

import boto3

SAMPLES = json.loads((Path(__file__).parent / "samples.json").read_text())
TARGET = os.environ.get("TARGET_FUNCTION_ARN", "")
MIN_ACCURACY = float(os.environ.get("MIN_ACCURACY", "0.8"))


@cache
def _lambda():
    return boto3.client("lambda")


@cache
def _codedeploy():
    return boto3.client("codedeploy")


def score(labelled: list[dict], invoke) -> float:
    records = [{"issue_id": str(i), "notes": s["notes"]} for i, s in enumerate(labelled)]
    predicted = {r["issue_id"]: r["category"] for r in invoke(records)}
    return sum(predicted.get(str(i)) == s["category"] for i, s in enumerate(labelled)) / len(labelled)


def _invoke_target(records: list[dict]) -> list[dict]:
    resp = _lambda().invoke(FunctionName=TARGET, Payload=json.dumps({"records": records}).encode())
    if resp.get("FunctionError"):
        raise RuntimeError(resp["Payload"].read().decode())
    return json.loads(resp["Payload"].read())


def handler(event, _context):
    try:
        accuracy = score(SAMPLES, _invoke_target)
        passed = accuracy >= MIN_ACCURACY
        print(json.dumps({"target": TARGET, "accuracy": accuracy, "min_accuracy": MIN_ACCURACY, "passed": passed}))
    except Exception as exc:
        print(json.dumps({"target": TARGET, "error": str(exc)}))
        passed = False
    _codedeploy().put_lifecycle_event_hook_execution_status(
        deploymentId=event["DeploymentId"],
        lifecycleEventHookExecutionId=event["LifecycleEventHookExecutionId"],
        status="Succeeded" if passed else "Failed",
    )
