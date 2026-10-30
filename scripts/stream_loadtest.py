import argparse
import base64
import hashlib
import io
import json
import os
import random
import time
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import boto3

import telemetry
from stream_producer import Simulator, put_with_retry, run, sensors


class LocalKinesis:
    """In-memory stream: shards by MD5 of the partition key like Kinesis, with optional throttling."""

    def __init__(self, shards: int = 1, throttle_rate: float = 0.0, seed: int = 0):
        self.rng = random.Random(seed)
        self.shards: list[list[dict]] = [[] for _ in range(shards)]
        self.throttle_rate = throttle_rate
        self._seq = 49_000_000_000_000_000_000

    def put_records(self, StreamName: str, Records: list[dict]) -> dict:  # noqa: N803 (boto3 signature)
        results, failed = [], 0
        for r in Records:
            if self.rng.random() < self.throttle_rate:
                results.append({"ErrorCode": "ProvisionedThroughputExceededException"})
                failed += 1
                continue
            shard = int(hashlib.md5(r["PartitionKey"].encode()).hexdigest(), 16) % len(self.shards)
            self._seq += 1
            self.shards[shard].append({"seq": str(self._seq), "pk": r["PartitionKey"], "data": r["Data"]})
            results.append({"SequenceNumber": str(self._seq), "ShardId": f"shardId-{shard:012d}"})
        return {"FailedRecordCount": failed, "Records": results}


class FakeS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str) -> None:  # noqa: N803
        self.objects[Key] = Body


class FakeStore:
    """Same contract as telemetry.StateStore, including the conditional write."""

    def __init__(self):
        self.items: dict[str, dict] = {}

    def load(self, sensors: list[str]) -> dict[str, dict]:
        telemetry._chaos()
        return {s: dict(self.items[s]) for s in sensors if s in self.items}

    def save(self, sensor: str, state: dict, prev_ts: int | None) -> None:
        current = self.items.get(sensor)
        if (current is None) != (prev_ts is None) or (current and current["last_ts"] != prev_ts):
            raise telemetry.TransientError("conditional check failed")
        self.items[sensor] = dict(state)


@dataclass
class LocalConsumer:
    """Mimics a Lambda event source mapping: per-shard checkpoints, batches, retry from the reported failure."""

    stream: LocalKinesis
    store: FakeStore
    batch_size: int = 100
    max_retries: int = 10
    checkpoints: dict = field(default_factory=dict)
    dead_letters: list[bytes] = field(default_factory=list)
    stats: dict = field(default_factory=lambda: {"invocations": 0, "partial_failures": 0, "dead_lettered": 0})

    def drain(self) -> None:
        for shard_no, recs in enumerate(self.stream.shards):
            pos, attempts = self.checkpoints.get(shard_no, 0), 0
            while pos < len(recs):
                batch = recs[pos : pos + self.batch_size]
                event = {
                    "Records": [
                        {
                            "eventID": f"shardId-{shard_no:012d}:{r['seq']}",
                            "kinesis": {
                                "data": base64.b64encode(r["data"]).decode(),
                                "sequenceNumber": r["seq"],
                                "partitionKey": r["pk"],
                                "approximateArrivalTimestamp": time.time(),
                            },
                        }
                        for r in batch
                    ]
                }
                self.stats["invocations"] += 1
                with redirect_stdout(io.StringIO()):  # EMF lines are for CloudWatch, not the terminal
                    failures = telemetry.handler(event, None, store=self.store)["batchItemFailures"]
                if not failures:
                    pos, attempts = pos + len(batch), 0
                    continue
                self.stats["partial_failures"] += 1
                idx = [r["seq"] for r in batch].index(failures[0]["itemIdentifier"])
                attempts = attempts + 1 if idx == 0 else 0
                pos += idx
                if attempts > self.max_retries:
                    self.stats["dead_lettered"] += 1
                    self.dead_letters += [r["data"] for r in batch]
                    pos, attempts = pos + len(batch), 0
            self.checkpoints[shard_no] = pos


def verify(sim: Simulator, objects: dict[str, bytes], dead_letters: list[bytes] = ()) -> dict:
    """Compare what landed in S3 with the simulator's ground truth for this run."""
    ids: list[str] = []
    quarantined = 0
    for key, body in objects.items():
        if key.startswith(telemetry.TELEMETRY_PREFIX):
            ids += [
                i for i in (json.loads(line)["reading_id"] for line in body.splitlines()) if i.startswith(sim.run_id)
            ]
        elif key.startswith(telemetry.QUARANTINE_PREFIX):
            quarantined += sum(1 for line in body.splitlines() if sim.run_id in json.loads(line)["data"])
    issues = [k for k in objects if k.startswith(f"{telemetry.RAW_PREFIX}telemetry-")]
    expected_issues = sum(1 for e in sim.excursions if e.out_of_spec_emitted >= telemetry.EXCURSION_READINGS)
    unique = len(set(ids))
    in_dlq, bad_in_dlq = set(), 0
    for data in dead_letters:
        rec, errors = telemetry.parse(data)
        if errors:
            bad_in_dlq += sim.run_id.encode() in data
        elif rec["reading_id"].startswith(sim.run_id):
            in_dlq.add(rec["reading_id"])
    return {
        "valid_sent": sim.counts["valid"],
        "stored_unique": unique,
        "dead_lettered_readings": len(in_dlq - set(ids)),
        "lost": sim.counts["valid"] - unique - len(in_dlq - set(ids)),
        "duplicates_sent": sim.counts["duplicates"],
        "duplicates_stored": len(ids) - unique,
        "bad_sent": sim.counts["bad"],
        "quarantined_with_run_id": quarantined,
        "dead_lettered_bad": bad_in_dlq,
        "excursions": len(sim.excursions),
        "issues_expected": expected_issues,
        "issues_raised": len(issues),
    }


def run_local(args) -> dict:
    os.environ["FAIL_RATE"] = str(args.fail_rate)
    s3, store = FakeS3(), FakeStore()
    telemetry._s3 = lambda: s3
    stream = LocalKinesis(args.shards, args.throttle_rate, args.seed)
    consumer = LocalConsumer(stream, store)
    sim = Simulator(
        sensors(args.sensors, args.seed),
        excursion_rate=args.excursion_rate,
        bad_rate=args.bad_rate,
        dup_rate=args.dup_rate,
        seed=args.seed,
    )
    ticks = int(args.duration * args.hz)
    start, put = time.perf_counter(), {"sent": 0, "throttled": 0, "failed": 0}
    t0 = int(time.time() * 1000)
    for i in range(ticks):
        stats = put_with_retry(stream, "local", sim.tick(t0 + int(i * 1000 / args.hz)))
        put = {k: put[k] + stats[k] for k in put}
        if i % 5 == 4:  # consumer keeps up a few ticks behind, as a polling event source would
            consumer.drain()
    consumer.drain()
    elapsed = time.perf_counter() - start
    os.environ.pop("FAIL_RATE")
    return {
        "mode": "local",
        "records_put": put["sent"],
        "put_throttled_and_retried": put["throttled"],
        "put_failed": put["failed"],
        "wall_seconds": round(elapsed, 2),
        "records_per_second": round(put["sent"] / elapsed),
        **consumer.stats,
        **verify(sim, s3.objects, consumer.dead_letters),
    }


def _outputs(stack: str) -> dict:
    out = boto3.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]["Outputs"]
    return {o["OutputKey"]: o["OutputValue"] for o in out}


def _set_fail_rate(lam, fn: str, rate: float) -> None:
    env = lam.get_function_configuration(FunctionName=fn)["Environment"]["Variables"]
    lam.update_function_configuration(FunctionName=fn, Environment={"Variables": env | {"FAIL_RATE": str(rate)}})
    lam.get_waiter("function_updated_v2").wait(FunctionName=fn)


def _read_prefix(s3, bucket: str, prefix: str, since: datetime) -> dict[str, bytes]:
    out = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            if o["LastModified"] >= since:
                out[o["Key"]] = s3.get_object(Bucket=bucket, Key=o["Key"])["Body"].read()
    return out


def _stat(cw, namespace: str, metric: str, dims: list, start, end, stats=None, extended=None) -> dict:
    resp = cw.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric,
        Dimensions=dims,
        StartTime=start,
        EndTime=end,
        Period=int((end - start).total_seconds()) // 60 * 60 or 60,
        **({"Statistics": stats} if stats else {}),
        **({"ExtendedStatistics": extended} if extended else {}),
    )
    points = resp["Datapoints"]
    if not points:
        return {}
    merged: dict = {}
    for p in points:
        for k, v in {**{s: p.get(s) for s in stats or []}, **p.get("ExtendedStatistics", {})}.items():
            merged[k] = max(merged.get(k, 0), v) if k != "Sum" else merged.get(k, 0) + v
    return merged


def run_live(args) -> dict:
    out = _outputs(args.stack)
    stream, fn, bucket = out["TelemetryStreamName"], out["TelemetryFunctionName"], out["BucketName"]
    lam, s3, cw = boto3.client("lambda"), boto3.client("s3"), boto3.client("cloudwatch")
    sim = Simulator(
        sensors(args.sensors, args.seed),
        excursion_rate=args.excursion_rate,
        bad_rate=args.bad_rate,
        dup_rate=args.dup_rate,
        seed=args.seed,
    )
    if args.fail_rate:
        _set_fail_rate(lam, fn, args.fail_rate)
    start = datetime.now(UTC) - timedelta(seconds=5)
    try:
        put = run(sim, boto3.client("kinesis"), stream, args.hz, args.duration)
        print("draining...")
        deadline, last = time.time() + args.drain_timeout, -1
        while time.time() < deadline:
            objects = {}
            for prefix in (
                telemetry.TELEMETRY_PREFIX,
                telemetry.QUARANTINE_PREFIX,
                f"{telemetry.RAW_PREFIX}telemetry-",
            ):
                objects |= _read_prefix(s3, bucket, prefix, start)
            result = verify(sim, objects)
            if result["stored_unique"] >= result["valid_sent"] or result["stored_unique"] == last:
                break
            last = result["stored_unique"]
            time.sleep(10)
    finally:
        if args.fail_rate:
            _set_fail_rate(lam, fn, 0)
    end = datetime.now(UTC) + timedelta(minutes=1)
    stage = [{"Name": "Stage", "Value": "telemetry"}]
    latency = _stat(cw, "CxPipeline", "LatencyMs", stage, start, end, extended=["p50", "p90", "p99"])
    iterator_age = _stat(
        cw, "AWS/Lambda", "IteratorAge", [{"Name": "FunctionName", "Value": fn}], start, end, ["Maximum"]
    )
    retried = _stat(cw, "CxPipeline", "retried", stage, start, end, ["Sum"])
    return {
        "mode": "live",
        "records_put": put["sent"],
        "put_throttled_and_retried": put["throttled"],
        "put_failed": put["failed"],
        "records_per_second": round(put["sent"] / put["elapsed"]),
        "latency_ms_p50_p90_p99": [round(latency.get(p, 0)) for p in ("p50", "p90", "p99")],
        "iterator_age_max_ms": iterator_age.get("Maximum"),
        "records_retried": retried.get("Sum", 0),
        **result,
    }


def report(result: dict) -> None:
    width = max(map(len, result))
    for k, v in result.items():
        print(f"{k:<{width}}  {v}")
    checks = {
        "no readings silently lost": result["lost"] == 0,
        "retries added no duplicates": result["duplicates_stored"] <= result["duplicates_sent"],
        "every bad record quarantined": result["quarantined_with_run_id"] + result["dead_lettered_bad"]
        == result["bad_sent"],
        "one issue per excursion": result["issues_raised"] == result["issues_expected"],
    }
    print()
    for name, ok in checks.items():
        print(f"{'PASS' if ok else 'FAIL'}  {name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load and fault test for the telemetry stream")
    parser.add_argument("mode", choices=["local", "live"])
    parser.add_argument("--stack", default="CxPipeline")
    parser.add_argument("--sensors", type=int, default=20)
    parser.add_argument("--hz", type=float, default=1.0)
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--shards", type=int, default=1, help="local only")
    parser.add_argument("--bad-rate", type=float, default=0.02)
    parser.add_argument("--dup-rate", type=float, default=0.01)
    parser.add_argument("--excursion-rate", type=float, default=0.003)
    parser.add_argument("--fail-rate", type=float, default=0.0, help="share of consumer writes that fail")
    parser.add_argument("--throttle-rate", type=float, default=0.0, help="local only: share of puts rejected")
    parser.add_argument("--drain-timeout", type=int, default=180)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--out", help="write the result as JSON")
    args = parser.parse_args()

    result = run_local(args) if args.mode == "local" else run_live(args)
    report(result)
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2, default=str)


if __name__ == "__main__":
    main()
