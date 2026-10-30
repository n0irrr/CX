import argparse
import json

import pytest

import telemetry
from stream_loadtest import LocalKinesis, run_local
from stream_producer import Simulator, put_with_retry, sensors

T0 = 1_790_000_000_000


def test_sensors_are_deterministic_and_valid():
    s = sensors(12, seed=3)
    assert s == sensors(12, seed=3) and len(set(s)) == 12
    assert all(telemetry.TAG.match(tag) and point in telemetry.POINTS for _, tag, point in s)


def test_clean_stream_stays_in_spec():
    sim = Simulator(sensors(10), excursion_rate=0)
    for i in range(200):
        for _, data in sim.tick(T0 + i * 1000):
            rec, errors = telemetry.parse(data)
            low, high = telemetry.POINTS[rec["point"]]["spec"]
            assert errors == [] and low <= rec["value"] <= high


def test_injected_bad_records_are_all_rejected():
    sim = Simulator(sensors(10), excursion_rate=0, bad_rate=0.2, seed=1)
    rejected = sum(bool(telemetry.parse(d)[1]) for i in range(100) for _, d in sim.tick(T0 + i * 1000))
    assert rejected == sim.counts["bad"] > 0


def test_excursions_leave_spec():
    sim = Simulator(sensors(10), excursion_rate=0.05, seed=2)
    for i in range(200):
        sim.tick(T0 + i * 1000)
    assert sim.excursions and all(e.out_of_spec_emitted >= 5 for e in sim.excursions if e.remaining <= 0)


class ThrottlingClient:
    def __init__(self, rejections: int):
        self.rejections, self.calls, self.received = rejections, 0, []

    def put_records(self, StreamName, Records):  # noqa: N803
        self.calls += 1
        results = []
        for r in Records:
            if self.rejections:
                self.rejections -= 1
                results.append({"ErrorCode": "ProvisionedThroughputExceededException"})
            else:
                self.received.append(r["Data"])
                results.append({"SequenceNumber": "1"})
        return {"FailedRecordCount": sum("ErrorCode" in r for r in results), "Records": results}


def test_put_resends_only_rejected_records():
    client = ThrottlingClient(rejections=3)
    items = [("pk", json.dumps({"n": i}).encode()) for i in range(10)]
    stats = put_with_retry(client, "s", items)
    assert stats == {"sent": 10, "throttled": 3, "failed": 0}
    assert sorted(client.received) == sorted(d for _, d in items)


def test_put_chunks_at_500():
    client = ThrottlingClient(rejections=0)
    put_with_retry(client, "s", [("pk", b"x")] * 1200)
    assert client.calls == 3


def test_local_stream_routes_a_sensor_to_one_shard():
    stream = LocalKinesis(shards=4)
    stream.put_records("s", [{"PartitionKey": "CRAH-SIN1-A-01", "Data": b"x"}] * 20)
    assert sorted(len(s) for s in stream.shards) == [0, 0, 0, 20]


def _args(**kw):
    defaults = {"fail_rate": 0.0, "throttle_rate": 0.0, "shards": 2, "seed": 0, "sensors": 20, "hz": 1.0}
    defaults |= {"duration": 180, "bad_rate": 0.02, "dup_rate": 0.01, "excursion_rate": 0.005}
    return argparse.Namespace(**defaults | kw)


@pytest.mark.parametrize("fail_rate", [0.0, 0.2])
def test_soak_no_loss_no_double_issues(fail_rate):
    """Continuous stream with bad records, duplicates, throttling and failing consumer writes:
    everything valid lands exactly once (or is accounted for in the DLQ) and each excursion raises one issue."""
    r = run_local(_args(fail_rate=fail_rate, throttle_rate=0.05))
    assert r["records_put"] > 3500 and r["put_failed"] == 0
    assert r["lost"] == 0
    assert r["duplicates_stored"] <= r["duplicates_sent"]
    assert r["quarantined_with_run_id"] + r["dead_lettered_bad"] == r["bad_sent"]
    assert r["issues_raised"] == r["issues_expected"] > 0
    if fail_rate:
        assert r["partial_failures"] > 0
