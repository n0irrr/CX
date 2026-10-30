import base64
import csv
import io
import json

import pytest

import telemetry
import validator
from stream_loadtest import FakeS3, FakeStore

T0 = 1_790_000_000_000


def _reading(n: int, value: float = 21.0, tag: str = "CRAH-SIN1-A-01", point: str = "supply_air_temp", **kw) -> dict:
    reading = {"reading_id": f"r-{tag}-{n}", "site": "SIN1", "equipment_tag": tag, "point": point, "value": value}
    return reading | {"ts": T0 + n * 1000} | kw


def _event(*payloads, start_seq: int = 1000) -> dict:
    records = []
    for i, p in enumerate(payloads):
        data = p if isinstance(p, bytes) else json.dumps(p).encode()
        records.append(
            {
                "eventID": f"shardId-000000000000:{start_seq + i}",
                "kinesis": {
                    "data": base64.b64encode(data).decode(),
                    "sequenceNumber": str(start_seq + i),
                    "partitionKey": "pk",
                    "approximateArrivalTimestamp": T0 / 1000,
                },
            }
        )
    return {"Records": records}


@pytest.fixture
def s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(telemetry, "_s3", lambda: fake)
    monkeypatch.delenv("FAIL_RATE", raising=False)
    return fake


def _rows(s3: FakeS3, prefix: str) -> list[dict]:
    return [json.loads(line) for k, b in s3.objects.items() if k.startswith(prefix) for line in b.splitlines()]


def _issues(s3: FakeS3) -> list[str]:
    return [k for k in s3.objects if k.startswith("raw/telemetry-")]


def test_valid_reading_parses():
    rec, errors = telemetry.parse(json.dumps(_reading(1)).encode())
    assert errors == [] and rec["value"] == 21.0


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (b'{"value": ', "malformed:json"),
        (b"[1, 2]", "malformed:not_object"),
        ({k: v for k, v in _reading(1).items() if k != "ts"}, "missing:ts"),
        (_reading(1, value="n/a"), "invalid:value"),
        (_reading(1, value=True), "invalid:value"),
        (_reading(1, point="discharge_pressure"), "invalid:point"),
        (_reading(1, value=999.0), "implausible:value"),
        (_reading(1, equipment_tag="AHU 1"), "invalid:equipment_tag"),
        (_reading(1, ts="yesterday"), "invalid:ts"),
    ],
)
def test_bad_readings_are_flagged(payload, expected):
    data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    assert expected in telemetry.parse(data)[1]


def test_batch_splits_readings_and_quarantine(s3):
    resp = telemetry.handler(_event(_reading(1), b"garbage", _reading(2)), None, store=FakeStore())
    assert resp == {"batchItemFailures": []}
    assert [r["reading_id"] for r in _rows(s3, "telemetry/")] == ["r-CRAH-SIN1-A-01-1", "r-CRAH-SIN1-A-01-2"]
    assert _rows(s3, "telemetry-quarantine/")[0]["errors"] == ["malformed:json"]


def test_poison_records_never_fail_the_batch(s3):
    assert telemetry.handler(_event(b"\xff\xfe", b"{}", b"null"), None, store=FakeStore())["batchItemFailures"] == []
    assert len(_rows(s3, "telemetry-quarantine/")) == 3


def test_duplicates_in_a_batch_are_stored_once(s3):
    telemetry.handler(_event(_reading(1), _reading(1)), None, store=FakeStore())
    assert len(_rows(s3, "telemetry/")) == 1


def test_sustained_excursion_raises_one_issue(s3):
    store = FakeStore()
    hot = [_reading(n, value=29.5) for n in range(1, 9)]
    telemetry.handler(_event(*hot[:3]), None, store=store)
    assert _issues(s3) == []
    telemetry.handler(_event(*hot[3:], start_seq=2000), None, store=store)  # threshold crossed across batches
    [key] = _issues(s3)
    issue = s3.objects[key].decode()
    assert "CRAH-SIN1-A-01" in issue and "supply air temp" in issue and ",open," in issue


def test_short_blip_raises_nothing(s3):
    readings = [_reading(1, 29.0), _reading(2, 29.0), _reading(3, 21.0), _reading(4, 29.0), _reading(5, 29.0)]
    telemetry.handler(_event(*readings), None, store=FakeStore())
    assert _issues(s3) == []


def test_new_excursion_after_recovery_raises_again(s3):
    store = FakeStore()
    readings = (
        [_reading(n, 29.0) for n in range(1, 6)] + [_reading(6, 21.0)] + [_reading(n, 29.0) for n in range(7, 12)]
    )
    telemetry.handler(_event(*readings), None, store=store)
    assert len(_issues(s3)) == 2


def test_replayed_batch_is_idempotent(s3):
    store = FakeStore()
    event = _event(*[_reading(n, 29.0) for n in range(1, 7)])
    telemetry.handler(event, None, store=store)
    state, objects = dict(store.items), dict(s3.objects)
    telemetry.handler(event, None, store=store)
    assert store.items == state and s3.objects == objects


def test_validated_issue_flows_into_batch_pipeline(s3):
    telemetry.handler(_event(*[_reading(n, 29.0) for n in range(1, 6)]), None, store=FakeStore())
    rows = list(csv.DictReader(io.StringIO(s3.objects[_issues(s3)[0]].decode())))
    clean, bad = validator.validate_rows(rows)
    assert len(clean) == 1 and bad == []


class FlakyStore(FakeStore):
    def __init__(self, fail_sensors=(), fail_load=False):
        super().__init__()
        self.fail_sensors, self.fail_load = set(fail_sensors), fail_load

    def load(self, sensors):
        if self.fail_load:
            raise telemetry.TransientError("throttled")
        return super().load(sensors)

    def save(self, sensor, state, prev_ts):
        if sensor in self.fail_sensors:
            raise telemetry.TransientError("throttled")
        super().save(sensor, state, prev_ts)


def test_state_read_failure_retries_whole_batch(s3):
    resp = telemetry.handler(_event(_reading(1, 29.0), _reading(2)), None, store=FlakyStore(fail_load=True))
    assert resp == {"batchItemFailures": [{"itemIdentifier": "1000"}]}
    assert _rows(s3, "telemetry/") == []


def test_partial_failure_commits_only_records_before_it(s3):
    ok, bad_tag = "CRAH-SIN1-A-01", "CRAH-SIN1-A-02"
    readings = [_reading(1, 29.0, tag=ok), _reading(1, 29.0, tag=bad_tag), _reading(2, 29.0, tag=ok)]
    store = FlakyStore(fail_sensors={f"{bad_tag}#supply_air_temp"})
    resp = telemetry.handler(_event(*readings), None, store=store)
    assert resp == {"batchItemFailures": [{"itemIdentifier": "1001"}]}
    assert [r["reading_id"] for r in _rows(s3, "telemetry/")] == [f"r-{ok}-1"]
    assert store.items[f"{ok}#supply_air_temp"]["run"] == 2

    # Lambda retries from 1001; the healthy sensor's reading 2 comes round again and must not count twice.
    store.fail_sensors.clear()
    retry = {"Records": _event(*readings)["Records"][1:]}
    assert telemetry.handler(retry, None, store=store) == {"batchItemFailures": []}
    assert store.items[f"{ok}#supply_air_temp"]["run"] == 2
    assert sorted(r["reading_id"] for r in _rows(s3, "telemetry/")) == [f"r-{ok}-1", f"r-{ok}-2", f"r-{bad_tag}-1"]


def test_storage_failure_retries_whole_batch(s3, monkeypatch):
    def boom(*_a, **_k):
        raise telemetry.TransientError("s3 down")

    monkeypatch.setattr(telemetry, "_put_jsonl", boom)
    resp = telemetry.handler(_event(_reading(1), _reading(2)), None, store=FakeStore())
    assert resp == {"batchItemFailures": [{"itemIdentifier": "1000"}]}


def test_metrics_include_latency(s3, capsys):
    telemetry.handler(_event(_reading(1), b"bad"), None, store=FakeStore())
    emf = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert emf["readings"] == 1 and emf["quarantined"] == 1 and len(emf["LatencyMs"]) == 1
    assert {"Name": "LatencyMs", "Unit": "Milliseconds"} in emf["_aws"]["CloudWatchMetrics"][0]["Metrics"]
