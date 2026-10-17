import json

import pytest

import validator

GOOD = {
    "issue_id": "CX-000001",
    "site": "SIN1",
    "building": "A",
    "equipment_type": "UPS",
    "equipment_tag": "UPS-SIN1-A-01",
    "manufacturer": "Voltaris",
    "contractor": "Apex M&E",
    "cx_level": "L3",
    "severity": "high",
    "status": "closed",
    "opened_at": "2026-03-01",
    "closed_at": "2026-03-05",
    "notes": "Loose termination on incomer",
}


class FakeS3:
    def __init__(self, objects: dict[str, str]):
        self.objects = objects

    def get_object(self, Bucket, Key):
        return {"Body": type("B", (), {"read": lambda _: self.objects[Key].encode()})()}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[Key] = Body.decode()


def test_valid_row_passes():
    assert validator.validate_row(GOOD)[1] == []


@pytest.mark.parametrize(
    ("patch", "expected"),
    [
        ({"severity": "urgent"}, "invalid:severity"),
        ({"issue_id": "CX-12"}, "invalid:issue_id"),
        ({"notes": ""}, "missing:notes"),
        ({"opened_at": "31/12/2026"}, "invalid:opened_at"),
        ({"closed_at": "2026-02-01"}, "order:closed_before_opened"),
        ({"status": "open"}, "conflict:open_with_closed_at"),
        ({"closed_at": ""}, "missing:closed_at"),
        ({"notes": "x" * 2001}, "invalid:notes_too_long"),
    ],
)
def test_invalid_rows_are_flagged(patch, expected):
    assert expected in validator.validate_row(GOOD | patch)[1]


def test_duplicate_ids_are_quarantined():
    clean, bad = validator.validate_rows([GOOD, GOOD])
    assert len(clean) == 1
    assert bad[0]["errors"] == ["duplicate:issue_id"]
    assert bad[0]["line_no"] == 3


def _csv(rows, fields=validator.FIELDS):
    return "\n".join([",".join(fields)] + [",".join(r[f] for f in fields) for r in rows])


def test_process_splits_clean_and_quarantine(monkeypatch):
    s3 = FakeS3({"raw/batch.csv": _csv([GOOD, GOOD | {"issue_id": "CX-000002", "severity": "urgent"}])})
    monkeypatch.setattr(validator, "_s3", lambda: s3)

    result = validator.process("bucket", "raw/batch.csv")

    assert result == {"key": "raw/batch.csv", "valid": 1, "quarantined": 1}
    clean_key = next(k for k in s3.objects if k.startswith("clean/"))
    assert clean_key.endswith("/batch.jsonl") and "ingest_date=" in clean_key
    assert json.loads(s3.objects[clean_key])["source_key"] == "raw/batch.csv"


def test_missing_columns_quarantines_whole_file(monkeypatch):
    fields = [f for f in validator.FIELDS if f != "notes"]
    s3 = FakeS3({"raw/bad.csv": _csv([GOOD], fields)})
    monkeypatch.setattr(validator, "_s3", lambda: s3)

    assert validator.process("bucket", "raw/bad.csv")["valid"] == 0
    quarantine = json.loads(next(v for k, v in s3.objects.items() if k.startswith("quarantine/")))
    assert quarantine["errors"] == ["schema:missing_column:notes"]
