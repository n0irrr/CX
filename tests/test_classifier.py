import json

import pytest

import classifier


class FakeBedrock:
    def __init__(self, responder):
        self.responder = responder
        self.calls = 0

    def converse(self, **kwargs):
        self.calls += 1
        items = json.loads(kwargs["messages"][0]["content"][0]["text"])
        self.last_kwargs = kwargs
        content = [{"reasoningContent": {"reasoningText": {"text": ""}}}, {"text": self.responder(items)}]
        return {"output": {"message": {"content": content}}}


def _records(*notes):
    return [{"issue_id": f"CX-{i:06d}", "notes": n} for i, n in enumerate(notes, start=1)]


@pytest.mark.parametrize(
    ("notes", "expected"),
    [
        ("Faulty rectifier module, alarm persists", "equipment_defect"),
        ("Loose termination on incomer", "installation_workmanship"),
        ("Insufficient service clearance in front of panel", "design_issue"),
        ("BMS point mapping shows wrong temperature", "controls_integration"),
        ("O&M manual not submitted", "documentation"),
        ("Unit tripped during load bank test", None),
        ("BMS shows alarm, vendor suspects faulty sensor", None),
    ],
)
def test_rule_classify(notes, expected):
    assert classifier.rule_classify(notes) == expected


def test_only_ambiguous_records_reach_llm():
    bedrock = FakeBedrock(lambda items: json.dumps([{"id": i["id"], "category": "equipment_defect"} for i in items]))
    out = classifier.classify(_records("Loose termination", "Unit tripped under load"), bedrock)

    assert bedrock.calls == 1
    assert [r["classified_by"] for r in out] == ["rules", "llm"]
    assert out[1]["category"] == "equipment_defect"


def test_invalid_llm_output_falls_back():
    bedrock = FakeBedrock(lambda items: "Sorry, I can't help with that.")
    out = classifier.classify(_records("Unit tripped under load"), bedrock)
    assert out[0] | {"issue_id": None} == {
        "issue_id": None,
        "notes": "Unit tripped under load",
        "category": "unclassified",
        "classified_by": "fallback",
    }


def test_unknown_category_is_rejected():
    bedrock = FakeBedrock(lambda items: json.dumps([{"id": items[0]["id"], "category": "aliens"}]))
    assert classifier.classify(_records("Unit noisy"), bedrock)[0]["classified_by"] == "fallback"


def test_llm_calls_are_batched(monkeypatch):
    monkeypatch.setattr(classifier, "BATCH_SIZE", 2)
    bedrock = FakeBedrock(lambda items: json.dumps([{"id": i["id"], "category": "design_issue"} for i in items]))
    out = classifier.classify(_records(*["Unit noisy under load"] * 5), bedrock)

    assert bedrock.calls == 3
    assert {r["category"] for r in out} == {"design_issue"}


def test_parse_labels_tolerates_wrapping_text():
    text = 'Here you go:\n[{"id": "CX-000001", "category": "documentation"}]\nDone.'
    assert classifier._parse_labels(text) == {"CX-000001": "documentation"}


def test_rules_only_mode_skips_llm(monkeypatch):
    monkeypatch.setattr(classifier, "LLM_ENABLED", False)
    bedrock = FakeBedrock(lambda items: "[]")
    out = classifier.classify(_records("Loose termination", "Unit tripped under load"), bedrock)

    assert bedrock.calls == 0
    assert [r["classified_by"] for r in out] == ["rules", "fallback"]


def test_request_has_no_sampling_params():
    bedrock = FakeBedrock(lambda items: "[]")
    classifier.classify(_records("Unit tripped under load"), bedrock)
    assert "temperature" not in bedrock.last_kwargs["inferenceConfig"]
