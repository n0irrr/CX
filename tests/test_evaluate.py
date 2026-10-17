import json

import classifier
import evaluate


class FakeBedrock:
    def converse(self, **kwargs):
        items = json.loads(kwargs["messages"][0]["content"][0]["text"])
        text = json.dumps([{"id": i["id"], "category": "design_issue"} for i in items])
        return {"output": {"message": {"content": [{"text": text}]}}, "usage": {"inputTokens": 100, "outputTokens": 10}}


ROWS = [
    {"issue_id": "EV-0001", "notes": "Loose termination on feeder", "category": "installation_workmanship"},
    {"issue_id": "EV-0002", "notes": "Floor loading exceeds slab capacity", "category": "design_issue"},
    {"issue_id": "EV-0003", "notes": "Warranty letter outstanding", "category": "documentation"},
]


def test_rules_only_scores(monkeypatch):
    monkeypatch.setattr(classifier, "LLM_ENABLED", False)
    report = evaluate.evaluate([dict(r) for r in ROWS])
    assert report["accuracy"] == round(1 / 3, 3)
    assert report["rule_coverage"] == round(1 / 3, 3)
    assert report["rule_precision"] == 1.0


def test_llm_path_and_usage(monkeypatch):
    monkeypatch.setattr(classifier, "LLM_ENABLED", True)
    tracker = evaluate.UsageTracker(FakeBedrock())
    report = evaluate.evaluate([dict(r) for r in ROWS], tracker)
    assert report["accuracy"] == round(2 / 3, 3)
    assert (tracker.calls, tracker.input_tokens) == (1, 100)


def test_bundled_set_is_balanced():
    rows = evaluate.load(evaluate.DEFAULT_SET)
    counts = {c: sum(r["category"] == c for r in rows) for c in classifier.CATEGORIES}
    assert set(counts.values()) == {14}
