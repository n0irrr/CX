import argparse
import csv
import json
from collections import Counter
from pathlib import Path

import classifier

DEFAULT_SET = Path(__file__).resolve().parent.parent / "eval" / "labelled_notes.csv"


class UsageTracker:
    def __init__(self, client):
        self.client = client
        self.input_tokens = self.output_tokens = self.calls = 0

    def converse(self, **kwargs):
        resp = self.client.converse(**kwargs)
        usage = resp.get("usage", {})
        self.input_tokens += usage.get("inputTokens", 0)
        self.output_tokens += usage.get("outputTokens", 0)
        self.calls += 1
        return resp


def load(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return [{"issue_id": f"EV-{i:04d}", **row} for i, row in enumerate(csv.DictReader(f), start=1)]


def evaluate(rows: list[dict], client=None) -> dict:
    truth = {r["issue_id"]: r["category"] for r in rows}
    records = [{"issue_id": r["issue_id"], "notes": r["notes"]} for r in rows]
    out = classifier.classify(records, client)

    by_rules = [r for r in out if r["classified_by"] == "rules"]
    correct = [r for r in out if r["category"] == truth[r["issue_id"]]]
    support = Counter(truth.values())
    hits = Counter(r["category"] for r in correct)
    per_category = {c: round(hits[c] / max(1, support[c]), 3) for c in classifier.CATEGORIES}
    rules_correct = sum(r["category"] == truth[r["issue_id"]] for r in by_rules)
    errors = Counter((truth[r["issue_id"]], r["category"]) for r in out if r["category"] != truth[r["issue_id"]])
    return {
        "n": len(out),
        "accuracy": round(len(correct) / len(out), 3),
        "rule_coverage": round(len(by_rules) / len(out), 3),
        "rule_precision": round(rules_correct / max(1, len(by_rules)), 3),
        "classified_by": dict(Counter(r["classified_by"] for r in out)),
        "per_category_recall": per_category,
        "top_errors": [{"truth": t, "predicted": p, "count": n} for (t, p), n in errors.most_common(5)],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Score the classifier against a hand-labelled set")
    parser.add_argument("--set", type=Path, default=DEFAULT_SET)
    parser.add_argument("--llm", action="store_true", help="send rule misses to Bedrock (needs AWS credentials)")
    parser.add_argument("--input-price", type=float, default=0.10, help="USD per 1M input tokens")
    parser.add_argument("--output-price", type=float, default=0.50, help="USD per 1M output tokens")
    args = parser.parse_args()

    rows = load(args.set)
    classifier.LLM_ENABLED = args.llm
    tracker = UsageTracker(classifier._bedrock()) if args.llm else None
    report = {"mode": "rules+llm" if args.llm else "rules-only", "model": classifier.MODEL_ID if args.llm else None}
    report |= evaluate(rows, tracker)

    if tracker:
        cost = (tracker.input_tokens * args.input_price + tracker.output_tokens * args.output_price) / 1e6
        report |= {
            "bedrock_calls": tracker.calls,
            "input_tokens": tracker.input_tokens,
            "output_tokens": tracker.output_tokens,
            "cost_usd": round(cost, 6),
            "cost_per_1000_issues_usd": round(cost / len(rows) * 1000, 4),
        }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
