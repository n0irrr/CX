from datetime import date

import classifier
import generate_data
import validator


def test_clean_data_passes_validation():
    rows = generate_data.generate(500, bad_rate=0, today=date(2026, 10, 1))
    clean, bad = validator.validate_rows(rows)
    assert len(clean) == 500 and not bad


def test_corrupted_rows_are_caught():
    rows = generate_data.generate(500, bad_rate=0.1, today=date(2026, 10, 1))
    _, bad = validator.validate_rows(rows)
    assert 30 <= len(bad) <= 70


def test_mix_of_rule_and_llm_paths():
    rows = generate_data.generate(1000, bad_rate=0, today=date(2026, 10, 1))
    rule_share = sum(classifier.rule_classify(r["notes"]) is not None for r in rows) / len(rows)
    assert 0.6 <= rule_share <= 0.85
