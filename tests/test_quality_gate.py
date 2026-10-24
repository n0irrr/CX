import quality_gate


def _labelled():
    return [{"notes": f"note {i}", "category": c} for i, c in enumerate(["documentation", "design_issue"] * 5)]


def test_score_counts_matching_labels():
    labelled = _labelled()

    def invoke(records):
        return [{"issue_id": r["issue_id"], "category": "documentation"} for r in records]

    assert quality_gate.score(labelled, invoke) == 0.5


def test_score_treats_missing_labels_as_wrong():
    assert quality_gate.score(_labelled(), lambda records: []) == 0


def test_bundled_samples_cover_every_category():
    assert {s["category"] for s in quality_gate.SAMPLES} == {
        "equipment_defect",
        "installation_workmanship",
        "design_issue",
        "controls_integration",
        "documentation",
    }


def test_handler_fails_deployment_below_threshold(monkeypatch):
    statuses = []

    class FakeCodeDeploy:
        def put_lifecycle_event_hook_execution_status(self, **kwargs):
            statuses.append(kwargs["status"])

    monkeypatch.setattr(quality_gate, "_codedeploy", lambda: FakeCodeDeploy())
    monkeypatch.setattr(quality_gate, "_invoke_target", lambda records: [])
    quality_gate.handler({"DeploymentId": "d-1", "LifecycleEventHookExecutionId": "h-1"}, None)
    assert statuses == ["Failed"]
