import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

from infra.stack import CxPipelineStack


def _template(**context) -> Template:
    app = cdk.App(context={"budget_email": "test@example.com", **context})
    return Template.from_stack(CxPipelineStack(app, "Test", env=cdk.Environment(region="ap-southeast-1")))


def test_pipeline_resources():
    t = _template()
    for handler in ("validator.handler", "classifier.handler"):
        t.has_resource_properties(
            "AWS::Lambda::Function",
            {"Handler": handler, "Runtime": "python3.12", "DeadLetterConfig": Match.any_value()},
        )
    t.resource_count_is("AWS::Lambda::Alias", 2)
    t.resource_count_is("AWS::Glue::Table", 2)
    t.resource_count_is("AWS::Budgets::Budget", 1)


def test_canary_deployments_with_alarm_rollback():
    t = _template()
    t.resource_count_is("AWS::CodeDeploy::DeploymentGroup", 2)
    t.has_resource_properties(
        "AWS::CodeDeploy::DeploymentGroup",
        {
            "DeploymentConfigName": "CodeDeployDefault.LambdaCanary10Percent5Minutes",
            "AlarmConfiguration": {"Enabled": True, "Alarms": Match.any_value()},
            "AutoRollbackConfiguration": {
                "Enabled": True,
                "Events": Match.array_with(["DEPLOYMENT_STOP_ON_ALARM"]),
            },
        },
    )


def test_bucket_is_locked_down():
    _template().has_resource_properties(
        "AWS::S3::Bucket",
        {
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            }
        },
    )


def _bedrock_statements(t: Template) -> list:
    return [
        st
        for policy in t.find_resources("AWS::IAM::Policy").values()
        for st in policy["Properties"]["PolicyDocument"]["Statement"]
        if st["Action"] == "bedrock:InvokeModel"
    ]


def test_rules_only_mode_drops_bedrock_access():
    assert _bedrock_statements(_template())
    t = _template(llm_enabled="false")
    assert not _bedrock_statements(t)
    t.has_resource_properties(
        "AWS::Lambda::Function",
        {"Handler": "classifier.handler", "Environment": {"Variables": Match.object_like({"LLM_ENABLED": "false"})}},
    )
