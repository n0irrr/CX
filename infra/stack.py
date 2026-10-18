from pathlib import Path

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_budgets as budgets
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_codedeploy as codedeploy
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from constructs import Construct

SRC = Path(__file__).resolve().parent.parent / "src"
DATABASE = "cx_issues"
WORKGROUP = "cx-pipeline"

RECORD_COLUMNS = [
    "issue_id",
    "site",
    "building",
    "equipment_type",
    "equipment_tag",
    "manufacturer",
    "contractor",
    "cx_level",
    "severity",
    "status",
    "opened_at",
    "closed_at",
    "notes",
    "source_key",
]
TABLES = {
    "issues": ("enriched/", [(c, "string") for c in RECORD_COLUMNS + ["category", "classified_by"]]),
    "quarantine": ("quarantine/", [("line_no", "int"), ("errors", "array<string>"), ("source_key", "string")]),
}


class CxPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ctx = self.node.try_get_context
        model_id = ctx("model_id") or "global.amazon.nova-2-lite-v1:0"
        llm_enabled = str(ctx("llm_enabled") or "true").lower() == "true"

        bucket = s3.Bucket(
            self,
            "DataBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[s3.LifecycleRule(prefix="athena-results/", expiration=Duration.days(7))],
        )

        validator, validator_live = self._function(
            "Validator",
            "validator",
            Duration.seconds(60),
            {"CLEAN_PREFIX": "clean/", "QUARANTINE_PREFIX": "quarantine/"},
        )
        classifier, classifier_live = self._function(
            "Classifier",
            "classifier",
            Duration.minutes(5),
            {
                "MODEL_ID": model_id,
                "LLM_ENABLED": str(llm_enabled).lower(),
                "BATCH_SIZE": "20",
                "CLEAN_PREFIX": "clean/",
                "ENRICHED_PREFIX": "enriched/",
            },
        )

        bucket.grant_read(validator, "raw/*")
        bucket.grant_put(validator, "clean/*")
        bucket.grant_put(validator, "quarantine/*")
        bucket.grant_read(classifier, "clean/*")
        bucket.grant_put(classifier, "enriched/*")
        if llm_enabled:
            classifier.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["bedrock:InvokeModel"],
                    resources=[
                        "arn:aws:bedrock:*::foundation-model/*",
                        f"arn:aws:bedrock:*:{self.account}:inference-profile/*",
                    ],
                )
            )

        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(validator_live),
            s3.NotificationKeyFilter(prefix="raw/", suffix=".csv"),
        )
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(classifier_live),
            s3.NotificationKeyFilter(prefix="clean/", suffix=".jsonl"),
        )

        self._catalog(bucket)

        athena.CfnWorkGroup(
            self,
            "Workgroup",
            name=WORKGROUP,
            recursive_delete_option=True,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                bytes_scanned_cutoff_per_query=1_000_000_000,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{bucket.bucket_name}/athena-results/"
                ),
            ),
        )

        if email := ctx("budget_email"):
            self._budget(email, float(ctx("budget_usd") or 10))

        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(self, "AthenaDatabase", value=DATABASE)
        CfnOutput(self, "AthenaWorkgroup", value=WORKGROUP)

    def _function(self, name: str, folder: str, timeout: Duration, env: dict):
        fn = lambda_.Function(
            self,
            name,
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset(str(SRC / folder)),
            handler=f"{folder}.handler",
            timeout=timeout,
            memory_size=256,
            environment=env,
            retry_attempts=2,
            dead_letter_queue_enabled=True,
        )
        live = lambda_.Alias(self, f"{name}Live", alias_name="live", version=fn.current_version)
        alarm = cw.Alarm(
            self,
            f"{name}ErrorsAlarm",
            metric=live.metric_errors(period=Duration.minutes(1)),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        codedeploy.LambdaDeploymentGroup(
            self,
            f"{name}Deployment",
            alias=live,
            deployment_config=codedeploy.LambdaDeploymentConfig.CANARY_10_PERCENT_5_MINUTES,
            alarms=[alarm],
        )
        return fn, live

    def _catalog(self, bucket: s3.Bucket) -> None:
        db = glue.CfnDatabase(
            self,
            "Database",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(name=DATABASE),
        )
        for name, (prefix, columns) in TABLES.items():
            location = f"s3://{bucket.bucket_name}/{prefix}"
            table = glue.CfnTable(
                self,
                f"{name.title()}Table",
                catalog_id=self.account,
                database_name=DATABASE,
                table_input=glue.CfnTable.TableInputProperty(
                    name=name,
                    table_type="EXTERNAL_TABLE",
                    parameters={
                        "classification": "json",
                        "projection.enabled": "true",
                        "projection.ingest_date.type": "date",
                        "projection.ingest_date.format": "yyyy-MM-dd",
                        "projection.ingest_date.range": "2026-01-01,NOW",
                        "projection.ingest_date.interval": "1",
                        "projection.ingest_date.interval.unit": "DAYS",
                        "storage.location.template": f"{location}ingest_date=${{ingest_date}}/",
                    },
                    partition_keys=[glue.CfnTable.ColumnProperty(name="ingest_date", type="string")],
                    storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                        location=location,
                        columns=[glue.CfnTable.ColumnProperty(name=c, type=t) for c, t in columns],
                        input_format="org.apache.hadoop.mapred.TextInputFormat",
                        output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                        serde_info=glue.CfnTable.SerdeInfoProperty(
                            serialization_library="org.openx.data.jsonserde.JsonSerDe"
                        ),
                    ),
                ),
            )
            table.add_resource_dependency(db)

    def _budget(self, email: str, limit_usd: float) -> None:
        budgets.CfnBudget(
            self,
            "MonthlyBudget",
            budget=budgets.CfnBudget.BudgetDataProperty(
                budget_type="COST",
                time_unit="MONTHLY",
                budget_limit=budgets.CfnBudget.SpendProperty(amount=limit_usd, unit="USD"),
            ),
            notifications_with_subscribers=[
                budgets.CfnBudget.NotificationWithSubscribersProperty(
                    notification=budgets.CfnBudget.NotificationProperty(
                        notification_type="ACTUAL",
                        comparison_operator="GREATER_THAN",
                        threshold=80,
                    ),
                    subscribers=[budgets.CfnBudget.SubscriberProperty(subscription_type="EMAIL", address=email)],
                )
            ],
        )
