from pathlib import Path

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_budgets as budgets
from aws_cdk import aws_cloudwatch as cw
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_codedeploy as codedeploy
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kinesis as kinesis
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as event_sources
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subs
from aws_cdk import aws_sqs as sqs
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
TELEMETRY_TABLES = {
    "telemetry": (
        "telemetry/",
        [
            ("reading_id", "string"),
            ("site", "string"),
            ("equipment_tag", "string"),
            ("point", "string"),
            ("value", "double"),
            ("unit", "string"),
            ("ts", "bigint"),
            ("in_spec", "boolean"),
            ("arrived_at", "bigint"),
            ("sequence_number", "string"),
        ],
    ),
    "telemetry_quarantine": (
        "telemetry-quarantine/",
        [("sequence_number", "string"), ("errors", "array<string>"), ("data", "string")],
    ),
}


class CxPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ctx = self.node.try_get_context
        model_id = ctx("model_id") or "global.amazon.nova-2-lite-v1:0"
        llm_enabled = str(ctx("llm_enabled") or "true").lower() == "true"

        self.alerts = sns.Topic(self, "Alerts", display_name="CX pipeline alerts")
        if alert_email := ctx("alert_email"):
            self.alerts.add_subscription(subs.EmailSubscription(alert_email))

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

        validator, validator_live, _ = self._function(
            "Validator",
            "validator",
            Duration.seconds(60),
            {"CLEAN_PREFIX": "clean/", "QUARANTINE_PREFIX": "quarantine/"},
        )
        classifier, classifier_live, classifier_deployment = self._function(
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
            self._quality_gate(classifier, classifier_deployment)
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

        db = self._catalog(bucket, TABLES)
        self._quarantine_alarm()
        if str(ctx("streaming") or "false").lower() == "true":
            self._telemetry(bucket)
            self._catalog(bucket, TELEMETRY_TABLES, db)

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
        dlq_alarm = cw.Alarm(
            self,
            f"{name}DeadLettersAlarm",
            alarm_description=f"{name} events failed all retries and are waiting in the dead-letter queue",
            metric=fn.dead_letter_queue.metric_approximate_number_of_messages_visible(period=Duration.minutes(5)),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        for a in (alarm, dlq_alarm):
            a.add_alarm_action(cw_actions.SnsAction(self.alerts))
        deployment = codedeploy.LambdaDeploymentGroup(
            self,
            f"{name}Deployment",
            alias=live,
            deployment_config=codedeploy.LambdaDeploymentConfig.CANARY_10_PERCENT_5_MINUTES,
            alarms=[alarm],
        )
        return fn, live, deployment

    def _quality_gate(self, target: lambda_.Function, deployment: codedeploy.LambdaDeploymentGroup) -> None:
        # Scores the new version on labelled notes before any traffic shifts; catches releases that
        # run without errors but classify badly, which the errors alarm cannot see.
        gate = lambda_.Function(
            self,
            "QualityGate",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset(str(SRC / "quality_gate")),
            handler="quality_gate.handler",
            timeout=Duration.minutes(3),
            memory_size=256,
            environment={"TARGET_FUNCTION_ARN": target.current_version.function_arn, "MIN_ACCURACY": "0.8"},
        )
        target.current_version.grant_invoke(gate)
        deployment.add_pre_hook(gate)

    def _quarantine_alarm(self) -> None:
        def metric(name: str) -> cw.Metric:
            return cw.Metric(
                namespace="CxPipeline",
                metric_name=name,
                dimensions_map={"Stage": "validator"},
                statistic="Sum",
                period=Duration.minutes(5),
            )

        rate = cw.MathExpression(
            expression="IF(valid + quarantined > 0, quarantined / (valid + quarantined), 0)",
            using_metrics={"valid": metric("valid"), "quarantined": metric("quarantined")},
            label="Quarantine rate",
            period=Duration.minutes(5),
        )
        cw.Alarm(
            self,
            "QuarantineRateAlarm",
            alarm_description="More than 5% of uploaded rows were quarantined in a 5-minute window",
            metric=rate,
            threshold=0.05,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(cw_actions.SnsAction(self.alerts))

    def _telemetry(self, bucket: s3.Bucket) -> None:
        # Opt-in (-c streaming=true): a provisioned shard bills by the hour whether or not data flows.
        stream = kinesis.Stream(
            self,
            "TelemetryStream",
            shard_count=1,
            retention_period=Duration.hours(24),
            encryption=kinesis.StreamEncryption.MANAGED,
        )
        state = dynamodb.Table(
            self,
            "SensorState",
            partition_key=dynamodb.Attribute(name="sensor", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        dlq = sqs.Queue(self, "TelemetryDeadLetters", retention_period=Duration.days(14), enforce_ssl=True)
        fn = lambda_.Function(
            self,
            "Telemetry",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset(str(SRC / "telemetry")),
            handler="telemetry.handler",
            timeout=Duration.seconds(60),
            memory_size=256,
            environment={"BUCKET": bucket.bucket_name, "STATE_TABLE": state.table_name, "EXCURSION_READINGS": "5"},
        )
        fn.add_event_source(
            event_sources.KinesisEventSource(
                stream,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=100,
                max_batching_window=Duration.seconds(1),
                report_batch_item_failures=True,
                bisect_batch_on_error=True,
                retry_attempts=10,
                max_record_age=Duration.hours(1),
                on_failure=event_sources.SqsDlq(dlq),
            )
        )
        for prefix in ("telemetry/*", "telemetry-quarantine/*", "raw/telemetry-*"):
            bucket.grant_put(fn, prefix)
        state.grant_read_write_data(fn)

        alarms = [
            cw.Alarm(
                self,
                "TelemetryIteratorAgeAlarm",
                alarm_description="Telemetry consumer is more than a minute behind the stream",
                metric=fn.metric("IteratorAge", statistic="Maximum", period=Duration.minutes(1)),
                threshold=60_000,
                evaluation_periods=3,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            ),
            cw.Alarm(
                self,
                "TelemetryDeadLettersAlarm",
                alarm_description="Telemetry batches exhausted their retries and are waiting in the dead-letter queue",
                metric=dlq.metric_approximate_number_of_messages_visible(period=Duration.minutes(5)),
                threshold=1,
                evaluation_periods=1,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            ),
        ]
        for a in alarms:
            a.add_alarm_action(cw_actions.SnsAction(self.alerts))

        CfnOutput(self, "TelemetryStreamName", value=stream.stream_name)
        CfnOutput(self, "TelemetryFunctionName", value=fn.function_name)

    def _catalog(self, bucket: s3.Bucket, tables: dict, db: glue.CfnDatabase | None = None) -> glue.CfnDatabase:
        db = db or glue.CfnDatabase(
            self,
            "Database",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(name=DATABASE),
        )
        for name, (prefix, columns) in tables.items():
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
        return db

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
