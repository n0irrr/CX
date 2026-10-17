import os

import aws_cdk as cdk

from infra.stack import CxPipelineStack

app = cdk.App()
CxPipelineStack(
    app,
    "CxPipeline",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION", "ap-southeast-1"),
    ),
)
app.synth()
