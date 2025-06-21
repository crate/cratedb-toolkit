import logging
import os
from pathlib import Path

from lorrystream.util.common import setup_logging

from cratedb_toolkit.iac.aws import DynamoDBKinesisPipe, LambdaFactory, LambdaPythonImage

logger = logging.getLogger(__name__)


def main():
    """
    A recipe to deploy a data relay stack to Amazon AWS.

    Pipeline:
    - DynamoDB CDC -> Kinesis Stream -> Python Lambda via OCI -> CrateDB

    Ingredients:
    - DynamoDB CDC to Kinesis
    - Lambda function, shipped per OCI image
    - CrateDB Cloud

    Prerequisites: Register an OCI repository.
    """

    # Build and publish OCI image that includes the AWS Lambda function.
    python_image = LambdaPythonImage(
        name="kinesis-cratedb-processor-lambda",
        entrypoint_file=Path("./cratedb_toolkit/io/processor/kinesis_lambda.py"),
        entrypoint_handler="kinesis_lambda.handler",
    )
    python_image.publish()

    # Define an AWS CloudFormation software stack.
    stack = DynamoDBKinesisPipe(
        project="testdrive-dynamodb",
        stage="dev",
        region="eu-central-1",
        description="DynamoDB CDC -> Kinesis Stream -> Python Lambda via OCI -> CrateDB",
        table_name="demo-source",
        stream_name="dynamodb-cdc",
        environment={
            "CRATEDB_CLUSTER_URL": os.environ.get("CRATEDB_CLUSTER_URL", "crate://"),
            "CRATEDB_TABLE": "demo-sink",
        },
    )

    # Add components to the stack.
    stack.table().processor(
        LambdaFactory(
            name="DynamoDBCrateDBProcessor",
            oci_uri=python_image.uri,
            handler=python_image.entrypoint_handler,
        )
    ).connect()

    # Deploy stack.
    stack.deploy()
    logger.info(f"Deployed stack: {stack}")

    # Refresh the OCI image.
    # TODO: Detect when changed.
    stack.deploy_processor_image()


if __name__ == "__main__":
    setup_logging()
    main()
