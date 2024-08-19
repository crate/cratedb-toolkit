import logging

from cratedb_toolkit.io.dynamodb.copy import DynamoDBFullLoad

logger = logging.getLogger(__name__)


def dynamodb_copy(source_url, target_url, progress: bool = False):
    """

    Synopsis
    --------
    export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
    ctk load table dynamodb://AWS_ACCESS_KEY:AWS_SECRET_ACCESS_KEY@localhost:4566/us-east-1/ProductCatalog
    ctk load table dynamodb://AWS_ACCESS_KEY:AWS_SECRET_ACCESS_KEY@localhost:4566/arn:aws:dynamodb:us-east-1:000000000000:table/ProductCatalog

    ctk load table dynamodb://arn:aws:dynamodb:us-east-1:000000000000:table/ProductCatalog
                              arn:aws:dynamodb:us-east-1:841394475918:table/stream-demo

    ctk load table dynamodb://LSIAQAAAAAAVNCBMPNSG:dummy@localhost:4566/ProductCatalog?region=eu-central-1

    Resources
    ---------
    https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/programming-with-python.html

    Backlog
    -------
    Currently, it is not directly possible to address DynamoDB tables by ARN, i.e. for using a different AccountID.
    - https://github.com/boto/boto3/issues/2658
    - https://stackoverflow.com/questions/71019941/how-to-point-to-the-arn-of-a-dynamodb-table-instead-of-using-the-name-when-using
    - https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/configure-cross-account-access-to-amazon-dynamodb.html
    """
    logger.info("Invoking DynamoDBFullLoad")
    ddb_full = DynamoDBFullLoad(
        dynamodb_url=source_url,
        cratedb_url=target_url,
        progress=progress,
    )
    ddb_full.start()
    return True
