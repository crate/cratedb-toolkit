import logging
import typing as t

import boto3
from commons_codec.transform.dynamodb_model import PrimaryKeySchema
from yarl import URL

logger = logging.getLogger(__name__)


class DynamoDBAdapter:
    def __init__(self, dynamodb_url: URL):
        self.session = boto3.Session(
            aws_access_key_id=dynamodb_url.user,
            aws_secret_access_key=dynamodb_url.password,
            region_name=dynamodb_url.query.get("region"),
        )
        endpoint_url = None
        if dynamodb_url.host and dynamodb_url.host.lower() != "aws":
            endpoint_url = f"http://{dynamodb_url.host}:{dynamodb_url.port}"
        self.dynamodb_resource = self.session.resource("dynamodb", endpoint_url=endpoint_url)
        self.dynamodb_client = self.session.client("dynamodb", endpoint_url=endpoint_url)

    def scan(
        self,
        table_name: str,
        batch_size: int = 100,
        consistent_read: bool = False,
        on_error: t.Literal["log", "raise"] = "log",
    ) -> t.Generator[t.Dict, None, None]:
        """
        Fetch and generate all items from a DynamoDB table, with pagination.

        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Pagination
        """
        key = None
        while True:
            try:
                scan_kwargs = {"TableName": table_name, "ConsistentRead": consistent_read, "Limit": batch_size}
                if key is not None:
                    scan_kwargs.update({"ExclusiveStartKey": key})
                response = self.dynamodb_client.scan(**scan_kwargs)
                yield response
                key = response.get("LastEvaluatedKey", None)
                if key is None:
                    break
            except Exception as ex:
                if on_error == "log":
                    logger.exception("Error reading DynamoDB table")
                elif on_error == "raise":
                    raise
                else:
                    raise ValueError(f"Unknown 'on_error' value: {on_error}") from ex
                break

    def count_records(self, table_name: str):
        table = self.dynamodb_resource.Table(table_name)
        return table.item_count

    def primary_key_schema(self, table_name: str) -> PrimaryKeySchema:
        """
        Return primary key information for given table, derived from `KeySchema` [1] and `AttributeDefinition` [2].

        AttributeType:
        - S - the attribute is of type String
        - N - the attribute is of type Number
        - B - the attribute is of type Binary

        [1] https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#DDB-CreateTable-request-KeySchema
        [2] https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeDefinition.html
        """
        table = self.dynamodb_resource.Table(table_name)
        return PrimaryKeySchema.from_table(table)
