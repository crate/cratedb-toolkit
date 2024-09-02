import logging
import typing as t

import boto3
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
        page_size: int = 1000,
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
                scan_kwargs = {"TableName": table_name, "ConsistentRead": consistent_read, "Limit": page_size}
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
