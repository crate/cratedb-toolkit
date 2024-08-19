import boto3
from yarl import URL


class DynamoDBAdapter:
    def __init__(self, dynamodb_url: URL, echo: bool = False):
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

    def scan(self, table_name: str):
        """
        Return all items from DynamoDB table.
        """
        return self.dynamodb_client.scan(TableName=table_name)

    def count_records(self, table_name: str):
        table = self.dynamodb_resource.Table(table_name)
        return table.item_count
