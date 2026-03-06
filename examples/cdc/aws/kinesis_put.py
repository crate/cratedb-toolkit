import base64
import json

from yarl import URL

from cratedb_toolkit.io.kinesis.adapter import KinesisStreamAdapter


def main():

    # Address of the Kinesis stream.
    kinesis_url = URL("kinesis://LSIAQAAAAAAVNCBMPNSG:dummy@localhost:4566/cdc-stream?region=eu-central-1")

    # DynamoDB CDC data payload example.
    data = {
        "awsRegion": "us-east-1",
        "eventID": "b581c2dc-9d97-44ed-94f7-cb77e4fdb740",
        "eventName": "INSERT",
        "userIdentity": None,
        "recordFormat": "application/json",
        "tableName": "testdrive",
        "dynamodb": {
            "ApproximateCreationDateTime": 1720800199717446,
            "Keys": {"id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"}},
            "NewImage": {
                "id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"},
                "data": {"M": {"temperature": {"N": "42.42"}, "humidity": {"N": "84.84"}}},
                "meta": {"M": {"timestamp": {"S": "2024-07-12T01:17:42"}, "device": {"S": "foo"}}},
            },
            "SizeBytes": 156,
            "ApproximateCreationDateTimePrecision": "MICROSECOND",
        },
        "eventSource": "aws:dynamodb",
    }

    # Payload wrapped in Kinesis event structure, as delivered to Lambda consumers by AWS.
    payload = {
        "eventID": "shardId-000000000006:49590338271490256608559692538361571095921575989136588898",
        "kinesis": {
            "sequenceNumber": "49590338271490256608559692538361571095921575989136588898",
            "data": base64.b64encode(json.dumps(data).encode("utf-8")).decode(),
        },
    }

    kinesis = KinesisStreamAdapter(kinesis_url)
    kinesis.produce(payload)


if __name__ == "__main__":
    main()
