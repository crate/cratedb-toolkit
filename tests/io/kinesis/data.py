"""
A few basic DMS events captured from RDS PostgreSQL via Kinesis.
"""

DMS_CDC_CREATE_TABLE = {
    "kinesis": {
        "shard_id": "shardId-000000000002",
        "seq_no": "49663978758868311932389618382582776745412103230134943778",
        "ts": "2025-06-10 11:28:47.198000+00:00",
        "partition": "DZ6OKLFYA5BWVG4RYROQUHXU4E.public.demo",
        "stream_name": "arn:aws:kinesis:eu-central-1:931394475905:stream/testdrive-dms-postgresql-dev-stream",
    },
    "kinesis_msg_id": "Vqn8XKNBuM6QmccUo0I3",
    "control": {
        "table-def": {
            "columns": {
                "id": {"type": "INT32", "nullable": False},
                "name": {"type": "STRING", "nullable": True},
                "age": {"type": "INT32", "nullable": True},
                "attributes": {"type": "STRING", "nullable": True},
            },
            "primary-key": ["id"],
        }
    },
    "metadata": {
        "timestamp": "2025-06-10T11:28:47.195247Z",
        "record-type": "control",
        "operation": "create-table",
        "partition-key-type": "task-id",
        "partition-key-value": "DZ6OKLFYA5BWVG4RYROQUHXU4E",
        "schema-name": "public",
        "table-name": "demo",
    },
}
DMS_CDC_INSERT_BASIC = {
    "kinesis": {
        "shard_id": "shardId-000000000002",
        "seq_no": "49663978758868311932389618382594866003608249521882005538",
        "ts": "2025-06-10 11:28:47.329000+00:00",
        "partition": "public.demo.393",
        "stream_name": "arn:aws:kinesis:eu-central-1:931394475905:stream/testdrive-dms-postgresql-dev-stream",
    },
    "kinesis_msg_id": "h7FftCDO9ZvbFP/wCWKv",
    "data": {"id": 393, "name": "Test", "age": 4, "attributes": None},
    "metadata": {
        "timestamp": "2025-06-10T11:28:47.326434Z",
        "record-type": "data",
        "operation": "load",
        "partition-key-type": "primary-key",
        "partition-key-value": "public.demo.393",
        "schema-name": "public",
        "table-name": "demo",
    },
}
