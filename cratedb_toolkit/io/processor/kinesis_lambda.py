# Copyright (c) 2021-2024, Crate.io Inc.
# Distributed under the terms of the Apache 2 license.
"""
Consume an AWS Kinesis Stream and relay into CrateDB.
https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis-example.html
https://docs.aws.amazon.com/lambda/latest/dg/python-logging.html
https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis-example.html#with-kinesis-example-create-function

In order to run, this module/program needs the following 3rd party
libraries, defined using inline script metadata.
"""

# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "commons-codec==0.0.2",
#   "sqlalchemy-cratedb==0.38.0",
# ]
# ///
import base64
import json
import logging
import os
import sys
import typing as t

import sqlalchemy as sa
from commons_codec.transform.dynamodb import DynamoCDCTranslatorCrateDB

logger = logging.getLogger(__name__)

# TODO: Control using environment variable.
logger.setLevel("INFO")

# TODO: Control using environment variables.
USE_BATCH_PROCESSING: bool = False
ON_ERROR: t.Literal["exit", "noop", "raise"] = "exit"

# TODO: Control `echo` using environment variable.
engine = sa.create_engine(os.environ.get("CRATEDB_SQLALCHEMY_URL", "crate://"), echo=True)

# TODO: Automatically create destination table? How?
cdc = DynamoCDCTranslatorCrateDB(table_name=os.environ.get("CRATEDB_TABLE", "default"))


def handler(event, context):
    """
    Implement partial batch response for Lambda functions that receive events from
    a Kinesis stream. The function reports the batch item failures in the response,
    signaling to Lambda to retry those messages later.
    """

    cur_record_sequence_number = ""
    logger.info("context: %s", context)

    for record in event["Records"]:
        try:
            # Log and decode event.
            # TODO: Remove log statements.
            logger.info(f"Processed Kinesis Event - EventID: {record['eventID']}")
            logger.info(f"Event Data: {record}")
            record_data = json.loads(base64.b64decode(record["kinesis"]["data"]).decode("utf-8"))
            logger.info(f"Record Data: {record_data}")

            # Process record.
            sql = cdc.to_sql(record_data)
            run_sql(sql)

            # Bookkeeping.
            cur_record_sequence_number = record["kinesis"]["sequenceNumber"]

        except Exception as ex:
            error_message = "An error occurred"
            logger.exception(error_message)
            if USE_BATCH_PROCESSING:
                # Return failed record's sequence number.
                return {"batchItemFailures": [{"itemIdentifier": cur_record_sequence_number}]}
            if ON_ERROR == "exit":
                sys.exit(6)
            if ON_ERROR == "raise":
                raise ex

    logger.info(f"Successfully processed {len(event['Records'])} records.")
    if USE_BATCH_PROCESSING:
        return {"batchItemFailures": []}
    return None


def run_sql(sql: str):
    """
    Execute an SQL statement.

    TODO: Optimize performance.
    """
    with engine.connect() as connection:
        connection.execute(sa.text(sql))
