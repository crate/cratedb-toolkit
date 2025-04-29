# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.

# ruff: noqa: S608
import json
import logging
import os
import time
import typing as t
from uuid import uuid4

import urllib3
from crate import client
from queryanonymizer import anonymize, deanonymize

from cratedb_toolkit.model import DatabaseAddress

logger = logging.getLogger(__name__)

TRACING = False


last_execution_ts = 0
sys_jobs_log = {}
bucket_list = [10, 50, 100, 500, 1000, 2000, 5000, 10000, 15000, 20000]
bucket_dict = {
    "10": 0,
    "50": 0,
    "100": 0,
    "500": 0,
    "1000": 0,
    "2000": 0,
    "5000": 0,
    "10000": 0,
    "15000": 0,
    "20000": 0,
    "INF": 0,
}

stmt_log_table: str
last_exec_table: str
cursor: t.Any
report_cursor: t.Any
last_scrape: int
interval: float
anonymize_sql: bool = False
deanonymize_sql: bool = False  # Added global flag for deanonymization
decoder_dict_path: str


def boot(
    address: DatabaseAddress,
    report_address: t.Optional[DatabaseAddress] = None,
    anonymize_statements: bool = False,
    decoder_dict_file: t.Optional[str] = None,
    deanonymize_statements: bool = False,
):
    # TODO: Refactor to non-global variables.
    global \
        stmt_log_table, \
        last_exec_table, \
        cursor, \
        report_cursor, \
        last_scrape, \
        interval, \
        anonymize_sql, \
        deanonymize_sql, \
        decoder_dict_path
    anonymize_sql = anonymize_statements
    deanonymize_sql = deanonymize_statements

    if (anonymize_sql or deanonymize_sql) and decoder_dict_file is None:
        raise ValueError("Decoder dictionary file is required when anonymization or deanonymization is enabled")

    schema = address.schema or "stats"

    if anonymize_sql and decoder_dict_file:
        decoder_dict_path = decoder_dict_file
        logger.info(f"SQL anonymization is enabled, using dictionary: {decoder_dict_path}")

    if deanonymize_sql and decoder_dict_file:
        decoder_dict_path = decoder_dict_file
        logger.info(f"SQL deanonymization is enabled, using dictionary: {decoder_dict_path}")

    interval = float(os.getenv("INTERVAL", 10))
    stmt_log_table = os.getenv("STMT_TABLE", f'"{schema}".jobstats_statements')
    last_exec_table = os.getenv("LAST_EXEC_TABLE", f'"{schema}".jobstats_last')

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    logger.info(f"Connecting to {address.httpuri}")
    conn = client.connect(
        address.httpuri,
        username=address.username,
        password=address.password,
        schema=schema,
        verify_ssl_cert=address.verify_ssl,
    )
    cursor = conn.cursor()

    if report_address:
        report_schema = report_address.schema or "stats"
        # Override the table names to use the report schema
        stmt_log_table = os.getenv("STMT_TABLE", f'"{report_schema}".jobstats_statements')
        last_exec_table = os.getenv("LAST_EXEC_TABLE", f'"{report_schema}".jobstats_last')

        logger.info(f"Using separate report database at {report_address.httpuri}")
        report_conn = client.connect(
            report_address.httpuri,
            username=report_address.username,
            password=report_address.password,
            schema=report_schema,
            verify_ssl_cert=report_address.verify_ssl,
        )
        report_cursor = report_conn.cursor()
    else:
        # If no separate reporting DB, use the same cursor for both
        report_cursor = cursor

    last_scrape = int(time.time() * 1000) - int(interval * 60000)

    dbinit()


def anonymize_statement(statement: str) -> str:
    """Anonymize SQL statement using queryanonymizer."""
    if anonymize_sql and anonymize is not None:
        try:
            # Load dictionary file each time
            encoder_dict = {}
            try:
                with open(decoder_dict_path, "r") as f:
                    encoder_dict = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError) as e:
                logger.warning(f"Could not load encoder dictionary: {e}")

            # Call anonymize and extract only the anonymized statement (first item)
            result = anonymize(
                query=statement,
                keywords_group="SQL",
                anonymize_strings_inside_square_brackets=True,
                anonymize_strings_inside_apostrophes=True,
                anonymize_strings_inside_quotation_marks=True,
                path_to_decoder_dictionary_file=decoder_dict_path,
                custom_encoder_dictionary=encoder_dict,
            )
            # Return only the anonymized statement string
            if isinstance(result, tuple) and len(result) > 0:
                return result[0]
            return result
        except Exception as e:
            logger.warning(f"Failed to anonymize statement: {e}")
    return statement


def deanonymize_statement(statement: str) -> str:
    """Deanonymize SQL statement using queryanonymizer."""
    if deanonymize_sql and decoder_dict_path:
        try:
            with open(decoder_dict_path, "r") as f:
                _ = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"Could not load decoder dictionary: {e}")

        # Call anonymize to decode the statement
        result = deanonymize(
            statement,
            path_to_decoder_dictionary_file=decoder_dict_path,
        )

        # Return only the deanonymized statement string
        if isinstance(result, tuple) and len(result) > 0:
            return result[0]
        return result
    return statement  # Return original statement if not deanonymizing


def dbinit():
    stmt = (
        f"CREATE TABLE IF NOT EXISTS {stmt_log_table} "
        f"(id TEXT, stmt TEXT, calls INT, bucket OBJECT, last_used TIMESTAMP, "
        f"username TEXT, query_type TEXT, avg_duration FLOAT, nodes ARRAY(TEXT))"
    )
    report_cursor.execute(stmt)
    stmt = f"SELECT id, stmt, calls, bucket, username, query_type, avg_duration, nodes, last_used FROM {stmt_log_table}"
    report_cursor.execute(stmt)
    init_stmts(report_cursor.fetchall())
    stmt = f"CREATE TABLE IF NOT EXISTS {last_exec_table} (last_execution TIMESTAMP)"
    report_cursor.execute(stmt)
    stmt = f"SELECT last_execution FROM {last_exec_table}"
    report_cursor.execute(stmt)
    init_last_execution(report_cursor.fetchall())


def init_last_execution(last_execution):
    global last_execution_ts
    if len(last_execution) == 0:
        last_execution_ts = 0
        stmt = f"INSERT INTO {last_exec_table} (last_execution) VALUES (?)"
        report_cursor.execute(stmt, (0,))
    else:
        last_execution_ts = last_execution[0][0]


def init_stmts(stmts):
    for stmt in stmts:
        stmt_id = stmt[0]
        stmt_column = stmt[1]
        calls = stmt[2]
        bucket = stmt[3]
        user = stmt[4]
        stmt_type = stmt[5]
        avg_duration = stmt[6]
        nodes = stmt[7]
        last_used = stmt[8]

        if stmt_column not in sys_jobs_log:
            sys_jobs_log[stmt_column] = {
                "id": stmt_id,
                "size": 0,
                "info": [],
                "calls": calls,
                "bucket": bucket,
                "user": user,
                "type": stmt_type,
                "avg_duration": avg_duration,
                "nodes": nodes,
                "last_used": last_used,
                "in_db": True,
                "changed": False,
            }


def write_stats_to_db():
    logger.info(f"Writing statistics to database table: {stmt_log_table}")
    write_query_stmt = (
        f"INSERT INTO {stmt_log_table} "
        f"(id, stmt, calls, bucket, username, query_type, avg_duration, nodes, last_used) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    update_query_stmt = (
        f"UPDATE {stmt_log_table} SET calls = ?, avg_duration = ?, nodes = ?, bucket = ?, last_used = ? WHERE id = ?"
    )
    write_params = []
    for key in sys_jobs_log.keys():
        if not sys_jobs_log[key]["in_db"]:
            write_params.append(
                [
                    sys_jobs_log[key]["id"],
                    key,
                    sys_jobs_log[key]["calls"],
                    sys_jobs_log[key]["bucket"],
                    sys_jobs_log[key]["user"],
                    sys_jobs_log[key]["type"],
                    sys_jobs_log[key]["avg_duration"],
                    sys_jobs_log[key]["nodes"],
                    sys_jobs_log[key]["last_used"],
                ]
            )
            sys_jobs_log[key]["in_db"] = True
            sys_jobs_log[key]["changed"] = False
        elif sys_jobs_log[key]["changed"]:
            report_cursor.execute(
                update_query_stmt,
                (
                    sys_jobs_log[key]["calls"],
                    sys_jobs_log[key]["avg_duration"],
                    sys_jobs_log[key]["nodes"],
                    sys_jobs_log[key]["bucket"],
                    sys_jobs_log[key]["last_used"],
                    sys_jobs_log[key]["id"],
                ),
            )
            sys_jobs_log[key]["changed"] = False
    if len(write_params) > 0:
        report_cursor.executemany(write_query_stmt, write_params)

    stmt = f"UPDATE {last_exec_table} SET last_execution = ?"
    report_cursor.execute(stmt, (last_scrape,))


def read_stats():
    stmt = (
        f"SELECT id, stmt, calls, avg_duration, bucket, username, query_type, nodes, last_used "
        f"FROM {stmt_log_table} ORDER BY calls DESC, avg_duration DESC;"
    )
    report_cursor.execute(stmt)
    results = report_cursor.fetchall()

    # Deanonymize statements if needed
    if deanonymize_sql and decoder_dict_path:
        deanonymized_results = []
        for row in results:
            row_list = list(row)
            if row_list[1]:  # Check if stmt (at index 1) exists
                row_list[1] = deanonymize_statement(row_list[1])
            deanonymized_results.append(tuple(row_list))
        results = deanonymized_results

    init_stmts(results)
    return sys_jobs_log


def assign_to_bucket(bucket, duration):
    found = False
    for element in bucket_list:
        if duration < element:
            found = True
            bucket[str(element)] += 1
            break
    if not found:
        bucket["INF"] += 1

    return bucket


def update_statistics(query_results):
    global sys_jobs_log
    for result in query_results:
        started = result[0]
        ended = result[1]
        classification = result[2]
        stmt = result[3]
        user = result[4]
        node = json.dumps(result[5])

        # Anonymize the statement if requested
        stmt = anonymize_statement(stmt)

        duration = ended - started
        if stmt not in sys_jobs_log:
            sys_jobs_log[stmt] = {
                "id": str(uuid4()),
                "calls": 0,
                "bucket": dict(bucket_dict),
                "user": user,
                "type": classification["type"],
                "avg_duration": duration,
                "in_db": False,
                "last_used": started,
                "nodes": [],
                "changed": True,
            }
        sys_jobs_log[stmt]["changed"] = True
        sys_jobs_log[stmt]["avg_duration"] = (sys_jobs_log[stmt]["avg_duration"] + duration) / 2
        sys_jobs_log[stmt]["bucket"] = assign_to_bucket(sys_jobs_log[stmt]["bucket"], duration)
        sys_jobs_log[stmt]["last_used"] = started
        sys_jobs_log[stmt]["calls"] += 1
        sys_jobs_log[stmt]["nodes"].append(node)
        sys_jobs_log[stmt]["nodes"] = list(set(sys_jobs_log[stmt]["nodes"]))  # only save unique nodes
    if TRACING:
        logger.info(f"Updated statistics: {sys_jobs_log}")


def scrape_db():
    global last_scrape
    logger.info("Reading sys.jobs_log")
    next_scrape = int(time.time() * 1000)
    stmt = (
        f"SELECT "
        f"started, ended, classification, stmt, username, node "
        f"FROM sys.jobs_log "
        f"WHERE "
        f"stmt NOT LIKE '%sys.%' AND "
        f"stmt NOT LIKE '%information_schema.%' "
        f"AND ended BETWEEN {last_scrape} AND {next_scrape} "
        f"ORDER BY ended DESC"
    )

    cursor.execute(stmt)
    result = cursor.fetchall()
    update_statistics(result)
    last_scrape = next_scrape


def record_once():
    logger.info("Recording information snapshot")
    scrape_db()
    write_stats_to_db()


def record_forever():
    while True:
        record_once()
        logger.info(f"Sleeping for {interval} seconds")
        time.sleep(interval)


def main():
    boot(address=DatabaseAddress.from_string("http://crate@localhost:4200"))
    record_forever()


if __name__ == "__main__":
    main()
