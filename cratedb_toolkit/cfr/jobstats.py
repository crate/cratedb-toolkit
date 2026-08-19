# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.

# ruff: noqa: S608
import hashlib
import io
import json
import logging
import os
import time
import typing as t
from contextlib import redirect_stdout
from uuid import uuid4

import urllib3
from crate import client
from queryanonymizer import anonymize, deanonymize

from cratedb_toolkit.model import DatabaseAddress

logger = logging.getLogger(__name__)

TRACING = False

# How far back to look for jobs when no watermark has been recorded yet, in seconds.
INITIAL_LOOKBACK_SECONDS = 600

last_execution_ts = 0
sys_jobs_log: t.Dict[str, t.Dict[str, t.Any]] = {}
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

# All of those are assigned by `boot()`.
stmt_log_table: str = ""
last_exec_table: str = ""
cursor: t.Any = None
report_cursor: t.Any = None
last_scrape: int = 0
interval: float = 10.0
anonymize_sql: bool = False
deanonymize_sql: bool = False  # Added global flag for deanonymization
decoder_dict_path: str = ""


def reset_state():
    """
    Discard all accumulated in-memory statistics.

    The collector keeps its statistics in module-global state. Without resetting it,
    a second `boot()` within the same process starts off with statements flagged as
    already stored (`in_db=True`), which makes `write_stats_to_db` issue an `UPDATE`
    against a table that has no such row, silently dropping the statistics.
    """
    global last_execution_ts
    last_execution_ts = 0
    sys_jobs_log.clear()


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
    reset_state()
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
    initial_lookback = float(os.getenv("INITIAL_LOOKBACK_SECONDS", INITIAL_LOOKBACK_SECONDS))
    stmt_log_table = os.getenv("STMT_TABLE", f'"{schema}".jobstats_statements')
    last_exec_table = os.getenv("LAST_EXEC_TABLE", f'"{schema}".jobstats_last')

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    logger.info(f"Connecting to {address.safe}")
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

    dbinit()

    # Resume from the recorded watermark, so a restarted collector neither re-counts jobs
    # it has already processed, nor misses jobs which ran while it was not running.
    if isinstance(last_execution_ts, (int, float)) and last_execution_ts > 0:
        last_scrape = int(last_execution_ts)
        logger.info(f"Resuming from recorded watermark: {last_scrape}")
    else:
        last_scrape = int(time.time() * 1000) - int(initial_lookback * 1000)
        logger.info(f"No watermark recorded yet, looking back {initial_lookback} seconds")


def redacted_statement(statement: str) -> str:
    """
    Return a placeholder for a statement which could not be anonymized.
    """
    digest = hashlib.sha256(statement.encode("utf-8")).hexdigest()[:16]
    return f"<redacted: {digest}>"


def anonymize_statement(statement: str) -> str:
    """
    Anonymize SQL statement using queryanonymizer.

    When anonymization fails, the statement is redacted rather than stored in clear text.
    """
    if not anonymize_sql:
        return statement
    try:
        # Load dictionary file each time
        encoder_dict = {}
        try:
            with open(decoder_dict_path, "r") as f:
                encoder_dict = json.load(f)
        except FileNotFoundError:
            logger.info(f"No decoder dictionary found yet, creating a new one: {decoder_dict_path}")
        except json.JSONDecodeError as e:
            logger.warning(f"Could not load encoder dictionary, continuing without it: {e}")

        # Call anonymize and extract only the anonymized statement (first item).
        with redirect_stdout(io.StringIO()):
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
        logger.warning(f"Failed to anonymize statement, redacting it instead: {e}")
        return redacted_statement(statement)


def deanonymize_statement(statement: str) -> str:
    """Deanonymize SQL statement using queryanonymizer."""
    if deanonymize_sql and decoder_dict_path:
        try:
            with open(decoder_dict_path, "r") as f:
                _ = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"Could not load decoder dictionary: {e}")

        # Call anonymize to decode the statement, again discarding its output on stdout.
        try:
            with redirect_stdout(io.StringIO()):
                result = deanonymize(
                    statement,
                    path_to_decoder_dictionary_file=decoder_dict_path,
                )
        except Exception as e:
            logger.warning(f"Failed to deanonymize statement: {e}")
            return statement

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
    # Refresh before reading, so statistics written by a previous run are seen.
    report_cursor.execute(f"REFRESH TABLE {stmt_log_table}")
    stmt = f"SELECT id, stmt, calls, bucket, username, query_type, avg_duration, nodes, last_used FROM {stmt_log_table}"
    report_cursor.execute(stmt)
    init_stmts(fetch_records(report_cursor))
    stmt = f"CREATE TABLE IF NOT EXISTS {last_exec_table} (last_execution TIMESTAMP)"
    report_cursor.execute(stmt)
    report_cursor.execute(f"REFRESH TABLE {last_exec_table}")
    # Aggregate, so the outcome does not depend on the order records are returned in.
    # The table can hold more than one record, for example after an interrupted startup.
    stmt = f"SELECT MAX(last_execution) AS last_execution, COUNT(*) AS record_count FROM {last_exec_table}"
    report_cursor.execute(stmt)
    init_last_execution(fetch_records(report_cursor)[0])


def fetch_records(db_cursor) -> t.List[t.Dict[str, t.Any]]:
    """
    Return the result of the most recent query as records, keyed by column name.
    """
    column_names = [column[0] for column in db_cursor.description]
    return [dict(zip(column_names, row)) for row in db_cursor.fetchall()]


def init_last_execution(watermark: t.Dict[str, t.Any]):
    """
    Adopt the recorded watermark, and create it when it does not exist yet.
    """
    global last_execution_ts
    last_execution_ts = watermark.get("last_execution") or 0
    if not watermark.get("record_count"):
        stmt = f"INSERT INTO {last_exec_table} (last_execution) VALUES (?)"
        report_cursor.execute(stmt, (last_execution_ts,))
        # Refresh, so the `UPDATE` of `write_stats_to_db` finds the record just inserted.
        report_cursor.execute(f"REFRESH TABLE {last_exec_table}")


def init_stmts(records: t.Iterable[t.Dict[str, t.Any]]):
    """
    Adopt statistics read from the database, without clobbering accumulated ones.
    """
    for record in records:
        stmt_column = record["stmt"]
        if stmt_column not in sys_jobs_log:
            sys_jobs_log[stmt_column] = {
                "id": record["id"],
                "size": 0,
                "info": [],
                "calls": record["calls"],
                "bucket": record["bucket"],
                "user": record["username"],
                "type": record["query_type"],
                "avg_duration": record["avg_duration"],
                "nodes": record["nodes"],
                "last_used": record["last_used"],
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
    write_keys = []
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
            write_keys.append(key)
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
        results = report_cursor.executemany(write_query_stmt, write_params) or []

        outcomes = list(results) + [None] * (len(write_params) - len(results))
        for key, outcome in zip(write_keys, outcomes):
            if outcome is not None and outcome.get("rowcount", 0) < 0:
                logger.warning(
                    f"Storing statistics failed, retrying on the next cycle: {outcome.get('error_message') or outcome}"
                )
                continue
            sys_jobs_log[key]["in_db"] = True
            sys_jobs_log[key]["changed"] = False

    stmt = f"UPDATE {last_exec_table} SET last_execution = ?"
    report_cursor.execute(stmt, (last_scrape,))


def read_stats():
    stmt = (
        f"SELECT id, stmt, calls, bucket, username, query_type, avg_duration, nodes, last_used "
        f"FROM {stmt_log_table} ORDER BY calls DESC, avg_duration DESC;"
    )
    report_cursor.execute(stmt)
    results = fetch_records(report_cursor)

    # Deanonymize statements if needed
    if deanonymize_sql and decoder_dict_path:
        for record in results:
            if record["stmt"]:
                record["stmt"] = deanonymize_statement(record["stmt"])

    # The records just read are authoritative. Without discarding what `dbinit` has read
    # before, deanonymized statements would be reported next to their anonymized form.
    sys_jobs_log.clear()
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
        # Keep the most recent execution, independently of the order records arrive in.
        sys_jobs_log[stmt]["last_used"] = max(sys_jobs_log[stmt]["last_used"] or 0, started)
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
        # Half-open interval: The watermark itself has been processed already, so a job
        # ending exactly on it must not be counted a second time.
        f"AND ended > {last_scrape} AND ended <= {next_scrape} "
        # Oldest first, so the *decaying* average of `update_statistics` ends up weighing
        # the most recent execution most heavily.
        f"ORDER BY ended ASC"
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
