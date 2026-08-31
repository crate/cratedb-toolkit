"""
Unit tests for the job statistics collector, which do not need a database.

The collector accumulates its statistics in Python, not in SQL, so the bucket
assignment and the average computation are verified here explicitly.
"""

import typing as t

import pytest

pytest.importorskip("queryanonymizer", reason="Skipping tests because queryanonymizer is not installed")

from cratedb_toolkit.cfr import jobstats  # noqa: E402
from cratedb_toolkit.model import DatabaseAddress  # noqa: E402

pytestmark = pytest.mark.cfr


@pytest.fixture(autouse=True)
def reset_collector_state():
    """
    Provide each test case with a pristine collector, and leave no state behind.
    """
    jobstats.reset_state()
    jobstats.anonymize_sql = False
    jobstats.deanonymize_sql = False
    yield
    jobstats.reset_state()
    jobstats.anonymize_sql = False
    jobstats.deanonymize_sql = False


def job(started=1_000, duration=50, stmt="SELECT 1", query_type="SELECT", username="crate", node_name="node-1"):
    """
    Produce a single `sys.jobs_log` record, in the shape `scrape_db` yields it.
    """
    return (started, started + duration, {"type": query_type}, stmt, username, {"id": "n1", "name": node_name})


def db_record(stmt="SELECT 1", **overrides):
    """
    Produce a single record of the statistics table, in the shape `fetch_records` yields it.
    """
    record = {
        "id": "id-1",
        "stmt": stmt,
        "calls": 99,
        "bucket": dict(jobstats.bucket_dict),
        "username": "crate",
        "query_type": "SELECT",
        "avg_duration": 1.5,
        "nodes": ["n1"],
        "last_used": 1_000,
    }
    record.update(overrides)
    return record


class FakeCursor:
    """
    Record statements instead of executing them, and reply with canned records.
    """

    def __init__(self, column_names=None, rows=None):
        self.description = [(name,) for name in column_names or []]
        self.rows = rows or []
        self.statements: t.List[str] = []

    def execute(self, statement, parameters=None):
        self.statements.append(statement)

    def fetchall(self):
        return self.rows


# Bucket assignment.


@pytest.mark.parametrize(
    ("duration", "expected"),
    [
        (0, "10"),
        (9, "10"),
        (10, "50"),
        (49, "50"),
        (50, "100"),
        (1_999, "2000"),
        (19_999, "20000"),
        (20_000, "INF"),
        (999_999, "INF"),
    ],
)
def test_assign_to_bucket(duration, expected):
    """
    Verify durations land in the bucket of the next larger threshold. Thresholds are exclusive.
    """
    bucket = dict(jobstats.bucket_dict)
    outcome = jobstats.assign_to_bucket(bucket, duration)
    assert outcome[expected] == 1
    assert sum(outcome.values()) == 1


def test_assign_to_bucket_mutates_in_place():
    """
    Verify `assign_to_bucket` updates and returns the very same dictionary.
    """
    bucket = dict(jobstats.bucket_dict)
    assert jobstats.assign_to_bucket(bucket, 5) is bucket


def test_bucket_dict_matches_bucket_list():
    """
    Verify the bucket thresholds and the bucket template do not drift apart.
    """
    assert list(jobstats.bucket_dict) == [str(threshold) for threshold in jobstats.bucket_list] + ["INF"]


# Statistics accumulation.


def test_update_statistics_new_statement():
    """
    Verify a previously unseen statement is recorded with a single call.
    """
    jobstats.update_statistics([job(started=1_000, duration=50)])

    assert list(jobstats.sys_jobs_log) == ["SELECT 1"]
    entry = jobstats.sys_jobs_log["SELECT 1"]
    assert entry["calls"] == 1
    assert entry["type"] == "SELECT"
    assert entry["user"] == "crate"
    assert entry["last_used"] == 1_000
    assert entry["avg_duration"] == 50
    assert entry["bucket"]["100"] == 1
    assert entry["in_db"] is False
    assert entry["changed"] is True
    assert entry["id"]


def test_update_statistics_repeated_statement():
    """
    Verify repeated statements accumulate, and that `avg_duration` is a decaying average.

    The collector computes `(previous + current) / 2` per sample, which weights recent
    executions much more heavily than an arithmetic mean would.
    """
    jobstats.update_statistics([job(duration=50)])
    jobstats.update_statistics([job(started=2_000, duration=100)])

    entry = jobstats.sys_jobs_log["SELECT 1"]
    assert entry["calls"] == 2
    assert entry["last_used"] == 2_000
    assert entry["avg_duration"] == 75.0
    assert entry["bucket"]["100"] == 1
    assert entry["bucket"]["500"] == 1


def test_update_statistics_deduplicates_nodes():
    """
    Verify each node is recorded only once per statement, no matter how often it runs there.
    """
    jobstats.update_statistics(
        [
            job(node_name="node-1"),
            job(node_name="node-1"),
            job(node_name="node-2"),
        ]
    )

    entry = jobstats.sys_jobs_log["SELECT 1"]
    assert entry["calls"] == 3
    assert len(entry["nodes"]) == 2


def test_update_statistics_separates_statements():
    """
    Verify statistics are keyed by statement.
    """
    jobstats.update_statistics([job(stmt="SELECT 1"), job(stmt="SELECT 2", query_type="SELECT")])

    assert sorted(jobstats.sys_jobs_log) == ["SELECT 1", "SELECT 2"]
    assert jobstats.sys_jobs_log["SELECT 1"]["calls"] == 1
    assert jobstats.sys_jobs_log["SELECT 2"]["calls"] == 1


# State handling.


def test_reset_state():
    """
    Verify `reset_state` discards accumulated statistics.

    Without it, a second `boot()` in the same process would consider statements to be
    stored already, and update rows which do not exist.
    """
    jobstats.update_statistics([job()])
    jobstats.last_execution_ts = 42
    assert jobstats.sys_jobs_log

    jobstats.reset_state()

    assert jobstats.sys_jobs_log == {}
    assert jobstats.last_execution_ts == 0


def test_init_stmts_keeps_accumulated_statistics():
    """
    Verify reading records from the database does not clobber freshly accumulated statistics.
    """
    jobstats.update_statistics([job(duration=50)])
    jobstats.init_stmts([db_record()])

    entry = jobstats.sys_jobs_log["SELECT 1"]
    assert entry["calls"] == 1
    assert entry["in_db"] is False


def test_init_stmts_adopts_database_records():
    """
    Verify records read from the database are adopted as already persisted.
    """
    jobstats.init_stmts([db_record()])

    entry = jobstats.sys_jobs_log["SELECT 1"]
    assert entry["id"] == "id-1"
    assert entry["calls"] == 99
    assert entry["avg_duration"] == 1.5
    assert entry["in_db"] is True
    assert entry["changed"] is False


def test_init_stmts_maps_columns_by_name():
    """
    Verify each column ends up in its own field.

    The readers of the statistics table use different `SELECT` orders. Addressing the
    columns by position made `jobstats view` report the average duration as the duration
    histogram, the histogram as the user, the user as the query type, and so on.
    """
    jobstats.init_stmts(
        [
            db_record(
                bucket={"10": 7},
                username="hotzenplotz",
                query_type="DDL",
                avg_duration=12.5,
                nodes=["node-1"],
                last_used=1_700_000_000_000,
            )
        ]
    )

    entry = jobstats.sys_jobs_log["SELECT 1"]
    assert entry["bucket"] == {"10": 7}
    assert entry["user"] == "hotzenplotz"
    assert entry["type"] == "DDL"
    assert entry["avg_duration"] == 12.5
    assert entry["nodes"] == ["node-1"]
    assert entry["last_used"] == 1_700_000_000_000


def test_fetch_records():
    """
    Verify query results are keyed by column name.
    """
    cursor = FakeCursor(column_names=["id", "stmt"], rows=[("id-1", "SELECT 1"), ("id-2", "SELECT 2")])
    assert jobstats.fetch_records(cursor) == [
        {"id": "id-1", "stmt": "SELECT 1"},
        {"id": "id-2", "stmt": "SELECT 2"},
    ]


def test_init_last_execution_adopts_watermark(mocker):
    """
    Verify a recorded watermark is adopted, without touching the table.
    """
    cursor = FakeCursor()
    mocker.patch.object(jobstats, "report_cursor", cursor)
    mocker.patch.object(jobstats, "last_exec_table", '"testdrive".jobstats_last')

    jobstats.init_last_execution({"last_execution": 1_700_000_000_000, "record_count": 1})

    assert jobstats.last_execution_ts == 1_700_000_000_000
    assert cursor.statements == []


def test_init_last_execution_creates_watermark(mocker):
    """
    Verify a missing watermark record is created, so it can be updated later on.
    """
    cursor = FakeCursor()
    mocker.patch.object(jobstats, "report_cursor", cursor)
    mocker.patch.object(jobstats, "last_exec_table", '"testdrive".jobstats_last')

    jobstats.init_last_execution({"last_execution": None, "record_count": 0})

    assert jobstats.last_execution_ts == 0
    assert [statement.split()[0] for statement in cursor.statements] == ["INSERT", "REFRESH"]


def test_scrape_db_uses_half_open_interval(mocker):
    """
    Verify the collection window excludes the watermark, and includes the current moment.

    An inclusive lower bound would count a job ending exactly on the watermark twice.
    """
    cursor = FakeCursor()
    mocker.patch.object(jobstats, "cursor", cursor)
    mocker.patch.object(jobstats, "last_scrape", 1_700_000_000_000)

    jobstats.scrape_db()

    statement = cursor.statements[0]
    assert "ended > 1700000000000" in statement
    assert f"ended <= {jobstats.last_scrape}" in statement
    assert "BETWEEN" not in statement
    assert jobstats.last_scrape > 1_700_000_000_000


# Anonymization.


def test_anonymize_statement_disabled():
    """
    Verify statements pass through unchanged while anonymization is disabled.
    """
    assert jobstats.anonymize_statement("SELECT * FROM foobar") == "SELECT * FROM foobar"


def test_deanonymize_statement_disabled():
    """
    Verify statements pass through unchanged while deanonymization is disabled.
    """
    assert jobstats.deanonymize_statement("SELECT * FROM foobar") == "SELECT * FROM foobar"


def test_redacted_statement():
    """
    Verify redaction discloses nothing but remains stable per statement.
    """
    redacted = jobstats.redacted_statement("SELECT * FROM secrets")
    assert redacted == jobstats.redacted_statement("SELECT * FROM secrets")
    assert redacted != jobstats.redacted_statement("SELECT * FROM other_secrets")
    assert "secrets" not in redacted
    assert redacted.startswith("<redacted: ")


def test_anonymize_statement_failure_redacts(mocker, caplog):
    """
    Verify a failing anonymization redacts the statement, instead of storing it in clear text.

    `queryanonymizer` raises on certain statements, for example
    `DROP SCHEMA IF EXISTS "testdrive-data" CASCADE`, when the decoder dictionary makes it
    build an invalid regular expression. Falling back to the original statement would leak
    exactly the information the option is meant to protect.
    """
    mocker.patch.object(jobstats, "anonymize_sql", True)
    mocker.patch.object(jobstats, "decoder_dict_path", "/tmp/does-not-exist.json")  # noqa: S108
    mocker.patch.object(jobstats, "anonymize", side_effect=Exception("bad character range V-A at position 7"))

    outcome = jobstats.anonymize_statement('DROP SCHEMA IF EXISTS "testdrive-data" CASCADE')

    assert outcome.startswith("<redacted: ")
    assert "testdrive-data" not in outcome
    assert "redacting it instead" in caplog.text


def test_anonymize_statement_suppresses_stdout(mocker, capsys):
    """
    Verify the chatter of `queryanonymizer` does not end up on stdout.

    The output of `ctk` is JSON, and needs to stay parseable.
    """
    mocker.patch.object(jobstats, "anonymize_sql", True)
    mocker.patch.object(jobstats, "decoder_dict_path", "/tmp/does-not-exist.json")  # noqa: S108

    def chatty(**kwargs):
        print("Decoder dictionary has 3 elements:")  # noqa: T201
        return ("SELECT 1", {})

    mocker.patch.object(jobstats, "anonymize", side_effect=chatty)

    assert jobstats.anonymize_statement("SELECT 2") == "SELECT 1"
    assert capsys.readouterr().out == ""


def test_deanonymize_statement_suppresses_stdout(mocker, capsys):
    """
    Verify the chatter of `queryanonymizer` does not corrupt the JSON output of `jobstats view`.
    """
    mocker.patch.object(jobstats, "deanonymize_sql", True)
    mocker.patch.object(jobstats, "decoder_dict_path", "/tmp/does-not-exist.json")  # noqa: S108

    def chatty(*args, **kwargs):
        print("SELECT 'secret' AS marker")  # noqa: T201
        return "SELECT 'secret' AS marker"

    mocker.patch.object(jobstats, "deanonymize", side_effect=chatty)

    assert jobstats.deanonymize_statement("SELECT 'xyz' AS abc") == "SELECT 'secret' AS marker"
    assert capsys.readouterr().out == ""


def test_deanonymize_statement_failure_keeps_statement(mocker, caplog):
    """
    Verify a failing deanonymization returns the stored statement, instead of aborting.
    """
    mocker.patch.object(jobstats, "deanonymize_sql", True)
    mocker.patch.object(jobstats, "decoder_dict_path", "/tmp/does-not-exist.json")  # noqa: S108
    mocker.patch.object(jobstats, "deanonymize", side_effect=Exception("kaputt"))

    assert jobstats.deanonymize_statement("SELECT 'xyz' AS abc") == "SELECT 'xyz' AS abc"
    assert "Failed to deanonymize statement" in caplog.text


def test_boot_anonymize_without_dictionary():
    """
    Verify anonymization without a decoder dictionary is rejected, before connecting.
    """
    with pytest.raises(ValueError) as ex:
        jobstats.boot(
            address=DatabaseAddress.from_string("crate://localhost:4200/"),
            anonymize_statements=True,
        )
    assert ex.match("Decoder dictionary file is required")


def test_boot_deanonymize_without_dictionary():
    """
    Verify deanonymization without a decoder dictionary is rejected, before connecting.
    """
    with pytest.raises(ValueError) as ex:
        jobstats.boot(
            address=DatabaseAddress.from_string("crate://localhost:4200/"),
            deanonymize_statements=True,
        )
    assert ex.match("Decoder dictionary file is required")
