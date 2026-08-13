# Copyright (c) 2021-2026, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
Test fixtures for CFR diagnostics bundle tests.
"""

import importlib.metadata
import logging

import pytest
from verlib2 import Version

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def click_kwargs():
    """
    Click 8.2 no longer understands `mix_stderr`.
    """
    kwargs = {}
    click_version = importlib.metadata.version("click")
    if Version(click_version) < Version("8.2"):
        kwargs = {"mix_stderr": False}
    return kwargs


VALIDATION_SCHEMA = "testdrive-cfr"

DDL_COMPLEX_TABLE = """
CREATE TABLE IF NOT EXISTS "doc"."cfr_complex" (
  id BIGINT,
  ts TIMESTAMP WITH TIME ZONE,
  month TIMESTAMP GENERATED ALWAYS AS date_trunc('month', ts),
  payload OBJECT(DYNAMIC) AS (nested_a TEXT, nested_b ARRAY(INT)),
  tags ARRAY(TEXT),
  loc GEO_POINT,
  vec FLOAT_VECTOR(4),
  descr TEXT INDEX USING FULLTEXT WITH (analyzer = 'english'),
  PRIMARY KEY (id, month)
) PARTITIONED BY (month)
CLUSTERED INTO 3 SHARDS
WITH (number_of_replicas = '0', refresh_interval = 5000, codec = 'best_compression')
"""

DDL_SIMPLE_TABLE = """
CREATE TABLE IF NOT EXISTS "doc"."cfr_simple" (id INT PRIMARY KEY, name TEXT)
"""

DDL_VIEW = """
CREATE VIEW "doc"."cfr_view" AS SELECT id, name FROM "doc"."cfr_simple"
"""

# A table outside `doc`, to prove schema capture is not limited to the default schema.
DDL_OTHER_SCHEMA_TABLE = f"""
CREATE TABLE IF NOT EXISTS "{VALIDATION_SCHEMA}"."cfr_other" (a TEXT)
"""

TEARDOWN = [
    'DROP VIEW IF EXISTS "doc"."cfr_view"',
    'DROP TABLE IF EXISTS "doc"."cfr_complex"',
    'DROP TABLE IF EXISTS "doc"."cfr_simple"',
    f'DROP TABLE IF EXISTS "{VALIDATION_SCHEMA}"."cfr_other"',
]


# CrateDB hands out in the clear do not reach the bundle.
FDW_SERVER = "ctk_probe"
FDW_PASSWORD = "cfr-fdw-supersecret"  # noqa: S105
FDW_URL = "jdbc:postgresql://example.invalid:5432/probe"

FDW_SETUP = [
    f"CREATE SERVER {FDW_SERVER} FOREIGN DATA WRAPPER jdbc OPTIONS (url '{FDW_URL}')",
    f"CREATE USER MAPPING FOR CURRENT_USER SERVER {FDW_SERVER} OPTIONS (\"user\" 'alice', password '{FDW_PASSWORD}')",
]

FDW_TEARDOWN = [
    f"DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER {FDW_SERVER}",
    f"DROP SERVER IF EXISTS {FDW_SERVER}",
]


@pytest.fixture
def cfr_foreign_data_wrapper(cratedb):
    """
    Provision an FDW server plus user mapping carrying a known password.
    """
    adapter = cratedb.database

    def teardown():
        for stmt in FDW_TEARDOWN:
            try:
                adapter.run_sql(stmt)
            except Exception as ex:  # noqa: PERF203
                logger.debug(f"Ignoring FDW teardown failure: {stmt}: {ex}")

    teardown()
    try:
        for stmt in FDW_SETUP:
            adapter.run_sql(stmt)
    except Exception as ex:
        teardown()
        pytest.skip(f"Cluster does not permit creating a foreign server: {ex}")

    yield adapter

    teardown()


@pytest.fixture
def cfr_validation_schema(cratedb):
    """
    Provision the validation schema from the feature's quickstart guide.

    Yields the database adapter, so test cases can query the cluster directly.
    """
    adapter = cratedb.database

    def teardown():
        for stmt in TEARDOWN:
            adapter.run_sql(stmt)

    teardown()
    for stmt in (DDL_SIMPLE_TABLE, DDL_COMPLEX_TABLE, DDL_VIEW, DDL_OTHER_SCHEMA_TABLE):
        adapter.run_sql(stmt)

    adapter.run_sql('INSERT INTO "doc"."cfr_simple" (id, name) VALUES (1, \'alpha\')')
    adapter.run_sql('REFRESH TABLE "doc"."cfr_simple"')

    yield adapter

    teardown()
