import datetime as dt
import os
from unittest import mock

import pytest
from click.testing import CliRunner

from cratedb_toolkit.adapter.rockset.cli import cli
from tests.conftest import TESTDRIVE_DATA_SCHEMA

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from cratedb_toolkit import __appname__, __version__
from cratedb_toolkit.adapter.rockset.server.main import app

client = TestClient(app)


def test_rockset_cli(cratedb):
    """
    Quickly verify `ctk rockset serve` roughly works.
    """
    with mock.patch("cratedb_toolkit.adapter.rockset.server.main.start_service"):
        runner = CliRunner(env={"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi})
        result = runner.invoke(
            cli,
            args="serve",
            catch_exceptions=False,
        )
        assert result.exit_code == 0


def test_rockset_http_root():
    """
    Just probing the HTTP root resource.
    """
    response = client.get("/")
    data = response.json()
    assert response.status_code == 200
    assert data["application_name"] == __appname__
    assert data["application_version"].startswith(__version__)
    assert dt.datetime.fromisoformat(data["system_time"]).year == dt.datetime.now().year


def test_rockset_add_documents(cratedb, mocker):
    """
    Verify adding documents.
    """
    mocker.patch.dict(os.environ, {"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi})

    response = client.post(
        f"/v1/orgs/self/ws/{TESTDRIVE_DATA_SCHEMA}/collections/foobar/docs",
        json={"data": [{"id": "foo", "field": "value"}]},
    )
    info = response.json()
    assert response.status_code == 200

    assert info == {
        "data": [
            {"_collection": "foobar", "_id": mock.ANY, "patch_id": mock.ANY, "status": "ADDED"},
        ],
        "last_offset": "f1:0:0:0:0",
    }

    # TODO: Query back data from database, and verify it.


def test_rockset_query(cratedb, mocker):
    """
    Verify querying documents.
    """
    mocker.patch.dict(os.environ, {"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi})

    # Create record.
    client.post(
        f"/v1/orgs/self/ws/{TESTDRIVE_DATA_SCHEMA}/collections/foobar/docs",
        json={"data": [{"id": "foo", "field": "value"}]},
    )

    response = client.post(
        "/v1/orgs/self/queries",
        json={"sql": {"query": f'SELECT * FROM "{TESTDRIVE_DATA_SCHEMA}"."foobar";'}},  # noqa: S608
    )
    info = response.json()
    assert response.status_code == 200

    assert info == {
        "collections": ["foobar"],
        "query_id": mock.ANY,
        "results": [{"id": "foo", "field": "value"}],
        "results_total_doc_count": 1,
        "stats": {"elapsed_time_ms": mock.ANY},
        "status": "COMPLETED",
    }

    # TODO: Query back data from database, and verify it.
