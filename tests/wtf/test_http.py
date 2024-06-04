import datetime as dt
import os

import pytest

pytest.importorskip("fastapi")


from fastapi.testclient import TestClient

from cratedb_toolkit import __appname__, __version__
from cratedb_toolkit.wtf.http import app

client = TestClient(app)


def test_http_root():
    response = client.get("/")
    data = response.json()
    assert response.status_code == 200
    assert data["application_name"] == __appname__
    assert data["application_version"].startswith(__version__)
    assert dt.datetime.fromisoformat(data["system_time"]).year == dt.datetime.now().year


def test_http_info(cratedb, mocker):
    mocker.patch.dict(os.environ, {"CRATEDB_SQLALCHEMY_URL": cratedb.database.dburi})

    response = client.get("/info/all")
    info = response.json()
    assert response.status_code == 200

    assert info["meta"]["application_name"] == __appname__
    assert info["meta"]["application_version"].startswith(__version__)
    assert dt.datetime.fromisoformat(info["meta"]["system_time"]).year == dt.datetime.now().year
    assert "elements" in info["meta"]
    assert len(info["meta"]["elements"]) > 15

    assert "data" in info
    assert info["data"]["database"]["cluster_nodes_count"] == 1
    assert info["data"]["system"]["application"]["name"] == __appname__

    assert "eco" in info["data"]["system"]
