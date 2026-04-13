from importlib.resources import files

import pytest

import tests.io.file.data
from cratedb_toolkit import DatabaseCluster, InputOutputResource, TableAddress
from tests.conftest import TESTDRIVE_DATA_SCHEMA

data_folder = files(tests.io.file.data)
ddl = (data_folder / "climate_ddl.sql").read_text().format(schema=TESTDRIVE_DATA_SCHEMA)
climate_json_json = (
    str(data_folder / "climate_json_json.csv") + "?quote-char='&pipe=json_array_to_wkt_point:geo_location"
)
climate_json_python = (
    str(data_folder / "climate_json_python.csv")
    + '?quote-char="&pipe=json_array_to_wkt_point:geo_location&pipe=python_to_json:data'
)
climate_wkt_json = str(data_folder / "climate_wkt_json.csv") + "?quote-char='"

table_address = TableAddress(schema=TESTDRIVE_DATA_SCHEMA, table="climate_data", if_exists="append")


@pytest.fixture(scope="function")
def provision_ddl(cratedb_synchronized) -> None:
    cratedb_synchronized.database.run_sql(ddl)


def test_load_csv_json_json(cratedb_synchronized, provision_ddl):
    cluster = DatabaseCluster.create(cluster_url=cratedb_synchronized.database.dburi)
    cluster.load_table(InputOutputResource(climate_json_json), target=table_address)
    cluster.adapter.refresh_table(table_address.fullname)
    assert cluster.adapter.count_records(table_address.fullname) == 3, "Wrong number of records returned"


def test_load_csv_json_python(cratedb_synchronized, provision_ddl):
    cluster = DatabaseCluster.create(cluster_url=cratedb_synchronized.database.dburi)
    cluster.load_table(InputOutputResource(climate_json_python), target=table_address)
    cluster.adapter.refresh_table(table_address.fullname)
    assert cluster.adapter.count_records(table_address.fullname) == 3, "Wrong number of records returned"


def test_load_csv_wkt_json(cratedb_synchronized, provision_ddl):
    cluster = DatabaseCluster.create(cluster_url=cratedb_synchronized.database.dburi)
    cluster.load_table(InputOutputResource(climate_wkt_json), target=table_address)
    cluster.adapter.refresh_table(table_address.fullname)
    assert cluster.adapter.count_records(table_address.fullname) == 3, "Wrong number of records returned"
