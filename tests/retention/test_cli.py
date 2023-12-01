# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import typing as t

import pytest
from click.testing import CliRunner
from docker.models.containers import Container
from sqlalchemy.exc import OperationalError
from testcontainers.core.container import DockerContainer

from cratedb_toolkit.retention.cli import cli
from tests.retention.conftest import TESTDRIVE_DATA_SCHEMA


def test_version():
    """
    CLI test: Invoke `cratedb-retention --version`.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args="--version",
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_setup_brief(caplog, cratedb, settings):
    """
    CLI test: Invoke `cratedb-retention setup`.
    """
    database_url = cratedb.get_connection_url()
    runner = CliRunner()

    caplog.set_level(logging.ERROR, "sqlalchemy")
    result = runner.invoke(
        cli,
        args=f'setup "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert cratedb.database.table_exists(settings.policy_table.fullname) is True
    assert 1 <= len(caplog.records) <= 2


def test_setup_verbose(caplog, cratedb, settings):
    """
    CLI test: Invoke `cratedb-retention setup`.
    """
    database_url = cratedb.get_connection_url()
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args=f'--verbose setup "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert cratedb.database.table_exists(settings.policy_table.fullname) is True
    assert 3 <= len(caplog.records) <= 15


def test_setup_dryrun(caplog, cratedb, settings):
    """
    CLI test: Invoke `cratedb-retention setup --dry-run`,
    and verify the table will not be created.
    """
    database_url = cratedb.get_connection_url()
    runner = CliRunner()

    result = runner.invoke(
        cli,
        args=f'setup --dry-run "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert cratedb.database.table_exists(settings.policy_table.fullname) is False
    assert "Pretending to execute SQL statement" in caplog.text


def test_setup_failure_no_dburi():
    """
    Verify that the program fails, when it is not able to obtain a
    database URI to connect to.
    """

    runner = CliRunner()
    result = runner.invoke(
        cli,
        args="--verbose setup",
        catch_exceptions=False,
    )
    assert result.exit_code == 2
    assert "Missing argument 'DBURI'" in result.output


def test_setup_failure_envvar_invalid_dburi(mocker):
    """
    Verify using the `CRATEDB_URI` environment variable, and that the
    program fails correctly, when pointing it to an arbitrary address.
    """

    mocker.patch("os.environ", {"CRATEDB_URI": "crate://localhost:5555"})

    runner = CliRunner()
    with pytest.raises(OperationalError) as ex:
        runner.invoke(
            cli,
            args="--verbose setup",
            catch_exceptions=False,
        )
    assert ex.match("No more Servers available")


def test_list_policies(store, capsys):
    """
    Verify a basic DELETE retention policy through the CLI.
    """

    database_url = store.database.dburi
    runner = CliRunner()

    # Invoke data retention through CLI interface.
    result = runner.invoke(
        cli,
        args=f'list-policies "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # TODO: Can't read STDOUT. Why?
    """
    out, err = capsys.readouterr()
    output = json.loads(out)
    item0 = output[0]

    assert item0["table_schema"] == "doc"
    assert item0["table_name"] == "raw_metrics"
    """


def test_run_delete_basic(store, database, raw_metrics, policies):
    """
    Verify a basic DELETE retention policy through the CLI.
    """

    database_url = store.database.dburi
    runner = CliRunner()

    # Check number of records in database.
    assert database.count_records(raw_metrics) == 6

    # Invoke data retention through CLI interface.
    result = runner.invoke(
        cli,
        args=f'run --cutoff-day=2024-12-31 --strategy=delete "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Check number of records in database.
    assert database.count_records(raw_metrics) == 0


def test_run_delete_dryrun(caplog, store, database, raw_metrics, policies):
    """
    Verify a basic DELETE retention policy through the CLI with `--dry-run` does not do anything.
    """

    database_url = store.database.dburi
    runner = CliRunner()

    # Check number of records in database.
    assert database.count_records(raw_metrics) == 6

    # Invoke data retention through CLI interface.
    result = runner.invoke(
        cli,
        args=f'run --cutoff-day=2024-12-31 --strategy=delete --dry-run "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "Pretending to execute SQL statement" in caplog.text

    # Check number of records in database.
    assert database.count_records(raw_metrics) == 6


def test_run_delete_with_tags_match(store, database, sensor_readings, policies):
    """
    Verify a basic DELETE retention policy through the CLI, with using correct (matching) tags.
    """

    database_url = store.database.dburi
    runner = CliRunner()

    # Check number of records in database.
    assert database.count_records(sensor_readings) == 9

    # Invoke data retention through CLI interface.
    result = runner.invoke(
        cli,
        args=f'run --cutoff-day=2024-12-31 --strategy=delete --tags=foo,bar "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Verify that records have been deleted.
    assert database.count_records(f'"{TESTDRIVE_DATA_SCHEMA}"."sensor_readings"') == 0


def test_run_delete_with_tags_unknown(caplog, store, database, sensor_readings, policies):
    """
    Verify a basic DELETE retention policy through the CLI, with using wrong (not matching) tags.
    """

    database_url = store.database.dburi
    runner = CliRunner()

    # Check number of records in database.
    assert database.count_records(sensor_readings) == 9

    # Invoke data retention through CLI interface.
    result = runner.invoke(
        cli,
        args=f'run --cutoff-day=2024-12-31 --strategy=delete --tags=foo,unknown "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert "No retention policies found with tags: ['foo', 'unknown']" in caplog.messages

    # Check number of records in database.
    # Records have not been deleted, because the tags did not match.
    assert database.count_records(sensor_readings) == 9


def test_run_reallocate(store, database, raw_metrics, raw_metrics_reallocate_policy):
    """
    CLI test: Invoke `cratedb-retention run --strategy=reallocate`.
    """

    database_url = store.database.dburi
    runner = CliRunner()

    # Check number of records in database.
    assert database.count_records(raw_metrics) == 6

    # Invoke data retention through CLI interface.
    result = runner.invoke(
        cli,
        args=f'run --cutoff-day=2024-12-31 --strategy=reallocate "{database_url}"',
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Check number of records in database.
    # FIXME: Currently, the test for this strategy apparently does not remove any records.
    #        The reason is because the scenario can't easily be simulated on a single-node
    #        cluster. The suite would need to orchestrate at least two nodes.
    assert database.count_records(raw_metrics) == 6


def test_run_snapshot_aws_s3(caplog, store, database, sensor_readings, sensor_readings_snapshot_policy, minio):
    """
    Verify the "SNAPSHOT" strategy using an object storage with AWS S3 API.
    Invokes `cratedb-retention run --strategy=snapshot`.
    """

    # Acquire runtime information from MinIO container. In order to let CrateDB talk to
    # MinIO, we need its Docker-internal IP address (172.17.0.x), not the exposed one.
    s3_endpoint = minio.get_real_host_address()

    # Prepare a "bucket" on S3 storage.
    minio.get_client().make_bucket("cratedb-cold-storage")

    # Create CrateDB repository on an object storage with AWS S3 API.
    # https://crate.io/docs/crate/reference/en/latest/sql/statements/create-repository.html#s3
    s3_config = minio.get_config()
    database.ensure_repository_s3(
        name="export_cold",
        typename="s3",
        protocol="http",
        endpoint=s3_endpoint,
        access_key=s3_config["access_key"],
        secret_key=s3_config["secret_key"],
        bucket="cratedb-cold-storage",
        drop=True,
    )

    # Check number of records in database.
    assert database.count_records(sensor_readings) == 9

    # Invoke data retention through CLI interface.
    database_url = store.database.dburi
    runner = CliRunner()
    runner.invoke(
        cli,
        args=f'run --cutoff-day=2024-05-15 --strategy=snapshot "{database_url}"',
        catch_exceptions=False,
    )

    # Check number of records in database.
    assert database.count_records(sensor_readings) == 4

    # Verify that the S3 bucket has been populated correctly, and that the snapshot has the right shape.
    object_names = minio.list_object_names(bucket_name="cratedb-cold-storage")
    assert_snapshot_shape(object_names)


@pytest.mark.skip(reason="CrateDB does not support custom endpoints for Azure Blob Storage")
def test_run_snapshot_azure_blob(caplog, store, database, sensor_readings, sensor_readings_snapshot_policy, azurite):
    """
    Verify the "SNAPSHOT" strategy using an object storage with Azure Blob Storage API.
    Invokes `cratedb-retention run --strategy=snapshot`.
    """

    # Acquire runtime information from Azurite container. In order to let CrateDB talk to
    # Azurite, we need its Docker-internal IP address (172.17.0.x), not the exposed one.
    storage_endpoint = azurite.get_real_host_address()

    # Prepare a "container" on Azure storage.
    azurite.create_container("cratedb-cold-storage")

    # Create CrateDB repository on an object storage with Azure Blob Storage API.
    # https://crate.io/docs/crate/reference/en/latest/sql/statements/create-repository.html#azure
    database.ensure_repository_az(
        name="export_cold",
        typename="azure",
        protocol="http",
        endpoint=storage_endpoint,
        account=azurite._AZURITE_ACCOUNT_NAME,
        key=azurite._AZURITE_ACCOUNT_KEY,
        container="cratedb-cold-storage",
        drop=True,
    )

    # Check number of records in database.
    assert database.count_records(sensor_readings) == 9

    # Invoke data retention through CLI interface.
    database_url = store.database.dburi
    runner = CliRunner()
    runner.invoke(
        cli,
        args=f'run --cutoff-day=2024-05-15 --strategy=snapshot "{database_url}"',
        catch_exceptions=False,
    )

    # Check number of records in database.
    assert database.count_records(sensor_readings) == 4

    # Verify that the AZ blob container has been populated correctly, and that the snapshot has the right shape.
    blob_names = azurite.list_blob_names(container_name="cratedb-cold-storage")
    assert_snapshot_shape(blob_names)


def test_run_snapshot_fs(caplog, cratedb, store, database, sensor_readings, sensor_readings_snapshot_policy):
    """
    Verify the "SNAPSHOT" strategy using classic filesystem storage.
    Invokes `cratedb-retention run --strategy=snapshot`.
    """

    # Acquire OCI container handle, to introspect it.
    tc_container: DockerContainer = cratedb.cratedb
    oci_container: Container = tc_container.get_wrapped_container()

    # Define snapshot directory.
    # Note that the path is located _inside_ the OCI container where CrateDB is running.
    snapshot_path = "/tmp/snapshots/cratedb-cold-storage"  # noqa: S108

    # Delete snapshot directory.
    oci_container.exec_run(f"rm -rf {snapshot_path}")

    # Create CrateDB repository on the filesystem.
    # https://crate.io/docs/crate/reference/en/latest/sql/statements/create-repository.html#fs
    database.ensure_repository_fs(
        name="export_cold",
        typename="fs",
        location=snapshot_path,
        drop=True,
    )

    # Check number of records in database.
    assert database.count_records(sensor_readings) == 9

    # Invoke data retention through CLI interface.
    database_url = store.database.dburi
    runner = CliRunner()
    runner.invoke(
        cli,
        args=f'run --cutoff-day=2024-05-15 --strategy=snapshot "{database_url}"',
        catch_exceptions=False,
    )

    # Check number of records in database.
    assert database.count_records(sensor_readings) == 4

    # Verify that the filesystem has been populated correctly, and that the snapshot has the right shape.
    ls_result = oci_container.exec_run(f"ls {snapshot_path}")
    file_names = ls_result.output.decode().splitlines()
    assert_snapshot_shape(file_names)


def assert_snapshot_shape(file_names: t.List[str]):
    """
    Strip random fragments from snapshot file names, and compare against reference.
    ['index-0', 'index.latest', 'meta-HaxVcmMiRMyhT2o_rVFqUw.dat', 'snap-HaxVcmMiRMyhT2o_rVFqUw.dat', 'indices/']
    """  # noqa: ERA001, E501
    assert len(file_names) >= 5
    base_names = sorted([name.split("-")[0].replace("/", "") for name in file_names])
    assert base_names == ["index", "index.latest", "indices", "meta", "snap"]
