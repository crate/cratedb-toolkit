import pytest

import cratedb_toolkit
from cratedb_toolkit import DatabaseCluster, ManagedCluster


def test_cluster_managed_authorized(cloud_environment, cloud_cluster_id, cloud_cluster_name):
    """
    When supplying valid API credentials and cluster address, the cluster is detected.
    """
    mc = ManagedCluster.from_env()
    mc.probe()
    assert mc.info.cloud_id == cloud_cluster_id
    assert mc.info.cloud_name == cloud_cluster_name


def test_cluster_managed_unauthorized():
    """
    When no cluster address information is provided, the program bails out.
    """
    cratedb_toolkit.configure(
        settings_accept_env=True,
    )

    with pytest.raises(ValueError) as ex:
        ManagedCluster.from_env()
    assert ex.match("One of the settings is required, but none of them have been specified.")


def test_cluster_universal_managed(cloud_environment, cloud_cluster_id, cloud_cluster_name):
    """
    Validate the `UniversalCluster` factory class for "managed" CrateDB Cloud clusters.
    """
    mc = DatabaseCluster.create(cluster_name=cloud_cluster_name).probe()
    assert mc.info.cloud_id == cloud_cluster_id
    assert mc.info.cloud_name == cloud_cluster_name

    # Run two queries to detect context patching flaws.
    assert mc.query("SELECT 42") == [{"42": 42}]
    assert mc.query("SELECT 42") == [{"42": 42}]


def test_cluster_universal_standalone_sqlalchemy_url(cratedb):
    """
    Validate the `UniversalCluster` factory class for "standalone" CrateDB clusters, connecting per SQLAlchemy URL.
    """
    mc = DatabaseCluster.create(sqlalchemy_url=cratedb.get_connection_url()).probe()
    assert mc.query("SELECT 42") == [{"42": 42}]


def test_cluster_universal_standalone_http_url(cratedb):
    """
    Validate the `UniversalCluster` factory class for "standalone" CrateDB clusters, connecting per HTTP URL.
    """
    mc = DatabaseCluster.create(http_url=cratedb.get_http_url()).probe()
    assert mc.query("SELECT 42") == [{"42": 42}]
