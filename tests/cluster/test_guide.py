import pytest

from cratedb_toolkit.cluster.guide import DataImportGuide
from cratedb_toolkit.cluster.model import ClusterInformation
from cratedb_toolkit.io.croud import CloudJob


@pytest.fixture
def cluster_info(cloud_cluster_name):
    return ClusterInformation.from_id_or_name(cluster_name=cloud_cluster_name)


def test_guiding_texts_with_job(cloud_environment, cluster_info):
    """
    Validate the `DataImportGuide` class with success and error messages.
    """
    gt = DataImportGuide(
        cluster_info=cluster_info,
        job=CloudJob(info={"destination": {"table": "testdrive.demo"}}),
    )
    assert "Excellent, that worked well." in gt.success()
    assert "ctk shell --cluster-name" in gt.success()
    assert "That went south." in gt.error()


def test_guiding_texts_without_job(cloud_environment, cluster_info):
    """
    Validate the `DataImportGuide` class erroring out when no job info is provided.
    """
    gt = DataImportGuide(
        cluster_info=cluster_info,
        job=CloudJob(),
    )
    assert "Excellent, that worked well." in gt.success()
    assert "ctk shell --cluster-name" not in gt.success()
    assert "That went south." in gt.error()
