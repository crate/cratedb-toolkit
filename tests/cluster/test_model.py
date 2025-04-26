import pytest

from cratedb_toolkit.cluster.model import ClusterInformation
from cratedb_toolkit.exception import CroudException


def test_cluster_info_managed_unauthorized():
    with pytest.raises(CroudException) as ex:
        ClusterInformation.from_id_or_name(cluster_id="foo", cluster_name="bar")
    assert ex.match("401 - Unauthorized")
