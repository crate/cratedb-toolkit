import dataclasses
import typing as t

from cratedb_toolkit.cluster.croud import CloudCluster, CloudManager
from cratedb_toolkit.exception import CroudException


@dataclasses.dataclass
class ClusterInformation:
    """
    Manage a database cluster's information.
    """

    cratedb: t.Any = dataclasses.field(default_factory=dict)
    cloud: t.Dict[str, t.Any] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_id_or_name(cls, cluster_id: str = None, cluster_name: str = None) -> "ClusterInformation":
        """
        Look up cluster by identifier (UUID) or name.
        """
        if cluster_id is not None:
            return cls.from_id(cluster_id=cluster_id)
        elif cluster_name is not None:
            return cls.from_name(cluster_name=cluster_name)
        else:
            raise ValueError("Failed to address cluster: Either cluster identifier or name needs to be specified")

    @classmethod
    def from_id(cls, cluster_id: str) -> "ClusterInformation":
        """
        Look up cluster by identifier (UUID).
        """

        cc = CloudCluster(cluster_id=cluster_id)
        return ClusterInformation(cloud=cc.get_info())

    @classmethod
    def from_name(cls, cluster_name: str) -> "ClusterInformation":
        """
        Look up cluster by name.
        """

        cm = CloudManager()
        cluster_list = cm.list_clusters()
        for cluster in cluster_list:
            if cluster["name"] == cluster_name:
                return ClusterInformation(cloud=cluster)
        raise CroudException(f"Cluster not found: {cluster_name}")

    def asdict(self):
        return dataclasses.asdict(self)
