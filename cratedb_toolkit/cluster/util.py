import logging

from cratedb_toolkit.cluster.croud import CloudCluster, CloudManager
from cratedb_toolkit.exception import CroudException
from cratedb_toolkit.model import ClusterInformation

logger = logging.getLogger(__name__)


def get_cluster_info(cluster_id: str) -> ClusterInformation:
    cc = CloudCluster(cluster_id=cluster_id)
    return ClusterInformation(cloud=cc.get_info())


def get_cluster_by_name(cluster_name: str) -> ClusterInformation:
    cm = CloudManager()
    cluster_list = cm.list_clusters()
    for cluster in cluster_list:
        if cluster["name"] == cluster_name:
            return ClusterInformation(cloud=cluster)
    raise CroudException(f"Cluster not found: {cluster_name}")


def deploy_cluster(cluster_name: str, subscription_id: str = None) -> ClusterInformation:
    cm = CloudManager()
    # TODO: Only create new project when needed. Otherwise, use existing project.
    project = cm.create_project(name=cluster_name)
    project_id = project["id"]
    logger.info(f"Created project: {project_id}")
    cluster_info = cm.deploy_cluster(name=cluster_name, project_id=project_id, subscription_id=subscription_id)
    return cluster_info
