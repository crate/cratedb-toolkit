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


def get_cluster_by_id_or_name(cluster_id: str = None, cluster_name: str = None):
    if cluster_id is not None:
        return get_cluster_info(cluster_id=cluster_id)
    elif cluster_name is not None:
        return get_cluster_by_name(cluster_name=cluster_name)
    else:
        raise ValueError("Failed to address cluster: Either cluster identifier or name needs to be specified")


def deploy_cluster(cluster_name: str, subscription_id: str = None, organization_id: str = None) -> ClusterInformation:
    cm = CloudManager()
    # TODO: Only create new project when needed. Otherwise, use existing project.
    project = cm.create_project(name=cluster_name, organization_id=organization_id)
    project_id = project["id"]
    logger.info(f"Created project: {project_id}")
    cluster_info = cm.deploy_cluster(name=cluster_name, project_id=project_id, subscription_id=subscription_id)
    return cluster_info
