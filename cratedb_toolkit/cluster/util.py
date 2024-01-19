from cratedb_toolkit.model import ClusterInformation


def get_cluster_info(cluster_id: str) -> ClusterInformation:
    cluster_info = ClusterInformation()
    try:
        from cratedb_toolkit.cluster.croud import CloudCluster

        cc = CloudCluster(cluster_id=cluster_id)
        cluster_info.cloud = cc.get_info()
    except ImportError:
        pass
    return cluster_info
