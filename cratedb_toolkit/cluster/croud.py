from cratedb_toolkit.util.croud import CroudCall, CroudWrapper


class CloudCluster:
    """
    A wrapper around the CrateDB Cloud cluster API through the `croud` package.
    """

    def __init__(self, cluster_id: str):
        self.cluster_id = cluster_id

    def get_info(self):
        """
        Get cluster information.

        ctk cluster info
        croud clusters get e1e38d92-a650-48f1-8a70-8133f2d5c400 --format=json
        """
        from croud.clusters.commands import clusters_get
        from croud.parser import Argument

        call = CroudCall(
            fun=clusters_get,
            specs=[
                Argument(
                    "id",
                    type=str,
                    help="The ID of the cluster.",
                )
            ],
            arguments=[
                self.cluster_id,
            ],
        )

        wr = CroudWrapper(call=call)
        return wr.invoke()
