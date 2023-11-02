from cratedb_toolkit.util.croud import CroudCall, CroudWrapper


def jobs_list(cratedb_cloud_cluster_id: str, output_format: str = None, decode_output: bool = True):
    from croud.clusters.commands import import_jobs_list
    from croud.parser import Argument

    call = CroudCall(
        fun=import_jobs_list,
        specs=[Argument("--cluster-id", type=str, required=True, help="The cluster the import jobs belong to.")],
        arguments=[
            f"--cluster-id={cratedb_cloud_cluster_id}",
        ],
    )

    wr = CroudWrapper(call, output_format=output_format, decode_output=decode_output)
    return wr.invoke()
