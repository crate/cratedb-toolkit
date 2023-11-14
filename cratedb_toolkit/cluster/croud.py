import json

from cratedb_toolkit.util.croud import CroudCall, CroudWrapper


class CloudManager:
    """
    A wrapper around the CrateDB Cloud cluster API through the `croud` package.
    """

    def get_cluster_list(self):
        """
        Get list of clusters

        croud clusters list --format=json
        """
        from croud.clusters.commands import clusters_list
        from croud.parser import Argument

        call = CroudCall(
            fun=clusters_list,
            specs=[
                Argument(
                    "-p",
                    "--project-id",
                    type=str,
                    required=False,
                    help="The project ID to use.",
                ),
                Argument(
                    "--org-id",
                    type=str,
                    required=False,
                    help="The organization ID to use.",
                ),
            ],
            arguments=[],
        )

        wr = CroudWrapper(call=call)
        return wr.invoke()

    def create_project(self, name: str):
        """
        Create project.

        croud projects list --format=json
        croud projects create --name "foobar" -o json | jq -r '.id'

        """  # noqa: E501
        # TODO: Add more parameters, like `--org-id`, `--region`.
        from croud.__main__ import command_tree
        from croud.projects.commands import project_create

        call = CroudCall(
            fun=project_create,
            specs=command_tree["projects"]["commands"]["create"]["extra_args"],
            arguments=[
                f"--name={name}",
            ],
        )

        wr = CroudWrapper(call=call)
        return wr.invoke()

    def deploy_cluster(self, name: str, project_id: str):
        """
        Deploy cluster.

        croud clusters deploy --subscription-id $sub --project $proj  --product-name s2 --tier default --cluster-name $clustername --version $nightly  --username $user --password $pw --channel nightly -o json
        croud clusters get e1e38d92-a650-48f1-8a70-8133f2d5c400 --format=json

        """  # noqa: E501
        # TODO: `--product-name=crfree` is not always the right choice. ;]
        # TODO: How to select CrateDB nightly, like `--version=nightly`?
        # TODO: Add more parameters, like `--org-id`, `--channel`, `--unit`, and more.
        # TODO: What about `--sudo`?
        from croud.__main__ import command_tree
        from croud.clusters.commands import clusters_deploy

        call = CroudCall(
            fun=clusters_deploy,
            specs=command_tree["clusters"]["commands"]["deploy"]["extra_args"],
            arguments=[
                "--subscription-id=ba8592d2-b85b-4b21-b1c1-fbb99bc3c419",
                f"--project-id={project_id}",
                "--tier=default",
                "--product-name=crfree",
                f"--cluster-name={name}",
                "--version=5.5.0",
                "--username=admin",
                "--password=H3IgNXNvQBJM3CiElOiVHuSp6CjXMCiQYhB4I9dLccVHGvvvitPSYr1vTpt4",
            ],
        )

        wr = CroudWrapper(call=call, decode_output=False)

        # FIXME: Fix `croud clusters deploy`.
        #        It yields *two* payloads to stdout, making it
        #        unusable in JSON-capturing situations.
        # The main advantage of the `JSONDecoder` class is that it also provides
        # a `.raw_decode` method, which will ignore extra data after the end of the JSON.
        # https://stackoverflow.com/a/75168292
        payload = wr.invoke()
        decoder = json.JSONDecoder()
        data = decoder.raw_decode(payload)

        return data


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
