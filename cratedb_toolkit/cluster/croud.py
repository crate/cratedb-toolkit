import json
import os
import typing as t
from pathlib import Path

from cratedb_toolkit.model import InputOutputResource, TableAddress
from cratedb_toolkit.util.croud import CroudCall, CroudWrapper


class CloudManager:
    """
    A wrapper around the CrateDB Cloud API through the `croud` package, providing common methods.
    """

    def list_subscriptions(self):
        """
        Get list of clusters

        croud clusters list --format=json
        """
        from croud.__main__ import command_tree
        from croud.subscriptions.commands import subscriptions_list

        call = CroudCall(
            fun=subscriptions_list,
            specs=command_tree["subscriptions"]["commands"]["list"]["extra_args"],
            arguments=[],
        )

        wr = CroudWrapper(call=call)
        return wr.invoke()

    def list_clusters(self):
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

    def create_project(self, name: str, organization_id: str = None):
        """
        Create project.

        croud projects list --format=json
        croud projects create --name "foobar" -o json | jq -r '.id'

        """  # noqa: E501
        # TODO: Add more parameters, like `--org-id`, `--region`.
        from croud.__main__ import command_tree
        from croud.projects.commands import project_create

        # TODO: Refactor elsewhere.
        organization_id = organization_id or os.environ.get("CRATEDB_CLOUD_ORGANIZATION_ID")

        call = CroudCall(
            fun=project_create,
            specs=command_tree["projects"]["commands"]["create"]["extra_args"],
            arguments=[
                f"--org-id={organization_id}",
                f"--name={name}",
            ],
        )

        wr = CroudWrapper(call=call)
        return wr.invoke()

    def deploy_cluster(self, name: str, project_id: str, subscription_id: str = None):
        """
        Deploy cluster.

        croud clusters deploy --subscription-id $sub --project $proj  --product-name s2 --tier default --cluster-name $clustername --version $nightly  --username $user --password $pw --channel nightly -o json
        croud clusters get e1e38d92-a650-48f1-8a70-8133f2d5c400 --format=json

        """  # noqa: E501
        # TODO: Use specific subscription, or, if only one exists, use it.
        #       Alternatively, acquire value from user environment.
        # TODO: `--product-name=crfree` is not always the right choice. ;]
        # TODO: Auto-generate cluster name when not given.
        # TODO: How to select CrateDB nightly, like `--version=nightly`?
        # TODO: Let the user provide the credentials.
        # TODO: Add more parameters, like `--org-id`, `--channel`, `--unit`, and more.
        # TODO: What about `--sudo`?
        from croud.__main__ import command_tree
        from croud.clusters.commands import clusters_deploy

        # TODO: Refactor elsewhere.
        subscription_id = subscription_id or os.environ.get("CRATEDB_CLOUD_SUBSCRIPTION_ID")

        # Automatically use subscription, if there is only a single one. Otherwise, croak.
        if subscription_id is None:
            subscriptions = self.list_subscriptions()
            if not subscriptions:
                raise ValueError("Not selecting a subscription automatically, because there are none.")
            if len(subscriptions) > 1:
                subscriptions_text = json.dumps(subscriptions, indent=2)
                raise ValueError(
                    "Not selecting a subscription automatically, because there is more than one in your "  # noqa: S608
                    "account. Please select one from this list by choosing the relevant UUID from the "
                    f"`id` attribute, and supply it to the `CRATEDB_CLOUD_SUBSCRIPTION_ID` environment "
                    f"variable:\n{subscriptions_text}"
                )
            subscription_id = subscriptions[0]["id"]

        if subscription_id is None:
            raise ValueError("Failed to obtain a subscription identifier")

        call = CroudCall(
            fun=clusters_deploy,
            specs=command_tree["clusters"]["commands"]["deploy"]["extra_args"],
            arguments=[
                f"--subscription-id={subscription_id}",
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
    A wrapper around the CrateDB Cloud API through the `croud` package, providing methods specific to a cluster.
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

    def list_jobs(self):
        from croud.clusters.commands import import_jobs_list
        from croud.parser import Argument

        call = CroudCall(
            fun=import_jobs_list,
            specs=[Argument("--cluster-id", type=str, required=True, help="The cluster the import jobs belong to.")],
            arguments=[
                f"--cluster-id={self.cluster_id}",
            ],
        )

        wr = CroudWrapper(call=call)
        return wr.invoke()

    def create_import_job(self, resource: InputOutputResource, target: TableAddress) -> t.Dict[str, t.Any]:
        from croud.__main__ import import_job_create_common_args
        from croud.clusters.commands import import_jobs_create_from_file, import_jobs_create_from_url
        from croud.parser import Argument

        specs: t.List[Argument] = import_job_create_common_args

        url_argument = Argument("--url", type=str, required=True, help="The URL the import file will be read from.")

        file_id_argument = Argument(
            "--file-id",
            type=str,
            required=False,
            help="The file ID that will be used for the "
            "import. If not specified then --file-path"
            " must be specified. "
            "Please refer to `croud organizations "
            "files` for more info.",
        )
        file_path_argument = Argument(
            "--file-path",
            type=str,
            required=False,
            help="The file in your local filesystem that "
            "will be used. If not specified then "
            "--file-id must be specified. "
            "Please note the file will become visible "
            "under `croud organizations files list`.",
        )

        # Compute command-line arguments for invoking `croud`.
        # FIXME: This call is redundant.
        path = Path(resource.url)

        # TODO: Sanitize table name. Which characters are allowed?
        if path.exists():
            specs.append(file_path_argument)
            specs.append(file_id_argument)
            arguments = [
                f"--cluster-id={self.cluster_id}",
                f"--file-path={resource.url}",
                f"--table={target.table}",
                f"--file-format={resource.format}",
            ]
            fun = import_jobs_create_from_file
        else:
            specs.append(url_argument)
            arguments = [
                f"--cluster-id={self.cluster_id}",
                f"--url={resource.url}",
                f"--table={target.table}",
                f"--file-format={resource.format}",
            ]
            fun = import_jobs_create_from_url

        if resource.compression is not None:
            arguments += [f"--compression={resource.compression}"]

        call = CroudCall(
            fun=fun,
            specs=specs,
            arguments=arguments,
        )

        wr = CroudWrapper(call=call)
        return wr.invoke()
