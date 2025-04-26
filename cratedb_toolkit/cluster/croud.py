import json
import os
import typing as t
from pathlib import Path

from cratedb_toolkit.exception import CroudException
from cratedb_toolkit.model import InputOutputResource, TableAddress
from cratedb_toolkit.util.croud import CroudCall, CroudWrapper

# Default to a stable version if not specified in the environment.
# TODO: Use `latest` CrateDB by default, or even `nightly`?
DEFAULT_CRATEDB_VERSION = "5.10.4"


class CloudManager:
    """
    A wrapper around the CrateDB Cloud API through the `croud` package, providing common methods.
    """

    def list_subscriptions(self):
        """
        Get list of subscriptions.

        croud subscriptions list --format=json
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
        Get list of clusters.

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

    def list_projects(self):
        """
        Get list of projects.

        croud projects list --format=json
        """
        from croud.parser import Argument
        from croud.projects.commands import projects_list

        call = CroudCall(
            fun=projects_list,
            specs=[
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

        arguments = ["--name", name]

        if organization_id is not None:
            arguments += ["--org-id", organization_id]

        call = CroudCall(
            fun=project_create,
            specs=command_tree["projects"]["commands"]["create"]["extra_args"],
            arguments=arguments,
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

        # Automatically use a subscription if there is only a single one. Otherwise, croak.
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

        # TODO: Add documentation about those environment variables.
        cratedb_version = os.environ.get("CRATEDB_VERSION", DEFAULT_CRATEDB_VERSION)
        username = os.environ.get("CRATEDB_USERNAME")
        password = os.environ.get("CRATEDB_PASSWORD")

        if not username or not password:
            raise ValueError(
                "Username and password must be set using the environment variables "
                "`CRATEDB_USERNAME` and `CRATEDB_PASSWORD`. These are required for "
                "accessing a CrateDB Cloud cluster."
            )

        call = CroudCall(
            fun=clusters_deploy,
            specs=command_tree["clusters"]["commands"]["deploy"]["extra_args"],
            arguments=[
                "--subscription-id",
                subscription_id,
                "--project-id",
                project_id,
                "--tier",
                "default",
                "--product-name",
                "crfree",
                "--cluster-name",
                name,
                "--version",
                cratedb_version,
                "--username",
                username,
                "--password",
                password,
            ],
        )

        wr = CroudWrapper(call=call, decode_output=False)
        return wr.invoke_safedecode()

    def resume_cluster(self, identifier: str):
        """
        Resume cluster.
        """
        from croud.__main__ import command_tree
        from croud.clusters.commands import clusters_set_suspended

        call = CroudCall(
            fun=clusters_set_suspended,
            specs=command_tree["clusters"]["commands"]["set-suspended-state"]["extra_args"],
            arguments=[
                "--cluster-id",
                identifier,
                "--value",
                "false",
            ],
        )

        wr = CroudWrapper(call=call, decode_output=False)
        return wr.invoke_safedecode()

    def suspend_cluster(self, identifier: str):
        """
        Suspend cluster.
        """
        from croud.__main__ import command_tree
        from croud.clusters.commands import clusters_set_suspended

        call = CroudCall(
            fun=clusters_set_suspended,
            specs=command_tree["clusters"]["commands"]["set-suspended-state"]["extra_args"],
            arguments=[
                "--cluster-id",
                identifier,
                "--value",
                "true",
            ],
        )

        wr = CroudWrapper(call=call, decode_output=False)
        try:
            return wr.invoke_safedecode()
        except CroudException as e:
            if "This cluster is already suspended" in str(e):
                pass
            else:
                raise


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
                "--cluster-id",
                self.cluster_id,
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

        if self.cluster_id is None:
            raise ValueError("Cluster ID is not set")
        if resource.url is None:
            raise ValueError("Source URL is not set")
        if resource.format is None:
            raise ValueError("Source format is not set")
        if target.table is None:
            raise ValueError("Target table is not set")

        # Compute command-line arguments for invoking `croud`.
        # TODO: Sanitize table name - which characters are allowed?
        is_remote = "://" in resource.url
        if not is_remote and Path(resource.url).exists():
            specs.append(file_path_argument)
            specs.append(file_id_argument)
            arguments = [
                "--cluster-id",
                self.cluster_id,
                "--file-path",
                resource.url,
                "--table",
                target.table,
                "--file-format",
                resource.format,
            ]
            fun = import_jobs_create_from_file
        else:
            specs.append(url_argument)
            arguments = [
                "--cluster-id",
                self.cluster_id,
                "--url",
                resource.url,
                "--table",
                target.table,
                "--file-format",
                resource.format,
            ]
            fun = import_jobs_create_from_url

        if resource.compression is not None:
            arguments += ["--compression", resource.compression]

        call = CroudCall(
            fun=fun,
            specs=specs,
            arguments=arguments,
        )

        wr = CroudWrapper(call=call)
        return wr.invoke()
