import dataclasses
import datetime as dt
import json
import logging
import os
import typing as t
from pathlib import Path

from croud.clusters.commands import _wait_for_completed_operation
from croud.projects.commands import _transform_backup_location
from keyrings.cryptfile.cryptfile import CryptFileKeyring

from cratedb_toolkit.exception import CroudException
from cratedb_toolkit.meta.release import CrateDBRelease
from cratedb_toolkit.model import InputOutputResource, TableAddress
from cratedb_toolkit.util.croud import CroudCall, CroudClient, CroudWrapper

# Default to a stable version if not specified in the environment.
# TODO: Use `latest` CrateDB by default, or even `nightly`?
DEFAULT_CRATEDB_VERSION = "5.10.4"


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class CloudRootUrlGenerator:
    """
    Helper for generating CrateDB Cloud API URLs.
    """

    org_id: t.Optional[str] = None

    def with_organization(self, org_id: t.Optional[str]):
        self.org_id = org_id
        return self

    @property
    def cluster_deploy(self):
        if not self.org_id:
            raise ValueError("Organization ID is not set")
        return f"/api/v2/organizations/{self.org_id}/clusters/"

    @property
    def cluster_list(self):
        if self.org_id:
            return f"/api/v2/organizations/{self.org_id}/clusters/"
        else:
            return "/api/v2/clusters/"

    @property
    def project_list(self):
        if self.org_id:
            return f"/api/v2/organizations/{self.org_id}/projects/"
        else:
            return "/api/v2/projects/"

    @property
    def subscription_list(self):
        if self.org_id:
            return f"/api/v2/organizations/{self.org_id}/subscriptions/"
        else:
            return "/api/v2/subscriptions/"


@dataclasses.dataclass
class CloudClusterUrlGenerator:
    """
    Helper for generating CrateDB Cloud API URLs.
    """

    cluster_id: t.Optional[str] = None

    def with_cluster_id(self, cluster_id: str):
        self.cluster_id = cluster_id
        return self

    def validate(self):
        if not self.cluster_id:
            raise ValueError("Cluster ID is not set")

    @property
    def home(self):
        self.validate()
        return f"/api/v2/clusters/{self.cluster_id}/"

    @property
    def suspend(self):
        self.validate()
        return f"/api/v2/clusters/{self.cluster_id}/suspend/"

    @property
    def jwt(self):
        self.validate()
        return f"/api/v2/clusters/{self.cluster_id}/jwt/"

    @property
    def import_jobs(self):
        self.validate()
        return f"/api/v2/clusters/{self.cluster_id}/import-jobs/"


@dataclasses.dataclass
class CloudRootServices:
    """
    A wrapper around the CrateDB Cloud API through the `croud` package, providing common methods.
    """

    org_id: t.Optional[str] = None
    project_id: t.Optional[str] = None
    subscription_id: t.Optional[str] = None

    def __post_init__(self):
        self.client = CroudClient.create()
        self.url = CloudRootUrlGenerator().with_organization(self.org_id)

    def with_organization(self, org_id: t.Optional[str] = None):
        self.org_id = org_id
        return self

    def list_subscriptions(self):
        """
        Get list of subscriptions.

        croud subscriptions list --format=json
        """
        data, errors = self.client.get(self.url.subscription_list)
        if data is None:
            if not errors:
                errors = "Unknown error"
            raise CroudException(f"Getting list of subscriptions failed: {errors}")
        return data

    def list_clusters(self):
        """
        Get the list of clusters.

        ```
        croud clusters list --format=json
        ```
        """
        params = {}
        if self.project_id:
            params["project_id"] = self.project_id
        data, errors = self.client.get(self.url.cluster_list, params=params)
        if data is None:
            if not errors:
                errors = "Unknown error"
            raise CroudException(f"Getting list of clusters failed: {errors}")
        return data

    def list_projects(self):
        """
        Get the list of projects.

        croud projects list --format=json
        """
        data, errors = self.client.get(self.url.project_list)
        if data is None:
            if not errors:
                errors = "Unknown error"
            raise CroudException(f"Getting list of projects failed: {errors}")
        for item in data:
            item["backup_location"] = _transform_backup_location(item["backup_location"])
        return data

    def create_project(self, name: str, organization_id: str = None):
        """
        Create project.

        ```
        croud projects list --format=json
        croud projects create --name "foobar" -o json | jq -r '.id'
        ```
        """
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

    def get_or_create_project(self, name: str) -> str:
        """
        Get or create a project by name.

        A project is a container for clusters.
        """
        project_id = None
        try:
            projects = self.list_projects()
            for project in projects:
                if project["name"] == name:
                    project_id = project["id"]
                    logger.info(f"Using existing project: {project_id}")
                    break
        except Exception as ex:
            logger.warning(f"Error finding existing project: {ex}")

        # Create a new project if none exists.
        if not project_id:
            project = self.create_project(name=name, organization_id=self.org_id)
            project_id = project["id"]
            logger.info(f"Created project: {project_id}")

        return project_id

    def deploy_cluster(self, name: str, project_id: str, subscription_id: str = None):
        """
        Deploy cluster.

        croud clusters deploy --subscription-id $sub --project $proj  --product-name s2 --tier default --cluster-name $clustername --version $nightly  --username $user --password $pw --channel nightly -o json
        croud clusters get e1e38d92-a650-48f1-8a70-8133f2d5c400 --format=json

        """  # noqa: E501
        # TODO: Use specific subscription, or, if only one exists, use that one.
        #       Alternatively, acquire value from user environment.
        # TODO: Auto-generate cluster name when not given.
        # TODO: Obtain more parameters, like `--org-id`, `--unit`, etc.
        # TODO: What about adding/forwarding `--sudo`?
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
        cratedb_cloud_tier = os.environ.get("CRATEDB_CLOUD_TIER", "default")
        cratedb_cloud_product = os.environ.get("CRATEDB_CLOUD_PRODUCT", "crfree")  # s2, etc.
        cratedb_channel = os.environ.get("CRATEDB_CHANNEL", "stable")
        cratedb_version = os.environ.get("CRATEDB_VERSION", DEFAULT_CRATEDB_VERSION)
        username = os.environ.get("CRATEDB_USERNAME")
        password = os.environ.get("CRATEDB_PASSWORD")

        if not username or not password:
            raise ValueError(
                "Username and password must be set using the environment variables "
                "`CRATEDB_USERNAME` and `CRATEDB_PASSWORD`. These are required for "
                "accessing a CrateDB Cloud cluster after deployment."
            )

        # Optionally, select CrateDB Nightly.
        if cratedb_version == "nightly":
            cratedb_channel = "nightly"
            cratedb_version = CrateDBRelease().nightly.cloud_version
        logger.info(f"Using CrateDB version: {cratedb_version} (channel: {cratedb_channel})")

        call = CroudCall(
            fun=clusters_deploy,
            specs=command_tree["clusters"]["commands"]["deploy"]["extra_args"],
            arguments=[
                "--subscription-id",
                subscription_id,
                "--project-id",
                project_id,
                "--tier",
                cratedb_cloud_tier,
                "--product-name",
                cratedb_cloud_product,
                "--cluster-name",
                name,
                "--channel",
                cratedb_channel,
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


@dataclasses.dataclass
class CloudClusterServices:
    """
    A wrapper around the CrateDB Cloud API through the `croud` package, providing methods specific to a cluster.
    """

    cluster_id: str

    def __post_init__(self):
        self.client = CroudClient.create()
        self.url = CloudClusterUrlGenerator().with_cluster_id(self.cluster_id)

    def info(self):
        """
        Get cluster information.

        ctk cluster info
        croud clusters get e1e38d92-a650-48f1-8a70-8133f2d5c400 --format=json
        """
        data, errors = self.client.get(self.url.home)
        if data is None:
            if not errors:
                errors = "Unknown error"
            raise CroudException(f"Getting cluster information failed: {errors}")
        return data

    def suspend(self):
        return self.set_suspended({"suspended": True})

    def resume(self):
        return self.set_suspended({"suspended": False})

    def set_suspended(self, body: t.Dict[str, t.Any]):
        data, errors = self.client.put(self.url.suspend, body=body)
        if data is None:
            if not errors:
                errors = "Unknown error"
            raise CroudException(f"Suspending or resuming cluster failed: {errors}")
        _wait_for_completed_operation(
            client=self.client,
            cluster_id=self.cluster_id,
            request_params={"type": "SUSPEND", "limit": 1},
        )
        return data

    def list_jobs(self):
        """
        croud clusters import-jobs list
        """
        data, errors = self.client.get(self.url.import_jobs)
        if data is None:
            if not errors:
                errors = "Unknown error"
            raise CroudException(f"Getting list of import jobs failed: {errors}")
        return data

    def create_import_job(self, resource: InputOutputResource, target: TableAddress) -> t.Dict[str, t.Any]:
        """
        croud clusters import-jobs create
        """
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

    def get_jwt_token(self) -> t.Dict[str, str]:
        """
        Retrieve per-cluster JWT token from keyring, falling back to API.
        """
        kr = CryptFileKeyring()
        kr.keyring_key = os.getenv("CTK_KEYRING_CRYPTFILE_PASSWORD") or "TruaframEkEk"
        key = "ctk-cluster-jwt"
        data = None
        data_raw = kr.get_password(key, self.cluster_id)
        if data_raw is not None:
            data = json.loads(data_raw)
            now = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat()
            if now > data["expiry"]:
                logger.info("JWT token expired")
                data = None
        if data is None:
            data = self._get_jwt_token()
            kr.set_password(key, self.cluster_id, json.dumps(data))
        return data

    def _get_jwt_token(self) -> t.Dict[str, str]:
        """
        Retrieve per-cluster JWT token.
        """
        logger.info("Retrieving JWT token from API")
        data, errors = self.client.get(self.url.jwt)
        if data is None:
            if not errors:
                errors = "Unknown error"
            raise CroudException(f"Getting JWT token failed: {errors}")
        return data
