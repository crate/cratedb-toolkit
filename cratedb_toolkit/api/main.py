import abc
import dataclasses
import json
import logging
import sys
import typing as t
from abc import abstractmethod
from pathlib import Path

from boltons.urlutils import URL

from cratedb_toolkit.api.guide import GuidingTexts
from cratedb_toolkit.cluster.util import deploy_cluster, get_cluster_by_name, get_cluster_info
from cratedb_toolkit.config import CONFIG
from cratedb_toolkit.exception import CroudException, OperationFailed
from cratedb_toolkit.io.croud import CloudJob
from cratedb_toolkit.model import ClusterInformation, DatabaseAddress, InputOutputResource, TableAddress
from cratedb_toolkit.util.data import asbool
from cratedb_toolkit.util.runtime import flexfun
from cratedb_toolkit.util.setting import RequiredMutuallyExclusiveSettingsGroup, Setting

logger = logging.getLogger(__name__)


class ClusterBase(abc.ABC):
    @abstractmethod
    def load_table(
        self, source: InputOutputResource, target: TableAddress, transformation: t.Union[Path, None] = None
    ):
        raise NotImplementedError("Child class needs to implement this method")


class ManagedCluster(ClusterBase):
    """
    Wrap a managed CrateDB database cluster on CrateDB Cloud.
    """

    settings_spec = RequiredMutuallyExclusiveSettingsGroup(
        Setting(
            name="--cluster-id",
            envvar="CRATEDB_CLOUD_CLUSTER_ID",
            help="The CrateDB Cloud cluster identifier, an UUID",
        ),
        Setting(
            name="--cluster-name",
            envvar="CRATEDB_CLOUD_CLUSTER_NAME",
            help="The CrateDB Cloud cluster name",
        ),
    )

    def __init__(
        self,
        id: str = None,  # noqa: A002
        name: str = None,
        address: DatabaseAddress = None,
        info: ClusterInformation = None,
    ):
        self.id = id
        self.name = name
        self.address = address
        self.info: ClusterInformation = info or ClusterInformation()
        self.exists: bool = False
        if self.id is None and self.name is None:
            raise ValueError("Failed to address cluster: Either cluster identifier or name needs to be specified")

    @classmethod
    @flexfun(domain="settings")
    def from_env(cls) -> "ManagedCluster":
        """
        Obtain CrateDB Cloud cluster identifier or name from user environment.
        The settings are mutually exclusive.

        When the toolkit environment is configured with `settings_accept_cli`,
        the settings can be specified that way:

            --cluster-id=e1e38d92-a650-48f1-8a70-8133f2d5c400
            --cluster-name=Hotzenplotz

        When the toolkit environment is configured with `settings_accept_env`,
        the settings can be specified that way:

            export CRATEDB_CLOUD_CLUSTER_ID=e1e38d92-a650-48f1-8a70-8133f2d5c400
            export CRATEDB_CLOUD_CLUSTER_NAME=Hotzenplotz
        """
        if not CONFIG.settings_accept_cli or not CONFIG.settings_accept_env:
            raise ValueError(
                "Unable to obtain cluster identifier or name without accepting settings from user environment"
            )
        try:
            cluster_id, cluster_name = cls.settings_spec.obtain_settings()
            return cls(id=cluster_id, name=cluster_name)

        # TODO: With `flexfun`, can this section be improved?
        except ValueError as ex:
            logger.error(f"Failed to address cluster: {ex}")
            if CONFIG.settings_errors == "exit":
                sys.exit(1)
            else:
                raise

    def stop(self) -> "ManagedCluster":
        logger.warning("Stopping cluster not implemented yet")
        return self

    def delete(self) -> "ManagedCluster":
        return self

    def probe(self) -> "ManagedCluster":
        """
        Probe a CrateDB Cloud cluster, API-wise.
        """
        try:
            if self.id:
                self.info = get_cluster_info(cluster_id=self.id)
                self.name = self.info.cloud["name"]
            elif self.name:
                self.info = get_cluster_by_name(self.name)
                self.id = self.info.cloud["id"]
            else:
                self.exists = False
                raise ValueError("Failed to address cluster: Either cluster identifier or name needs to be specified")
        except CroudException as ex:
            self.exists = False
            if "Cluster not found" not in str(ex):
                raise
        if self.info.cloud:
            self.exists = True
            logger.info(f"Cluster information: name={self.info.cloud.get('name')}, url={self.info.cloud.get('url')}")
        return self

    @flexfun(domain="runtime")
    def start(self) -> "ManagedCluster":
        """
        Start a database cluster.
        When cluster does not exist, acquire/deploy it.
        """
        logger.info(f"Deploying/starting/resuming CrateDB Cloud Cluster: id={self.id}, name={self.name}")
        self.acquire()
        return self

    def acquire(self) -> "ManagedCluster":
        """
        Acquire a database cluster.
        This means going through the steps of deploy and/or start, as applicable.

        - When cluster does not exist, create/deploy it.
        - When a cluster exists, but is stopped/hibernated, start/resume it.
        """
        self.probe()
        if not self.exists:
            logger.info(f"Cluster does not exist, deploying it: id={self.id}, name={self.name}")
            self.deploy()
            self.probe()
            if not self.exists:
                # TODO: Is it possible to gather and propagate more information why the deployment failed?
                raise CroudException(f"Deployment of cluster failed: {self.name}")
        return self

    def deploy(self) -> "ManagedCluster":
        """
        Run the cluster deployment procedure.
        """
        # FIXME: Accept id or name.
        if self.name is None:
            raise ValueError("Need cluster name to deploy")
        deploy_cluster(self.name)
        return self

    @flexfun(domain="runtime")
    def load_table(
        self,
        source: InputOutputResource,
        target: t.Optional[TableAddress] = None,
        transformation: t.Union[Path, None] = None,
    ) -> CloudJob:
        """
        Load data into a database table on CrateDB Cloud.

        Synopsis
        --------
        export CRATEDB_CLOUD_CLUSTER_ID=95998958-4d96-46eb-a77a-a894e7dde128
        ctk load table https://cdn.crate.io/downloads/datasets/cratedb-datasets/cloud-tutorials/data_weather.csv.gz

        https://console.cratedb.cloud
        """
        from cratedb_toolkit.io.croud import CloudIo

        self.probe()
        target = target or TableAddress()

        # FIXME: Accept id or name.
        if self.id is None:
            raise ValueError("Need cluster identifier to load table")

        try:
            cio = CloudIo(cluster_id=self.id)
        except CroudException as ex:
            msg = f"Connecting to cluster resource failed: {self.id}. Reason: {ex}"
            logger.exception(msg)
            raise OperationFailed(msg) from ex

        try:
            cloud_job = cio.load_resource(resource=source, target=target)
            logger.info("Job information:\n%s", json.dumps(cloud_job.info, indent=2))
            # TODO: Explicitly report about `failed_records`, etc.
            texts = GuidingTexts(
                admin_url=self.info.cloud["url"],
                table_name=cloud_job.info["destination"]["table"],
            )
            if cloud_job.success:
                logger.info("Data loading was successful: %s", texts.success())
                return cloud_job
            else:
                # TODO: Add "reason" to exception message.
                message = f"Data loading failed: {cloud_job.message}"
                logger.error(f"{message}{texts.error()}")
                raise OperationFailed(message)

            # When exiting so, it is expected that error logging has taken place appropriately.
        except CroudException as ex:
            msg = "Data loading failed: Unknown error"
            logger.exception(msg)
            raise OperationFailed(msg) from ex


@dataclasses.dataclass
class StandaloneCluster(ClusterBase):
    """
    Wrap a standalone CrateDB database cluster.
    """

    address: DatabaseAddress
    info: t.Optional[ClusterInformation] = None

    def load_table(
        self, source: InputOutputResource, target: TableAddress, transformation: t.Union[Path, None] = None
    ):
        """
        Load data into a database table on a standalone CrateDB Server.

        Synopsis
        --------
        export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo

        ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
        ctk load table mongodb://localhost:27017/testdrive/demo
        """
        source_url_obj = URL(source.url)
        source_url = source.url
        target_url = self.address.dburi

        if source_url_obj.scheme.startswith("dynamodb"):
            from cratedb_toolkit.io.dynamodb.api import dynamodb_copy

            if dynamodb_copy(str(source_url_obj), target_url, progress=True):
                return True
            else:
                logger.error("Data loading failed or incomplete")
                return False

        elif (
            source_url_obj.scheme.startswith("influxdb")
            or source_url.endswith(".lp")
            or source_url.endswith(".lp.gz")
        ):
            from cratedb_toolkit.io.influxdb import influxdb_copy

            http_scheme = "http"
            if asbool(source_url_obj.query_params.get("ssl")):
                http_scheme = "https"
            source_url_obj.scheme = source_url_obj.scheme.replace("influxdb2", http_scheme)
            if influxdb_copy(str(source_url_obj), target_url, progress=True):
                return True
            else:
                logger.error("Data loading failed or incomplete")
                return False

        elif source_url_obj.scheme.startswith("kinesis"):
            if "+cdc" in source_url_obj.scheme:
                from cratedb_toolkit.io.kinesis.api import kinesis_relay

                return kinesis_relay(str(source_url_obj), target_url)
            else:
                raise NotImplementedError("Loading full data via Kinesis not implemented yet")

        elif source_url_obj.scheme in ["file+bson", "http+bson", "https+bson", "mongodb", "mongodb+srv"]:
            if "+cdc" in source_url_obj.scheme:
                source_url_obj.scheme = source_url_obj.scheme.replace("+cdc", "")

                from cratedb_toolkit.io.mongodb.api import mongodb_relay_cdc

                return mongodb_relay_cdc(
                    source_url_obj,
                    target_url,
                    transformation=transformation,
                )
            else:
                from cratedb_toolkit.io.mongodb.api import mongodb_copy

                if mongodb_copy(
                    source_url_obj,
                    target_url,
                    transformation=transformation,
                    progress=True,
                ):
                    return True
                else:
                    logger.error("Data loading failed or incomplete")
                    return False

        else:
            raise NotImplementedError(f"Importing resource not implemented yet: {source_url_obj}")
