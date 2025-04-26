import dataclasses
import json
import logging
import sys
import time
import typing as t
from functools import lru_cache
from pathlib import Path

import click
from boltons.urlutils import URL

from cratedb_toolkit.api.guide import GuidingTexts
from cratedb_toolkit.api.model import ClientBundle, ClusterBase
from cratedb_toolkit.cluster.croud import CloudManager
from cratedb_toolkit.cluster.model import ClusterInformation
from cratedb_toolkit.config import CONFIG
from cratedb_toolkit.exception import CroudException, OperationFailed
from cratedb_toolkit.io.croud import CloudJob
from cratedb_toolkit.model import DatabaseAddress, InputOutputResource, TableAddress
from cratedb_toolkit.util.data import asbool
from cratedb_toolkit.util.database import DatabaseAdapter
from cratedb_toolkit.util.runtime import flexfun
from cratedb_toolkit.util.setting import (
    Setting,
    check_mutual_exclusiveness,
    obtain_settings,
)

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ManagedClusterSettings:
    """
    Settings for managing a CrateDB Cloud cluster.
    """

    cluster_id: t.Union[str, None] = None
    cluster_name: t.Union[str, None] = None
    subscription_id: t.Union[str, None] = None
    organization_id: t.Union[str, None] = None
    username: t.Union[str, None] = None
    password: t.Union[str, None] = None

    settings_spec = [
        Setting(
            click=click.Option(
                param_decls=["--cluster-id"],
                envvar="CRATEDB_CLOUD_CLUSTER_ID",
                help="CrateDB Cloud cluster identifier (UUID)",
            ),
            group="cluster-identifier",
        ),
        Setting(
            click=click.Option(
                param_decls=["--cluster-name"],
                envvar="CRATEDB_CLOUD_CLUSTER_NAME",
                help="CrateDB Cloud cluster name",
            ),
            group="cluster-identifier",
        ),
        Setting(
            click=click.Option(
                param_decls=["--subscription-id"],
                envvar="CRATEDB_CLOUD_SUBSCRIPTION_ID",
                help="CrateDB Cloud subscription identifier (UUID). Optionally needed for deploying clusters.",
            ),
        ),
        Setting(
            click=click.Option(
                param_decls=["--organization-id"],
                envvar="CRATEDB_CLOUD_ORGANIZATION_ID",
                help="CrateDB Cloud organization identifier (UUID). Optionally needed for deploying clusters.",
            ),
        ),
        Setting(
            click=click.Option(
                param_decls=["--username"],
                envvar="CRATEDB_USERNAME",
                help="Username for connecting to CrateDB.",
            ),
        ),
        Setting(
            click=click.Option(
                param_decls=["--password"],
                envvar="CRATEDB_PASSWORD",
                help="Password for connecting to CrateDB.",
            ),
        ),
    ]

    @classmethod
    def from_cli_or_env(cls):
        settings = obtain_settings(specs=cls.settings_spec)
        check_mutual_exclusiveness(specs=cls.settings_spec, settings=settings)
        return cls(**settings)


class ManagedCluster(ClusterBase):
    """
    Manage a CrateDB database cluster on CrateDB Cloud.
    """

    def __init__(
        self,
        id: str = None,  # noqa: A002
        name: str = None,
        settings: ManagedClusterSettings = None,
        address: DatabaseAddress = None,
        info: ClusterInformation = None,
    ):
        self.id = id
        self.name = name
        self.settings = settings or ManagedClusterSettings()
        self.address = address
        self.info: ClusterInformation = info or ClusterInformation()
        self.exists: bool = False

        # Default settings and sanity checks.
        self.id = self.id or self.settings.cluster_id
        self.name = self.name or self.settings.cluster_name
        if self.id is None and self.name is None:
            raise ValueError("Failed to address cluster: Either cluster identifier or name needs to be specified")

        self.cm = CloudManager()

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
        if not CONFIG.settings_accept_cli and not CONFIG.settings_accept_env:
            raise ValueError(
                "Unable to obtain cluster identifier or name without accepting settings from user environment"
            )

        settings = ManagedClusterSettings.from_cli_or_env()
        try:
            return cls(settings=settings)

        # TODO: With `flexfun`, can this section be improved?
        except ValueError as ex:
            logger.error(f"Failed to address cluster: {ex}")
            if CONFIG.settings_errors == "exit":
                sys.exit(1)
            else:
                raise

    def stop(self) -> "ManagedCluster":
        raise NotImplementedError("Stopping cluster not implemented yet")
        return self

    def delete(self) -> "ManagedCluster":
        raise NotImplementedError("Deleting cluster not implemented yet")
        return self

    def probe(self) -> "ManagedCluster":
        """
        Probe a CrateDB Cloud cluster, API-wise.

        TODO: Investigate callers, and reduce number of invocations.
        """
        try:
            self.info = ClusterInformation.from_id_or_name(cluster_id=self.id, cluster_name=self.name)
            self.id = self.info.cloud["id"]
            self.name = self.info.cloud["name"]
        except (CroudException, ValueError) as ex:
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
        If the cluster does not exist, deploy it first.

        Command: ctk cluster start
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
        if self.exists:
            suspended = self.info.cloud.get("suspended")
            if suspended is True:
                logger.info(f"Cluster is suspended, resuming it: id={self.id}, name={self.name}")
                self.resume()
            elif suspended is False:
                logger.info(f"Cluster is running: id={self.id}, name={self.name}")
            else:
                raise CroudException(f"Cluster in unknown state: {self.name}")
        else:
            logger.info(f"Cluster does not exist, deploying it: id={self.id}, name={self.name}")
            self.deploy()
            logger.info(f"Cluster deployed: id={self.id}, name={self.name}")
            self.probe()
            if not self.exists:
                # TODO: Is it possible to gather and propagate more information why the deployment failed?
                raise CroudException(f"Deployment of cluster failed: {self.name}")
        return self

    def deploy(self) -> "ManagedCluster":
        """
        Run the cluster deployment procedure.

        Command: ctk cluster start
        """
        # FIXME: Accept id or name.
        if self.name is None:
            raise ValueError("Need cluster name to deploy")
        # FIXME: Only create new project when needed. Otherwise, use existing project.
        project = self.cm.create_project(name=self.name, organization_id=self.settings.organization_id)
        project_id = project["id"]
        logger.info(f"Created project: {project_id}")
        cluster_info = self.cm.deploy_cluster(
            name=self.name, project_id=project_id, subscription_id=self.settings.subscription_id
        )

        # Wait a bit to let the deployment settle, mostly to work around DNS propagation issues.
        time.sleep(3.25)

        return cluster_info

    def resume(self) -> "ManagedCluster":
        """
        Resume a database cluster.

        Command: ctk cluster start
        """
        if self.id is None:
            raise ValueError("Need cluster identifier to resume")
        logger.info(f"Resuming CrateDB Cloud Cluster: id={self.id}, name={self.name}")
        self.cm.resume_cluster(identifier=self.id)
        self.probe()
        return self

    def suspend(self) -> "ManagedCluster":
        """
        Suspend a database cluster.

        Command: ctk cluster suspend
        """
        if self.id is None:
            raise ValueError("Need cluster identifier to suspend")
        logger.info(f"Suspending CrateDB Cloud Cluster: id={self.id}, name={self.name}")
        self.cm.suspend_cluster(identifier=self.id)
        self.probe()
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
                cluster_info=self.info,
                job=cloud_job,
            )
            if cloud_job.success:
                logger.info("Data loading was successful.\n%s\n", texts.success())
                return cloud_job
            else:
                # TODO: Add "reason" to exception message.
                message = f"Data loading failed.\n{cloud_job.message}"
                logger.error(f"{message}\n{texts.error()}\n")
                raise OperationFailed(message)

        # When exiting so, it is expected that error logging has taken place appropriately.
        except CroudException as ex:
            msg = "Data loading failed: Unknown error"
            logger.exception(msg)
            raise OperationFailed(msg) from ex

    @lru_cache(maxsize=1)  # noqa: B019
    def get_client_bundle(self, username: str = None, password: str = None) -> ClientBundle:
        """
        Return a bundle of client handles to the CrateDB Cloud cluster database.

        - adapter: A high-level `DatabaseAdapter` instance, offering a few convenience methods.
        - dbapi: A DBAPI connection object, as provided by SQLAlchemy's `dbapi_connection`.
        - sqlalchemy: An SQLAlchemy `Engine` object.
        """
        cratedb_http_url = self.info.cloud["url"]
        logger.info(f"Connecting to database cluster at: {cratedb_http_url}")
        if username is None:
            username = self.settings.username
        if password is None:
            password = self.settings.password
        address = DatabaseAddress.from_httpuri(cratedb_http_url)
        address.with_credentials(username=username, password=password)
        adapter = DatabaseAdapter(address.dburi)
        return ClientBundle(
            adapter=adapter,
            dbapi=adapter.connection.connection.dbapi_connection,
            sqlalchemy=adapter.engine,
        )

    def query(self, sql: str):
        """
        Shortcut method to submit a database query in SQL format, and retrieve the results.
        """
        client_bundle = self.get_client_bundle()
        return client_bundle.adapter.run_sql(sql, records=True)


@dataclasses.dataclass
class StandaloneCluster(ClusterBase):
    """
    Wrap a standalone CrateDB database cluster.
    """

    address: DatabaseAddress
    info: t.Optional[ClusterInformation] = None

    def load_table(self, source: InputOutputResource, target: TableAddress, transformation: t.Union[Path, None] = None):
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
            source_url_obj.scheme.startswith("influxdb") or source_url.endswith(".lp") or source_url.endswith(".lp.gz")
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

    def get_client_bundle(self, username: str = None, password: str = None) -> ClientBundle:
        raise NotImplementedError("Not implemented for `StandaloneCluster` yet")
