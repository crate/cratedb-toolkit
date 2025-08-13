# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import dataclasses
import json
import logging
import time
import typing as t
from copy import deepcopy
from pathlib import Path

import click
from boltons.urlutils import URL

from cratedb_toolkit.cluster.croud import CloudClusterServices, CloudRootServices
from cratedb_toolkit.cluster.guide import DataImportGuide
from cratedb_toolkit.cluster.model import ClientBundle, ClusterBase, ClusterInformation
from cratedb_toolkit.config import CONFIG
from cratedb_toolkit.exception import (
    CroudException,
    DatabaseAddressMissingError,
    OperationFailed,
)
from cratedb_toolkit.io.ingestr.api import ingestr_copy, ingestr_select
from cratedb_toolkit.model import ClusterAddressOptions, DatabaseAddress, InputOutputResource, TableAddress
from cratedb_toolkit.util.client import jwt_token_patch
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
                envvar="CRATEDB_CLUSTER_ID",
                help="CrateDB Cloud cluster identifier (UUID)",
            ),
            group="cluster-identifier",
        ),
        Setting(
            click=click.Option(
                param_decls=["--cluster-name"],
                envvar="CRATEDB_CLUSTER_NAME",
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

    POST_DEPLOY_WAIT_ATTEMPTS = 10
    POST_DEPLOY_WAIT_PAUSE = 1

    def __init__(
        self,
        cluster_id: str = None,
        cluster_name: str = None,
        settings: ManagedClusterSettings = None,
        address: DatabaseAddress = None,
        info: ClusterInformation = None,
        stop_on_exit: bool = False,
    ):
        super().__init__()
        self.cluster_id = cluster_id
        self.cluster_name = cluster_name
        self.settings = settings or ManagedClusterSettings()
        self.address = address
        self._info: t.Optional[ClusterInformation] = info
        self.stop_on_exit = stop_on_exit
        self.exists: bool = False

        # Default settings and sanity checks.
        self.cluster_id = self.cluster_id or self.settings.cluster_id or None
        self.cluster_name = self.cluster_name or self.settings.cluster_name or None
        if not self.cluster_id and not self.cluster_name:
            raise DatabaseAddressMissingError(
                "Failed to address cluster: Either cluster identifier or name needs to be specified"
            )

        self.root = CloudRootServices()
        self.operation: t.Optional[CloudClusterServices] = None
        self._client_bundle: t.Optional[ClientBundle] = None

    def __enter__(self):
        """Enter the context manager, ensuring the cluster is running."""
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager, suspending the cluster."""
        if exc_type is not None:
            # Log the exception but still attempt to suspend the cluster
            logger.error(f"Exception occurred while shutting down: {exc_type.__name__}: {exc_val}")

        try:
            self.close_connections()
        except Exception as ex:
            logger.error(f"Failed to close connections: {ex}")
        finally:
            if self.stop_on_exit:
                try:
                    self.stop()
                    logger.info(f"Successfully stopped cluster: id={self.cluster_id}, name={self.cluster_name}")
                except Exception as ex:
                    logger.error(f"Failed to stop cluster: {ex}")

        # Don't suppress the original exception.
        return False

    @classmethod
    @flexfun(domain="settings")
    def from_env(cls, stop_on_exit: bool = False) -> "ManagedCluster":
        """
        Obtain CrateDB Cloud cluster identifier or name from the user environment.
        The settings are mutually exclusive.

        When the toolkit environment is configured with `settings_accept_cli`,
        the settings can be specified that way:

            --cluster-id='<YOUR_CLUSTER_ID_HERE>'
            --cluster-name='<YOUR_CLUSTER_NAME_HERE>'

        When the toolkit environment is configured with `settings_accept_env`,
        the settings can be specified that way:

            export CRATEDB_CLUSTER_ID='<YOUR_CLUSTER_ID_HERE>'
            export CRATEDB_CLUSTER_NAME='<YOUR_CLUSTER_NAME_HERE>'
        """
        if not CONFIG.settings_accept_cli and not CONFIG.settings_accept_env:
            raise DatabaseAddressMissingError(
                "Unable to obtain cluster identifier or name without accepting settings from user environment"
            )

        settings = ManagedClusterSettings.from_cli_or_env()
        try:
            return cls(settings=settings, stop_on_exit=stop_on_exit)

        # TODO: With `flexfun`, can this section be improved?
        except DatabaseAddressMissingError as ex:
            logger.error(f"Failed to address cluster: {ex}")
            if CONFIG.settings_errors == "exit":
                raise SystemExit(1) from ex
            raise

    def delete(self) -> "ManagedCluster":
        raise NotImplementedError("Deleting cluster not implemented yet")
        return self

    @property
    def info(self) -> ClusterInformation:
        if self._info is None:
            raise ValueError("Cluster information not yet available")
        return self._info

    def probe(self, refresh: bool = False) -> "ManagedCluster":
        """
        Probe a CrateDB Cloud cluster, API-wise.
        """
        if refresh:
            self._info = None
        if self._info is not None:
            return self
        try:
            self._info = ClusterInformation.from_id_or_name(cluster_id=self.cluster_id, cluster_name=self.cluster_name)
            self.cluster_id = self.info.cloud["id"]
            self.cluster_name = self.info.cloud["name"]
            self.address = DatabaseAddress.from_http_uri(self.info.cloud["url"])
            if self.cluster_id:
                self.operation = CloudClusterServices(cluster_id=self.cluster_id)

        except (CroudException, DatabaseAddressMissingError) as ex:
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
        logger.info(
            f"Deploying/starting/resuming CrateDB Cloud Cluster: id={self.cluster_id}, name={self.cluster_name}"
        )
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
                logger.info(f"Cluster is suspended, resuming it: id={self.cluster_id}, name={self.cluster_name}")
                self.resume()
                logger.info(f"Cluster resumed: id={self.cluster_id}, name={self.cluster_name}")
                self.probe()
            elif suspended is False:
                logger.info(f"Cluster is running: id={self.cluster_id}, name={self.cluster_name}")
            else:
                raise CroudException(f"Cluster in unknown state: {self.cluster_name}")
        else:
            logger.info(f"Cluster does not exist, deploying it: id={self.cluster_id}, name={self.cluster_name}")
            self.deploy()
            logger.info(f"Cluster deployed: id={self.cluster_id}, name={self.cluster_name}")
            self.probe()
            if not self.exists:
                # TODO: Gather and propagate more information why the deployment failed.
                #       Capture deployment logs or status if available.
                #       Possibly use details from `last_async_operation`.
                raise CroudException(f"Deployment of cluster failed: {self.cluster_name}")
        return self

    def deploy(self) -> "ManagedCluster":
        """
        Run the cluster deployment procedure.

        Command: ctk cluster start
        """
        if self.cluster_name is None:
            raise DatabaseAddressMissingError("Need cluster name to deploy")

        # Find the existing project by name (equals cluster name).
        project_id = self.root.get_or_create_project(name=self.cluster_name)

        # Deploy the cluster and retrieve cluster information.
        cluster_info = self.root.deploy_cluster(
            name=self.cluster_name, project_id=project_id, subscription_id=self.settings.subscription_id
        )

        # Wait a bit to let the deployment settle, mostly to work around DNS propagation issues.
        attempts = 0
        while not self.info.ready and attempts < self.POST_DEPLOY_WAIT_ATTEMPTS:
            attempts += 1
            self.probe()
            time.sleep(self.POST_DEPLOY_WAIT_PAUSE)

        self.probe()
        if not self.info.ready:
            raise CroudException(f"Cluster deployment failed: {self.cluster_name}")

        return cluster_info

    def resume(self) -> "ManagedCluster":
        """
        Resume a database cluster.

        Command: ctk cluster start
        """
        if self.cluster_id is None:
            raise DatabaseAddressMissingError("Need cluster identifier to resume cluster")
        logger.info(f"Resuming CrateDB Cloud Cluster: id={self.cluster_id}, name={self.cluster_name}")
        if self.operation:
            self.operation.resume()
        self.probe()
        return self

    def stop(self) -> "ManagedCluster":
        """
        Suspend a database cluster.

        Command: ctk cluster suspend
        """
        if self.cluster_id is None:
            raise DatabaseAddressMissingError("Need cluster identifier to stop cluster")
        logger.info(f"Stopping CrateDB Cloud Cluster: id={self.cluster_id}, name={self.cluster_name}")
        if self.operation:
            self.operation.suspend()
        self.probe()
        return self

    @flexfun(domain="runtime")
    def load_table(
        self,
        source: InputOutputResource,
        target: t.Optional[TableAddress] = None,
        transformation: t.Union[Path, None] = None,
    ) -> "ManagedCluster":
        """
        Load data into managed CrateDB Cloud cluster.

        Synopsis
        --------
        export CRATEDB_CLUSTER_ID=95998958-4d96-46eb-a77a-a894e7dde128
        ctk load table https://cdn.crate.io/downloads/datasets/cratedb-datasets/cloud-tutorials/data_weather.csv.gz

        https://console.cratedb.cloud
        """
        from cratedb_toolkit.io.croud import CloudIo

        self.probe()
        target = target or TableAddress()

        if self.cluster_id is None:
            raise DatabaseAddressMissingError("Need cluster identifier to load table")

        try:
            cio = CloudIo(cluster_id=self.cluster_id)
        except CroudException as ex:
            msg = f"Connecting to cluster resource failed: {self.cluster_id}. Reason: {ex}"
            logger.exception(msg)
            raise OperationFailed(msg) from ex

        try:
            cloud_job = cio.load_resource(resource=source, target=target)
            logger.info("Job information:\n%s", json.dumps(cloud_job.info, indent=2))
            # TODO: Explicitly report about `failed_records`, etc.
            texts = DataImportGuide(
                cluster_info=self.info,
                job=cloud_job,
            )
            if cloud_job.success:
                logger.info("Data loading was successful.\n%s\n", texts.success())
            else:
                # TODO: Add "reason" to exception message.
                message = f"Data loading failed.\n{cloud_job.message}"
                logger.error(f"{message}\n{texts.error()}\n")
                raise OperationFailed(message)

        # When exiting so, it is expected that error logging has taken place appropriately.
        except CroudException as ex:
            msg = f"Data loading failed: {ex}"
            logger.exception(msg)
            raise OperationFailed(msg) from ex

        return self

    @property
    def adapter(self) -> DatabaseAdapter:
        """
        Provide database adapter instance.
        """
        if self.address is None:
            raise DatabaseAddressMissingError()
        return DatabaseAdapter(dburi=self.address.dburi, jwt=self.info.jwt)

    def get_client_bundle(self, username: str = None, password: str = None) -> ClientBundle:
        """
        Return a bundle of client handles to the CrateDB Cloud cluster database.

        - adapter: A high-level `DatabaseAdapter` instance, offering a few convenience methods.
        - dbapi: A DBAPI connection object, as provided by SQLAlchemy's `dbapi_connection`.
        - sqlalchemy: An SQLAlchemy `Engine` object.
        """

        # Store the client bundle, effectively making it a singleton.
        if self._client_bundle is not None:
            return self._client_bundle

        cratedb_http_url = self.info.cloud["url"]
        logger.info(f"Connecting to database cluster at: {cratedb_http_url}")
        if username is None:
            username = self.settings.username
        if password is None:
            password = self.settings.password
        address = DatabaseAddress.from_http_uri(cratedb_http_url).with_credentials(username=username, password=password)
        adapter = DatabaseAdapter(address.dburi)
        self._client_bundle = ClientBundle(
            adapter=adapter,
            dbapi=adapter.connection.connection.dbapi_connection,
            sqlalchemy=adapter.engine,
        )
        return self._client_bundle

    def query(self, sql: str):
        """
        Shortcut method to submit a database query in SQL format, and retrieve the results.

        # TODO: Use the `records` package for invoking SQL statements.
        """
        # Ensure we have cluster connection details.
        if not self.info or not self.info.cloud.get("url"):
            self.probe()
        with jwt_token_patch(self.info.jwt.token):
            client_bundle = self.get_client_bundle()
            return client_bundle.adapter.run_sql(sql, records=True)


@dataclasses.dataclass
class StandaloneCluster(ClusterBase):
    """
    Wrap a standalone CrateDB database cluster.
    """

    address: DatabaseAddress
    info: t.Optional[ClusterInformation] = None
    exists: bool = False
    _load_table_result: t.Optional[bool] = None
    _client_bundle: t.Optional[ClientBundle] = None

    def __post_init__(self):
        super().__init__()
        if self.address is None or self.address.dburi is None:
            raise DatabaseAddressMissingError(
                "Failed to address cluster: Either SQLAlchemy or HTTP URL needs to be specified"
            )

    def probe(self) -> "StandaloneCluster":
        """
        Probe a standalone CrateDB cluster, API-wise.
        """
        try:
            client_bundle = self.get_client_bundle()
            result = client_bundle.adapter.run_sql("SELECT 42", records=True)
            self.exists = result == [{"42": 42}]
            if self.exists:
                logger.info(f"Successfully connected to standalone cluster at: {self.address.safe}")
        except Exception as ex:
            self.exists = False
            logger.debug(f"Failed to connect to standalone cluster: {ex}")
        return self

    @property
    def adapter(self) -> DatabaseAdapter:
        """
        Provide database adapter instance.
        """
        return DatabaseAdapter(dburi=self.address.dburi)

    def get_client_bundle(self, username: str = None, password: str = None) -> ClientBundle:
        """
        Return a bundle of client handles to the CrateDB Cloud cluster database.

        - adapter: A high-level `DatabaseAdapter` instance, offering a few convenience methods.
        - dbapi: A DBAPI connection object, as provided by SQLAlchemy's `dbapi_connection`.
        - sqlalchemy: An SQLAlchemy `Engine` object.
        """

        # Store the client bundle, effectively making it a singleton.
        if self._client_bundle is not None:
            return self._client_bundle

        logger.info(f"Connecting to database cluster at: {self.address}")
        address = deepcopy(self.address)
        if username is not None and password is not None:
            address = address.with_credentials(username=username, password=password)
        adapter = DatabaseAdapter(address.dburi)
        self._client_bundle = ClientBundle(
            adapter=adapter,
            dbapi=adapter.connection.connection.dbapi_connection,
            sqlalchemy=adapter.engine,
        )
        return self._client_bundle

    def query(self, sql: str):
        """
        Shortcut method to submit a database query in SQL format, and retrieve the results.

        # TODO: Use the `records` package for invoking SQL statements.
        """
        client_bundle = self.get_client_bundle()
        return client_bundle.adapter.run_sql(sql, records=True)

    def load_table(
        self, source: InputOutputResource, target: TableAddress, transformation: t.Union[Path, None] = None
    ) -> "StandaloneCluster":
        """
        Load data into unmanaged CrateDB cluster.

        Synopsis
        --------
        export CRATEDB_CLUSTER_URL=crate://crate@localhost:4200/testdrive/demo

        ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
        ctk load table mongodb://localhost:27017/testdrive/demo
        ctk load table kinesis+dms:///arn:aws:kinesis:eu-central-1:831394476016:stream/testdrive
        ctk load table kinesis+dms:///path/to/dms-over-kinesis.jsonl
        """
        source_url = source.url
        target_url = self.address.dburi
        source_url_obj = URL(source.url)

        if source_url_obj.scheme.startswith("dynamodb"):
            from cratedb_toolkit.io.dynamodb.api import dynamodb_copy

            if dynamodb_copy(str(source_url_obj), target_url, progress=True):
                self._load_table_result = True
            else:
                logger.error("Data loading failed or incomplete")
                self._load_table_result = False

        elif (
            source_url_obj.scheme.startswith("influxdb") or source_url.endswith(".lp") or source_url.endswith(".lp.gz")
        ):
            from cratedb_toolkit.io.influxdb import influxdb_copy

            http_scheme = "http"
            if asbool(source_url_obj.query_params.get("ssl")):
                http_scheme = "https"
            source_url_obj.scheme = source_url_obj.scheme.replace("influxdb2", http_scheme)
            if influxdb_copy(str(source_url_obj), target_url, progress=True):
                self._load_table_result = True
            else:
                logger.error("Data loading failed or incomplete")
                self._load_table_result = False

        elif source_url_obj.scheme.startswith("kinesis"):
            from cratedb_toolkit.io.kinesis.api import kinesis_relay

            kinesis_relay(
                source_url=source_url_obj,
                target_url=target_url,
                recipe=transformation,
            )
            self._load_table_result = True

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
                    self._load_table_result = True
                else:
                    logger.error("Data loading failed or incomplete")
                    self._load_table_result = False

        elif ingestr_select(source_url):
            if ingestr_copy(source_url, self.address, progress=True):
                self._load_table_result = True
            else:
                logger.error("Data loading failed or incomplete")
                self._load_table_result = False

        else:
            raise NotImplementedError(f"Importing resource not implemented yet: {source_url_obj}")

        return self


class DatabaseCluster:
    """
    Manage a CrateDB Cloud or standalone cluster.
    """

    @classmethod
    def from_params(cls, **params: dict) -> t.Union[ManagedCluster, StandaloneCluster]:
        """
        Factory method to create a cluster instance based on the provided parameters (Python **kwargs).
        """
        return DatabaseCluster.create(**ClusterAddressOptions.from_params(**params).asdict())

    @classmethod
    def from_options(cls, options: ClusterAddressOptions) -> t.Union[ManagedCluster, StandaloneCluster]:
        """
        Factory method to create a cluster instance based on the provided options (ClusterAddressOptions).
        """
        return DatabaseCluster.create(**options.asdict())

    @classmethod
    def create(
        cls, cluster_id: str = None, cluster_name: str = None, cluster_url: str = None
    ) -> t.Union[ManagedCluster, StandaloneCluster]:
        """
        Create the cluster instance based on the provided parameters.

        - Create a cloud-managed cluster handle when cluster ID or cluster name is provided.
        - Create a standalone cluster handle when direct connection URLs are provided.

        Args:
            cluster_id: CrateDB Cloud cluster ID
            cluster_name: CrateDB Cloud cluster name
            cluster_url: SQLAlchemy or HTTP connection URL

        Returns:
            A ManagedCluster or StandaloneCluster instance.

        Raises:
            DatabaseAddressMissingError: If no connection information is provided.
            DatabaseAddressDuplicateError: If multiple connection methods are provided.
        """

        # Cluster handle factory: Managed vs. standalone.
        cluster: t.Union[ManagedCluster, StandaloneCluster]
        if cluster_url is not None:
            address = DatabaseAddress.from_string(cluster_url)
            cluster = StandaloneCluster(address=address)
        elif cluster_id is not None or cluster_name is not None:
            cluster = ManagedCluster(cluster_id=cluster_id, cluster_name=cluster_name)
            cluster.probe()
        else:
            raise DatabaseAddressMissingError()

        return cluster
