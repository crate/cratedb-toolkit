import abc
import dataclasses
import json
import logging
import typing as t
from abc import abstractmethod
from pathlib import Path

from boltons.urlutils import URL

from cratedb_toolkit.api.guide import GuidingTexts
from cratedb_toolkit.cluster.util import get_cluster_info
from cratedb_toolkit.exception import CroudException, OperationFailed
from cratedb_toolkit.model import ClusterInformation, DatabaseAddress, InputOutputResource, TableAddress
from cratedb_toolkit.util.data import asbool

logger = logging.getLogger(__name__)


class ClusterBase(abc.ABC):
    @abstractmethod
    def load_table(
        self, resource: InputOutputResource, target: TableAddress, transformation: t.Union[Path, None] = None
    ):
        raise NotImplementedError("Child class needs to implement this method")


@dataclasses.dataclass
class ManagedCluster(ClusterBase):
    """
    Wrap a managed CrateDB database cluster on CrateDB Cloud.
    """

    cloud_id: str
    address: t.Optional[DatabaseAddress] = None
    info: t.Optional[ClusterInformation] = None

    def __post_init__(self):
        logger.info(f"Connecting to CrateDB Cloud Cluster: {self.cloud_id}")

    def load_table(
        self,
        resource: InputOutputResource,
        target: t.Optional[TableAddress] = None,
        transformation: t.Union[Path, None] = None,
    ):
        """
        Load data into a database table on CrateDB Cloud.

        Synopsis
        --------
        export CRATEDB_CLOUD_CLUSTER_ID=95998958-4d96-46eb-a77a-a894e7dde128
        ctk load table https://cdn.crate.io/downloads/datasets/cratedb-datasets/cloud-tutorials/data_weather.csv.gz

        https://console.cratedb.cloud
        """
        from cratedb_toolkit.io.croud import CloudIo

        target = target or TableAddress()

        try:
            cluster_info = get_cluster_info(cluster_id=self.cloud_id)
            logger.info(
                f"Cluster information: name={cluster_info.cloud.get('name')}, url={cluster_info.cloud.get('url')}"
            )
            cio = CloudIo(cluster_id=self.cloud_id)
        except CroudException as ex:
            msg = f"Connecting to cluster resource failed: {self.cloud_id}. Reason: {ex}"
            if "Resource not found" in str(ex):
                logger.error(msg)
                return None, False
            logger.exception(msg)
            raise OperationFailed(msg) from ex

        try:
            job_info, success = cio.load_resource(resource=resource, target=target)
            logger.info("Job information:\n%s", json.dumps(job_info, indent=2))
            # TODO: Explicitly report about `failed_records`, etc.
            texts = GuidingTexts(
                admin_url=cluster_info.cloud["url"],
                table_name=job_info["destination"]["table"],
            )
            if success:
                logger.info("Data loading was successful: %s", texts.success())
                return job_info, success
            else:
                # TODO: Add "reason" to exception message.
                logger.error(f"Data loading failed: {texts.error()}")
                raise OperationFailed("Data loading failed")

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
        self, resource: InputOutputResource, target: TableAddress, transformation: t.Union[Path, None] = None
    ):
        """
        Load data into a database table on a standalone CrateDB Server.

        Synopsis
        --------
        export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo

        ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
        ctk load table mongodb://localhost:27017/testdrive/demo
        """
        source_url_obj = URL(resource.url)
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
            or resource.url.endswith(".lp")
            or resource.url.endswith(".lp.gz")
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
