import abc
import dataclasses
import json
import logging
import typing as t
from abc import abstractmethod

from cratedb_toolkit.api.guide import GuidingTexts
from cratedb_toolkit.cluster.util import get_cluster_info
from cratedb_toolkit.exception import OperationFailed
from cratedb_toolkit.io.croud import CloudIo
from cratedb_toolkit.model import ClusterInformation, DatabaseAddress, TableAddress, WebResource
from cratedb_toolkit.util.croud import CroudException

logger = logging.getLogger(__name__)


class ClusterBase(abc.ABC):
    @abstractmethod
    def load_table(self, resource: WebResource, target: TableAddress):
        raise NotImplementedError("Child class needs to implement this method")


@dataclasses.dataclass
class ManagedCluster(ClusterBase):
    cloud_id: str
    address: t.Optional[DatabaseAddress] = None
    info: t.Optional[ClusterInformation] = None

    def load_table(self, resource: WebResource, target: TableAddress):
        """
        export CRATEDB_CLOUD_CLUSTER_ID=95998958-4d96-46eb-a77a-a894e7dde128
        ctk load table https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_weather.csv.gz

        https://console.cratedb.cloud
        """
        cluster_info = get_cluster_info(cluster_id=self.cloud_id)
        cio = CloudIo(cluster_id=self.cloud_id)

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
            else:
                msg = f"Data loading failed: {texts.error()}"
                logger.error(msg)
                raise OperationFailed(msg)

        # When exiting so, it is expected that error logging has taken place appropriately.
        except CroudException as ex:
            msg = "Data loading failed: Unknown error"
            logger.exception(msg)
            raise OperationFailed(msg) from ex


@dataclasses.dataclass
class StandaloneCluster(ClusterBase):
    address: DatabaseAddress
    info: t.Optional[ClusterInformation] = None

    def load_table(self, resource: WebResource, target: TableAddress):
        """
        Synopsis
        --------
        export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo

        ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
        ctk load table mongodb://localhost:27017/testdrive/demo
        """
        source_url = resource.url
        target_url = self.address.dburi
        if source_url.startswith("influxdb"):
            from cratedb_toolkit.io.influxdb import influxdb_copy

            source_url = source_url.replace("influxdb2://", "http://")
            if not influxdb_copy(source_url, target_url, progress=True):
                msg = "Data loading failed"
                logger.error(msg)
                raise OperationFailed(msg)
        elif source_url.startswith("mongodb"):
            from cratedb_toolkit.io.mongodb.api import mongodb_copy

            if not mongodb_copy(source_url, target_url, progress=True):
                msg = "Data loading failed"
                logger.error(msg)
                raise OperationFailed(msg)
        else:
            raise NotImplementedError("Importing resource not implemented yet")
