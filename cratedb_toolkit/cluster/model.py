import abc
import dataclasses
import logging
import typing as t
from copy import deepcopy
from pathlib import Path

import crate.client
import sqlalchemy as sa

from cratedb_toolkit.cluster.croud import CloudClusterServices, CloudRootServices
from cratedb_toolkit.exception import CroudException, DatabaseAddressMissingError
from cratedb_toolkit.model import InputOutputResource, TableAddress
from cratedb_toolkit.util.database import DatabaseAdapter

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class JwtResponse:
    expiry: str
    refresh: str
    token: str

    def get_token(self):
        # TODO: Persist token across sessions.
        # TODO: Refresh automatically when expired.
        return self.token


@dataclasses.dataclass
class ClusterInformation:
    """
    Manage a database cluster's information.
    """

    cratedb: t.Dict[str, t.Any] = dataclasses.field(default_factory=dict)
    cloud: t.Dict[str, t.Any] = dataclasses.field(default_factory=dict)

    @property
    def cloud_id(self) -> str:
        if "id" not in self.cloud:
            raise ValueError("Cloud cluster information is missing 'id' field")
        return self.cloud["id"]

    @property
    def cloud_name(self) -> str:
        if "name" not in self.cloud:
            raise ValueError("Cloud cluster information is missing 'name' field")
        return self.cloud["name"]

    @classmethod
    def from_id_or_name(
        cls,
        cluster_id: t.Optional[str] = None,
        cluster_name: t.Optional[str] = None,
    ) -> "ClusterInformation":
        """
        Look up the cluster by identifier (UUID) or name, in that order.
        """
        if cluster_id is not None:
            return cls.from_id(cluster_id=cluster_id)
        elif cluster_name is not None:
            return cls.from_name(cluster_name=cluster_name)
        else:
            raise DatabaseAddressMissingError(
                "Failed to address cluster: Either cluster identifier or name needs to be specified"
            )

    @classmethod
    def from_id(cls, cluster_id: str) -> "ClusterInformation":
        """
        Look up cluster by identifier (UUID).
        """

        cc = CloudClusterServices(cluster_id=cluster_id)
        return ClusterInformation(cloud=cc.info())

    @classmethod
    def from_name(cls, cluster_name: str) -> "ClusterInformation":
        """
        Look up cluster by name.
        """

        cm = CloudRootServices()
        cluster_list = cm.list_clusters()
        for cluster in cluster_list:
            if cluster["name"] == cluster_name:
                return ClusterInformation(cloud=cluster)
        raise CroudException(f"Cluster not found: {cluster_name}")

    def asdict(self) -> t.Dict[str, t.Any]:
        return deepcopy(dataclasses.asdict(self))

    @property
    def jwt(self) -> JwtResponse:
        """
        Return per-cluster JWT token response.
        """
        cc = CloudClusterServices(cluster_id=self.cloud_id)
        return JwtResponse(**cc.get_jwt_token())


@dataclasses.dataclass
class ClientBundle:
    """
    Provide userspace with a client bundle of connections to the database.
    """

    adapter: DatabaseAdapter
    dbapi: crate.client.connection.Connection
    sqlalchemy: sa.engine.Engine

    def close(self):
        """
        Close all database connections created to this cluster.
        Should be called when the cluster handle is no longer needed.
        """

        try:
            self.adapter.close()
        except Exception as e:
            logger.warning(f"Error closing database adapter: {e}")

        try:
            self.dbapi.close()
        except Exception as e:
            logger.warning(f"Error closing DBAPI connection: {e}")


class ClusterBase(abc.ABC):
    """
    A common base class for all cluster-related functionality across CrateDB and CrateDB Cloud.
    """

    def __init__(self):
        self._client_bundle = None

    @abc.abstractmethod
    def load_table(self, source: InputOutputResource, target: TableAddress, transformation: t.Union[Path, None] = None):
        """
        Load data from a source into a target table.

        Args:
            source: The source data resource
            target: The target table address
            transformation: Optional path to a transformation script or function
        """
        raise NotImplementedError("Child class needs to implement this method")

    @abc.abstractmethod
    def get_client_bundle(self) -> ClientBundle:
        raise NotImplementedError("Child class needs to implement this method")

    def close_connections(self):
        """
        Close all database connections created to this cluster.
        Should be called when the cluster handle is no longer needed.
        """
        if self._client_bundle is not None:
            self._client_bundle.close()
            self._client_bundle = None
