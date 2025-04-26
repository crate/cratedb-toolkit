import abc
import dataclasses
import typing as t
from pathlib import Path

import crate.client
import sqlalchemy as sa

from cratedb_toolkit.model import InputOutputResource, TableAddress
from cratedb_toolkit.util.database import DatabaseAdapter


@dataclasses.dataclass
class ClientBundle:
    """
    Provide userspace with a client bundle of connections to the database.
    """

    adapter: DatabaseAdapter
    dbapi: crate.client.connection.Connection
    sqlalchemy: sa.engine.Engine


class ClusterBase(abc.ABC):
    """
    A common base class for all cluster-related functionality across CrateDB and CrateDB Cloud.
    """

    @abc.abstractmethod
    def load_table(self, source: InputOutputResource, target: TableAddress, transformation: t.Union[Path, None] = None):
        raise NotImplementedError("Child class needs to implement this method")

    @abc.abstractmethod
    def get_client_bundle(self) -> ClientBundle:
        raise NotImplementedError("Child class needs to implement this method")
