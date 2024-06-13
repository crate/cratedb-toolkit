#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
# This is for Python 3.7 and 3.8 to support generic types
# like `dict` instead of `typing.Dict
from __future__ import annotations

import logging
import os
from typing import Optional

from testcontainers.core.config import MAX_TRIES
from testcontainers.core.generic import DbContainer
from testcontainers.core.waiting_utils import wait_container_is_ready, wait_for_logs

from cratedb_toolkit.testing.testcontainers.util import KeepaliveContainer, asbool
from cratedb_toolkit.util import DatabaseAdapter

logger = logging.getLogger(__name__)


class CrateDBContainer(KeepaliveContainer, DbContainer):
    """
    CrateDB database container.

    Example:

        The example spins up a CrateDB database and connects to it using
        SQLAlchemy and its Python driver.

        .. doctest::

            >>> from tests.testcontainers.cratedb import CrateDBContainer
            >>> import sqlalchemy

            >>> cratedb_container = CrateDBContainer("crate:5.2.3")
            >>> cratedb_container.start()
            >>> with cratedb_container as cratedb:
            ...     engine = sqlalchemy.create_engine(cratedb.get_connection_url())
            ...     with engine.begin() as connection:
            ...         result = connection.execute(sqlalchemy.text("select version()"))
            ...         version, = result.fetchone()
            >>> version
            'CrateDB 5.2.3...'
    """

    CRATEDB_USER = os.environ.get("CRATEDB_USER", "crate")
    CRATEDB_PASSWORD = os.environ.get("CRATEDB_PASSWORD", "")
    CRATEDB_DB = os.environ.get("CRATEDB_DB", "doc")
    KEEPALIVE = asbool(os.environ.get("CRATEDB_KEEPALIVE", os.environ.get("TC_KEEPALIVE", False)))
    CMD_OPTS = {
        "discovery.type": "single-node",
        "node.attr.storage": "hot",
        "path.repo": "/tmp/snapshots",
    }

    def __init__(
        self,
        image: str = "crate/crate:nightly",
        ports: Optional[dict] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
        cmd_opts: Optional[dict] = None,
        **kwargs,
    ) -> None:
        """
        :param image: docker hub image path with optional tag
        :param ports: optional dict that maps a port inside the container to a port on the host machine;
                      `None` as a map value generates a random port;
                      Dicts are ordered. By convention the first key-val pair is designated to the HTTP interface.
                      Example: {4200: None, 5432: 15432} - port 4200 inside the container will be mapped
                      to a random port on the host, internal port 5432 for PSQL interface will be mapped
                      to the 15432 port on the host.
        :param user:  optional username to access the DB; if None, try `CRATEDB_USER` environment variable
        :param password: optional password to access the DB; if None, try `CRATEDB_PASSWORD` environment variable
        :param dbname: optional database name to access the DB; if None, try `CRATEDB_DB` environment variable
        :param cmd_opts: an optional dict with CLI arguments to be passed to the DB entrypoint inside the container
        :param kwargs: misc keyword arguments
        """
        super().__init__(image=image, **kwargs)

        self._name = "testcontainers-cratedb"

        cmd_opts = cmd_opts or {}
        self._command = self._build_cmd({**self.CMD_OPTS, **cmd_opts})

        self.CRATEDB_USER = user or self.CRATEDB_USER
        self.CRATEDB_PASSWORD = password or self.CRATEDB_PASSWORD
        self.CRATEDB_DB = dbname or self.CRATEDB_DB

        self.port_mapping = ports if ports else {4200: None}
        self.port_to_expose, _ = list(self.port_mapping.items())[0]

    @staticmethod
    def _build_cmd(opts: dict) -> str:
        """
        Return a string with command options concatenated and optimised for ES5 use
        """
        cmd = []
        for key, val in opts.items():
            if isinstance(val, bool):
                val = str(val).lower()
            cmd.append(f"-C{key}={val}")
        return " ".join(cmd)

    def _configure_ports(self) -> None:
        """
        Bind all the ports exposed inside the container to the same port on the host
        """
        # If host_port is `None`, a random port to be generated
        for container_port, host_port in self.port_mapping.items():
            self.with_bind_ports(container=container_port, host=host_port)

    def _configure_credentials(self) -> None:
        self.with_env("CRATEDB_USER", self.CRATEDB_USER)
        self.with_env("CRATEDB_PASSWORD", self.CRATEDB_PASSWORD)
        self.with_env("CRATEDB_DB", self.CRATEDB_DB)

    def _configure(self) -> None:
        self._configure_ports()
        self._configure_credentials()

    def get_connection_url(self, dialect: str = "crate", host: Optional[str] = None) -> str:
        """
        Return a connection URL to the DB

        :param host: optional string
        :param dialect: a string with the dialect name to generate a DB URI
        :return: string containing a connection URL to te DB
        """
        # TODO: When using `db_name=self.CRATEDB_DB`:
        #       Connection.__init__() got an unexpected keyword argument 'database'
        return super()._create_connection_url(
            dialect=dialect,
            username=self.CRATEDB_USER,
            password=self.CRATEDB_PASSWORD,
            host=host,
            port=self.port_to_expose,
        )

    @wait_container_is_ready()
    def _connect(self):
        # TODO: Better use a network connectivity health check?
        #       In `testcontainers-java`, there is the `HttpWaitStrategy`.
        # TODO: Provide a client instance.
        wait_for_logs(self, predicate="o.e.n.Node.*started", timeout=MAX_TRIES)


class CrateDBTestAdapter:
    """
    A little helper wrapping Testcontainer's `CrateDBContainer` and
    CrateDB Toolkit's `DatabaseAdapter`, agnostic of the test framework.
    """

    def __init__(self, crate_version: str = "nightly", **kwargs):
        self.cratedb: Optional[CrateDBContainer] = None
        self.database: Optional[DatabaseAdapter] = None
        self.image: str = "crate/crate:{}".format(crate_version)

    def start(self, **kwargs):
        """
        Start testcontainer, used for tests set up
        """
        self.cratedb = CrateDBContainer(image=self.image, **kwargs)
        self.cratedb.start()
        self.database = DatabaseAdapter(dburi=self.get_connection_url())

    def stop(self):
        """
        Stop testcontainer, used for tests tear down
        """
        if self.cratedb:
            self.cratedb.stop()

    def reset(self, tables: Optional[list] = None):
        """
        Drop tables from the given list, used for tests set up or tear down
        """
        if tables and self.database:
            for reset_table in tables:
                self.database.connection.exec_driver_sql(
                    f"DROP TABLE IF EXISTS {self.database.quote_relation_name(reset_table)};"
                )

    def get_connection_url(self, *args, **kwargs):
        """
        Return a URL for SQLAlchemy DB engine
        """
        if self.cratedb:
            return self.cratedb.get_connection_url(*args, **kwargs)
        return None

    def get_http_url(self, **kwargs):
        """
        Return a URL for CrateDB's HTTP endpoint
        """
        return self.get_connection_url(dialect="http", **kwargs)

    @property
    def http_url(self):
        """
        Return a URL for CrateDB's HTTP endpoint.

        Used to stay backward compatible with the downstream code.
        """
        return self.get_http_url()
