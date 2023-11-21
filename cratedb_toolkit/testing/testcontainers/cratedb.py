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
        "cluster.routing.allocation.disk.threshold_enabled": False,
        "node.attr.storage": "hot",
        "path.repo": "/tmp/snapshots",
    }

    def __init__(
        self,
        image: str = "crate/crate:nightly",
        port: int = 4200,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
        dialect: str = "crate",
        cmd_opts: Optional[dict] = None,
        extra_ports: Optional[list] = None,
        **kwargs,
    ) -> None:
        super().__init__(image=image, **kwargs)

        self._name = "testcontainers-cratedb"  # -{os.getpid()}

        cmd_opts = cmd_opts if cmd_opts else {}
        self._command = self._build_cmd({**self.CMD_OPTS, **cmd_opts})

        self.CRATEDB_USER = user or self.CRATEDB_USER
        self.CRATEDB_PASSWORD = password or self.CRATEDB_PASSWORD
        self.CRATEDB_DB = dbname or self.CRATEDB_DB

        self.port_to_expose = port
        self.extra_ports = extra_ports or []
        self.dialect = dialect

    @staticmethod
    def _build_cmd(opts: dict) -> str:
        """
        Return a string with command options concatenated and optimised for ES5 use
        """
        cmd = []
        for key, val in opts.items():
            if isinstance(val, bool):
                val = str(val).lower()
            cmd.append("-C{}={}".format(key, val))
        return " ".join(cmd)

    def _configure(self) -> None:
        ports = [*[self.port_to_expose], *self.extra_ports]
        self.with_exposed_ports(*ports)
        self.with_env("CRATEDB_USER", self.CRATEDB_USER)
        self.with_env("CRATEDB_PASSWORD", self.CRATEDB_PASSWORD)
        self.with_env("CRATEDB_DB", self.CRATEDB_DB)

    def get_connection_url(self, host=None, dialect=None) -> str:
        # TODO: When using `db_name=self.CRATEDB_DB`:
        #       Connection.__init__() got an unexpected keyword argument 'database'
        return super()._create_connection_url(
            dialect=dialect or self.dialect,
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


class TestDrive:
    """
    Use different schemas for storing the subsystem database tables, and the
    test/example data, so that they do not accidentally touch the default `doc`
    schema.
    """

    EXT_SCHEMA = "testdrive-ext"
    DATA_SCHEMA = "testdrive-data"

    RESET_TABLES = [
        f'"{EXT_SCHEMA}"."retention_policy"',
        f'"{DATA_SCHEMA}"."raw_metrics"',
        f'"{DATA_SCHEMA}"."sensor_readings"',
        f'"{DATA_SCHEMA}"."testdrive"',
        f'"{DATA_SCHEMA}"."foobar"',
        f'"{DATA_SCHEMA}"."foobar_unique_single"',
        f'"{DATA_SCHEMA}"."foobar_unique_composite"',
        # cratedb_toolkit.io.{influxdb,mongodb}
        '"testdrive"."demo"',
    ]


class CrateDBFixture:
    """
    A little helper wrapping Testcontainer's `CrateDBContainer` and
    CrateDB Toolkit's `DatabaseAdapter`, agnostic of the test framework.
    """

    def __init__(self, crate_version: str = "nightly"):
        self.cratedb: Optional[CrateDBContainer] = None
        self.image: str = "crate/crate:{}".format(crate_version)
        self.database: Optional[DatabaseAdapter] = None
        self.setup()

    def setup(self):
        self.cratedb = CrateDBContainer(image=self.image)
        self.cratedb.start()
        self.database = DatabaseAdapter(dburi=self.get_connection_url())

    def finalize(self):
        if self.cratedb:
            self.cratedb.stop()

    def reset(self, tables: Optional[str] = None):
        if tables and self.database:
            for reset_table in tables:
                self.database.connection.exec_driver_sql(f"DROP TABLE IF EXISTS {reset_table};")

    def get_connection_url(self, *args, **kwargs):
        if self.cratedb:
            return self.cratedb.get_connection_url(*args, **kwargs)
        return None

    @property
    def http_url(self):
        """
        Return a URL for HTTP interface
        """
        return self.get_connection_url(dialect="http")
