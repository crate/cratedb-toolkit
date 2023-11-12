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
import os

from influxdb_client import InfluxDBClient
from testcontainers.core.config import MAX_TRIES
from testcontainers.core.generic import DbContainer
from testcontainers.core.waiting_utils import wait_container_is_ready, wait_for_logs

from cratedb_toolkit.testing.testcontainers.util import KeepaliveContainer


class InfluxDB2Container(KeepaliveContainer, DbContainer):
    """
    InfluxDB database container.

    - https://en.wikipedia.org/wiki/Influxdb

    Example:

        The example spins up an InfluxDB2 database instance.
    """

    INFLUXDB_VERSION = os.environ.get("INFLUXDB_VERSION", "latest")

    ORGANIZATION = "example"
    TOKEN = "token"  # noqa: S105

    # TODO: Dual-port use with 8083+8086.
    def __init__(
        self,
        image: str = f"influxdb:{INFLUXDB_VERSION}",
        port: int = 8086,
        dialect: str = "influxdb2",
        **kwargs,
    ) -> None:
        super().__init__(image=image, **kwargs)

        self._name = "testcontainers-influxdb"  # -{os.getpid()}

        self.port_to_expose = port
        self.dialect = dialect

        self.with_exposed_ports(self.port_to_expose, 8083)

        self.debug = False

    def _configure(self) -> None:
        self.with_env("DOCKER_INFLUXDB_INIT_MODE", "setup")
        self.with_env("DOCKER_INFLUXDB_INIT_USERNAME", "admin")
        self.with_env("DOCKER_INFLUXDB_INIT_PASSWORD", "secret1234")
        self.with_env("DOCKER_INFLUXDB_INIT_ORG", self.ORGANIZATION)
        self.with_env("DOCKER_INFLUXDB_INIT_BUCKET", "default")
        self.with_env("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", self.TOKEN)

    def get_connection_url(self, host=None) -> str:
        return super()._create_connection_url(
            dialect="http",
            username=self.ORGANIZATION,
            password=self.TOKEN,
            host=host,
            port=self.port_to_expose,
        )

    @wait_container_is_ready()
    def _connect(self) -> InfluxDBClient:
        # TODO: Better use a network connectivity health check?
        #       In `testcontainers-java`, there is the `HttpWaitStrategy`.
        wait_for_logs(self, predicate="Listening.*tcp-listener.*8086", timeout=MAX_TRIES)
        return InfluxDBClient(url=self.get_connection_url(), org=self.ORGANIZATION, token=self.TOKEN, debug=self.debug)

    def get_connection_client(self) -> InfluxDBClient:
        return self._connect()
