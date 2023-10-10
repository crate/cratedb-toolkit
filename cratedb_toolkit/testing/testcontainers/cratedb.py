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
from testcontainers.core.docker_client import DockerClient
from testcontainers.core.generic import DbContainer
from testcontainers.core.waiting_utils import wait_for_logs

logger = logging.getLogger(__name__)


class CrateDBContainer(DbContainer):
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
    CRATEDB_KEEPALIVE = os.environ.get("CRATEDB_KEEPALIVE", os.environ.get("TC_KEEPALIVE", False))

    # TODO: Dual-port use with 4200+5432.
    def __init__(
        self,
        image: str = "crate:latest",
        port: int = 4200,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
        dialect: str = "crate",
        keepalive: bool = False,
        **kwargs,
    ) -> None:
        super(CrateDBContainer, self).__init__(image=image, **kwargs)

        self._name = "testcontainers-cratedb"  # -{os.getpid()}
        self._command = "-Cdiscovery.type=single-node -Ccluster.routing.allocation.disk.threshold_enabled=false"
        # TODO: Generalize by obtaining more_opts from caller.
        self._command += " -Cnode.attr.storage=hot"
        self._command += " -Cpath.repo=/tmp/snapshots"

        self.CRATEDB_USER = user or self.CRATEDB_USER
        self.CRATEDB_PASSWORD = password or self.CRATEDB_PASSWORD
        self.CRATEDB_DB = dbname or self.CRATEDB_DB

        self.keepalive = keepalive or self.CRATEDB_KEEPALIVE
        self.port_to_expose = port
        self.dialect = dialect

        self.with_exposed_ports(self.port_to_expose)

    def _configure(self) -> None:
        self.with_env("CRATEDB_USER", self.CRATEDB_USER)
        self.with_env("CRATEDB_PASSWORD", self.CRATEDB_PASSWORD)
        self.with_env("CRATEDB_DB", self.CRATEDB_DB)

        """
        if "CI" in os.environ:
            docker_host = get_docker_host()
            self.with_env("DOCKER_HOST", docker_host).with_env("DOCKER_CERT_PATH", "").with_env("DOCKER_TLS_VERIFY", "")
        """

    def get_connection_url(self, host=None) -> str:
        # TODO: When using `db_name=self.CRATEDB_DB`:
        #       Connection.__init__() got an unexpected keyword argument 'database'
        return super()._create_connection_url(
            dialect=self.dialect,
            username=self.CRATEDB_USER,
            password=self.CRATEDB_PASSWORD,
            host=host,
            port=self.port_to_expose,
        )

    def _connect(self):
        # TODO: Better use a network connectivity health check?
        #       In `testcontainers-java`, there is the `HttpWaitStrategy`.
        wait_for_logs(self, predicate="o.e.n.Node.*started", timeout=MAX_TRIES)

    def start(self):
        """
        Improved `start()` method, supporting service-keepalive.

        In order to keep the service running where it normally would be torn down,
        define the `CRATEDB_KEEPALIVE` or `TC_KEEPALIVE` environment variables.
        """

        self._configure()

        logger.info("Pulling image %s", self.image)
        docker_client = self.get_docker_client()

        # Check if container is already running, and whether it should be reused.
        containers_running = docker_client.client.api.containers(all=True, filters={"name": self._name})
        start_container = not containers_running

        if start_container:
            logger.info("Starting CrateDB")
            self._container = docker_client.run(
                self.image,
                command=self._command,
                detach=True,
                environment=self.env,
                ports=self.ports,
                name=self._name,
                volumes=self.volumes,
                **self._kwargs,
            )
        else:
            container_id = containers_running[0]["Id"]
            self._container = docker_client.client.containers.get(container_id)

        logger.info("Container started: %s", self._container.short_id)
        self._connect()
        return self

    def stop(self, **kwargs):
        if not self.keepalive:
            logger.info("Stopping CrateDB")
            return super().stop()
        return None


def get_docker_host():
    """
    https://github.com/testcontainers/testcontainers-python/blob/main/core/tests/test_docker_in_docker.py
    """
    # real dind isn't possible (AFAIK) in CI
    # forwarding the socket to a container port is at least somewhat the same
    client = DockerClient()
    not_really_dind = client.run(
        image="alpine/socat",
        command="tcp-listen:2375,fork,reuseaddr unix-connect:/var/run/docker.sock",
        volumes={"/var/run/docker.sock": {"bind": "/var/run/docker.sock"}},
        detach=True,
    )

    not_really_dind.start()

    # get ip address for DOCKER_HOST
    # avoiding DockerContainer class here to prevent code changes affecting the test
    specs = client.get_container(not_really_dind.id)
    docker_host_ip = specs["NetworkSettings"]["Networks"]["bridge"]["IPAddress"]
    docker_host = f"tcp://{docker_host_ip}:2375"
    return docker_host
