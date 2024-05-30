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

from testcontainers.core.container import DockerContainer

from cratedb_toolkit.util.data import asbool

logger = logging.getLogger(__name__)


class ExtendedDockerContainer(DockerContainer):
    """
    An extended Testcontainer.

    - Provide convenience methods for getting the Docker-internal endpoint address.
      TODO: Maybe rename to `get_bridge_host_*`?
    """

    def get_real_host_ip(self) -> str:
        """
        To let containers talk to each other, explicitly provide the real IP address
        of the container. In corresponding jargon, it appears to be the "bridge IP".
        """
        return self.get_docker_client().bridge_ip(self._container.id)

    def get_real_host_address(self) -> str:
        """
        Provide Docker-internal full endpoint address `<host>:<port>` of the service.
        For example, `172.17.0.4:9000`.
        """
        return f"{self.get_real_host_ip()}:{self.port_to_expose}"


class KeepaliveContainer(DockerContainer):
    """
    Improved `start()`/`stop()` methods, supporting service-keepalive.

    In order to keep the service running where it normally would be torn down,
    define the `TC_KEEPALIVE` environment variable.
    """

    KEEPALIVE = asbool(os.environ.get("TC_KEEPALIVE", False))

    def __init__(
        self,
        *args,
        **kwargs,
    ) -> None:
        self.keepalive = self.KEEPALIVE
        if "keepalive" in kwargs:
            self.keepalive = kwargs["keepalive"]
            del kwargs["keepalive"]
        super().__init__(*args, **kwargs)

    def start(self):
        """
        Improved `start()` method, supporting service-keepalive.

        In order to keep the service running where it normally would be torn down,
        define the `CRATEDB_KEEPALIVE` or `TC_KEEPALIVE` environment variables.
        """

        self._configure()

        if self._name is None:
            raise ValueError(
                "KeepaliveContainer does not support unnamed containers. Use `.with_name()` to assign a name."
            )

        docker_client = self.get_docker_client()

        # Check if container is already running, and whether it should be reused.
        logger.info(f"Searching for container: {self._name}")
        containers = docker_client.client.api.containers(all=True, filters={"name": self._name})

        if not containers:
            logger.info(f"Pulling image: {self.image}")
            docker_client.client.images.pull(self.image)
            logger.info(f"Creating container from image: {self.image}")
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
            logger.info(f"Container created: {self._container.name}")
        else:
            container_id = containers[0]["Id"]
            container_names = containers[0]["Names"]
            logger.info(f"Found container for reuse: {container_id} ({container_names})")
            self._container = docker_client.client.containers.get(container_id)
            container_name = self._container.name
            if self._container.status != "running":
                logger.info(f"Starting container: {container_id} ({container_name})")
                self._container.start()

        self._connect()
        return self

    def stop(self, **kwargs):
        """
        Shut down container again, unless "keepalive" is enabled.
        """
        if not self.keepalive:
            logger.info("Stopping container")
            return super().stop()
        return None
