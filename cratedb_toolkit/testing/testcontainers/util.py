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
from testcontainers.core.container import DockerContainer


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
