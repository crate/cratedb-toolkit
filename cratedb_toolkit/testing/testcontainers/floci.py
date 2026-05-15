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
import re

from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.core.waiting_utils import wait_for_logs

from cratedb_toolkit.testing.testcontainers.util import KeepaliveContainer


class FlociContainerWithKeepalive(KeepaliveContainer):
    """
    A Testcontainer for Floci with improved configurability.

    It honors the `TC_KEEPALIVE` and `FLOCI_VERSION` environment variables.

    Defining `TC_KEEPALIVE` sets a signal to not shut down the container
    after running the test cases, to speed up later invocations.

    `FLOCI_VERSION` defines the designated Floci version, which is useful when
    used within a test matrix.
    """

    FLOCI_PORT = 4566
    # Pinned to a released tag rather than `latest`. Floci is a young project;
    # bump after verifying the Kinesis and DynamoDB suites pass against the new image.
    FLOCI_VERSION = os.environ.get("FLOCI_VERSION", "1.5.11")

    def __init__(
        self,
        image: str = f"floci/floci:{FLOCI_VERSION}",
        **kwargs,
    ) -> None:
        super().__init__(image=image, **kwargs)
        self.with_name("testcontainers-floci")
        self.with_exposed_ports(self.FLOCI_PORT)

    def _configure(self):
        self.waiting_for(LogMessageWaitStrategy(re.compile(r"AWS Local Emulator Ready")))

    def _connect(self):
        """
        Wait for Floci to be fully ready.

        ``KeepaliveContainer.start()`` calls ``_configure()`` and ``_connect()``
        hooks. Without this, Kinesis and DynamoDB service APIs can receive
        requests before Floci is ready.
        """
        if not self._wait_strategy:
            raise ValueError("No wait strategy defined")
        wait_for_logs(self, predicate=self._wait_strategy, timeout=60)

    def get_url(self) -> str:
        """
        Return the mapped Floci edge endpoint.
        """
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.FLOCI_PORT)
        return f"http://{host}:{port}"
