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

from testcontainers.localstack import LocalStackContainer

from cratedb_toolkit.testing.testcontainers.util import KeepaliveContainer


class LocalStackContainerWithKeepalive(KeepaliveContainer, LocalStackContainer):
    """
    A Testcontainer for LocalStack with improved configurability.

    It honors the `TC_KEEPALIVE` and `LOCALSTACK_VERSION` environment variables.

    Defining `TC_KEEPALIVE` sets a signal to not shut down the container
    after running the test cases, to speed up later invocations.

    `LOCALSTACK_VERSION` defines the designated LocalStack version, which is
    useful when used within a test matrix.
    """

    LOCALSTACK_VERSION = os.environ.get("LOCALSTACK_VERSION", "4.5")

    def __init__(
        self,
        image: str = f"localstack/localstack:{LOCALSTACK_VERSION}",
        **kwargs,
    ) -> None:
        super().__init__(image=image, **kwargs)
        self.with_name("testcontainers-localstack")
