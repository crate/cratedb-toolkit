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

from testcontainers.mongodb import MongoDbContainer

from cratedb_toolkit.testing.testcontainers.util import KeepaliveContainer


class MongoDbContainerWithKeepalive(KeepaliveContainer, MongoDbContainer):
    """
    A Testcontainer for MongoDB with improved configurability.

    It honors the `TC_KEEPALIVE` and `MONGODB_VERSION` environment variables.

    Defining `TC_KEEPALIVE` will set a signal not to shut down the container
    after running the test cases, in order to speed up subsequent invocations.

    `MONGODB_VERSION` will define the designated MongoDB version, which is
    useful when used within a test matrix. Its default value is `latest`.
    """

    MONGODB_VERSION = os.environ.get("MONGODB_VERSION", "latest")

    def __init__(
        self,
        image: str = f"mongo:{MONGODB_VERSION}",
        **kwargs,
    ) -> None:
        super().__init__(image=image, **kwargs)
        self.with_name("testcontainers-mongodb")
