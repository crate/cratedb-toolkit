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
import time
import typing as t

import pymongo.errors
from pymongo import MongoClient
from testcontainers.core.exceptions import ContainerStartException
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

    NAME = "testcontainers-mongodb-vanilla"
    MONGODB_VERSION = os.environ.get("MONGODB_VERSION", "latest")
    TIMEOUT = 5000
    DIRECT_CONNECTION = False

    def __init__(
        self,
        image: str = f"mongo:{MONGODB_VERSION}",
        **kwargs,
    ) -> None:
        super().__init__(image=image, **kwargs)
        self.with_name(self.NAME)

    def get_connection_client(self) -> MongoClient:
        return MongoClient(
            self.get_connection_url(),
            directConnection=self.DIRECT_CONNECTION,
            socketTimeoutMS=self.TIMEOUT,
            connectTimeoutMS=self.TIMEOUT,
            serverSelectionTimeoutMS=self.TIMEOUT,
        )


class MongoDbReplicasetContainer(MongoDbContainerWithKeepalive):
    """
    A Testcontainer for MongoDB with transparent replica set configuration.

    Overwritten to nullify MONGO_INITDB_ROOT_USERNAME + _PASSWORD,
    and username + password, because replicaset + authentication
    is more complicated to configure.
    """

    NAME = "testcontainers-mongodb-replicaset"
    DIRECT_CONNECTION = True

    def _configure(self) -> None:
        self.with_command("mongod --replSet testcontainers-rs")
        self.with_env("MONGO_DB", self.dbname)

    def _create_connection_url(
        self,
        dialect: str,
        host: t.Optional[str] = None,
        port: t.Optional[int] = None,
        dbname: t.Optional[str] = None,
        **kwargs,
    ) -> str:
        from testcontainers.core.utils import raise_for_deprecated_parameter

        if raise_for_deprecated_parameter(kwargs, "db_name", "dbname"):
            raise ValueError(f"Unexpected arguments: {','.join(kwargs)}")
        if self._container is None:
            raise ContainerStartException("container has not been started")
        host = host or self.get_container_host_ip()
        port = self.get_exposed_port(port)
        url = f"{dialect}://{host}:{port}"
        if dbname:
            url = f"{url}/{dbname}"
        return url

    def get_connection_url(self) -> str:
        return self._create_connection_url(
            dialect="mongodb",
            port=self.port,
        )

    def _connect(self) -> None:
        """
        Connect to MongoDB, and establish replica set.

        https://www.mongodb.com/docs/v5.0/reference/command/replSetInitiate/
        https://www.mongodb.com/docs/v5.0/reference/method/rs.initiate/#mongodb-method-rs.initiate

        https://www.mongodb.com/docs/v5.0/reference/command/replSetGetStatus/
        https://www.mongodb.com/docs/v5.0/reference/method/rs.status/#mongodb-method-rs.status
        """
        super()._connect()

        rs_config = {"_id": "testcontainers-rs", "members": [{"_id": 0, "host": "localhost:27017"}]}

        client = self.get_connection_client()
        db = client.get_database("admin")
        for _ in range(10):
            response = db.command("ping")
            if response["ok"]:
                break
            time.sleep(0.5)

        try:
            db.command({"replSetInitiate": rs_config})
        except pymongo.errors.OperationFailure as ex:
            if ex.details is None or ex.details["codeName"] != "AlreadyInitialized":
                raise

        response = db.command({"replSetGetStatus": 1})
        if not response["myState"]:
            raise IOError("MongoDB replica set failed")

        for _ in range(10):
            if client.is_primary:
                return
            time.sleep(0.5)

        raise IOError("Unable to spin up MongoDB with replica set")
