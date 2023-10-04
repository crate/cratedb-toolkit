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
import typing as t

from azure.storage.blob import BlobServiceClient, ContainerClient
from testcontainers.azurite import AzuriteContainer

from cratedb_toolkit.testing.testcontainers.util import ExtendedDockerContainer


class ExtendedAzuriteContainer(ExtendedDockerContainer, AzuriteContainer):
    """
    An extended Testcontainer for Azurite, emulating Microsoft Azure Blob Storage.

    Features
    - Use the `latest` OCI image from https://quay.io/.
    - Provide convenience methods for getting the Docker-internal endpoint address.

    References
    - https://en.wikipedia.org/wiki/Object_storage
    - https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite
    - https://learn.microsoft.com/en-us/azure/storage/blobs/use-azurite-to-run-automated-tests
    - https://github.com/azure/azurite
    """

    def get_real_host_address(self) -> str:
        """
        Provide Docker-internal full endpoint address `<host>:<port>` of the service.
        For example, `172.17.0.4:10000`.
        """
        return f"{self.get_real_host_ip()}:{self._BLOB_SERVICE_PORT}"

    def get_container_endpoint(self, container_name: str):
        container = self.get_container(container_name)
        hostname = self.get_real_host_address()
        return container._format_url(hostname=hostname)

    def get_blob_service_client(self) -> BlobServiceClient:
        """
        Client handle to communicate with the service.
        """
        connection_string = self.get_connection_string()
        return BlobServiceClient.from_connection_string(connection_string, api_version="2019-12-12")

    def create_container(self, container_name: str) -> ContainerClient:
        """
        Create a blob container, and return container client.
        """
        blob_service_client = self.get_blob_service_client()
        return blob_service_client.create_container(container_name)

    def get_container(self, container_name: str) -> ContainerClient:
        """
        Get a blob container, and return container client.
        """
        blob_service_client = self.get_blob_service_client()
        return blob_service_client.get_container_client(container_name)

    def list_blob_names(self, container_name: str) -> t.List[str]:
        """
        Return list of blob names within given container.
        """
        container = self.get_container(container_name)
        return list(container.list_blob_names())
