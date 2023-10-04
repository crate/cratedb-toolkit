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

from testcontainers.minio import MinioContainer

from cratedb_toolkit.testing.testcontainers.util import ExtendedDockerContainer


class ExtendedMinioContainer(ExtendedDockerContainer, MinioContainer):
    """
    An extended Testcontainer for MinIO, emulating AWS S3.

    Features
    - Use the `latest` OCI image from https://quay.io/.
    - Provide convenience methods for getting the Docker-internal endpoint address.

    References
    - https://en.wikipedia.org/wiki/Object_storage
    - https://en.wikipedia.org/wiki/Amazon_S3
    - https://github.com/minio/minio
    """

    def __init__(self, *args, **kwargs):
        # Use most recent stable release of MinIO.
        image = "quay.io/minio/minio:latest"
        kwargs.setdefault("image", image)

        super().__init__(*args, **kwargs)

    def list_object_names(self, bucket_name: str) -> t.List[str]:
        """
        Return list of object names within given bucket.
        """
        objects = self.get_client().list_objects(bucket_name=bucket_name)
        return [obj.object_name for obj in objects]
