"""
Example program using the official Rockset Python Client with a CrateDB backend.
Here: "Documents" and "Queries" APIs, basic usage.

Usage
=====
- Install dependencies: Run `pip install 'rockset<2.2'`.
- Adjust configuration: Define `ROCKSET_APISERVER` environment variable.
- Run program: Run `python basic.py`

Documentation
=============
- https://github.com/rockset/rockset-python-client/blob/main/docs/DocumentsApi.md#add_documents
- https://github.com/rockset/rockset-python-client/blob/main/docs/QueriesApi.md#query
- https://docs.rockset.com/documentation/reference/adddocuments
- https://docs.rockset.com/documentation/reference/query
"""

import os
from enum import Enum
from pprint import pprint

from rockset import Configuration, RocksetClient
from rockset.model.query_parameter import QueryParameter
from rockset.model.query_request_sql import QueryRequestSql


# Define endpoint of API Emulator.
class Connections(str, Enum):
    rockset_apiserver = os.environ.get("ROCKSET_APISERVER", "http://localhost:4243")
    rockset_apikey = os.environ.get("ROCKSET_APIKEY", "abc123")


def main():
    # Create an instance of the Rockset client.
    rs_config = Configuration(
        host=Connections.rockset_apiserver,
        api_key=Connections.rockset_apikey,
    )
    rs = RocksetClient(config=rs_config)

    # TODO: Create Collection.
    # rs.Collections.create_kafka_collection()  # noqa: ERA001

    # Add Documents.
    api_response = rs.Documents.add_documents(
        collection="collection_example",
        data=[{"id": "foo", "field": "value"}],
    )
    pprint(api_response)  # noqa: T203

    # Submit SQL query.
    api_response = rs.Queries.query(
        sql=QueryRequestSql(
            parameters=[
                QueryParameter(
                    name="_id",
                    type="string",
                    value="foo",
                ),
            ],
            query="SELECT * FROM commons.collection_example WHERE id = :_id",
        ),
    )
    pprint(api_response)  # noqa: T203


if __name__ == "__main__":
    main()
