# Copyright (c) 2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
Rockset Documents API mock, adapting to CrateDB backend.

https://github.com/rockset/rockset-python-client/blob/main/docs/DocumentsApi.md
https://docs.rockset.com/documentation/reference/adddocuments
"""

import logging

import pandas as pd
import typing_extensions as t
from fastapi import APIRouter, Depends, Request
from sqlalchemy_cratedb import insert_bulk
from sqlalchemy_cratedb.support import table_kwargs
from vasuki import generate_nagamani19_hash

from cratedb_toolkit.adapter.rockset.server.dependencies import database_adapter
from cratedb_toolkit.util import DatabaseAdapter

logger = logging.getLogger()


router = APIRouter(
    prefix="/v1/orgs/self/ws/{workspace}/collections",
)


@router.post("/{collection}/docs")
async def add_documents(
    request: Request, workspace: str, collection: str, cratedb: t.Annotated[DatabaseAdapter, Depends(database_adapter)]
):
    # Assign unique identifiers to this operation.
    # TODO: Store audit records.
    _id = generate_nagamani19_hash()
    patch_id = generate_nagamani19_hash()

    # Decode request data into dataframe.
    response = await request.json()
    documents = response["data"]
    data = pd.DataFrame.from_records(documents)

    logger.info(f"Inserting records into CrateDB: schema={workspace}, table={collection}")
    with table_kwargs(crate_column_policy="'dynamic'"):
        data.to_sql(
            name=collection,
            schema=workspace,
            con=cratedb.engine,
            index=False,
            if_exists="append",
            method=insert_bulk,
        )

    # TODO: Make it optional?
    cratedb.refresh_table(f'"{workspace}"."{collection}"')

    # TODO: Properly convey back error messages.
    """
    error1 = {
        "column": 0,
        "error_id": "string",
        "line": 0,
        "message": "collection not found",
        "query_id": "string",
        "trace_id": "string",
        "type": "INVALIDINPUT",
        "virtual_instance_rrn": "rrn:vi:use1a1:123e4567-e89b-12d3-a456-556642440000"
    }

    error2 = {
        "message": "Authentication Failure",
        "message_key": null,
        "type": "AUTHEXCEPTION",
        "line": null,
        "column": null,
        "trace_id": null,
        "error_id": null,
        "query_id": null,
        "virtual_instance_rrn": null,
        "virtual_instance_id": null,
        "internal_errors": null
    }
    """

    return {
        "data": [{"_collection": collection, "_id": _id, "patch_id": patch_id, "status": "ADDED"}],
        # TODO: Properly format `last_offset`.
        # https://docs.rockset.com/documentation/docs/write-api#verify-collection-is-updated
        "last_offset": "f1:0:0:0:0",
    }
