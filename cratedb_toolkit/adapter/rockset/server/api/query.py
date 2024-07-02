# Copyright (c) 2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
Rockset Queries API mock, adapting to CrateDB backend.

https://docs.rockset.com/documentation/reference/query
https://github.com/rockset/rockset-python-client/blob/main/docs/QueriesApi.md
"""

import logging
import time
from collections import OrderedDict

import typing_extensions as t
from boltons.iterutils import flatten
from fastapi import APIRouter, Depends, Request
from vasuki import generate_nagamani19_hash

from cratedb_toolkit.adapter.rockset.server.dependencies import database_adapter
from cratedb_toolkit.util import DatabaseAdapter
from cratedb_toolkit.util.database import get_table_names

logger = logging.getLogger(__name__)


router = APIRouter(dependencies=[Depends(database_adapter)])


def translate_parameters(parameters: t.Sequence) -> t.Dict[str, str]:
    params = OrderedDict()
    for parameter in parameters:
        params[parameter["name"]] = parameter["value"]
    return params


@router.post("/v1/orgs/self/queries")
async def execute(request: Request, adapter: t.Annotated[DatabaseAdapter, Depends(database_adapter)]):
    # Assign unique identifiers to this operation.
    # TODO: Store audit records.
    query_id = generate_nagamani19_hash()

    # Decode request.
    user_request = await request.json()
    time_start = time.time_ns()

    # Decode SQL query expression.
    sql = user_request["sql"]["query"]
    table_names = flatten(get_table_names(sql))

    parameters = None
    if "parameters" in user_request["sql"]:
        parameters = translate_parameters(user_request["sql"]["parameters"])

    results = adapter.run_sql(
        sql=sql,
        parameters=parameters,
        records=True,
    )
    time_duration = time.time_ns() - time_start
    return {
        "collections": table_names,
        "query_id": query_id,
        "results": results,
        "results_total_doc_count": len(results),
        "stats": {
            "elapsed_time_ms": int(time_duration / 1000 / 1000),
        },
        "status": "COMPLETED",
    }
