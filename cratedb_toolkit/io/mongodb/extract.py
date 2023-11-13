# -*- coding: utf-8; -*-
#
# Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.

"""
Export a schema definition from a MongoDB collection.

This will iterate over a collection (either totally, or partially) and build
up a description of the schema of the MongoDB collection.

Within the schema, each field in the collection will be described with two
fields:

- "count", being the number of entries in the collection that have this field.
- "types", being the types present for those entries.

For each type in a field's types, it will have a count that signifies the number
of entries of that field with that data type. If it is an object, it will also
contain a schema of the object's types. If it is an array, it will contain
a list of types that are present in the arrays, as well as their counts.

An example schema may look like:

{
    "count": 10,
    "document": {
        "ts": {
            "count": 10,
            "types": {"DATETIME": {"count": 10}
            }
        },
        "payload": {
            "count": 10,
            "types": {
                "OBJECT": {
                    "count": 10,
                    "document": {
                        "temp": {
                            "count": 10,
                            "types": {"FLOAT": {"count": 4}, "INTEGER": {"count": 6}}
                        }
                    }
                }
            }
        }
    }
}
"""

import typing as t

import bson
from pymongo.collection import Collection
from rich import progress

progressbar = progress.Progress(
    progress.TextColumn("{task.description} ", justify="left"),
    progress.BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}% ({task.completed}/{task.total})",
    "•",
    progress.TimeRemainingColumn(),
)


def extract_schema_from_collection(collection: Collection, partial: bool) -> t.Dict[str, t.Any]:
    """
    Extract a schema definition from a collection.

    If the extraction is partial, only the first document in the collection is
    used to create the schema.
    """

    schema: dict = {"count": 0, "document": {}}
    if partial:
        count = 1
    else:
        count = collection.estimated_document_count()
    with progressbar:
        t = progressbar.add_task(collection.name, total=count)
        try:
            for document in collection.find():
                schema["count"] += 1
                schema["document"] = extract_schema_from_document(document, schema["document"])
                progressbar.update(t, advance=1)
                if partial:
                    break
        except KeyboardInterrupt:
            return schema
    return schema


def extract_schema_from_document(document: dict, schema: dict):
    """
    Extract and update schema definition from a given document.
    """

    for k, v in document.items():
        if k not in schema:
            schema[k] = {"count": 0, "types": {}}

        item_type = get_type(v)
        if item_type not in schema[k]["types"]:
            if item_type == "OBJECT":
                schema[k]["types"][item_type] = {"count": 0, "document": {}}
            elif item_type == "ARRAY":
                schema[k]["types"][item_type] = {"count": 0, "types": {}}
            else:
                schema[k]["types"][item_type] = {"count": 0}

        schema[k]["count"] += 1
        schema[k]["types"][item_type]["count"] += 1
        if item_type == "OBJECT":
            schema[k]["types"][item_type]["document"] = extract_schema_from_document(
                v, schema[k]["types"][item_type]["document"]
            )
        elif item_type == "ARRAY":
            schema[k]["types"][item_type]["types"] = extract_schema_from_array(
                v, schema[k]["types"][item_type]["types"]
            )
    return schema


def extract_schema_from_array(array: list, schema: dict):
    """
    Extract and update a schema definition for a list.
    """

    for item in array:
        t = get_type(item)
        if t not in schema:
            if t == "OBJECT":
                schema[t] = {"count": 0, "document": {}}
            elif t == "ARRAY":
                schema[t] = {"count": 0, "types": {}}
            else:
                schema[t] = {"count": 0}

        schema[t]["count"] += 1
        if t == "OBJECT":
            schema[t]["document"] = extract_schema_from_document(item, schema[t]["document"])
        elif t == "ARRAY":
            schema[t]["types"] = extract_schema_from_array(item, schema[t]["types"])
    return schema


TYPES_MAP = {
    # bson types
    bson.ObjectId: "OID",
    bson.datetime.datetime: "DATETIME",
    bson.Timestamp: "TIMESTAMP",
    bson.int64.Int64: "INT64",
    # primitive types
    str: "STRING",
    bool: "BOOLEAN",
    int: "INTEGER",
    float: "FLOAT",
    # collection types
    list: "ARRAY",
    dict: "OBJECT",
}


def get_type(o):
    return TYPES_MAP.get(type(o), "UNKNOWN")
