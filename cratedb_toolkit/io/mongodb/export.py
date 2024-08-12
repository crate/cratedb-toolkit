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
Export the documents from a MongoDB collection as JSON, to be ingested into CrateDB.
"""

import calendar
import typing as t

import bsonjs
import dateutil.parser as dateparser
import orjson as json
import pymongo.collection

from cratedb_toolkit.io.mongodb.transform import TransformationManager
from cratedb_toolkit.io.mongodb.util import sanitize_field_names


def date_converter(value):
    if isinstance(value, int):
        return value
    dt = dateparser.parse(value)
    return calendar.timegm(dt.utctimetuple()) * 1000


def timestamp_converter(value):
    if len(str(value)) <= 10:
        return value * 1000
    return value


type_converter = {
    "date": date_converter,
    "timestamp": timestamp_converter,
    "undefined": lambda x: None,
}


def extract_value(value, parent_type=None):
    """
    Decode MongoDB Extended JSON.

    - https://www.mongodb.com/docs/manual/reference/mongodb-extended-json-v1/
    - https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/
    """
    if isinstance(value, dict):
        if len(value) == 1:
            for k, v in value.items():
                if k.startswith("$"):
                    return extract_value(v, k.lstrip("$"))
        return {k.lstrip("$"): extract_value(v, parent_type) for (k, v) in value.items()}
    if isinstance(value, list):
        return [extract_value(v, parent_type) for v in value]
    if parent_type:
        converter = type_converter.get(parent_type)
        if converter:
            return converter(value)
    return value


def convert(d):
    """
    Decode MongoDB Extended JSON, considering CrateDB specifics.
    """
    newdict = {}
    for k, v in sanitize_field_names(d).items():
        newdict[k] = extract_value(v)
    return newdict


def collection_to_json(
    collection: pymongo.collection.Collection, fp: t.IO[t.Any], tm: TransformationManager = None, limit: int = 0
):
    """
    Export a MongoDB collection's documents to standard JSON.
    The output is suitable to be consumed by the `cr8` program.

    collection
      a Pymongo collection object.

    file
      a file-like object (stream).
    """
    for document in collection.find().limit(limit):
        bson_json = bsonjs.dumps(document.raw)
        json_object = json.loads(bson_json)
        data = convert(json_object)
        if tm:
            data = tm.apply_transformations(collection.database.name, collection.name, data)
        fp.write(json.dumps(data))
        fp.write(b"\n")
