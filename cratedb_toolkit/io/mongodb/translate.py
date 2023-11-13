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
Translate a MongoDB collection schema into a CrateDB CREATE TABLE expression.

Given a generated MongoDB collection schema, this will translate that schema
into a CREATE TABLE statement, mapping fields to columns and the collection
name to the table name.

In the case where there are type conflicts (for example, 40% of the values
for a field are integers, and 60% are strings), the translator will choose
the type with the greatest proportion.
"""

from functools import reduce

TYPES = {
    "DATETIME": "TIMESTAMP WITH TIME ZONE",
    "INT64": "INTEGER",
    "STRING": "TEXT",
    "BOOLEAN": "BOOLEAN",
    "INTEGER": "INTEGER",
    "FLOAT": "FLOAT",
    "ARRAY": "ARRAY",
    "OBJECT": "OBJECT",
}

BASE = """
CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (\n{columns}\n);
"""

COLUMN = '"{column_name}" {type}'

OBJECT = "OBJECT ({object_type}) AS (\n{definition}\n)"


def get_columns_definition(columns):
    columns_definition = []
    for column in columns:
        if column[1]:
            item = f"{column[1]}\n{column[0]}"
        else:
            item = column[0]
        columns_definition.append(item)
    return columns_definition


def translate_object(schema):
    """
    Translate an object field schema definition into a CrateDB dynamic object column.
    """

    columns = []
    object_type = "DYNAMIC"
    for fieldname, field in schema.items():
        sql_type, comment = determine_type(field)
        columns.append((COLUMN.format(column_name=fieldname, type=sql_type), comment))
    columns_definition = get_columns_definition(columns)
    return OBJECT.format(
        object_type=object_type,
        definition=",\n".join(columns_definition),
    )


def translate_array(schema):
    """
    Translate an array field schema definition into a CrateDB array column.
    """

    subtype, comment = determine_type(schema)
    if comment:
        return f"{comment}\nARRAY({subtype})"
    else:
        return f"ARRAY({subtype})"


def determine_type(schema):
    """
    Determine the type of a specific field schema.
    """

    types = schema.get("types", {})
    type_ = max(types, key=lambda item: types[item]["count"])
    if type_ in TYPES:
        sql_type = TYPES.get(type_)
        if sql_type == "OBJECT":
            sql_type = translate_object(types["OBJECT"]["document"])
        elif sql_type == "ARRAY":
            sql_type = translate_array(types["ARRAY"])

        if len(types) > 1:
            return (sql_type, proportion_string(types))
        return (sql_type, None)
    return ("UNKNOWN", None)


def proportion_string(types: dict) -> str:
    """
    Convert a list of types into a string explaining the proportions of each type.
    """

    total = reduce(lambda x, y: x + types[y]["count"], list(types.keys()), 0)
    summary = "-- ⬇️ Types: "
    proportions = []
    for type_ in types:
        proportions.append(f"{type_}: {round((types[type_]['count']/total)*100, 2)}%")
    return " " + (summary + ", ".join(proportions))


def indent_sql(query: str) -> str:
    """
    Indent an SQL query based on opening and closing brackets.
    """

    indent = 0
    lines = query.split("\n")
    for idx, line in enumerate(lines):
        lines[idx] = (" " * indent) + line
        if len(line) >= 1:
            if line[-1] == "(":
                indent += 4
            elif line[-1] == ")":
                indent -= 4
    return "\n".join(lines)


def translate(schemas, schemaname: str = None):
    """
    Translate a schema definition for a set of MongoDB collection schemas.

    This results in a set of CrateDB compatible CREATE TABLE expressions
    corresponding to the set of MongoDB collection schemas.
    """
    schemaname = schemaname or "doc"

    tables = list(schemas.keys())
    sql_queries = {}
    for tablename in tables:
        collection = schemas[tablename]
        columns = []
        for fieldname, field in collection["document"].items():
            sql_type, comment = determine_type(field)
            if sql_type != "UNKNOWN":
                columns.append((COLUMN.format(column_name=fieldname, type=sql_type), comment))

        columns_definition = get_columns_definition(columns)
        sql_queries[tablename] = indent_sql(
            BASE.format(schema=schemaname, table=tablename, columns=",\n".join(columns_definition))
        )
    return sql_queries
