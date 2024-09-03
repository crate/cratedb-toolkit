# ruff: noqa: E402
import typing as t
import unittest
from collections import OrderedDict

import bson
import pytest

from cratedb_toolkit.io.mongodb import extract

pytestmark = pytest.mark.mongodb


class TestExtractTypes(unittest.TestCase):
    def test_primitive_types(self):
        data = {"a": "a", "b": True, "c": 3, "d": 4.4}
        expected = {"a": "STRING", "b": "BOOLEAN", "c": "INTEGER", "d": "FLOAT"}
        schema = trim_schema(extract.extract_schema_from_document(data, {}))
        self.assertDictEqual(schema, expected)

    def test_integer_types(self):
        """
        Validate extraction of numeric types INTEGER vs. BIGINT.
        """
        data = {
            "integer": 2147483647,
            "bigint": 1563051934000,
        }
        expected = {
            "integer": "INTEGER",
            "bigint": "INT64",
        }
        schema = trim_schema(extract.extract_schema_from_document(data, {}))
        self.assertDictEqual(schema, expected)

    def test_bson_types(self):
        data = {
            "datetime": bson.datetime.datetime.now(),
            "datetimems": bson.DatetimeMS(1563051934000),
            "decimal128": bson.Decimal128("42.42"),
            "int64": bson.Int64(42),
            "objectid": bson.ObjectId("55153a8014829a865bbf700d"),
            "timestamp": bson.Timestamp(0, 0),
        }
        expected = {
            "datetime": "DATETIME",
            "datetimems": "TIMESTAMP",
            "decimal128": "DECIMAL",
            "int64": "INT64",
            "objectid": "OID",
            "timestamp": "TIMESTAMP",
        }
        schema = trim_schema(extract.extract_schema_from_document(data, {}))
        self.assertDictEqual(schema, expected)

    def test_collection_types(self):
        data = {"a": [1, 2, 3], "b": {"a": "hello world"}}
        expected = {"a": "ARRAY", "b": "OBJECT"}
        schema = trim_schema(extract.extract_schema_from_document(data, {}))
        self.assertDictEqual(schema, expected)

    def test_list_subtypes(self):
        data = {
            "a": ["a", "b", 3],
            "b": [[1, 2, 3]],
            "c": [{"a": "a"}, {"a": "b"}],
        }

        subtypes = extract.extract_schema_from_array(data["a"], {})
        self.assertListEqual(["STRING", "INTEGER"], list(subtypes.keys()))

        subtypes = extract.extract_schema_from_array(data["b"], {})
        self.assertListEqual(["ARRAY"], list(subtypes.keys()))
        self.assertListEqual(["INTEGER"], list(subtypes["ARRAY"]["types"].keys()))

        subtypes = extract.extract_schema_from_array(data["c"], {})
        self.assertListEqual(["OBJECT"], list(subtypes.keys()))

    def test_object_type(self):
        data = {"a": {"b": "c"}}
        expected = {"a": "OBJECT"}
        schema = trim_schema(extract.extract_schema_from_document(data, {}))
        self.assertDictEqual(schema, expected)


class TestTypeCount(unittest.TestCase):
    def test_multiple_of_same_type(self):
        i = [{"a": 2}, {"a": 3}, {"a": 6}]
        s = {}
        for element in i:
            s = extract.extract_schema_from_document(element, s)
        self.assertEqual(len(s["a"]["types"]), 1)
        self.assertEqual(s["a"]["types"]["INTEGER"]["count"], 3)

    def test_multiple_of_different_type(self):
        i = [{"a": 2}, {"a": "Hello"}, {"a": True}]
        s = {}
        for element in i:
            s = extract.extract_schema_from_document(element, s)
        self.assertEqual(len(s["a"]["types"]), 3)
        self.assertEqual(s["a"]["types"]["INTEGER"]["count"], 1)
        self.assertEqual(s["a"]["types"]["STRING"]["count"], 1)
        self.assertEqual(s["a"]["types"]["BOOLEAN"]["count"], 1)


def get_types(schema_item) -> t.List[str]:
    return list(schema_item["types"].keys())


def trim_schema(schema) -> t.Dict[str, t.Any]:
    result = OrderedDict()
    for key, value in schema.items():
        types = get_types(value)
        result[key] = types[0]
    return result
