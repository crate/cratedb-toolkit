import unittest

import bson

from cratedb_toolkit.io.mongodb import extract


class TestExtractTypes(unittest.TestCase):
    def test_primitive_types(self):
        i = {"a": "a", "b": True, "c": 3, "d": 4.4}
        expected = {"a": "STRING", "b": "BOOLEAN", "c": "INTEGER", "d": "FLOAT"}
        s = extract.extract_schema_from_document(i, {})
        for key, value in expected.items():
            types = list(s[key]["types"].keys())
            self.assertListEqual([value], types)

    def test_bson_types(self):
        i = {
            "a": bson.ObjectId("55153a8014829a865bbf700d"),
            "b": bson.datetime.datetime.now(),
            "c": bson.Timestamp(0, 0),
        }
        expected = {"a": "OID", "b": "DATETIME", "c": "TIMESTAMP"}
        s = extract.extract_schema_from_document(i, {})
        for key, value in expected.items():
            types = list(s[key]["types"].keys())
            self.assertListEqual([value], types)

    def test_collection_types(self):
        i = {"a": [1, 2, 3], "b": {"a": "hello world"}}
        expected = {"a": "ARRAY", "b": "OBJECT"}
        s = extract.extract_schema_from_document(i, {})
        for key, value in expected.items():
            types = list(s[key]["types"].keys())
            self.assertListEqual([value], types)

    def test_list_subtypes(self):
        i = {
            "a": ["a", "b", 3],
            "b": [[1, 2, 3]],
            "c": [{"a": "a"}, {"a": "b"}],
        }

        subtypes = extract.extract_schema_from_array(i["a"], {})
        self.assertListEqual(["STRING", "INTEGER"], list(subtypes.keys()))

        subtypes = extract.extract_schema_from_array(i["b"], {})
        self.assertListEqual(["ARRAY"], list(subtypes.keys()))
        self.assertListEqual(["INTEGER"], list(subtypes["ARRAY"]["types"].keys()))

        subtypes = extract.extract_schema_from_array(i["c"], {})
        self.assertListEqual(["OBJECT"], list(subtypes.keys()))

    def test_object_type(self):
        i = {"a": {"b": "c"}}
        s = extract.extract_schema_from_document(i, {})
        self.assertListEqual(["OBJECT"], list(s["a"]["types"].keys()))


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
