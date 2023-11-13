import unittest

import pytest

from cratedb_toolkit.io.mongodb import translate

pytestmark = pytest.mark.mongodb


class TestTranslate(unittest.TestCase):
    def test_types_translation(self):
        i = [
            ("DATETIME", "TIMESTAMP WITH TIME ZONE"),
            ("INT64", "INTEGER"),
            ("STRING", "TEXT"),
            ("BOOLEAN", "BOOLEAN"),
            ("INTEGER", "INTEGER"),
            ("FLOAT", "FLOAT"),
        ]
        for test in i:
            i = {"count": 1, "types": {test[0]: {"count": 1}}}
            o, _ = translate.determine_type(i)
            self.assertEqual(o, test[1])

    def test_indeterminate_type(self):
        i = {
            "count": 3,
            "types": {
                "STRING": {"count": 1},
                "INTEGER": {"count": 1},
                "BOOLEAN": {"count": 1},
            },
        }
        expected_type = "TEXT"
        expected_comment = " -- ⬇️ Types: STRING: 33.33%, INTEGER: 33.33%, BOOLEAN: 33.33%"
        (o_type, o_comment) = translate.determine_type(i)
        self.assertEqual(o_type, expected_type)
        self.assertEqual(o_comment, expected_comment)

    def test_object_translation(self):
        i = {
            "a": {
                "count": 1,
                "types": {"STRING": {"count": 1}},
                "b": {"count": 1, "types": {"DATETIME": {"count": 1}}},
            }
        }
        o = translate.translate_object(i)
        self.assertEqual(" ".join(o.split()), 'OBJECT (DYNAMIC) AS ( "a" TEXT )')

    def test_array_translate(self):
        i = {"count": 1, "types": {"STRING": {"count": 1}}}
        o = translate.translate_array(i)
        self.assertEqual("ARRAY(TEXT)", o)
