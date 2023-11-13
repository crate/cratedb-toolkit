import unittest

from cratedb_toolkit.io.mongodb.util import parse_input_numbers


class TestInputNumberParser(unittest.TestCase):
    def test_numbers(self):
        s = "0 1 7 4"
        parsed = parse_input_numbers(s)
        self.assertEqual(parsed, [0, 1, 7, 4])

    def test_comma_seperated_numbers(self):
        s = "0, 1, 7, 4"
        parsed = parse_input_numbers(s)
        self.assertEqual(parsed, [0, 1, 7, 4])

    def test_mixed_numbers(self):
        s = "0 1, 7 4"
        parsed = parse_input_numbers(s)
        self.assertEqual(parsed, [0, 1, 7, 4])

    def test_range(self):
        s = "1-5"
        parsed = parse_input_numbers(s)
        self.assertEqual(parsed, [1, 2, 3, 4, 5])

    def test_inverse_range(self):
        s = "5-1"
        parsed = parse_input_numbers(s)
        self.assertEqual(parsed, [1, 2, 3, 4, 5])

    def test_mixed(self):
        s = "0 1, 3 5-8, 9 12-10"
        parsed = parse_input_numbers(s)
        self.assertEqual(parsed, [0, 1, 3, 5, 6, 7, 8, 9, 10, 11, 12])
