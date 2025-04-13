import json

import pytest
import yaml

from cratedb_toolkit.util.format import FlexibleFormatter, OutputFormat

source_data = {"thing": 42.42}
json_data = json.dumps(source_data, indent=2)


def test_formatter_json():
    formatter = FlexibleFormatter(source_data)
    assert formatter.format("json") == json_data
    assert formatter.format(OutputFormat.JSON) == json_data


def test_formatter_unknown():
    formatter = FlexibleFormatter(source_data)
    with pytest.raises(ValueError) as ex:
        formatter.format("foobar")
    assert ex.match("Unsupported format: foobar. Choose from: json, yaml, markdown, sql")


def test_formatter_yaml():
    formatter = FlexibleFormatter(source_data)
    yaml_data = yaml.dump(source_data, sort_keys=False)
    assert formatter.format("yaml") == yaml_data
    assert formatter.format(OutputFormat.YAML) == yaml_data


def test_formatter_markdown():
    formatter = FlexibleFormatter(source_data)
    result = formatter.format("markdown")
    assert "## thing" in result
    assert "42.42" in result


def test_formatter_not_implemented():
    formatter = FlexibleFormatter(source_data)
    with pytest.raises(NotImplementedError) as ex:
        formatter.format("sql")
    assert ex.match("Formatting to SQL needs a domain-specific implementation")


def test_formatter_with_empty_data():
    formatter = FlexibleFormatter({})
    assert formatter.format("json") == "{}"


def test_formatter_non_dict_markdown():
    formatter = FlexibleFormatter([1, 2, 3])
    with pytest.raises(NotImplementedError) as ex:
        formatter.format("markdown")
    assert ex.match("Unable to convert to Markdown")


def test_formatter_nested_dict_markdown():
    nested_data = {"section": {"key1": "value1", "key2": 42}}
    formatter = FlexibleFormatter(nested_data)
    result = formatter.format("markdown")
    assert "## section" in result
    assert "### key1" in result
    assert "value1" in result
    assert "### key2" in result
    assert "42" in result


def test_formatter_special_chars():
    special_data = {"special": "üñíçødé & special chars: /\\\"'"}
    formatter = FlexibleFormatter(special_data)
    # Test that formatting doesn't corrupt special characters
    assert "üñíçødé" in formatter.format("json")
    assert "\\xFC\\xF1\\xED\\xE7\\xF8d\\xE9" in formatter.format("yaml")
    assert "üñíçødé" in formatter.format("markdown")


def test_formatter_to_dict():
    original = {"data": [1, 2, 3]}
    formatter = FlexibleFormatter(original)
    dict_copy = formatter.to_dict()
    # Verify it's a deep copy
    assert dict_copy is not original
    assert dict_copy["data"] is not original["data"]
    # Modify original and verify the copy is unchanged
    original["data"].append(4)
    assert len(dict_copy["data"]) == 3


def test_formatter_with_none_input():
    formatter = FlexibleFormatter(None)
    assert formatter.to_dict() is None
