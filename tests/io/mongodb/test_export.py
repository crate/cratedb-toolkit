import pytest
from zyp.model.collection import CollectionTransformation
from zyp.model.treatment import Treatment

from cratedb_toolkit.io.mongodb.export import CrateDBConverter

pytestmark = pytest.mark.mongodb


def test_convert_basic():
    data_in = {
        "_id": {
            "$oid": "56027fcae4b09385a85f9344",
        },
        "value": {
            "id": 42,
            "date": {"$date": "2015-09-23T10:32:42.33Z"},
            "list": [
                {"id": "foo", "value": "something"},
                {"id": "bar", "value": {"$date": "2015-09-24T10:32:42.33Z"}},
            ],
        },
    }

    data_out = {
        "_id": "56027fcae4b09385a85f9344",
        "value": {
            "date": 1443004362000,
            "id": 42,
            "list": [
                {"id": "foo", "value": "something"},
                {"id": "bar", "value": 1443090762000},
            ],
        },
    }
    converter = CrateDBConverter()
    assert converter.convert(data_in) == data_out


def test_convert_with_treatment_ignore_complex_list():
    data_in = {
        "_id": {
            "$oid": "56027fcae4b09385a85f9344",
        },
        "value": {
            "id": 42,
            "date": {"$date": "2015-09-23T10:32:42.33Z"},
            "some_complex_list": [
                {"id": "foo", "value": "something"},
                {"id": "bar", "value": {"$date": "2015-09-24T10:32:42.33Z"}},
            ],
        },
    }

    data_out = {
        "_id": "56027fcae4b09385a85f9344",
        "value": {
            "date": 1443004362000,
            "id": 42,
        },
    }

    treatment = Treatment(ignore_complex_lists=True)
    converter = CrateDBConverter(transformation=CollectionTransformation(treatment=treatment))
    assert converter.convert(data_in) == data_out


def test_convert_with_treatment_all_options():
    data_in = {
        "_id": {
            "$oid": "56027fcae4b09385a85f9344",
        },
        "ignore_toplevel": 42,
        "value": {
            "id": 42,
            "date": {"$date": "2015-09-23T10:32:42.33Z"},
            "ignore_nested": 42,
        },
        "to_list": 42,
        "to_string": 42,
        "to_dict_scalar": 42,
        "to_dict_list": [{"user": 42}],
    }

    data_out = {
        "_id": "56027fcae4b09385a85f9344",
        "value": {
            "date": 1443004362000,
            "id": 42,
        },
        "to_list": [42],
        "to_string": "42",
        "to_dict_scalar": {"id": 42},
        "to_dict_list": [{"user": {"id": 42}}],
    }
    treatment = Treatment(
        ignore_complex_lists=False,
        ignore_field=["ignore_toplevel", "ignore_nested"],
        convert_list=["to_list"],
        convert_string=["to_string"],
        convert_dict=[
            {"name": "to_dict_scalar", "wrapper_name": "id"},
            {"name": "user", "wrapper_name": "id"},
        ],
    )
    converter = CrateDBConverter(transformation=CollectionTransformation(treatment=treatment))
    assert converter.convert(data_in) == data_out
