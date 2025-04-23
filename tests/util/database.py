import pytest

from cratedb_toolkit.util.database import DatabaseAdapter


def test_quote_relation_name_single():
    """
    Verify quoting a simple or full-qualified relation name.
    """
    database = DatabaseAdapter
    assert database.quote_relation_name("my_table") == "my_table"
    assert database.quote_relation_name("my-table") == '"my-table"'
    assert database.quote_relation_name("MyTable") == '"MyTable"'
    assert database.quote_relation_name('"MyTable"') == '"MyTable"'
    assert database.quote_relation_name("my_schema.my_table") == "my_schema.my_table"
    assert database.quote_relation_name("my-schema.my_table") == '"my-schema".my_table'
    assert database.quote_relation_name('"wrong-quoted-fqn.my_table"') == '"wrong-quoted-fqn.my_table"'
    assert database.quote_relation_name('"my_schema"."my_table"') == '"my_schema"."my_table"'
    # reserved keyword must be quoted
    assert database.quote_relation_name("table") == '"table"'


def test_quote_relation_name_double():
    """
    Verify quoting a relation name twice does not cause any harm.
    """
    database = DatabaseAdapter
    input_fqn = "foo-bar.baz_qux"
    output_fqn = '"foo-bar".baz_qux'
    assert database.quote_relation_name(input_fqn) == output_fqn
    assert database.quote_relation_name(output_fqn) == output_fqn


def test_quote_relation_name_with_invalid_fqn():
    database = DatabaseAdapter
    with pytest.raises(ValueError):
        database.quote_relation_name("my-db.my-schema.my-table")
