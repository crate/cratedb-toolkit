import pytest

from cratedb_toolkit.util import DatabaseAdapter


def test_quote_relation_name():
    database = DatabaseAdapter(dburi="crate://localhost")
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


def test_quote_relation_name_with_invalid_fqn():
    database = DatabaseAdapter(dburi="crate://localhost")
    with pytest.raises(ValueError):
        database.quote_relation_name("my-db.my-schema.my-table")
