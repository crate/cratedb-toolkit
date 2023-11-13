from cratedb_toolkit.model import DatabaseAddress


def test_database_address_to_httpuri_standard():
    address = DatabaseAddress.from_string("crate://user:password@example.org/schema/table")
    assert address.httpuri == "http://user:password@example.org/schema/table"


def test_database_address_to_httpuri_ssl():
    address = DatabaseAddress.from_string("crate://user:password@example.org/schema/table?ssl=true")
    assert address.httpuri == "https://user:password@example.org/schema/table"


def test_database_address_from_httpuri_standard():
    address = DatabaseAddress.from_httpuri("http://user:password@example.org/schema/table")
    assert address.dburi == "crate://user:password@example.org/schema/table"


def test_database_address_from_httpuri_ssl():
    address = DatabaseAddress.from_httpuri("https://user:password@example.org/schema/table")
    assert address.dburi == "crate://user:password@example.org/schema/table?ssl=true"
