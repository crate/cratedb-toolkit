from cratedb_toolkit.model import DatabaseAddress


def test_database_address_to_httpuri_standard_default_port():
    address = DatabaseAddress.from_string("crate://user:password@example.org/schema/table")
    assert address.httpuri == "http://user:password@example.org:4200/schema/table"


def test_database_address_to_httpuri_standard_with_port():
    address = DatabaseAddress.from_string("crate://user:password@example.org:3333/schema/table")
    assert address.httpuri == "http://user:password@example.org:3333/schema/table"


def test_database_address_to_httpuri_ssl_default_port():
    address = DatabaseAddress.from_string("crate://user:password@example.org/schema/table?ssl=true")
    assert address.httpuri == "https://user:password@example.org:4200/schema/table"


def test_database_address_to_httpuri_ssl_with_port():
    address = DatabaseAddress.from_string("crate://user:password@example.org:3333/schema/table?ssl=true")
    assert address.httpuri == "https://user:password@example.org:3333/schema/table"


def test_database_address_from_httpuri_standard():
    address = DatabaseAddress.from_http_uri("http://user:password@example.org/schema/table")
    assert address.dburi == "crate://user:password@example.org/schema/table"


def test_database_address_from_httpuri_ssl():
    address = DatabaseAddress.from_http_uri("https://user:password@example.org/schema/table")
    assert address.dburi == "crate://user:password@example.org/schema/table?ssl=true"


def test_database_address_to_httpuri_sslmode_require():
    address = DatabaseAddress.from_string("crate://user:password@example.org/schema/table?sslmode=require")
    assert address.httpuri == "https://user:password@example.org:4200/schema/table"
    assert address.verify_ssl is False


def test_database_address_to_httpuri_sslmode_verify_ca():
    address = DatabaseAddress.from_string("crate://user:password@example.org/schema/table?sslmode=verify-ca")
    assert address.httpuri == "https://user:password@example.org:4200/schema/table"
    assert address.verify_ssl is True
