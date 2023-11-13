import dataclasses
import typing as t
from copy import deepcopy

from boltons.urlutils import URL

from cratedb_toolkit.util.database import decode_database_table


@dataclasses.dataclass
class DatabaseAddress:
    """
    Manage a database address, which is either a SQLAlchemy-
    compatible database URI, or a regular HTTP URL.
    """

    uri: URL

    @classmethod
    def from_string(cls, url):
        """
        Factory method to create an instance from an SQLAlchemy database URL in string format.
        """
        return cls(uri=URL(url))

    @classmethod
    def from_httpuri(cls, url):
        """
        Factory method to create an instance from an HTTP URL in string format.
        """
        uri = URL(url)
        if uri.scheme == "https":
            uri.query_params["ssl"] = "true"
        uri.scheme = "crate"
        return cls(uri=uri)

    @property
    def dburi(self) -> str:
        """
        Return a string representation of the database URI.
        """
        return str(self.uri)

    @property
    def httpuri(self) -> str:
        """
        Return the `http(s)://` variant of the database URI.
        """
        uri = deepcopy(self.uri)
        uri.scheme = "http"
        if "ssl" in uri.query_params:
            if uri.query_params["ssl"]:
                uri.scheme = "https"
            del uri.query_params["ssl"]
        return str(uri)

    @property
    def safe(self):
        """
        Return a string representation of the database URI, safe for printing.
        The password is stripped from the URL, and replaced by `REDACTED`.
        """
        uri = deepcopy(self.uri)
        uri.password = "REDACTED"  # noqa: S105
        return str(uri)

    def decode(self) -> t.Tuple[URL, "TableAddress"]:
        """
        Decode database and table names, and sanitize database URI.
        """
        database, table = decode_database_table(self.dburi)
        uri = deepcopy(self.uri)
        uri.path = ""
        return uri, TableAddress(database, table)


@dataclasses.dataclass
class TableAddress:
    """
    Manage a table address, which is made of "<schema>"."<table>".
    """

    schema: t.Optional[str] = None
    table: t.Optional[str] = None

    @property
    def fullname(self):
        if self.schema is None and self.table is None:
            raise ValueError("Uninitialized table address can not be serialized")
        if self.schema and self.table:
            return f'"{self.schema}"."{self.table}"'
        else:
            return f'"{self.table}"'


@dataclasses.dataclass
class ClusterInformation:
    """
    Manage a database cluster's information.
    """

    cratedb: t.Any = dataclasses.field(default_factory=dict)
    cloud: t.Dict[str, t.Any] = dataclasses.field(default_factory=dict)

    def asdict(self):
        return dataclasses.asdict(self)


@dataclasses.dataclass
class InputOutputResource:
    """
    Manage information about an input or output resource.
    """

    url: str
    format: t.Optional[str] = None  # noqa: A003
    compression: t.Optional[str] = None
