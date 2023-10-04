import dataclasses
from copy import deepcopy

from boltons.urlutils import URL


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
        Factory method to create an instance from a URI in string format.
        """
        return cls(uri=URL(url))

    @property
    def dburi(self):
        """
        Return a string representation of the database URI.
        """
        return str(self.uri)

    @property
    def safe(self):
        """
        Return a string representation of the database URI, safe for printing.
        The password is stripped from the URL, and replaced by `REDACTED`.
        """
        uri = deepcopy(self.uri)
        uri.password = "REDACTED"  # noqa: S105
        return str(uri)


@dataclasses.dataclass
class TableAddress:
    """
    Manage a table address, which is made of "<schema>"."<table>".
    """

    schema: str
    table: str

    @property
    def fullname(self):
        return f'"{self.schema}"."{self.table}"'
