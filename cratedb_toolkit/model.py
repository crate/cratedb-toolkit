# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import dataclasses
import typing as t
from copy import deepcopy
from urllib.parse import urljoin

from attr import Factory
from attrs import define
from boltons.dictutils import subdict
from boltons.urlutils import URL

from cratedb_toolkit.exception import DatabaseAddressDuplicateError, DatabaseAddressMissingError
from cratedb_toolkit.util.data import asbool


@dataclasses.dataclass
class ClusterAddressOptions:
    """
    Manage and validate input options for a cluster address.
    """

    cluster_id: t.Optional[str] = None
    cluster_name: t.Optional[str] = None
    cluster_url: t.Optional[str] = None

    def __post_init__(self):
        self.validate()

    @classmethod
    def from_params(cls, **params: t.Any) -> "ClusterAddressOptions":
        return cls(**subdict(params, keep=["cluster_id", "cluster_name", "cluster_url"]))

    def validate(self):
        cluster_options = (self.cluster_id, self.cluster_name, self.cluster_url)
        self.check_mutual(*cluster_options)
        return self

    @staticmethod
    def check_mutual(*args):
        # Count the number of non-empty options.
        options_count = sum(1 for option in args if option is not None and option.strip())
        # Fail if no address option was provided.
        if options_count == 0:
            raise DatabaseAddressMissingError()
        # Fail if more than one address option was provided.
        if options_count > 1:
            raise DatabaseAddressDuplicateError()

    def asdict(self):
        return dataclasses.asdict(self)


@dataclasses.dataclass
class DatabaseAddress:
    """
    Manage a database address, which is either an SQLAlchemy-compatible database URI or a regular HTTP URL.
    """

    uri: URL

    @classmethod
    def from_string(cls, url: str) -> "DatabaseAddress":
        """
        Factory method to create an instance from an SQLAlchemy database URL in string format.
        """
        # Parse the URL to determine the scheme.
        parsed_url = URL(url)
        if parsed_url.scheme == "crate":
            return cls.from_sqlalchemy_uri(url)
        else:
            return cls.from_http_uri(url)

    @classmethod
    def from_sqlalchemy_uri(cls, url: str) -> "DatabaseAddress":
        """
        Factory method to create an instance from an SQLAlchemy database URL in string format.
        """
        return cls(uri=URL(url))

    @classmethod
    def from_http_uri(cls, url: str) -> "DatabaseAddress":
        """
        Factory method to create an instance from an HTTP URL in string format.
        """
        uri = URL(url)
        if uri.scheme == "https":
            uri.query_params["ssl"] = "true"
        uri.scheme = "crate"
        return cls(uri=uri)

    def with_credentials(self, username: str = None, password: str = None):
        """
        Add credentials, with in-place modification.
        """
        if username is not None:
            self.uri.username = username
        if password is not None:
            self.uri.password = password
        return self

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
        if not uri.host:
            uri.host = "localhost"
        if not uri.port:
            uri.port = 4200

        sslmode = uri.query_params.pop("sslmode", "disable")
        use_ssl = asbool(uri.query_params.pop("ssl", "false")) or sslmode in [
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ]
        if use_ssl:
            uri.scheme = "https"
        return str(uri)

    def to_postgresql_url(self, port: int = 5432) -> URL:
        """
        Return the `postgresql://` variant of the database URI.
        """
        uri = deepcopy(self.uri)
        uri.scheme = "postgresql"
        if not uri.host:
            uri.host = "localhost"
        uri.port = port
        return uri

    def to_ingestr_url(self, port: int = 5432) -> URL:
        """
        Return the `cratedb://` variant of the database URI, suitable for `ingestr`.
        """
        uri = deepcopy(self.to_postgresql_url(port))
        uri.scheme = "cratedb"
        return uri

    @property
    def verify_ssl(self) -> bool:
        return self.uri.query_params.get("sslmode", "disable") not in ["disable", "require"]

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
        Decode database and table names and sanitize database URI.
        """
        from cratedb_toolkit.util.database import decode_database_table

        database, table = decode_database_table(self.dburi)
        uri = deepcopy(self.uri)
        if not uri.scheme.startswith("file"):
            uri.path = ""
        return uri, TableAddress(database, table)

    @property
    def username(self) -> t.Union[str, None]:
        """
        Return the username of the database URI.
        """
        return self.uri.username

    @property
    def password(self) -> t.Union[str, None]:
        """
        Return the password of the database URI.
        """
        return self.uri.password

    @property
    def schema(self) -> t.Union[str, None]:
        """
        Return the schema name from the URL path or from the `?schema=` query parameter of the database URI.
        """
        return self.uri.query_params.get("schema") or self.uri.path.lstrip("/")


@dataclasses.dataclass
class TableAddress:
    """
    Manage a table address, which is made of "<schema>"."<table>".
    """

    schema: t.Optional[str] = None
    table: t.Optional[str] = None

    @property
    def fullname(self):
        """
        Return a full-qualified quoted table identifier.
        """
        from cratedb_toolkit.util.database import DatabaseAdapter

        if not self.table:
            raise ValueError("Table name must be specified")
        # Default missing schema to CrateDB’s "doc"
        schema = self.schema or "doc"
        return DatabaseAdapter.quote_relation_name(f"{schema}.{self.table}")

    @classmethod
    def from_string(cls, table_name_full: str) -> "TableAddress":
        return TableAddress(*table_name_full.split("."))


@dataclasses.dataclass
class InputOutputResource:
    """
    Manage information about an input or output resource.
    """

    url: str
    format: t.Optional[str] = None  # noqa: A003
    compression: t.Optional[str] = None


@define
class AddressPair:
    """
    Manage two URL instances, specifically a pair of source/target URLs,
    where target is mostly a CrateDB Server, while source is any.
    """

    source_url: URL
    target_url: URL

    _source_url_query_parameters: t.Dict[str, t.Any] = Factory(dict)
    _target_url_query_parameters: t.Dict[str, t.Any] = Factory(dict)

    __SERVER_SCHEMES__ = ["http", "https", "mongodb", "mongodb+srv"]

    def navigate(self, source_path: str, target_path: str) -> "AddressPair":
        source_url = deepcopy(self.source_url)
        target_url = deepcopy(self.target_url)

        # Q: What the hack?
        # A: Adjustments about missing trailing slashes, business as usual.
        #    It makes subsequent `.navigate()` operations work.
        # Remark: It is not applicable for filesystem paths including wildcards,
        #         like `./datasets/*.ndjson`. In this case, `.navigate()` should
        #         strip the `*.ndjson` part, and replace it by the designated label.
        if source_url.scheme in self.__SERVER_SCHEMES__ and source_url.path[-1] != "/":
            source_url.path += "/"
        if target_url.path[-1] != "/":
            target_url.path += "/"

        source_url.path = urljoin(source_url.path, source_path)
        target_url.path = urljoin(target_url.path, target_path)

        return AddressPair(source_url, target_url)
