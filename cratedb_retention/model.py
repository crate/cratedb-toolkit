# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import dataclasses
import logging
import os
import typing as t
from collections import OrderedDict
from copy import deepcopy
from enum import Enum

from boltons.urlutils import URL

logger = logging.getLogger(__name__)


class RetentionStrategy(Enum):
    """
    Enumerate list of retention strategies.
    """

    DELETE = "DELETE"
    REALLOCATE = "REALLOCATE"
    SNAPSHOT = "SNAPSHOT"


@dataclasses.dataclass
class RetentionPolicy:
    """
    Manage a retention policy entity.

    This layout has to be synchronized with both the table definition per SQL DDL
    statement within `schema.sql`, and the query result representation per SQL DQL
    statement within `policy.sql`.

    To be more specific, the SQL DQL result returns two additional fields,
    `table_fullname`, and `retention_value`, when compared to the original
    DDL.

    TODO: Maybe this anomaly could be dissolved?
    """

    strategy: t.Union[RetentionStrategy, str] = dataclasses.field(
        metadata={"help": "Which kind of retention strategy to use or apply"},
    )
    tags: t.Optional[t.Union[list, set]] = dataclasses.field(
        default_factory=set,
        metadata={"help": "Tags for retention policy, used for grouping and filtering"},
    )
    table_schema: t.Optional[str] = dataclasses.field(
        default=None,
        metadata={"help": "The source table schema"},
    )
    table_name: t.Optional[str] = dataclasses.field(
        default=None,
        metadata={"help": "The source table name"},
    )
    table_fullname: t.Optional[str] = dataclasses.field(
        default=None,
        metadata={"read_only": True},
    )
    partition_column: t.Optional[str] = dataclasses.field(
        default=None,
        metadata={"help": "The source table column name used for partitioning"},
    )
    partition_value: t.Optional[str] = dataclasses.field(
        default=None,
        metadata={"read_only": True},
    )
    retention_period: t.Optional[int] = dataclasses.field(
        default=None,
        metadata={
            "help": "Retention period in days. The number of days data gets "
            "retained before applying the retention policy."
        },
    )
    reallocation_attribute_name: t.Optional[str] = dataclasses.field(
        default=None,
        metadata={"help": "Name of the node-specific custom attribute, when targeting specific nodes"},
    )
    reallocation_attribute_value: t.Optional[str] = dataclasses.field(
        default=None,
        metadata={"help": "Value of the node-specific custom attribute, when targeting specific nodes"},
    )
    target_repository_name: t.Optional[str] = dataclasses.field(
        default=None,
        metadata={"help": "The name of a repository created with `CREATE REPOSITORY ...`, when targeting a repository"},
    )

    @classmethod
    def from_record(cls, record):
        """
        Factory for creating instance from database record.
        """
        return cls(**cls.record_mapper(record))

    @classmethod
    def record_mapper(cls, record):
        """
        Map database record to instance attributes.
        """
        fields = dataclasses.fields(cls)
        out = OrderedDict()
        for index, field in enumerate(fields):
            out[field.name] = record[index]
        return out

    def to_storage_dict(self):
        """
        Return representation suitable for storing into database table using SQLAlchemy.
        """

        # Serialize to dictionary.
        data = dataclasses.asdict(self)

        # Strategy values are stored in lower case.
        data["strategy"] = str(data["strategy"].value).lower()

        # Converge list of tags to `OBJECT(DYNAMIC)` representation.
        tags = OrderedDict()
        for tag in data["tags"]:
            tags[tag] = "true"
        data["tags"] = tags

        # Purge fields not present in physical representation.
        del data["table_fullname"]
        del data["partition_value"]

        return data


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


def default_table_address():
    """
    The default address of the retention policy table.
    """
    schema = os.environ.get("CRATEDB_EXT_SCHEMA", "ext")
    return TableAddress(schema=schema, table="retention_policy")


@dataclasses.dataclass
class JobSettings:
    """
    Bundle all configuration and runtime settings.
    """

    # Database connection URI.
    database: DatabaseAddress = dataclasses.field(
        default_factory=lambda: DatabaseAddress.from_string("crate://localhost/")
    )

    # Retention strategy.
    strategy: t.Optional[RetentionStrategy] = None

    # Tags: For grouping, multi-tenancy, and more.
    tags: t.Set[str] = dataclasses.field(default_factory=set)

    # Retention cutoff timestamp.
    cutoff_day: t.Optional[str] = None

    # Where the retention policy table is stored.
    policy_table: TableAddress = dataclasses.field(default_factory=default_table_address)

    def to_dict(self):
        data = dataclasses.asdict(self)
        data["policy_table"] = self.policy_table
        return data
