# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import abc
import dataclasses
import logging
import os
import typing as t
from collections import OrderedDict
from enum import Enum

from cratedb_toolkit.model import DatabaseAddress, TableAddress

logger = logging.getLogger(__name__)


class RetentionStrategy(Enum):
    """
    Enumerate list of retention strategies.
    """

    DELETE = "DELETE"
    REALLOCATE = "REALLOCATE"
    SNAPSHOT = "SNAPSHOT"

    def to_database(self) -> str:
        """
        In the database, strategy values are stored in lower case.
        """
        return str(self.value).lower()


@dataclasses.dataclass
class RetentionPolicy:
    """
    Manage the database representation of a "retention policy" entity.

    This layout has to be synchronized with the corresponding table definition
    per SQL DDL statement within `schema.sql`.
    """

    strategy: RetentionStrategy = dataclasses.field(
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
    partition_column: t.Optional[str] = dataclasses.field(
        default=None,
        metadata={"help": "The source table column name used for partitioning"},
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

    id: t.Optional[str] = dataclasses.field(  # noqa: A003
        default=None,
        metadata={"help": "The retention policy identifier"},
    )

    @property
    def table_fullname(self) -> str:
        return f'"{self.table_schema}"."{self.table_name}"'

    @classmethod
    def from_record(cls, record) -> "RetentionPolicy":
        strategy = RetentionStrategy(record["strategy"].upper())
        del record["strategy"]
        return cls(strategy=strategy, **record)

    def to_storage_dict(self, identifier: t.Optional[str] = None) -> t.Dict[str, str]:
        """
        Return representation suitable for storing into database table using SQLAlchemy.
        """

        # Serialize to dictionary.
        data = dataclasses.asdict(self)

        # Marshal strategy type.
        data["strategy"] = self.strategy.to_database()

        # Marshal list of tags to `OBJECT(DYNAMIC)` representation.
        tags = OrderedDict()
        for tag in data["tags"]:
            tags[tag] = "true"
        data["tags"] = tags

        # Optionally add identifier.
        if identifier is not None:
            data["id"] = identifier

        return data


@dataclasses.dataclass
class RetentionTask:
    """
    Represent a retention task at runtime.

    Specialized retention tasks, like `ReallocateRetentionTask`, derive from this class.

    It mostly contains attributes from `RetentionPolicy`, but a) offers marshalled
    values only, and b) omits some settings/parameters which have already been resolved
    by previous processing layers.
    """

    table_schema: str
    table_name: str
    table_fullname: str
    partition_column: str
    partition_value: str
    reallocation_attribute_name: str
    reallocation_attribute_value: str
    target_repository_name: str

    @classmethod
    def factory(cls, **kwargs):
        """
        Create an instance from keyword arguments.
        """
        return cls(**kwargs)

    @abc.abstractmethod
    def to_sql(self) -> t.Union[str, t.List[str]]:
        """
        Return one or more SQL statements, which resemble this task.
        """
        pass


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

    # Only pretend to invoke retention tasks.
    dry_run: t.Optional[bool] = False

    def to_dict(self):
        data = dataclasses.asdict(self)
        data["policy_table"] = self.policy_table
        return data
