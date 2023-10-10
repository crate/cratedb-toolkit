# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import dataclasses
import os
import typing as t

from sqlalchemy_cratedb.support import quote_relation_name

from cratedb_toolkit.model import DatabaseAddress, TableAddress


@dataclasses.dataclass
class MaterializedView:
    """
    Manage the database representation of a "materialized view" entity.

    This layout has to be synchronized with the corresponding table definition
    per SQL DDL statement within `schema.sql`.
    """

    table_schema: t.Optional[str] = dataclasses.field(
        default=None,
        metadata={"help": "The target table schema"},
    )
    table_name: t.Optional[str] = dataclasses.field(
        default=None,
        metadata={"help": "The target table name"},
    )
    sql: t.Optional[str] = dataclasses.field(
        default=None,
        metadata={"help": "The SQL statement defining the emulated materialized view"},
    )

    id: t.Optional[str] = dataclasses.field(  # noqa: A003
        default=None,
        metadata={"help": "The materialized view identifier"},
    )

    @property
    def table_fullname(self) -> str:
        return quote_relation_name(f"{self.table_schema}.{self.table_name}")

    @property
    def staging_table_fullname(self) -> str:
        return quote_relation_name(f"{self.table_schema}-staging.{self.table_name}")

    @classmethod
    def from_record(cls, record) -> "MaterializedView":
        return cls(**record)

    def to_storage_dict(self, identifier: t.Optional[str] = None) -> t.Dict[str, str]:
        """
        Return representation suitable for storing into a database table using SQLAlchemy.

        Args:
            identifier: If provided, this will override any existing id in the instance.
        """

        # Serialize to dictionary.
        data = dataclasses.asdict(self)

        # Optionally add identifier.
        if identifier is not None:
            # Explicitly override any existing id.
            data["id"] = identifier

        return data


def default_table_address():
    """
    The default address of the materialized view management table.
    """
    schema = os.environ.get("CRATEDB_EXT_SCHEMA", "ext")
    return TableAddress(schema=schema, table="materialized_view")


@dataclasses.dataclass
class MaterializedViewSettings:
    """
    Bundle all configuration and runtime settings.
    """

    # Database connection URI.
    database: DatabaseAddress = dataclasses.field(
        default_factory=lambda: DatabaseAddress.from_string("crate://localhost/")
    )

    # The address of the materialized view table.
    materialized_table: TableAddress = dataclasses.field(default_factory=default_table_address)

    # Only pretend to invoke statements.
    dry_run: t.Optional[bool] = False

    def to_dict(self):
        data = dataclasses.asdict(self)
        data["materialized_table"] = self.materialized_table
        return data
