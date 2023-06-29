# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import dataclasses
import logging
import typing as t
from collections import OrderedDict
from copy import deepcopy
from enum import Enum
from importlib.resources import read_text

from boltons.urlutils import URL

from cratedb_retention.util.database import DatabaseTagHelper, run_sql, sql_and

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
    """

    strategy: str
    tags: list
    table_schema: str
    table_name: str
    table_fullname: str
    partition_column: str
    partition_value: str
    retention_period: int
    reallocation_attribute_name: str
    reallocation_attribute_value: str
    target_repository_name: str

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
    tags: t.Optional[t.List] = dataclasses.field(default_factory=list)

    # Retention cutoff timestamp.
    cutoff_day: t.Optional[str] = None

    # Where the retention policy table is stored.
    policy_table: TableAddress = dataclasses.field(
        default_factory=lambda: TableAddress(schema="ext", table="retention_policy")
    )

    def to_dict(self):
        data = dataclasses.asdict(self)
        data["policy_table"] = self.policy_table
        return data


@dataclasses.dataclass
class GenericRetention:
    """
    Represent a complete generic data retention job.
    """

    # Runtime context settings.
    settings: JobSettings

    # Which action class to use for deserializing records.
    _action_class: t.Any = dataclasses.field(init=False)

    # File name of SQL statement to load retention policies.
    _tasks_sql_file: t.Union[str, None] = dataclasses.field(init=False, default=None)

    # SQL statement to load retention policies.
    _tasks_sql_text: t.Union[str, None] = dataclasses.field(init=False, default=None)

    def __post_init__(self):
        from cratedb_retention.util.database import DatabaseAdapter

        self.database = DatabaseAdapter(dburi=self.settings.database.dburi)
        self.tag_helper = DatabaseTagHelper(database=self.database, tablename=self.settings.policy_table.fullname)

    def start(self):
        """
        Evaluate retention policies, and invoke actions.
        """

        for policy in self.get_policy_tasks():
            logger.info(f"Executing data retention policy: {policy}")
            sql_bunch: t.Iterable = policy.to_sql()
            if not isinstance(sql_bunch, t.List):
                sql_bunch = [sql_bunch]

            # Run a sequence of SQL statements for this task.
            # Stop the sequence once anyone fails.
            for sql in sql_bunch:
                try:
                    run_sql(dburi=self.settings.database.dburi, sql=sql)
                except Exception:
                    logger.exception(f"Data retention SQL statement failed: {sql}")
                    # TODO: Do not `raise`, but `break`. Other policies should be executed.
                    raise

    def get_where_clause(self, field_prefix: str = ""):
        """
        Compute SQL WHERE clause based on selected strategy and tags.
        """
        if self.settings.strategy is None:
            raise ValueError("Unable to build where clause without retention strategy")
        strategy = str(self.settings.strategy.value).lower()
        fragments = [f"{field_prefix}strategy='{strategy}'"]
        fragments += self.tag_helper.get_tags_sql(tags=self.settings.tags, field_prefix=field_prefix)
        return sql_and(fragments)

    def get_policy_tasks(self):
        """
        Resolve retention policy items.
        """
        if self._action_class is None:
            raise ValueError("Loading retention policies needs an action class")

        # Read baseline SQL clause, for selecting records from the retention policy
        # database table, to be interpolated into the other templates.
        policy_dql = read_text("cratedb_retention.strategy", "policy.sql")

        if self.settings.tags and not self.tag_helper.tags_exist(self.settings.tags):
            logger.warning(f"No retention policies found with tags: {self.settings.tags}")
            return []

        where_clause = self.get_where_clause(field_prefix="r.")
        logger.info(f"where_clause: {where_clause}")

        # Read SQL statement, and interpolate runtime settings as template variables.
        if self._tasks_sql_file:
            sql = read_text("cratedb_retention.strategy", self._tasks_sql_file)
        elif self._tasks_sql_text:
            sql = self._tasks_sql_text
        else:
            sql = f"""
{policy_dql}
WHERE
{where_clause}
;
            """
        tplvars = self.settings.to_dict()
        try:
            sql = sql.format(policy_dql=policy_dql, where_clause=where_clause)
        except KeyError:
            pass
        sql = sql.format_map(tplvars)

        # Load retention policies, already resolved against source table,
        # and retention period vs. cut-off date.
        policy_records = run_sql(self.settings.database.dburi, sql)
        if not policy_records:
            logger.warning("Retention policies and/or data table not found, or no data to be retired")
            return []

        logger.info(f"Loaded retention policies: {policy_records}")
        for record in policy_records:
            # Unmarshal entity from database table record to Python object.
            policy = self._action_class.from_record(record)
            yield policy
