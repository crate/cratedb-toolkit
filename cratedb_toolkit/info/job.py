import dataclasses
import logging
import time
from typing import List, Any
import polars as pl
import attr
import sqlparse
from boltons.iterutils import flatten
from sqlparse.tokens import Keyword

from cratedb_toolkit.util.database import DatabaseAdapter, get_table_names

logger = logging.getLogger(__name__)


@attr.define
class Operation:
    op: str
    stmt: str
    tables_symbols: List[str] = attr.field(factory=list)
    # tables_effective: List[str] = attr.field(factory=list)

@attr.define
class Operations:
    data: List[Operation]

    def foo(self):
        fj = [attr.asdict(j) for j in self.data]
        df = pl.from_records(fj)
        print(df)
        #grouped = df.group_by("tables_symbols").agg([pl.sum("tables_symbols"), pl.sum("op")])
        grouped = df.sql("SELECT tables_symbols, COUNT(op) FROM self GROUP BY tables_symbols")
        print(grouped)


class TableTraffic:

    def __init__(self, adapter: DatabaseAdapter):
        self.adapter = adapter

    def read_jobs_database(self, begin: int = 0, end: int = 0):
        logger.info("Reading sys.jobs_log")
        now = int(time.time() * 1000)
        end = end or now
        begin = begin or now - 600 * 60 * 1000
        stmt = (
            f"SELECT "
            f"started, ended, classification, stmt, username, node "
            f"FROM sys.jobs_log "
            f"WHERE "
            f"stmt NOT LIKE '%sys.%' AND "
            f"stmt NOT LIKE '%information_schema.%' "
            f"AND ended BETWEEN {begin} AND {end} "
            f"ORDER BY ended ASC"
        )
        return self.adapter.run_sql(stmt, records=True)

    def read_jobs(self, jobs) -> List[Operation]:
        result = []
        for job in jobs:
            sql = job["stmt"]
            result.append(self.parse_expression(sql))
        return result

    @staticmethod
    def parse_expression(sql: str) -> Operation:
        logger.debug(f"Analyzing SQL: {sql}")
        classifier = SqlStatementClassifier(expression=sql)
        if not classifier.operation:
            logger.warning(f"Unable to determine operation: {sql}")
        if not classifier.table_names:
            logger.warning(f"Unable to determine table names: {sql}")
        return Operation(
            op=classifier.operation,
            stmt=sql,
            tables_symbols=classifier.table_names,
        )

    def analyze_jobs(self, ops: Operations):
        ops.foo()

    def render(self):
        jobs = self.read_jobs_database()
        logger.info(f"Analyzing {len(jobs)} jobs")
        ops = Operations(self.read_jobs(jobs))
        jobsa = self.analyze_jobs(ops)
        logger.info(f"Result: {jobsa}")


@dataclasses.dataclass
class SqlStatementClassifier:
    """
    Helper to classify an SQL statement.

    Here, most importantly: Provide the `is_dql` property that
    signals truthfulness for read-only SQL SELECT statements only.
    """

    expression: str
    permit_all: bool = False

    _parsed_sqlparse: Any = dataclasses.field(init=False, default=None)

    def __post_init__(self) -> None:
        if self.expression is None:
            self.expression = ""
        if self.expression:
            self.expression = self.expression.strip()

    def parse_sqlparse(self) -> List[sqlparse.sql.Statement]:
        """
        Parse expression using traditional `sqlparse` library.
        """
        if self._parsed_sqlparse is None:
            self._parsed_sqlparse = sqlparse.parse(self.expression)
        return self._parsed_sqlparse

    @property
    def is_dql(self) -> bool:
        """
        Is it a DQL statement, which effectively invokes read-only operations only?
        """

        if not self.expression:
            return False

        if self.permit_all:
            return True

        # Check if the expression is valid and if it's a DQL/SELECT statement,
        # also trying to consider `SELECT ... INTO ...` and evasive
        # `SELECT * FROM users; \uff1b DROP TABLE users` statements.
        return self.is_select and not self.is_camouflage

    @property
    def is_select(self) -> bool:
        """
        Whether the expression is an SQL SELECT statement.
        """
        return self.operation == "SELECT"

    @property
    def operation(self) -> str:
        """
        The SQL operation: SELECT, INSERT, UPDATE, DELETE, CREATE, etc.
        """
        parsed = self.parse_sqlparse()
        return parsed[0].get_type().upper()

    @property
    def table_names(self) -> List[str]:
        """
        The SQL operation: SELECT, INSERT, UPDATE, DELETE, CREATE, etc.
        """
        return flatten(get_table_names(self.expression))

    @property
    def is_camouflage(self) -> bool:
        """
        Innocent-looking `SELECT` statements can evade filters.
        """
        return self.is_select_into or self.is_evasive

    @property
    def is_select_into(self) -> bool:
        """
        Use traditional `sqlparse` for catching `SELECT ... INTO ...` statements.
        Examples:
            SELECT * INTO foobar FROM bazqux
            SELECT * FROM bazqux INTO foobar
        """
        # Flatten all tokens (including nested ones) and match on type+value.
        statement = self.parse_sqlparse()[0]
        return any(
            token.ttype is Keyword and token.value.upper() == "INTO"
            for token in statement.flatten()
        )

    @property
    def is_evasive(self) -> bool:
        """
        Use traditional `sqlparse` for catching evasive SQL statements.

        A practice picked up from CodeRabbit was to reject multiple statements
        to prevent potential SQL injections. Is it a viable suggestion?

        Examples:

            SELECT * FROM users; \uff1b DROP TABLE users
        """
        parsed = self.parse_sqlparse()
        return len(parsed) > 1
