import contextlib
import dataclasses
import logging
import threading
from typing import Any, Callable, Dict, Tuple

import sqlparse
from sqlalchemy.exc import ProgrammingError
from sqlparse.tokens import Keyword

_protection_lock = threading.RLock()

logger = logging.getLogger(__name__)


def make_protected_run_sql(original_run_sql: Callable) -> Callable:
    """
    Replacement method for `SQLDatabase.run_sql` that only permits read-only queries.
    """

    def _protected_run_sql(self, command: str) -> Tuple[str, Dict]:
        if not sql_is_permitted(command):
            raise ProgrammingError(command, {}, ValueError("Rejected SQL command"))
        return original_run_sql(self, command)

    return _protected_run_sql


@contextlib.contextmanager
def enable_sql_gateway():
    """
    Enable the SQL gateway for software-enforced read-only queries.
    """
    from llama_index.core import SQLDatabase

    with _protection_lock:
        original_run_sql = SQLDatabase.run_sql
        try:
            SQLDatabase.run_sql = make_protected_run_sql(original_run_sql)  # ty: ignore[invalid-assignment]
            yield
        finally:
            SQLDatabase.run_sql = original_run_sql  # ty: ignore[invalid-assignment]


def sql_is_permitted(expression: str) -> bool:
    """
    Validate the SQL expression, only permit read queries by default.

    NOTE: For serious protections, please use a dedicated read-only database user.

    FIXME: Revisit implementation, it might be too naive or weak.
           Issue:    https://github.com/crate/cratedb-mcp/issues/10
           Question: Does SQLAlchemy provide a solid read-only mode, or any other library?
    """
    is_dql = SqlStatementClassifier(expression=expression).is_dql
    if is_dql:
        logger.info("Permitted SQL expression: %s", expression and expression[:50])
    else:
        logger.warning("Denied SQL expression: %s", expression and expression[:50])
    return is_dql


@dataclasses.dataclass
class SqlStatementClassifier:
    """
    Helper to classify an SQL statement.

    Here, most importantly: Provide the `is_dql` property that
    signals truthfulness for read-only SQL SELECT statements only.
    """

    expression: str

    _parsed_sqlparse: Any = dataclasses.field(init=False, default=None)

    def __post_init__(self) -> None:
        if self.expression is None:
            self.expression = ""
        if self.expression:
            self.expression = self.expression.strip()

    def parse_sqlparse(self) -> Tuple[sqlparse.sql.Statement, ...]:
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
        return any(token.ttype is Keyword and token.value.upper() == "INTO" for token in statement.flatten())

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
