# TODO: Maybe refactor to `sqlalchemy-cratedb` or `commons-codec` on another iteration?
import json
import typing as t
from functools import cached_property

import sqlalchemy as sa
from attr import Factory
from attrs import define
from commons_codec.model import SQLOperation
from pympler.asizeof import asizeof
from sqlalchemy.exc import ProgrammingError
from tqdm import tqdm

from cratedb_toolkit.util.database import logger


class BulkResultItem(t.TypedDict):
    """
    Define the shape of a CrateDB bulk request response item.
    """

    rowcount: int


@define
class BulkResponse:
    """
    Manage CrateDB bulk request responses.
    Accepts a list of bulk arguments (parameter list) and a list of bulk response items.

    https://cratedb.com/docs/crate/reference/en/latest/interfaces/http.html#bulk-operations

    TODO: Think about refactoring this to `sqlalchemy_cratedb.support`.
    """

    parameters: t.Union[t.List[t.Dict[str, t.Any]], None]
    cratedb_bulk_result: t.Union[t.List[BulkResultItem], None]

    @cached_property
    def failed_records(self) -> t.List[t.Dict[str, t.Any]]:
        """
        Compute list of failed records.

        CrateDB signals failed insert using `rowcount=-2`.

        https://cratedb.com/docs/crate/reference/en/latest/interfaces/http.html#error-handling
        """
        if self.parameters is None or self.cratedb_bulk_result is None:
            return []
        errors: t.List[t.Dict[str, t.Any]] = []
        for record, status in zip(self.parameters, self.cratedb_bulk_result):
            if status["rowcount"] != 1:
                errors.append(record)
        return errors

    @cached_property
    def parameter_count(self) -> int:
        """
        Compute bulk size / length of parameter list.
        """
        if not self.parameters:
            return 0
        return len(self.parameters)

    @cached_property
    def success_count(self) -> int:
        """
        Compute number of succeeding records within a batch.
        """
        return self.parameter_count - self.failed_count

    @cached_property
    def failed_count(self) -> int:
        """
        Compute number of failed records within a batch.
        """
        return len(self.failed_records)


@define
class BulkMetrics:
    """
    Manage a few details for a `BulkProcessor` task.
    """

    count_success_total: int = 0
    count_error_total: int = 0
    bytes_write_total: int = 0
    bytes_error_total: int = 0
    rate_current: int = 0
    rate_max: int = 0


@define
class BulkProcessor:
    """
    Generic driver to run a bulk operation against CrateDB, which can fall back to record-by-record operation.

    It aims to provide a combination of both performance/efficiency by using bulk operations,
    and also good usability and on-the-spot error message for records that fail to insert.

    Background: This is a canonical client-side API wrapper for CrateDB's bulk operations HTTP endpoint.
    https://cratedb.com/docs/crate/reference/en/latest/interfaces/http.html#bulk-operations
    """

    connection: sa.Connection
    data: t.Iterable[t.List[t.Dict[str, t.Any]]]
    batch_to_operation: t.Callable[[t.List[t.Dict[str, t.Any]]], SQLOperation]
    progress_bar: t.Union[tqdm, None] = None
    on_error: t.Literal["ignore", "raise"] = "ignore"
    debug: bool = False

    _metrics: BulkMetrics = Factory(BulkMetrics)

    @cached_property
    def log_level(self):
        if self.debug:
            return logger.exception
        else:
            return logger.warning

    def start(self) -> BulkMetrics:
        # Acquire batches of documents, convert to SQL operations, and submit to CrateDB.
        batch_count = 0
        for batch in self.data:
            batch_count += 1
            self.progress_bar and self.progress_bar.set_description("READ ")
            current_batch_size = len(batch)
            try:
                operation = self.batch_to_operation(batch)
            except Exception as ex:
                self._metrics.count_error_total += current_batch_size
                self.log_level(f"Computing query failed: {ex}")
                if self.on_error == "raise":
                    raise
                continue

            self._metrics.bytes_write_total += asizeof(operation)
            statement = sa.text(operation.statement)

            # Submit operation to CrateDB, using `bulk_args`.
            self.progress_bar and self.progress_bar.set_description("WRITE")
            try:
                cursor = self.connection.execute(statement=statement, parameters=operation.parameters)
                self.connection.commit()
                if cursor.rowcount > 0:
                    cratedb_bulk_result = getattr(cursor.context, "last_result", None)
                    bulk_response = BulkResponse(operation.parameters, cratedb_bulk_result)
                    failed_records = bulk_response.failed_records
                    count_success_local = bulk_response.success_count
                    self._metrics.count_success_total += bulk_response.success_count
                    self.progress_bar and self.progress_bar.update(n=bulk_response.success_count)
                else:
                    failed_records = operation.parameters
                    count_success_local = 0
                    self.progress_bar and self.progress_bar.update(n=1)

            # When a batch is of size one, an exception is raised.
            # Just signal the same condition as if a batch would have failed.
            except ProgrammingError:
                failed_records = [operation.parameters]
                count_success_local = 0

            # When bulk operations fail, try inserting failed records record-by-record,
            # in order to relay proper error messages to the user.
            if failed_records:
                logger.warning(
                    f"Incomplete batch #{batch_count}. Records processed: {count_success_local}/{current_batch_size}. "
                    f"Falling back to per-record operations."
                )
                for record in failed_records:
                    try:
                        cursor = self.connection.execute(statement=statement, parameters=record)
                        self.connection.commit()
                        if cursor.rowcount != 1:
                            raise IOError("Record has not been processed")
                        self._metrics.count_success_total += 1
                    except Exception as ex:
                        logger.error(f"Operation failed: {ex}")
                        logger.debug(f"Invalid record:\n{json.dumps(record, indent=2)}")
                        self._metrics.count_error_total += 1
                        self._metrics.bytes_error_total += asizeof(record)
                        if self.on_error == "raise":
                            raise
                    self.progress_bar and self.progress_bar.update(n=1)

        self.progress_bar and self.progress_bar.close()

        return self._metrics
