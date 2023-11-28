# Make Python 3.7 and 3.8 support generic types like `dict` instead of `typing.Dict`.
from __future__ import annotations

import copy
import logging
import warnings
from collections import deque
from typing import Any, Iterable, Mapping, Optional, Union

import sqlalchemy as sa
from bson import SON
from pymongo import CursorType, helpers
from pymongo.client_session import ClientSession
from pymongo.collation import validate_collation_or_none
from pymongo.collection import Collection
from pymongo.common import validate_is_document_type, validate_is_mapping
from pymongo.cursor import _QUERY_OPTIONS, Cursor, _Hint, _Sort
from pymongo.errors import InvalidOperation
from pymongo.message import _GetMore, _Query
from pymongo.read_preferences import _ServerMode
from pymongo.typings import _Address, _CollationIn, _DocumentType
from pymongo.write_concern import validate_boolean

from cratedb_toolkit.adapter.pymongo.reactor import mongodb_query, table_to_model
from cratedb_toolkit.adapter.pymongo.util import AmendedObjectId
from cratedb_toolkit.util import DatabaseAdapter

logger = logging.getLogger(__name__)


def cursor_factory(cratedb: DatabaseAdapter):
    class AmendedCursor(Cursor[_DocumentType]):
        _query_class = _Query
        _getmore_class = _GetMore

        def __init__(
            self,
            collection: Collection[_DocumentType],
            filter: Optional[Mapping[str, Any]] = None,  # noqa: A002
            projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
            skip: int = 0,
            limit: int = 0,
            no_cursor_timeout: bool = False,
            cursor_type: int = CursorType.NON_TAILABLE,
            sort: Optional[_Sort] = None,
            allow_partial_results: bool = False,
            oplog_replay: bool = False,
            batch_size: int = 0,
            collation: Optional[_CollationIn] = None,
            hint: Optional[_Hint] = None,
            max_scan: Optional[int] = None,
            max_time_ms: Optional[int] = None,
            max: Optional[_Sort] = None,  # noqa: A002
            min: Optional[_Sort] = None,  # noqa: A002
            return_key: Optional[bool] = None,
            show_record_id: Optional[bool] = None,
            snapshot: Optional[bool] = None,
            comment: Optional[Any] = None,
            session: Optional[ClientSession] = None,
            allow_disk_use: Optional[bool] = None,
            let: Optional[bool] = None,
        ) -> None:
            """Create a new cursor.

            Should not be called directly by application developers - see
            :meth:`~pymongo.collection.Collection.find` instead.

            .. seealso:: The MongoDB documentation on `cursors <https://dochub.mongodb.org/core/cursors>`_.
            """
            # Initialize all attributes used in __del__ before possibly raising
            # an error to avoid attribute errors during garbage collection.
            self.__collection: Collection[_DocumentType] = collection
            self.__id: Any = None
            self.__exhaust = False
            self.__sock_mgr: Any = None
            self.__killed = False
            self.__session: Optional[ClientSession]

            if session:
                self.__session = session
                self.__explicit_session = True
            else:
                self.__session = None
                self.__explicit_session = False

            spec: Mapping[str, Any] = filter or {}
            validate_is_mapping("filter", spec)
            if not isinstance(skip, int):
                raise TypeError("skip must be an instance of int")
            if not isinstance(limit, int):
                raise TypeError("limit must be an instance of int")
            validate_boolean("no_cursor_timeout", no_cursor_timeout)
            if no_cursor_timeout and not self.__explicit_session:
                warnings.warn(
                    "use an explicit session with no_cursor_timeout=True "
                    "otherwise the cursor may still timeout after "
                    "30 minutes, for more info see "
                    "https://mongodb.com/docs/v4.4/reference/method/"
                    "cursor.noCursorTimeout/"
                    "#session-idle-timeout-overrides-nocursortimeout",
                    UserWarning,
                    stacklevel=2,
                )
            if cursor_type not in (
                CursorType.NON_TAILABLE,
                CursorType.TAILABLE,
                CursorType.TAILABLE_AWAIT,
                CursorType.EXHAUST,
            ):
                raise ValueError("not a valid value for cursor_type")
            validate_boolean("allow_partial_results", allow_partial_results)
            validate_boolean("oplog_replay", oplog_replay)
            if not isinstance(batch_size, int):
                raise TypeError("batch_size must be an integer")
            if batch_size < 0:
                raise ValueError("batch_size must be >= 0")
            # Only set if allow_disk_use is provided by the user, else None.
            if allow_disk_use is not None:
                allow_disk_use = validate_boolean("allow_disk_use", allow_disk_use)

            if projection is not None:
                projection = helpers._fields_list_to_dict(projection, "projection")

            if let is not None:
                validate_is_document_type("let", let)

            self.__let = let
            self.__spec = spec
            self.__has_filter = filter is not None
            self.__projection = projection
            self.__skip = skip
            self.__limit = limit
            self.__batch_size = batch_size
            self.__ordering = sort and helpers._index_document(sort) or None
            self.__max_scan = max_scan
            self.__explain = False
            self.__comment = comment
            self.__max_time_ms = max_time_ms
            self.__max_await_time_ms: Optional[int] = None
            self.__max: Optional[Union[SON[Any, Any], _Sort]] = max
            self.__min: Optional[Union[SON[Any, Any], _Sort]] = min
            self.__collation = validate_collation_or_none(collation)
            self.__return_key = return_key
            self.__show_record_id = show_record_id
            self.__allow_disk_use = allow_disk_use
            self.__snapshot = snapshot
            self.__hint: Union[str, SON[str, Any], None]
            self.__set_hint(hint)

            # Exhaust cursor support
            # TODO: Implement.
            """
            if cursor_type == CursorType.EXHAUST:
                if self.__collection.database.client.is_mongos:
                    raise InvalidOperation("Exhaust cursors are not supported by mongos")
                if limit:
                    raise InvalidOperation("Can't use limit and exhaust together.")
                self.__exhaust = True
            """

            # This is ugly. People want to be able to do cursor[5:5] and
            # get an empty result set (old behavior was an
            # exception). It's hard to do that right, though, because the
            # server uses limit(0) to mean 'no limit'. So we set __empty
            # in that case and check for it when iterating. We also unset
            # it anytime we change __limit.
            self.__empty = False

            self.__data: deque = deque()
            self.__address: Optional[_Address] = None
            self.__retrieved = 0

            self.__codec_options = collection.codec_options
            # Read preference is set when the initial find is sent.
            self.__read_preference: Optional[_ServerMode] = None
            self.__read_concern = collection.read_concern

            self.__query_flags = cursor_type
            if no_cursor_timeout:
                self.__query_flags |= _QUERY_OPTIONS["no_timeout"]
            if allow_partial_results:
                self.__query_flags |= _QUERY_OPTIONS["partial"]
            if oplog_replay:
                self.__query_flags |= _QUERY_OPTIONS["oplog_replay"]

            # The namespace to use for find/getMore commands.
            self.__dbname = collection.database.name
            self.__collname = collection.name

            # Hack back the inheritance into the parent class.
            self._synthesize()

        def _synthesize(self):
            # Hack back the inheritance into the parent class, in order to save code.
            # Otherwise, it will yield errors like `AttributeError: 'AmendedCursor'
            # object has no attribute '_Cursor__explicit_session'`
            attrs = self.__dict__
            for name in list(attrs.keys()):
                if not name.startswith("_AmendedCursor"):
                    continue
                parent_name = name.replace("_AmendedCursor__", "_Cursor__")
                setattr(self, parent_name, getattr(self, name))

        def update_parent(self):
            self._Cursor__data = self.__data

        def next(self) -> _DocumentType:  # noqa: A002, A003
            """Advance the cursor."""
            if self.__empty:
                raise StopIteration
            if len(self.__data) or self._refresh():
                return self.__data.popleft()
            else:
                raise StopIteration

        __next__ = next

        def __enter__(self) -> Cursor[_DocumentType]:
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            self.close()

        def _refresh(self) -> int:
            """Refreshes the cursor with more data from Mongo.

            Returns the length of self.__data after refresh. Will exit early if
            self.__data is already non-empty. Raises OperationFailure when the
            cursor cannot be refreshed due to an error on the query.
            """
            if len(self.__data) or self.__killed:
                return len(self.__data)

            if not self.__session:
                self.__session = self.__collection.database.client._ensure_session()

            if self.__id is None:  # Query
                if (self.__min or self.__max) and not self.__hint:
                    raise InvalidOperation(
                        "Passing a 'hint' is required when using the min/max query"
                        " option to ensure the query utilizes the correct index"
                    )
                q = self._query_class(
                    self.__query_flags,
                    self.__collection.database.name,
                    self.__collection.name,
                    self.__skip,
                    self.__query_spec(),
                    self.__projection,
                    self.__codec_options,
                    self._read_preference(),
                    self.__limit,
                    self.__batch_size,
                    self.__read_concern,
                    self.__collation,
                    self.__session,
                    self.__collection.database.client,
                    self.__allow_disk_use,
                    self.__exhaust,
                )
                self.__send_message(q)
            elif self.__id:  # Get More
                if self.__limit:
                    limit = self.__limit - self.__retrieved
                    if self.__batch_size:
                        limit = min(limit, self.__batch_size)
                else:
                    limit = self.__batch_size
                # Exhaust cursors don't send getMore messages.
                g = self._getmore_class(
                    self.__dbname,
                    self.__collname,
                    limit,
                    self.__id,
                    self.__codec_options,
                    self._read_preference(),
                    self.__session,
                    self.__collection.database.client,
                    self.__max_await_time_ms,
                    self.__sock_mgr,
                    self.__exhaust,
                    self.__comment,
                )
                self.__send_message(g)

            return len(self.__data)

        def sort(self, key_or_list: _Hint, direction: Optional[Union[int, str]] = None) -> Cursor[_DocumentType]:
            """ """
            keys = helpers._index_list(key_or_list, direction)
            self.__ordering = helpers._index_document(keys)
            return self

        def __send_message(self, operation: Union[_Query, _GetMore]) -> None:
            """
            Usually sends a query or getmore operation and handles the response to/from a MongoDB server.
            Here, it will build an SQL query from the `operation`s metadata, and will have a conversation
            with a CrateDB server instead.

            TODO: OperationFailure / self.close() / PinnedResponse / explain / batching
            """
            metadata = sa.MetaData(schema=operation.db)
            table_name = operation.coll

            table = sa.Table(table_name, metadata, autoload_with=cratedb.engine)
            table.append_column(sa.Column("_id", sa.String(), primary_key=True, system=True))
            model = table_to_model(table)

            query = mongodb_query(
                model=model,
                filter=dict(self.__spec) or {},
                sort=self.__ordering and list(self.__ordering) or ["_id"],
            )
            records = query.fetchall(cratedb.connection)
            for record in records:
                record["_id"] = AmendedObjectId.from_str(record["_id"])
            self.__data = deque(records)
            self.__retrieved += len(records)
            self.__id = 0

            # Needed when manipulating `self.__data`, to synchronize
            # with the `Cursor` parent class.
            self.update_parent()

        def __query_spec(self) -> Mapping[str, Any]:
            """Get the spec to use for a query."""
            operators: dict[str, Any] = {}
            if self.__ordering:
                operators["$orderby"] = self.__ordering
            if self.__explain:
                operators["$explain"] = True
            if self.__hint:
                operators["$hint"] = self.__hint
            if self.__let:
                operators["let"] = self.__let
            if self.__comment:
                operators["$comment"] = self.__comment
            if self.__max_scan:
                operators["$maxScan"] = self.__max_scan
            if self.__max_time_ms is not None:
                operators["$maxTimeMS"] = self.__max_time_ms
            if self.__max:
                operators["$max"] = self.__max
            if self.__min:
                operators["$min"] = self.__min
            if self.__return_key is not None:
                operators["$returnKey"] = self.__return_key
            if self.__show_record_id is not None:
                # This is upgraded to showRecordId for MongoDB 3.2+ "find" command.
                operators["$showDiskLoc"] = self.__show_record_id
            if self.__snapshot is not None:
                operators["$snapshot"] = self.__snapshot

            if operators:
                # Make a shallow copy so we can cleanly rewind or clone.
                spec = copy.copy(self.__spec)

                # Allow-listed commands must be wrapped in $query.
                if "$query" not in spec:
                    # $query has to come first
                    spec = SON([("$query", spec)])

                if not isinstance(spec, SON):
                    # Ensure the spec is SON. As order is important this will
                    # ensure its set before merging in any extra operators.
                    spec = SON(spec)

                spec.update(operators)
                return spec
            # Have to wrap with $query if "query" is the first key.
            # We can't just use $query anytime "query" is a key as
            # that breaks commands like count and find_and_modify.
            # Checking spec.keys()[0] covers the case that the spec
            # was passed as an instance of SON or OrderedDict.
            elif "query" in self.__spec and (len(self.__spec) == 1 or next(iter(self.__spec)) == "query"):
                return SON({"$query": self.__spec})

            return self.__spec

        def __set_hint(self, index: Optional[_Hint]) -> None:
            if index is None:
                self.__hint = None
                return

            if isinstance(index, str):
                self.__hint = index
            else:
                self.__hint = helpers._index_document(index)

    return AmendedCursor
