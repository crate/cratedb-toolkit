# Make Python 3.7 and 3.8 support generic types like `dict` instead of `typing.Dict`.
from __future__ import annotations

import io
import logging
from collections import abc
from typing import Any, Iterable, Iterator, Mapping, Optional, Union

import pandas as pd
from bson.raw_bson import RawBSONDocument
from pymongo import common
from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.results import InsertManyResult, InsertOneResult
from pymongo.typings import _DocumentType
from sqlalchemy_cratedb.support import insert_bulk

from cratedb_toolkit.adapter.pymongo.cursor import cursor_factory
from cratedb_toolkit.adapter.pymongo.util import AmendedObjectId as ObjectId
from cratedb_toolkit.util import DatabaseAdapter

logger = logging.getLogger(__name__)


def collection_factory(cratedb: DatabaseAdapter):
    class AmendedCollection(Collection):
        def find(self: Collection, *args: Any, **kwargs: Any) -> Cursor[_DocumentType]:
            AmendedCursor = cursor_factory(cratedb=cratedb)
            return AmendedCursor(self, *args, **kwargs)

        def count_documents(
            self: Collection,
            filter: Mapping[str, Any],  # noqa: A002
            session: Optional[ClientSession] = None,
            comment: Optional[Any] = None,
            **kwargs: Any,
        ) -> int:
            """
            TODO: Make it more efficient.
            """
            filter = filter or {}  # noqa: A001
            return len(list(self.find(filter=filter, session=session, comment=comment, **kwargs)))

        @staticmethod
        def get_df_info(df: pd.DataFrame) -> str:
            buffer = io.StringIO()
            df.info(buf=buffer)
            buffer.seek(0)
            return buffer.read()

        def insert_one(
            self: Collection,
            document: Union[_DocumentType, RawBSONDocument],
            bypass_document_validation: bool = False,
            session: Optional[ClientSession] = None,
            comment: Optional[Any] = None,
        ) -> InsertOneResult:
            logger.debug(f"Reading document: {document}")
            data = pd.DataFrame.from_records([document])
            # logger.debug(f"Dataframe: {self.get_df_info(data)}, {data.tail()}")  # noqa: ERA001
            logger.debug(f"Inserting record into CrateDB: schema={self.database.name}, table={self.name}")

            object_id_cratedb: Optional[str] = None

            def insert_returning_id(pd_table, conn, keys, data_iter):
                """
                Use CrateDB's "bulk operations" endpoint as a fast path for pandas' and Dask's `to_sql()` [1] method.

                The idea is to break out of SQLAlchemy, compile the insert statement, and use the raw
                DBAPI connection client, in order to be able to amend the SQL statement, adding a
                `RETURNING _id` clause.

                The vanilla implementation, used by SQLAlchemy, is::

                    data = [dict(zip(keys, row)) for row in data_iter]
                    conn.execute(pd_table.table.insert(), data)
                """
                nonlocal object_id_cratedb

                # Compile SQL statement and materialize batch.
                sql = str(pd_table.table.insert().compile(bind=conn))
                data = list(data_iter)

                # Invoke amended insert operation, returning the record
                # identifier as surrogate to MongoDB's `ObjectId`.
                cursor = conn._dbapi_connection.cursor()
                cursor.execute(sql=sql + " RETURNING _id", parameters=data[0])
                outcome = cursor.fetchone()
                object_id_cratedb = outcome[0]
                cursor.close()

            # TODO: Either, or?
            data.to_sql(
                name=self.name,
                schema=self.database.name,
                con=cratedb.engine,
                index=False,
                # TODO: Handle `append` vs. `replace`.
                if_exists="append",
                method=insert_returning_id,
            )

            if object_id_cratedb is None:
                raise ValueError("Object may have been created, but there is no object id")

            object_id_mongodb = ObjectId.from_str(object_id_cratedb)
            logger.debug(f"Created object with id: {object_id_mongodb!r}")
            return InsertOneResult(inserted_id=object_id_mongodb, acknowledged=True)

        def insert_many(
            self,
            documents: Iterable[Union[_DocumentType, RawBSONDocument]],
            ordered: bool = True,
            bypass_document_validation: bool = False,
            session: Optional[ClientSession] = None,
            comment: Optional[Any] = None,
        ) -> InsertManyResult:
            if not isinstance(documents, abc.Iterable) or isinstance(documents, abc.Mapping) or not documents:
                raise TypeError("documents must be a non-empty list")
            inserted_ids: list[ObjectId] = []

            def gen() -> Iterator[Mapping[str, Any]]:
                """A generator that validates documents and handles _ids."""
                for document in documents:
                    common.validate_is_document_type("document", document)
                    if not isinstance(document, RawBSONDocument):
                        if "_id" in document:
                            identifier = ObjectId(document["_id"])
                        else:
                            identifier = ObjectId()
                            document["_id"] = str(identifier)  # type: ignore[index]
                        inserted_ids.append(identifier)
                    yield document

            logger.debug("Converting documents")
            documents_real = list(gen())

            logger.debug(f"Reading documents: {documents_real}")
            data = pd.DataFrame.from_records(documents_real)
            logger.debug(f"Dataframe: {self.get_df_info(data)}, {data.tail()}")  # noqa: ERA001
            logger.debug(f"Inserting records into CrateDB: schema={self.database.name}, table={self.name}")

            data.to_sql(
                name=self.name,
                schema=self.database.name,
                con=cratedb.engine,
                index=False,
                # TODO: Handle `append` vs. `replace`.
                if_exists="append",
                method=insert_bulk,
            )

            return InsertManyResult(inserted_ids, acknowledged=True)

    return AmendedCollection
