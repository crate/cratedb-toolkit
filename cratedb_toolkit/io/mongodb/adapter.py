import itertools
import logging
import typing as t
from abc import abstractmethod
from functools import cached_property
from pathlib import Path

import boltons.urlutils
import polars as pl
import pymongo
import yarl
from attrs import define, field
from boltons.urlutils import URL
from bson.raw_bson import RawBSONDocument
from undatum.common.iterable import IterableData

from cratedb_toolkit.io.mongodb.util import batches
from cratedb_toolkit.model import DatabaseAddress

logger = logging.getLogger(__name__)


@define
class MongoDBAdapterBase:
    address: DatabaseAddress
    effective_url: URL
    database_name: str
    collection_name: str

    _custom_query_parameters = ["batch-size", "limit", "offset"]

    @classmethod
    def from_url(cls, url: t.Union[str, boltons.urlutils.URL, yarl.URL]):
        if not isinstance(url, str):
            url = str(url)
        mongodb_address = DatabaseAddress.from_string(url)
        mongodb_uri, mongodb_collection_address = mongodb_address.decode()
        logger.info(f"Collection address: {mongodb_collection_address}")
        mongodb_database = mongodb_collection_address.schema
        mongodb_collection = mongodb_collection_address.table
        for custom_query_parameter in cls._custom_query_parameters:
            mongodb_uri.query_params.pop(custom_query_parameter, None)
        return cls(
            address=mongodb_address,
            effective_url=mongodb_uri,
            database_name=mongodb_database,
            collection_name=mongodb_collection,
        )

    def __attrs_post_init__(self):
        self.setup()

    @cached_property
    def batch_size(self) -> int:
        return int(self.address.uri.query_params.get("batch-size", 500))

    @cached_property
    def limit(self) -> int:
        return int(self.address.uri.query_params.get("limit", 0))

    @cached_property
    def offset(self) -> int:
        return int(self.address.uri.query_params.get("offset", 0))

    @abstractmethod
    def setup(self):
        raise NotImplementedError()

    @abstractmethod
    def record_count(self, filter_=None) -> int:
        raise NotImplementedError()

    @abstractmethod
    def query(self):
        raise NotImplementedError()


@define
class MongoDBFileAdapter(MongoDBAdapterBase):
    _path: Path = field(init=False)

    def setup(self):
        self._path = Path(self.address.uri.path)

    def record_count(self, filter_=None) -> int:
        """
        https://stackoverflow.com/a/27517681
        """
        f = open(self._path, "rb")
        bufgen = itertools.takewhile(lambda x: x, (f.raw.read(1024 * 1024) for _ in itertools.repeat(None)))
        return sum(buf.count(b"\n") for buf in bufgen if buf)

    def query(self):
        if not self._path.exists():
            raise FileNotFoundError(f"Resource not found: {self._path}")
        if self.offset:
            raise NotImplementedError("Using offsets is not supported by Polars' NDJSON reader")
        if self._path.suffix in [".ndjson", ".jsonl"]:
            data = pl.read_ndjson(self._path, batch_size=self.batch_size, n_rows=self.limit or None).to_dicts()
        elif ".bson" in str(self._path):
            data = IterableData(str(self._path), options={"format_in": "bson"}).iter()
        else:
            raise ValueError(f"Unsupported file type: {self._path.suffix}")
        return batches(data, self.batch_size)


@define
class MongoDBServerAdapter(MongoDBAdapterBase):
    _mongodb_client: pymongo.MongoClient = field(init=False)
    _mongodb_collection: pymongo.collection.Collection = field(init=False)

    def setup(self):
        self._mongodb_client: pymongo.MongoClient = pymongo.MongoClient(
            str(self.effective_url),
            document_class=RawBSONDocument,
            datetime_conversion="DATETIME_AUTO",
        )
        self._mongodb_collection = self._mongodb_client[self.database_name][self.collection_name]

    def record_count(self, filter_=None) -> int:
        filter_ = filter_ or {}
        return self._mongodb_collection.count_documents(filter=filter_)

    def query(self):
        data = self._mongodb_collection.find().batch_size(self.batch_size).skip(self.offset).limit(self.limit)
        return batches(data, self.batch_size)


def mongodb_adapter_factory(mongodb_uri: URL) -> MongoDBAdapterBase:
    if mongodb_uri.scheme.startswith("file"):
        return MongoDBFileAdapter.from_url(mongodb_uri)
    elif mongodb_uri.scheme.startswith("mongodb"):
        return MongoDBServerAdapter.from_url(mongodb_uri)
    raise ValueError("Unable to create MongoDB adapter")
