import glob
import itertools
import json
import logging
import typing as t
from abc import abstractmethod
from copy import deepcopy
from functools import cached_property
from pathlib import Path

import boltons.urlutils
import bson
import pymongo
import pymongo.collection
import pymongo.database
import yarl
from attrs import define, field
from boltons.urlutils import URL
from bson.raw_bson import RawBSONDocument
from undatum.common.iterable import IterableData

from cratedb_toolkit.io.mongodb.model import DocumentDict
from cratedb_toolkit.io.mongodb.util import batches
from cratedb_toolkit.model import DatabaseAddress
from cratedb_toolkit.util.data import asbool
from cratedb_toolkit.util.io import read_json

logger = logging.getLogger(__name__)


@define
class MongoDBAdapterBase:
    address: DatabaseAddress
    effective_url: URL
    database_name: str
    collection_name: str

    _custom_query_parameters = ["batch-size", "direct", "filter", "limit", "offset", "timeout"]
    _default_timeout = 5000

    direct: bool = False
    timeout: int = _default_timeout

    @classmethod
    def from_url(cls, url: t.Union[str, boltons.urlutils.URL, yarl.URL]):
        if not isinstance(url, str):
            url = str(url)
        mongodb_address = DatabaseAddress.from_string(url)
        mongodb_uri, mongodb_collection_address = mongodb_address.decode()
        mongodb_database = mongodb_collection_address.schema
        mongodb_collection = mongodb_collection_address.table
        direct = asbool(mongodb_uri.query_params.pop("direct", False))
        timeout = mongodb_uri.query_params.pop("timeout", cls._default_timeout)
        for custom_query_parameter in cls._custom_query_parameters:
            mongodb_uri.query_params.pop(custom_query_parameter, None)
        return cls(
            address=mongodb_address,
            effective_url=mongodb_uri,
            database_name=mongodb_database,
            collection_name=mongodb_collection,
            direct=direct,
            timeout=timeout,
        )

    def __attrs_post_init__(self):
        self.setup()

    @cached_property
    def batch_size(self) -> int:
        return int(self.address.uri.query_params.get("batch-size", 100))

    @cached_property
    def filter(self) -> t.Union[t.Dict[str, t.Any], None]:
        return json.loads(self.address.uri.query_params.get("filter", "null"))

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
    def get_collection_names(self) -> t.List[str]:
        raise NotImplementedError()

    @abstractmethod
    def record_count(self, filter_=None) -> int:
        raise NotImplementedError()

    @abstractmethod
    def query(self):
        raise NotImplementedError()

    @abstractmethod
    def subscribe_cdc(self, resume_after: t.Optional[DocumentDict] = None):
        raise NotImplementedError()


@define
class MongoDBFilesystemAdapter(MongoDBAdapterBase):
    _path: Path = field(init=False)

    def setup(self):
        self._path = Path(self.address.uri.path)

    def get_collection_names(self) -> t.List[str]:
        return sorted(glob.glob(str(self._path)))

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
        if self.filter:
            raise NotImplementedError("Using filter expressions is not supported by filesystem adapter")
        if self.limit:
            raise NotImplementedError("Using limit parameter is not supported by filesystem adapter")
        if self.offset:
            raise NotImplementedError("Using offset parameter is not supported by filesystem adapter")
        if self._path.suffix in [".json", ".jsonl", ".ndjson"]:
            data = read_json(str(self._path))
        elif ".bson" in str(self._path):
            data = IterableData(str(self._path), options={"format_in": "bson"}).iter()
        else:
            raise ValueError(f"Unsupported file type: {self._path.suffix}")
        return batches(data, self.batch_size)

    def subscribe_cdc(self, resume_after: t.Optional[DocumentDict] = None):
        raise NotImplementedError("Subscribing to a change stream is not supported by filesystem adapter")


@define
class MongoDBResourceAdapter(MongoDBAdapterBase):
    _url: URL = field(init=False)

    def setup(self):
        self._url = self.address.uri
        if "+bson" in self._url.scheme:
            self._url.scheme = self._url.scheme.replace("+bson", "")

    def get_collection_names(self) -> t.List[str]:
        raise NotImplementedError("HTTP+BSON loader does not support directory inquiry yet")

    def record_count(self, filter_=None) -> int:
        return -1

    def query(self):
        if self.filter:
            raise NotImplementedError("Using filter expressions is not supported by remote resource adapter")
        if self.limit:
            raise NotImplementedError("Using limit parameter is not supported by remote resource adapter")
        if self.offset:
            raise NotImplementedError("Using offset parameter is not supported by remote resource adapter")
        if self._url.path.endswith(".json") or self._url.path.endswith(".jsonl") or self._url.path.endswith(".ndjson"):
            data = read_json(str(self._url))
        elif self._url.path.endswith(".bson"):
            raise NotImplementedError("HTTP+BSON loader does not support .bson files yet.")
        else:
            raise ValueError(f"Unsupported file type: {self._url}")
        return batches(data, self.batch_size)

    def subscribe_cdc(self, resume_after: t.Optional[DocumentDict] = None):
        raise NotImplementedError("HTTP+BSON loader does not support subscribing to a change stream")


@define
class MongoDBServerAdapter(MongoDBAdapterBase):
    _mongodb_client: pymongo.MongoClient = field(init=False)
    _mongodb_database: pymongo.database.Database = field(init=False)
    _mongodb_collection: pymongo.collection.Collection = field(init=False)

    def setup(self):
        self._mongodb_client: pymongo.MongoClient = pymongo.MongoClient(
            str(self.effective_url),
            document_class=RawBSONDocument,
            datetime_conversion="DATETIME_AUTO",
            directConnection=self.direct,
            socketTimeoutMS=self.timeout,
            connectTimeoutMS=self.timeout,
            serverSelectionTimeoutMS=self.timeout,
        )
        if self.database_name:
            self._mongodb_database = self._mongodb_client.get_database(self.database_name)
        if self.collection_name:
            self._mongodb_collection = self._mongodb_database.get_collection(self.collection_name)

    @property
    def collection(self):
        return self._mongodb_collection

    def get_collection_names(self) -> t.List[str]:
        database = self._mongodb_client.get_database(self.database_name)
        return sorted(database.list_collection_names())

    def record_count(self, filter_=None) -> int:
        """
        # Exact. Takes too long on large collections.
        filter_ = filter_ or {}
        return self._mongodb_collection.count_documents(filter=filter_)
        """
        return self._mongodb_collection.estimated_document_count()

    def query(self):
        _filter = deepcopy(self.filter)
        # Evaluate `_id` filter field specially, by upcasting to `bson.ObjectId`.
        if _filter and "_id" in _filter:
            _filter["_id"] = bson.ObjectId(_filter["_id"])
        data = (
            self._mongodb_collection.find(filter=_filter)
            .batch_size(self.batch_size)
            .skip(self.offset)
            .limit(self.limit)
        )
        return batches(data, self.batch_size)

    def subscribe_cdc(self, resume_after: t.Optional[DocumentDict] = None):
        return self._mongodb_collection.watch(
            full_document="updateLookup", batch_size=self.batch_size, resume_after=resume_after
        )

    def create_collection(self):
        self._mongodb_database.create_collection(self.collection_name)
        self._mongodb_collection = self._mongodb_database.get_collection(self.collection_name)
        return self._mongodb_collection


def mongodb_adapter_factory(mongodb_uri: URL) -> MongoDBAdapterBase:
    if mongodb_uri.scheme.startswith("file"):
        return MongoDBFilesystemAdapter.from_url(mongodb_uri)
    elif mongodb_uri.scheme.startswith("http"):
        return MongoDBResourceAdapter.from_url(mongodb_uri)
    elif mongodb_uri.scheme.startswith("mongodb"):
        return MongoDBServerAdapter.from_url(mongodb_uri)
    raise ValueError("Unable to create MongoDB adapter")
