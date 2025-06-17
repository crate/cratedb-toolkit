import logging
import typing as t

from yarl import URL

from cratedb_toolkit.io.kinesis.adapter import KinesisAdapterBase
from tests.io.test_processor import wrap_kinesis

logger = logging.getLogger(__name__)


class KinesisTestManager:
    def __init__(self, url: str):
        url = URL(url).with_query({"region": "us-east-1", "create": "true"})
        self.adapter = KinesisAdapterBase.factory(url)

    def load_events(self, events: t.List[t.Dict[str, t.Any]]):
        for event in events:
            self.adapter.produce(wrap_kinesis(event))
