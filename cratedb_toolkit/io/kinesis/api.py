from boltons.urlutils import URL

from cratedb_toolkit.io.kinesis.relay import KinesisRelay
from cratedb_toolkit.util.data import asbool


def kinesis_relay(source_url: URL, target_url):
    once = asbool(source_url.query_params.get("once", "false"))
    ka = KinesisRelay(str(source_url), target_url)
    ka.start(once=once)
