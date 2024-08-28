from cratedb_toolkit.io.kinesis.relay import KinesisRelay


def kinesis_relay(source_url, target_url):
    ka = KinesisRelay(source_url, target_url)
    ka.start()
