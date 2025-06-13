from yarl import URL

from cratedb_toolkit.io.kinesis.adapter import KinesisStreamAdapter
from tests.io.test_processor import DYNAMODB_CDC_INSERT_NESTED, DYNAMODB_CDC_MODIFY_NESTED, wrap_kinesis


def main():
    ka = KinesisStreamAdapter(URL("kinesis://LSIAQAAAAAAVNCBMPNSG:dummy@localhost:4566/cdc-stream?region=eu-central-1"))
    ka.produce(wrap_kinesis(DYNAMODB_CDC_INSERT_NESTED))
    ka.produce(wrap_kinesis(DYNAMODB_CDC_MODIFY_NESTED))


if __name__ == "__main__":
    main()
