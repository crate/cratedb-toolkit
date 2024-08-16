import os
import sys

import pytest


@pytest.fixture
def reset_handler():
    try:
        del sys.modules["cratedb_toolkit.io.processor.kinesis_lambda"]
    except KeyError:
        pass


def test_processor_invoke_no_records(reset_handler, mocker, caplog):
    """
    Roughly verify that the unified Lambda handler works.
    """

    # Configure environment variables.
    handler_environment = {
        "MESSAGE_FORMAT": "dms",
    }
    mocker.patch.dict(os.environ, handler_environment)

    from cratedb_toolkit.io.processor.kinesis_lambda import handler

    event = {"Records": []}
    handler(event, None)

    assert "Successfully processed 0 records" in caplog.messages
