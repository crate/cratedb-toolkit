import logging

from influxio.core import copy

from cratedb_toolkit.model import DatabaseAddress

logger = logging.getLogger(__name__)


def influxdb_copy(source_url, target_url, progress: bool = False):
    """
    Synopsis
    --------
    export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
    ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
    """

    # Sanity checks.
    target_address = DatabaseAddress.from_string(target_url)
    url, table_address = target_address.decode()
    if table_address.table is None:
        raise ValueError("Table name is missing. Please adjust CrateDB database URL.")

    # Invoke copy operation.
    logger.info("Running InfluxDB copy")
    copy(source_url, target_url, progress=progress)
    return True
