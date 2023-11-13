import logging

from influxio.core import copy

logger = logging.getLogger(__name__)


def influxdb_copy(source_url, target_url, progress: bool = False):
    """
    Synopsis
    --------
    export CRATEDB_SQLALCHEMY_URL=crate://crate@localhost:4200/testdrive/demo
    ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
    """
    logger.info("Running InfluxDB copy")
    copy(source_url, target_url, progress=progress)
    return True
