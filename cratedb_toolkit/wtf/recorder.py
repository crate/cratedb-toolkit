# Copyright (c) 2023-2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import threading
import time

import sqlalchemy as sa

from cratedb_toolkit.util import DatabaseAdapter
from cratedb_toolkit.wtf.core import InfoContainer, JobInfoContainer

logger = logging.getLogger(__name__)


class InfoRecorder:
    """
    Record complete outcomes of `info` and `job-info`.
    """

    clusterinfo_table = "ext.clusterinfo"
    jobinfo_table = "ext.jobinfo"
    interval_seconds = 10

    def __init__(self, adapter: DatabaseAdapter, scrub: bool = False):
        self.adapter = adapter
        self.scrub = scrub

    def record_once(self):
        logger.info("Recording information snapshot")
        clusterinfo_sample = InfoContainer(adapter=self.adapter, scrub=self.scrub)
        jobinfo_sample = JobInfoContainer(adapter=self.adapter, scrub=self.scrub)

        for table, sample in ((self.clusterinfo_table, clusterinfo_sample), (self.jobinfo_table, jobinfo_sample)):
            self.adapter.connection.execute(
                sa.text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table}
                    (time TIMESTAMP DEFAULT NOW(), info OBJECT)
                """
                )
            )
            self.adapter.connection.execute(
                sa.text(f"INSERT INTO {table} (info) VALUES (:info)"),  # noqa: S608
                {"info": sample.to_dict()["data"]},
            )

    def record_forever(self):
        logger.info(f"Starting to record information snapshot each {self.interval_seconds} seconds")
        thread = threading.Thread(target=self.do_record_forever)
        thread.start()

    def do_record_forever(self):
        while True:
            try:
                self.record_once()
            except Exception:
                logger.exception("Failed to record information snapshot")
            logger.info(f"Sleeping for {self.interval_seconds} seconds")
            time.sleep(self.interval_seconds)
