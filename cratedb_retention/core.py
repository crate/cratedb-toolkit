# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import typing as t

from cratedb_retention.model import GenericRetention, JobSettings, RetentionStrategy
from cratedb_retention.strategy.delete import DeleteRetention
from cratedb_retention.strategy.reallocate import ReallocateRetention
from cratedb_retention.strategy.snapshot import SnapshotRetention

logger = logging.getLogger(__name__)


class RetentionJob:
    """
    The retention job implementation evaluates its configuration and runtime settings,
    and dispatches to corresponding retention strategy implementations.

    This is effectively the main application, implementing a retention and expiration
    management subsystem for CrateDB.
    """

    def __init__(self, settings: JobSettings):
        self.settings = settings

    def start(self):
        logger.info(f"Connecting to database: {self.settings.database.safe}")

        logger.info(
            f"Starting data retention using '{self.settings.strategy}' " f"and cut-off day '{self.settings.cutoff_day}'"
        )

        strategy = self.settings.strategy

        implementation: t.Type[GenericRetention]

        # Resolve strategy implementation.
        if strategy is RetentionStrategy.DELETE:
            implementation = DeleteRetention
        elif strategy is RetentionStrategy.REALLOCATE:
            implementation = ReallocateRetention
        elif strategy is RetentionStrategy.SNAPSHOT:
            implementation = SnapshotRetention
        else:
            raise NotImplementedError(f"Retention strategy {strategy} not implemented yet")

        # Propagate runtime context settings, and invoke job.
        # TODO: Add audit logging.
        # TODO: Add job tracking.
        job = implementation(settings=self.settings)
        job.start()
