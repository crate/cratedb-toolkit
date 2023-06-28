# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import logging
import typing as t

from cratedb_retention.model import GenericRetention, RetentionStrategy, Settings
from cratedb_retention.strategy.delete import DeleteRetention
from cratedb_retention.strategy.reallocate import ReallocateRetention
from cratedb_retention.strategy.snapshot import SnapshotRetention

logger = logging.getLogger(__name__)


class Engine:
    """
    Implementation of the retention and expiration management subsystem for CrateDB.

    This is the main application, effectively evaluating configuration settings,
    and dispatching to corresponding retention strategy implementations.
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def start(self):
        logger.info(f"Effective settings: {self.settings}")

        logger.info(
            f"Starting data retention with strategy '{self.settings.strategy}' "
            f"up to cutoff day '{self.settings.cutoff_day}' "
            f"on database '{self.settings.dburi}'"
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
