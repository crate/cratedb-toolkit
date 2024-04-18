# Copyright (c) 2021-2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import typing as t
from functools import cached_property

import boltons.ecoutils

from cratedb_toolkit.util.platform import PlatformInfo
from cratedb_toolkit.wtf.library import Library
from cratedb_toolkit.wtf.model import InfoContainerBase


class InfoContainer(InfoContainerBase):
    def register_builtins(self):
        self.elements.add(
            # General cluster health information.
            Library.Health.cluster_name,
            Library.Health.nodes_count,
            Library.Health.nodes_list,
            Library.Health.table_health,
            Library.Health.backups_recent,
            # Shard / node / partition allocation and rebalancing information.
            Library.Shards.allocation,
            Library.Shards.table_allocation,
            Library.Shards.node_shard_distribution,
            Library.Shards.table_shard_count,
            Library.Shards.rebalancing_progress,
            Library.Shards.rebalancing_status,
            Library.Shards.not_started,
            Library.Shards.not_started_count,
            Library.Shards.max_checkpoint_delta,
            Library.Shards.total_count,
            Library.Shards.translog_uncommitted,
            Library.Shards.translog_uncommitted_size,
        )

    @cached_property
    def cluster_name(self):
        return self.evaluate_element(Library.Health.cluster_name)

    def to_dict(self, data=None):
        return super().to_dict(data={"system": self.system(), "database": self.database()})

    def system(self):
        data: t.Dict[str, t.Any] = {}
        data["remark"] = (
            "This section includes system information about the machine running CrateDB "
            'Toolkit, effectively about the "compute" domain.'
        )
        data["application"] = PlatformInfo.application()
        data["eco"] = boltons.ecoutils.get_profile(scrub=self.scrub)
        # `version_info` is a list of mixed data types: [3, 11, 6, "final", 0].
        data["eco"]["python"]["version_info"] = str(data["eco"]["python"]["version_info"])
        # data["libraries"] = PlatformInfo.libraries()  # noqa: ERA001
        return data

    def database(self):
        data = {}
        data["remark"] = (
            "This section includes system and other diagnostics information about the CrateDB "
            'database cluster, effectively about the "storage" domain.'
        )
        for element in self.elements.items:
            data[element.name] = self.evaluate_element(element)
        return data


class LogContainer(InfoContainerBase):
    def register_builtins(self):
        self.elements.add(
            Library.Logs.user_queries_latest,
        )


class JobInfoContainer(InfoContainerBase):
    def register_builtins(self):
        self.elements.add(
            Library.JobInfo.age_range,
            Library.JobInfo.by_user,
            Library.JobInfo.duration_buckets,
            Library.JobInfo.duration_percentiles,
            Library.JobInfo.history100,
            Library.JobInfo.history_count,
            Library.JobInfo.performance15min,
            Library.JobInfo.running,
            Library.JobInfo.running_count,
            Library.JobInfo.top100_count,
            Library.JobInfo.top100_duration_individual,
            Library.JobInfo.top100_duration_total,
        )
