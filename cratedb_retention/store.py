# Copyright (c) 2021-2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
from cratedb_retention.model import JobSettings
from cratedb_retention.util.database import DatabaseAdapter


class RetentionPolicyStore:
    """
    A wrapper around the retention policy database table.
    """

    def __init__(self, settings: JobSettings):
        self.settings = settings
        self.db = DatabaseAdapter(dburi=self.settings.database.dburi)

    def get_records(self):
        sql = f"REFRESH TABLE {self.settings.policy_table.fullname};"
        self.db.run_sql(sql)
        sql = f"SELECT * FROM {self.settings.policy_table.fullname};"  # noqa: S608
        return self.db.run_sql(sql, records=True)
