# Copyright (c) 2021-2026, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
CrateDB Toolkit polyglot I/O dispatcher hub.
Add new data sources and data sinks at your disposal.
"""

import logging
import typing as t
from pathlib import Path

from boltons.urlutils import URL

from cratedb_toolkit.exception import (
    OperationFailed,
)
from cratedb_toolkit.io.ingestr.api import ingestr_copy, ingestr_select
from cratedb_toolkit.model import DatabaseAddress, InputOutputResource
from cratedb_toolkit.util.data import asbool

logger = logging.getLogger(__name__)


class IoHub:
    def load_table(
        self,
        source: InputOutputResource,
        target: DatabaseAddress,
        transformation: t.Union[Path, None] = None,
    ):
        """
        Load data into unmanaged CrateDB cluster.

        Synopsis
        --------
        export CRATEDB_CLUSTER_URL=crate://crate@localhost:4200/testdrive/demo

        ctk load table influxdb2://example:token@localhost:8086/testdrive/demo
        ctk load table mongodb://localhost:27017/testdrive/demo
        ctk load table kinesis+dms:///arn:aws:kinesis:eu-central-1:831394476016:stream/testdrive
        ctk load table kinesis+dms:///path/to/dms-over-kinesis.jsonl
        """
        source_url = source.url
        target_url = target.dburi
        source_url_obj = URL(source.url)

        if source_url_obj.scheme.startswith("dynamodb"):
            from cratedb_toolkit.io.dynamodb.api import dynamodb_copy

            return dynamodb_copy(str(source_url_obj), target_url, progress=True)

        elif (
            source_url_obj.scheme.startswith("influxdb") or source_url.endswith(".lp") or source_url.endswith(".lp.gz")
        ):
            from cratedb_toolkit.io.influxdb import influxdb_copy

            http_scheme = "http"
            if asbool(source_url_obj.query_params.get("ssl")):
                http_scheme = "https"
            source_url_obj.scheme = source_url_obj.scheme.replace("influxdb2", http_scheme)
            return influxdb_copy(str(source_url_obj), target_url, progress=True)

        elif source_url_obj.scheme.startswith("kinesis"):
            from cratedb_toolkit.io.kinesis.api import kinesis_relay

            return kinesis_relay(
                source_url=source_url_obj,
                target_url=target_url,
                recipe=transformation,
            )

        elif source_url_obj.scheme in [
            "file+bson",
            "http+bson",
            "https+bson",
            "mongodb",
            "mongodb+srv",
            "mongodb+cdc",
            "mongodb+srv+cdc",
        ]:
            if "+cdc" in source_url_obj.scheme:
                source_url_obj.scheme = source_url_obj.scheme.replace("+cdc", "")

                from cratedb_toolkit.io.mongodb.api import mongodb_relay_cdc

                return mongodb_relay_cdc(
                    source_url_obj,
                    target_url,
                    transformation=transformation,
                )
            else:
                from cratedb_toolkit.io.mongodb.api import mongodb_copy

                return mongodb_copy(
                    source_url_obj,
                    target_url,
                    transformation=transformation,
                    progress=True,
                )

        elif source_url_obj.scheme.startswith("deltalake") or source_url_obj.scheme.endswith("deltalake"):
            from cratedb_toolkit.io.deltalake import from_deltalake

            return from_deltalake(str(source_url_obj), target_url)

        elif source_url_obj.scheme.startswith("iceberg") or source_url_obj.scheme.endswith("iceberg"):
            from cratedb_toolkit.io.iceberg import from_iceberg

            return from_iceberg(str(source_url_obj), target_url)

        elif ingestr_select(source_url):
            return ingestr_copy(source_url, target, progress=True)

        else:
            raise NotImplementedError(f"Importing resource not implemented yet: {source_url_obj}")

    def save_table(
        self,
        source: DatabaseAddress,
        target: InputOutputResource,
        transformation: t.Union[Path, None] = None,
    ):
        """
        Export data from a database table on a standalone CrateDB Server.

        Note: The `transformation` parameter is not respected yet, but required by contract.
              In this spirit, it is reserved for later use.

        Synopsis
        --------
        export CRATEDB_CLUSTER_URL=crate://crate@localhost:4200/testdrive/demo

        ctk save table \
          "file+iceberg://./var/lib/iceberg/?catalog=default&namespace=demo&table=taxi_dataset"
        """
        source_url = source.dburi
        target_url_obj = URL(target.url)

        if target_url_obj.scheme.startswith("deltalake") or target_url_obj.scheme.endswith("deltalake"):
            from cratedb_toolkit.io.deltalake import to_deltalake

            if not to_deltalake(source_url, target.url):
                raise OperationFailed("Data export failed or incomplete")

        elif target_url_obj.scheme.startswith("iceberg") or target_url_obj.scheme.endswith("iceberg"):
            from cratedb_toolkit.io.iceberg import to_iceberg

            if not to_iceberg(source_url, target.url):
                raise OperationFailed("Data export failed or incomplete")

        else:
            raise OperationFailed(f"Exporting resource not implemented yet: {target_url_obj}")
