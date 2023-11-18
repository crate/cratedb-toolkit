# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the Apache 2 license.
"""
About
=====

This is an example program demonstrating how to load data from
files using the Import API interface of CrateDB Cloud into
a CrateDB Cloud Cluster.

The supported file types are CSV, JSON, Parquet, optionally with
gzip compression. They can be acquired from the local filesystem,
or from remote HTTP and AWS S3 resources.

Synopsis
========
::

    # Install utility package.
    pip install 'cratedb-toolkit'

    # Log in to CrateDB Cloud.
    croud login --idp azuread

    # Inquire list of available clusters.
    croud clusters list

    # Invoke example import of CSV and Parquet files.
    python examples/cloud_import.py --cluster-id e1e38d92-a650-48f1-8a70-8133f2d5c400

Usage
=====

The program assumes you are appropriately authenticated to the CrateDB Cloud
platform, for example using `croud login --idp azuread`.

For addressing a database cluster, the program obtains the cluster identifier
or name from the user's environment, using command-line arguments or environment
variables.

Configuration
=============

The configuration settings can be specified as CLI arguments::

    --cluster-id=e1e38d92-a650-48f1-8a70-8133f2d5c400
    --cluster-name=Hotzenplotz

Alternatively, you can use environment variables::

    export CRATEDB_CLOUD_CLUSTER_ID=e1e38d92-a650-48f1-8a70-8133f2d5c400
    export CRATEDB_CLOUD_CLUSTER_NAME=Hotzenplotz

Command line arguments take precedence. When omitted, the program will
fall back to probe the environment variables.

"""

import logging
from pprint import pprint  # noqa: F401

import cratedb_toolkit
from cratedb_toolkit import InputOutputResource, ManagedCluster, TableAddress
from cratedb_toolkit.util import setup_logging

logger = logging.getLogger(__name__)


def import_csv():
    """
    Import CSV file from HTTP, derive table name from file name.

    The corresponding shell commands are::

        ctk load table "https://github.com/crate/cratedb-datasets/raw/main/machine-learning/timeseries/nab-machine-failure.csv"
        ctk shell --command 'SELECT * FROM "nab-machine-failure" LIMIT 10;'
    """

    # Acquire database cluster handle, obtaining cluster identifier
    # or name from the user's environment.
    cluster = ManagedCluster.from_env().start()

    # Define data source.
    url = "https://cdn.crate.io/downloads/datasets/cratedb-datasets/machine-learning/timeseries/nab-machine-failure.csv"
    source = InputOutputResource(url=url)

    # Invoke import job. Without `target` argument, the destination
    # table name will be derived from the input file name.
    cluster.load_table(source=source)

    # Query data.
    """
    results = cluster.query('SELECT * FROM "nab-machine-failure" LIMIT 5;')
    pprint(results)  # noqa: T203
    """


def import_parquet():
    """
    Import Parquet file from HTTP, use specific schema and table names.

    The corresponding shell commands are::

        ctk load table "https://github.com/crate/cratedb-datasets/raw/main/timeseries/yc.2019.07-tiny.parquet.gz"
        ctk shell --command 'SELECT * FROM "testdrive"."yc-201907" LIMIT 10;'
    """

    # Acquire database cluster handle, obtaining cluster identifier
    # or name from the user's environment.
    cluster = ManagedCluster.from_env().start()

    # Define data source and target table.
    url = "https://cdn.crate.io/downloads/datasets/cratedb-datasets/timeseries/yc.2019.07-tiny.parquet.gz"
    source = InputOutputResource(url=url)
    target = TableAddress(schema="testdrive", table="yc-201907")

    # Invoke import job. The destination table name is explicitly specified.
    cluster.load_table(source=source, target=target)

    # Query data.
    """
    results = cluster.query('SELECT * FROM "yc-201907" LIMIT 5;')
    pprint(results)  # noqa: T203
    """


def main():
    import_csv()
    import_parquet()


if __name__ == "__main__":
    # Configure toolkit environment to be suitable for a CLI application, with
    # interactive guidance, and accepting configuration settings from the environment.
    setup_logging()
    cratedb_toolkit.configure(
        runtime_errors="exit",
        settings_accept_cli=True,
        settings_accept_env=True,
        settings_errors="exit",
    )

    # Run program.
    main()
