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
    uv tool install --upgrade 'cratedb-toolkit'

    # Log in to CrateDB Cloud.
    croud login --idp azuread

    # Inquire list of available clusters.
    croud clusters list

    # Invoke example import of CSV and Parquet files.
    python examples/python/cloud_import.py --cluster-id '<YOUR_CLUSTER_ID_HERE>'

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

    --cluster-id='<YOUR_CLUSTER_ID_HERE>'
    --cluster-name='<YOUR_CLUSTER_NAME_HERE>'

Alternatively, you can use environment variables::

    export CRATEDB_CLOUD_CLUSTER_ID='<YOUR_CLUSTER_ID_HERE>'
    export CRATEDB_CLOUD_CLUSTER_NAME='<YOUR_CLUSTER_NAME_HERE>'

Command line arguments take precedence. When omitted, the program will
fall back to probe the environment variables.

"""

import logging
from pprint import pprint  # noqa: F401

import cratedb_toolkit
from cratedb_toolkit import InputOutputResource, ManagedCluster, TableAddress
from cratedb_toolkit.util.common import setup_logging

logger = logging.getLogger(__name__)


def import_csv():
    """
    Import CSV file from HTTP, derive table name from file name.

    The corresponding shell commands are::

        ctk load table "https://cdn.crate.io/downloads/datasets/cratedb-datasets/machine-learning/timeseries/nab-machine-failure.csv"
        ctk shell --command 'SELECT * FROM "nab-machine-failure" LIMIT 10;'
    """

    # Acquire a database cluster handle and get cluster identifier or name from the user's environment.
    cluster = ManagedCluster.from_env().start()

    # Define data source.
    url = "https://cdn.crate.io/downloads/datasets/cratedb-datasets/machine-learning/timeseries/nab-machine-failure.csv"
    source = InputOutputResource(url=url)

    # Invoke the import job. Without the `target` argument, the destination
    # table name will be derived from the input file name.
    cluster.load_table(source=source)

    # Query data.
    results = cluster.query('SELECT * FROM "nab-machine-failure" LIMIT 5;')
    pprint(results)  # noqa: T203


def import_parquet():
    """
    Import Parquet file from HTTP, use specific schema and table names.

    The corresponding shell commands are::

        ctk load table "https://cdn.crate.io/downloads/datasets/cratedb-datasets/timeseries/yc.2019.07-tiny.parquet.gz"
        ctk shell --command 'SELECT * FROM "testdrive"."yc-201907" LIMIT 10;'
    """

    # Acquire a database cluster handle and get cluster identifier or name from the user's environment.
    cluster = ManagedCluster.from_env().start()

    # Define data source and target table.
    url = "https://cdn.crate.io/downloads/datasets/cratedb-datasets/timeseries/yc.2019.07-tiny.parquet.gz"
    source = InputOutputResource(url=url)
    target = TableAddress(schema="testdrive", table="yc-201907")

    # Invoke the import job. The destination table name is explicitly specified.
    cluster.load_table(source=source, target=target)

    # Query data.
    results = cluster.query('SELECT * FROM "testdrive"."yc-201907" LIMIT 5;')
    pprint(results)  # noqa: T203


def main():
    import_csv()
    import_parquet()


if __name__ == "__main__":
    # Configure a toolkit environment to be suitable for a CLI application, with
    # interactive guidance and accepting configuration settings from the environment.
    setup_logging()
    cratedb_toolkit.configure(
        runtime_errors="exit",
        settings_accept_cli=True,
        settings_accept_env=True,
        settings_errors="exit",
    )

    # Run the program.
    main()
