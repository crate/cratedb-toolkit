# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
About
=====

Example program demonstrating how to load data from files using
the CrateDB Cloud Import API.

The supported file types are CSV, JSON, Parquet, optionally with
gzip compression. They can be acquired from the local filesystem,
or from remote HTTP and AWS S3 resources.

The program obtains a single positional argument from the command line,
the CrateDB Cloud Cluster identifier. When omitted, it will fall back
to the `CRATEDB_CLOUD_CLUSTER_ID` environment variable.

Synopsis
========
::

    # Install package.
    pip install 'cratedb-toolkit'

    # Log in to CrateDB Cloud.
    croud login --idp azuread

    # Inquire list of available clusters.
    croud clusters list

    # Invoke import of CSV and Parquet files.
    python examples/cloud_import.py e1e38d92-a650-48f1-8a70-8133f2d5c400

"""
import logging
import os
import sys

from dotenv import find_dotenv, load_dotenv

from cratedb_toolkit.api.main import ManagedCluster
from cratedb_toolkit.model import InputOutputResource, TableAddress
from cratedb_toolkit.util.common import setup_logging

logger = logging.getLogger(__name__)


def import_csv(cluster_id: str):
    """
    Import CSV file from HTTP, derive table name from file name.

    ctk shell --command 'SELECT * FROM "nab-machine-failure" LIMIT 10;'
    """

    # Acquire database cluster resource handle.
    cluster = ManagedCluster(cloud_id=cluster_id)

    # Encapsulate source parameter.
    url = "https://github.com/crate/cratedb-datasets/raw/main/machine-learning/timeseries/nab-machine-failure.csv"
    resource = InputOutputResource(url=url)

    # Invoke import job. Without `target` argument, the destination
    # table name will be derived from the input file name.
    response, success = cluster.load_table(resource=resource)
    if not success:
        sys.exit(1)


def import_parquet(cluster_id: str):
    """
    Import Parquet file from HTTP, and use specific schema and table names.

    ctk shell --command 'SELECT * FROM "testdrive"."yc-201907" LIMIT 10;'
    """

    # Acquire database cluster resource handle.
    cluster = ManagedCluster(cloud_id=cluster_id)

    # Encapsulate source and target parameters.
    url = "https://github.com/crate/cratedb-datasets/raw/main/timeseries/yc.2019.07-tiny.parquet.gz"
    resource = InputOutputResource(url=url)
    target = TableAddress(schema="testdrive", table="yc-201907")

    # Invoke import job. The destination table name is explicitly specified.
    response, success = cluster.load_table(resource=resource, target=target)
    if not success:
        sys.exit(1)


def obtain_cluster_id() -> str:
    """
    Obtain the CrateDB Cloud Cluster identifier from the environment.

    - Use first positional argument from command line.
    - Fall back to `CRATEDB_CLOUD_CLUSTER_ID` environment variable.
    """
    load_dotenv(find_dotenv())

    try:
        cluster_id = sys.argv[1]
    except IndexError:
        cluster_id = os.environ.get("CRATEDB_CLOUD_CLUSTER_ID")

    if not cluster_id:
        raise ValueError(
            "Unable to obtain cluster identifier from command line or "
            "`CRATEDB_CLOUD_CLUSTER_ID` environment variable"
        )

    return cluster_id


def main():
    """
    Obtain cluster identifier, and run program.
    """
    try:
        cluster_id = obtain_cluster_id()
    except ValueError as ex:
        logger.error(ex)
        sys.exit(1)

    import_csv(cluster_id)
    import_parquet(cluster_id)


if __name__ == "__main__":
    setup_logging()
    main()
