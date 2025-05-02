# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the Apache 2 license.
"""
About
=====

Example program demonstrating how to manage a CrateDB Cloud database cluster.
It obtains a database cluster identifier or name, connects to the database
cluster, optionally deploys it, and runs an example workload.

Synopsis
========
::

    # Install utility package.
    uv tool install --upgrade 'cratedb-toolkit'

    # Log in to CrateDB Cloud.
    croud login --idp azuread

    # Optional: If your account uses multiple subscriptions, you will need
    # to select a specific one for invoking the cluster deployment operation.
    export CRATEDB_CLOUD_SUBSCRIPTION_ID='<YOUR_SUBSCRIPTION_ID_HERE>'

    # A quick usage example for fully unattended operation,
    # please adjust individual settings accordingly::

    export CRATEDB_CLOUD_API_KEY='<YOUR_API_KEY_HERE>'
    export CRATEDB_CLOUD_API_SECRET='<YOUR_API_SECRET_HERE>'
    export CRATEDB_CLOUD_ORGANIZATION_ID='<YOUR_ORG_ID_HERE>'

    # Initialize a cluster instance.
    python examples/python/cloud_cluster.py --cluster-name '<YOUR_CLUSTER_NAME_HERE>'

    # Inquire list of available clusters.
    croud clusters list

    # Run an example SQL command.
    ctk shell --command "SELECT * from sys.summits LIMIT 2;"

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

    export CRATEDB_CLUSTER_ID='<YOUR_CLUSTER_ID_HERE>'
    export CRATEDB_CLUSTER_NAME='<YOUR_CLUSTER_NAME_HERE>'

Command line arguments take precedence. When omitted, the program will
fall back to probe the environment variables.

"""

import logging
import sys
from pprint import pprint

import cratedb_toolkit
from cratedb_toolkit.util.common import setup_logging

logger = logging.getLogger(__name__)


def workload_procedural():
    """
    Use CTK Cluster API procedural.
    """

    from cratedb_toolkit import ManagedCluster

    # Acquire a database cluster handle, obtaining cluster identifier or name from the user's environment.
    cluster = ManagedCluster.from_env().start()

    # Report information about cluster.
    # TODO: Implement and use `cluster.{print,format}_info()` to report cluster information.
    pprint(cluster.info, stream=sys.stderr)  # noqa: T201

    # Run database workload.
    results = cluster.query("SELECT * from sys.summits LIMIT 2;")
    pprint(results, stream=sys.stderr)  # noqa: T201

    # Stop the cluster again.
    # Note: We intentionally don't stop the cluster here to prevent accidental
    # shutdown of resources that might be in use for demonstration purposes.
    # cluster.stop()


def workload_contextmanager():
    """
    Use CTK Cluster API as a context manager.
    """

    from cratedb_toolkit import ManagedCluster

    # Acquire a database cluster handle, and run database workload.
    with ManagedCluster.from_env() as cluster:
        pprint(cluster.query("SELECT * from sys.summits LIMIT 2;"), stream=sys.stderr)  # noqa: T201


def main():
    """
    Run a workload on a CrateDB database cluster on CrateDB Cloud.

        ctk cluster start --cluster-name '<YOUR_CLUSTER_NAME_HERE>'
        ctk shell --command "SELECT * from sys.summits LIMIT 2;"
    """
    workload_procedural()
    workload_contextmanager()


if __name__ == "__main__":
    # Configure the toolkit environment to be suitable for a CLI application, with
    # interactive guidance and accepting configuration settings from the environment.
    setup_logging()
    cratedb_toolkit.configure(
        runtime_errors="exit",
        settings_accept_cli=True,
        settings_accept_env=True,
        settings_errors="exit",
    )

    # Run program.
    main()
