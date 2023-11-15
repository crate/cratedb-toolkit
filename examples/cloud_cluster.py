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
    pip install 'cratedb-toolkit'

    # Log in to CrateDB Cloud.
    croud login --idp azuread

    # Initialize a cluster instance.
    python examples/cloud_cluster.py --cluster-name Hotzenplotz

    # Inquire list of available clusters.
    croud clusters list

    # Run an example SQL command.
    # TODO: Store per-cluster username and password in configuration file / system keyring.
    export CRATEDB_CLOUD_CLUSTER_ID='e1e38d92-a650-48f1-8a70-8133f2d5c400'
    export CRATEDB_USERNAME='admin'
    export CRATEDB_PASSWORD='H3IgNXNvQBJM3CiElOiVHuSp6CjXMCiQYhB4I9dLccVHGvvvitPSYr1vTpt4'
    ctk shell --command "SELECT * from sys.summits LIMIT 5;"

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
import json
import logging

import cratedb_toolkit
from cratedb_toolkit.util import setup_logging

logger = logging.getLogger(__name__)


def workload():
    """
    Run a workload on a CrateDB database cluster on CrateDB Cloud.

    ctk cluster start Hotzenplotz
    ctk shell --command "SELECT * from sys.summits LIMIT 5;"
    """

    from cratedb_toolkit import ManagedCluster

    # Acquire database cluster handle, obtaining cluster identifier
    # or name from the user's environment.
    cluster = ManagedCluster.from_env().start()

    # Report information about cluster.
    # TODO: Use `cluster.{print,format}_info()`.
    print(json.dumps(cluster.info.cloud))  # noqa: T201

    # Run database workload.
    # TODO: Enable acquiring a client handle.
    # cratedb = cluster.get_connection_client()  # noqa: ERA001
    # cratedb.run_sql("SELECT * from sys.summits LIMIT 5;")  # noqa: ERA001

    # Stop cluster again.
    cluster.stop()


def main():
    workload()


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
