# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
About
=====

Example program demonstrating how to manage a CrateDB Cloud
database cluster.

The program assumes you are appropriately authenticated to
CrateDB Cloud, for example using `croud login --idp azuread`.

The program obtains a single positional argument from the command line,
the CrateDB Cloud Cluster name. When omitted, it will fall back
to the `CRATEDB_CLOUD_CLUSTER_NAME` environment variable.

Synopsis
========
::

    # Install package.
    pip install 'cratedb-toolkit'

    # Log in to CrateDB Cloud.
    croud login --idp azuread

    # Deploy a cluster instance.
    python examples/cloud_cluster.py Hotzenplotz

    # Inquire list of available clusters.
    croud clusters list

    # Run an SQL command.
    export CRATEDB_CLOUD_CLUSTER_ID='e1e38d92-a650-48f1-8a70-8133f2d5c400'
    export CRATEDB_USERNAME='admin'
    export CRATEDB_PASSWORD='H3IgNXNvQBJM3CiElOiVHuSp6CjXMCiQYhB4I9dLccVHGvvvitPSYr1vTpt4'
    ctk shell --command "SELECT * from sys.summits LIMIT 5;"

References
==========

- https://github.com/crate/jenkins-dsl/blob/master/scripts/croud-deploy-nightly.sh
- https://github.com/coiled/examples/blob/main/geospatial.ipynb
"""
import json
import logging
import sys

from cratedb_toolkit.util.basic import obtain_cluster_name
from cratedb_toolkit.util.common import setup_logging

logger = logging.getLogger(__name__)


def workload(cluster_name: str):
    """
    Run a workload on a CrateDB database cluster on CrateDB Cloud.

    ctk deploy cluster Hotzenplotz
    ctk shell --command "SELECT * from sys.summits LIMIT 5;"
    """

    # Database cluster resource handle.
    from cratedb_toolkit.api import ManagedCluster

    # Acquire database cluster.
    cluster = ManagedCluster(name=cluster_name)
    cluster.acquire()

    # Report information about cluster.
    print(json.dumps(cluster.info.cloud))

    # Run database workload.
    # cratedb = cluster.get_connection_client()  # noqa: ERA001
    # cratedb.run_sql("SELECT * from sys.summits LIMIT 5;")  # noqa: ERA001


def main():
    """
    Obtain cluster name and SQL command, and run program.
    """
    try:
        cluster_name = obtain_cluster_name()
    except ValueError as ex:
        logger.error(ex)
        sys.exit(1)

    workload(cluster_name)


if __name__ == "__main__":
    setup_logging()
    main()
