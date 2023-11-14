import os
import sys

from dotenv import find_dotenv, load_dotenv


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


def obtain_cluster_name() -> str:
    """
    Obtain the CrateDB Cloud Cluster name from the environment.

    - Use first positional argument from command line.
    - Fall back to `CRATEDB_CLOUD_CLUSTER_NAME` environment variable.
    """
    load_dotenv(find_dotenv())

    try:
        cluster_id = sys.argv[1]
    except IndexError:
        cluster_id = os.environ.get("CRATEDB_CLOUD_CLUSTER_NAME")

    if not cluster_id:
        raise ValueError(
            "Unable to obtain cluster name from command line or " "`CRATEDB_CLOUD_CLUSTER_NAME` environment variable"
        )

    return cluster_id
