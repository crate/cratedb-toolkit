#!/usr/bin/env sh

# Copyright (c) 2023-2025, Crate.io Inc.
# Distributed under the terms of the Apache 2 license.
#
# About
# =====
#
# Example program demonstrating how to manage a CrateDB Cloud database cluster
# using shell scripting.
#
# It obtains a database cluster identifier or name, connects to the database
# cluster, optionally deploys it, and runs an example workload.
#
# Setup
# =====
#
# To install the client SDK CLI, use the `uv` package manager::
#
#   uv tool install --upgrade 'cratedb-toolkit'
#
# Configuration
# =============
#
# For addressing a database cluster, and obtaining corresponding credentials,
# the program uses environment variables, which you can define interactively,
# or store them within a `.env` file.
#
# Authentication
# --------------
#
# To authenticate with the CrateDB Cloud platform, use an interactive approach
# like `croud login --idp azuread`, or use headless mode via API keys specified
# per `CRATEDB_CLOUD_API_KEY` and `CRATEDB_CLOUD_API_SECRET` environment variables.
#
# If your database cluster hasn't been deployed yet, you will need to configure
# a pair of username/password access credentials, to be provided per
# `CRATEDB_USERNAME` and `CRATEDB_PASSWORD` environment variables.
#
# Cluster address
# ---------------
#
# Other than authentication information, you need to provide information about
# identifying the database cluster, for example using the `--cluster-id` or
# `--cluster-name` CLI options, or the `CRATEDB_CLUSTER_ID` or
# `CRATEDB_CLUSTER_NAME` environment variables.
#
# Cluster deployment
# ------------------
#
# If your cluster has not been deployed yet, the program also needs the
# organization identifier UUID, to be provided per `--org-id` CLI option
# or `CRATEDB_CLOUD_ORGANIZATION_ID` environment variable.
#
# If your account uses multiple subscriptions, you will also need to select
# a specific one for invoking the cluster deployment operation using the
# `CRATEDB_CLOUD_SUBSCRIPTION_ID` environment variable.
#
# Usage
# =====
#
# A quick usage example for fully unattended operation,
# please adjust individual settings accordingly::
#
#   export CRATEDB_CLOUD_API_KEY='<YOUR_API_KEY_HERE>'
#   export CRATEDB_CLOUD_API_SECRET='<YOUR_API_SECRET_HERE>'
#   export CRATEDB_CLUSTER_NAME='<YOUR_CLUSTER_NAME_HERE>'
#
# Initialize a cluster instance, and run basic query::
#
#   sh examples/shell/cloud_cluster.sh
#

# Exit on error.
set -e

# Start or resume the CrateDB Cloud cluster.
ctk cluster start

# Query the first 5 rows from a built-in table.
ctk shell --command "SELECT * from sys.summits LIMIT 5;"
