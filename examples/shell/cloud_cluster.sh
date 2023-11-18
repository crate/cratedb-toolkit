#!/bin/bash
# Copyright (c) 2023, Crate.io Inc.
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
# To install the client SDK, use `pip`::
#
#   pip install 'cratedb-toolkit'
#
# Configuration
# =============
#
# The program assumes you are appropriately authenticated to the CrateDB Cloud
# platform, for example using `croud login --idp azuread`. To inspect the list
# of available clusters, run `croud clusters list`.
#
# For addressing a database cluster, and obtaining corresponding credentials,
# the program uses environment variables, which you can define interactively,
# or store them within a `.env` file.
#
# You can use those configuration snippet as a blueprint. Please adjust the
# individual settings accordingly::
#
#   CRATEDB_CLOUD_CLUSTER_NAME=Hotzenplotz
#   CRATEDB_USERNAME='admin'
#   CRATEDB_PASSWORD='H3IgNXNvQBJM3CiElOiVHuSp6CjXMCiQYhB4I9dLccVHGvvvitPSYr1vTpt4'
#
# Usage
# =====
#
# Initialize a cluster instance::
#
#   bash examples/shell/cloud_cluster.sh
#
# Inquire list of available cluster instances::
#
#   croud clusters list


ctk cluster start
ctk shell --command "SELECT * from sys.summits LIMIT 2;"
