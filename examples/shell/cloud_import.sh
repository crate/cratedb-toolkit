#!/bin/bash
# Copyright (c) 2023, Crate.io Inc.
# Distributed under the terms of the Apache 2 license.
#
# About
# =====
#
# This is an example program demonstrating how to load data from
# files using the Import API interface of CrateDB Cloud into
# a CrateDB Cloud Cluster.
#
# The supported file types are CSV, JSON, Parquet, optionally with
# gzip compression. They can be acquired from the local filesystem,
# or from remote HTTP and AWS S3 resources.
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
# Initialize a cluster instance, and run a data import::
#
#   bash examples/shell/cloud_import.sh
#
# Query imported data::
#
#   ctk shell --command 'SELECT * FROM "nab-machine-failure" LIMIT 10;'


ctk cluster start
ctk load table "https://github.com/crate/cratedb-datasets/raw/main/machine-learning/timeseries/nab-machine-failure.csv"
ctk shell --command 'SELECT * FROM "nab-machine-failure" LIMIT 10;'
