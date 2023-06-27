#!/bin/bash

# Fail on error.
set -e

# Display all commands.
# set -x

flavor=$1

echo "Invoking cratedb-rollup"
cratedb-rollup --version
