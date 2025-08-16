#!/usr/bin/env bash

# Fail on error.
set -Eeuo pipefail

# Display all commands.
# set -x

echo "Invoking cratedb-toolkit"
exec cratedb-toolkit --version
