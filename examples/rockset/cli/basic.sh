# Example program using the official Rockset CLI to access the Rockset API with a CrateDB backend.
# Here: "Documents" and "Queries" APIs, basic usage.
#
# Usage
# =====
# - Install dependencies: Run `sh download.sh`.
# - Adjust configuration: Define `ROCKSET_APISERVER` environment variable.
# - Run program: Run `sh basic.sh`.
#
# Documentation
# =============
# - https://github.com/rockset/rockset-js/tree/master/packages/cli
# - https://docs.rockset.com/documentation/reference/adddocuments
# - https://docs.rockset.com/documentation/reference/query

export PATH="${PATH}:$(pwd)/rockset/bin"

export ROCKSET_APISERVER=${ROCKSET_APISERVER:-http://localhost:4243}
export ROCKSET_APIKEY=${ROCKSET_APIKEY:-abc123}

# Add Documents.
rockset api:documents:addDocuments commons bar --body data.yaml

# Submit SQL query.
rockset sql "SELECT * FROM commons.bar"
