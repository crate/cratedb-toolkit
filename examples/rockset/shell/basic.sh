# Example program using the curl HTTP client to access the Rockset API with a CrateDB backend.
# Here: "Documents" and "Queries" APIs, basic usage.
#
# Usage
# =====
# - Adjust configuration: Define `ROCKSET_APISERVER` environment variable.
# - Run program: Run `sh basic.sh`.
#
# Documentation
# =============
# - https://docs.rockset.com/documentation/reference/adddocuments
# - https://docs.rockset.com/documentation/reference/query

export ROCKSET_APISERVER=${ROCKSET_APISERVER:-http://localhost:4243}
export ROCKSET_APIKEY=${ROCKSET_APIKEY:-abc123}

# Add Documents.
echo '{"data": [{"id": "foo", "field": "value"}]}' | curl --request POST \
     --url "${ROCKSET_APISERVER}/v1/orgs/self/ws/commons/collections/foobar/docs" \
     --header "Authorization: ApiKey ${ROCKSET_APIKEY}" \
     --header 'accept: application/json' \
     --header 'content-type: application/json' \
     --data @-
echo

# Submit SQL query.
echo '{"sql": {"query": "SELECT * FROM commons.foobar;"}}' | curl --request POST \
     --url "${ROCKSET_APISERVER}/v1/orgs/self/queries" \
     --header "Authorization: ApiKey ${ROCKSET_APIKEY}" \
     --header 'accept: application/json' \
     --header 'content-type: application/json' \
     --data @-
