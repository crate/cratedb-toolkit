#!/bin/sh

# mock
ctk() {
  echo "$@"
}

test_cloud_cluster() {
  source examples/shell/cloud_cluster.sh
}

test_cloud_import() {
  source examples/shell/cloud_import.sh
}

# Load shUnit2.
HERE="$(dirname "$(realpath "$0")")"
. ${HERE}/../util/shunit2
