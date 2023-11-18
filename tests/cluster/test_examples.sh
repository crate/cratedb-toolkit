#!/bin/sh

# mock
ctk() {
  echo "$@"
}

test_cloud_cluster() {
  source examples/shell/cloud_cluster.sh
}

# Load shUnit2.
HERE="$(dirname "$(realpath "$0")")"
. ${HERE}/../util/shunit2
