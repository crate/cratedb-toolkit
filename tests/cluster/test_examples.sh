#!/usr/bin/env sh

# shUnit2-based test cases for example ctk shell programs.

test_cloud_cluster() {
  output=$(./examples/shell/cloud_cluster.sh 2>&1)
  assertTrue "Program did not produce output" '[ -n "$output" ]'
  assertContains "Output lacks logging information" "$output" 'Deploying/starting/resuming CrateDB Cloud Cluster'
  assertContains "Output lacks reference to cluster name" "$output" '"name": "testcluster"'
  assertContains "Output from sys.summits table missing" "$output" 'Mont Blanc'
}

test_cloud_import() {
  output=$(./examples/shell/cloud_import.sh 2>&1)
  assertTrue "Program did not produce output" '[ -n "$output" ]'
  assertContains "Output lacks logging information from ctk" "$output" 'Deploying/starting/resuming CrateDB Cloud Cluster'
  assertContains "Output lacks reference to cluster name" "$output" '"name": "testcluster"'
  assertContains "Output lacks logging information from croud" "$output" 'Import succeeded (status: SUCCEEDED)'
  assertContains "Output lacks logging information from croud" "$output" 'Data loading was successful'
  assertContains "Output lacks logging information from crash" "$output" 'CONNECT OK'
  assertContains "Output from nab-machine-failure table missing" "$output" '78.71041827'
}

# Load shUnit2.
#HERE="$(dirname "$(realpath "$0")")"
HERE="$(cd "$(dirname "$0")" && pwd -P)"
. "${HERE}/../util/shunit2"
