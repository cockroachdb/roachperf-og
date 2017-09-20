#!/usr/bin/env bash

echo "${TEST_CLUSTER_SSH_KEY}" > id_test_cluster.rsa
mkdir -p artifacts
docker run \
    --workdir=/go/src/github.com/cockroachdb/roachperf \
    --volume="${GOPATH%%:*}/src":/go/src \
    --volume="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)":/go/src/github.com/cockroachdb/roachperf \
    --rm \
    cockroachdb/builder:20170422-212842 ./run-nightly.sh
