#!/bin/bash

go get -u ./...
go install
eval $(ssh-agent)
ssh-add id_test_cluster.rsa
mkdir -p ~/.ssh
for i in {1..7}; do
    ssh-keyscan -H cockroach-denim-000$i.crdb.io >> ~/.ssh/known_hosts
done

curl -L https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.LATEST -o cockroach
chmod +x cockroach

roachperf denim put ./cockroach ./cockroach

cd artifacts
roachperf denim test nightly
