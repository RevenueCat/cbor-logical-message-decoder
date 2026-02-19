#!/bin/sh

cp ../target/cbordecoder-1.0-SNAPSHOT.jar ./
docker build . -t rc-dbz-eventrouter