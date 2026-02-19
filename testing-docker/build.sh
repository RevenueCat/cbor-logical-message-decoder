#!/bin/sh

cp ../target/cbordecoder-1.0.jar ./
docker build . -t rc-dbz-eventrouter
