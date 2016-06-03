#!/bin/bash
set -x
protoc --gofast_out=plugins=grpc:. *.proto
#protoc --go_out=plugins=grpc:. *.proto
go install .
