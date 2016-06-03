#!/bin/bash
set -x
#protoc --go_out=plugins=grpc:. *.proto
protoc --gofast_out=plugins=grpc:. --proto_path=$GOPATH/src:$GOPATH/src/github.com/gogo/protobuf/protobuf:. *.proto;
go install .
