#!/bin/bash
set -x
protoc --go_out=plugins=grpc:. *.proto
go install .
