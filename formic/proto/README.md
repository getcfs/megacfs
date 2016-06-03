# Rebuilding the grpc interface

Rebuilding the api requires a recent version of the google protobuf library as well as grpc-go. Once you've got protoc and grpc-go you can rebuild using:

`protoc --go_out=plugins=grpc:. *.proto`

# Install info

Install grpc-go and golang protobuf implementation:

```
go get github.com/grpc/grpc-go
go get github.com/golang/protobuf/proto
go get github.com/golang/protobuf/protoc-gen-go
```

## For OSX

To get a compatible version of protoc installed the easiest way is to use brew:

`brew update && brew install protobuf --devel`

## For other os's

See - https://developers.google.com/protocol-buffers/ for install instructions.

## Random useful links

1. https://developers.google.com/protocol-buffers/docs/proto3#scalar
2. http://www.grpc.io/docs/
