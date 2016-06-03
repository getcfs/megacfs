package flog

import (
	//"testing"

	"google.golang.org/grpc/grpclog"
)

var gprcLogger grpclog.Logger = ErrorLogger(Default)
