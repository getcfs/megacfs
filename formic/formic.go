package formic

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/gogo/protobuf/proto"
	"github.com/spaolacci/murmur3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var ErrZeroValue = errors.New("Got 0 length message")

// NOTE: Go Vet for some reason doesn't like the following Errorf (ignore gholt)
var ErrNotFound = grpc.Errorf(codes.NotFound, "Not Found")
var ErrNotEmpty = grpc.Errorf(codes.FailedPrecondition, "Not Empty")

func GetID(fsid []byte, inode, block uint64) []byte {
	// TODO: Figure out what arrangement we want to use for the hash
	h := murmur3.New128()
	h.Write(fsid)
	binary.Write(h, binary.BigEndian, inode)
	binary.Write(h, binary.BigEndian, block)
	s1, s2 := h.Sum128()
	b := bytes.NewBuffer([]byte(""))
	binary.Write(b, binary.BigEndian, s1)
	binary.Write(b, binary.BigEndian, s2)
	return b.Bytes()
}

func GetDeletedID(fsid []byte) []byte {
	h := murmur3.New128()
	h.Write([]byte("/system/"))
	h.Write(fsid)
	h.Write([]byte("/deleted"))
	s1, s2 := h.Sum128()
	b := bytes.NewBuffer([]byte(""))
	binary.Write(b, binary.BigEndian, s1)
	binary.Write(b, binary.BigEndian, s2)
	return b.Bytes()
}

func Marshal(pb proto.Message) ([]byte, error) {
	return proto.Marshal(pb)
}

func Unmarshal(buf []byte, pb proto.Message) error {
	if len(buf) == 0 {
		return ErrZeroValue
	}
	return proto.Unmarshal(buf, pb)
}
