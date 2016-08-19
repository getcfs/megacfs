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

var gerf = grpc.Errorf // To avoid a `go vet` quirk
var ErrNotFound = gerf(codes.NotFound, "Not Found")
var ErrNotEmpty = gerf(codes.FailedPrecondition, "Not Empty")

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

func GetSystemID(fsid []byte, dir string) []byte {
	h := murmur3.New128()
	h.Write([]byte("/system/"))
	h.Write(fsid)
	h.Write([]byte(dir))
	s1, s2 := h.Sum128()
	b := bytes.NewBuffer([]byte(""))
	binary.Write(b, binary.BigEndian, s1)
	binary.Write(b, binary.BigEndian, s2)
	return b.Bytes()
}

func GetDeletedID(fsid []byte) []byte {
	return GetSystemID(fsid, "deleted")
}

func GetDirtyID(fsid []byte) []byte {
	return GetSystemID(fsid, "dirty")
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
