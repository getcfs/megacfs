package formic

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/gogo/protobuf/proto"
	"github.com/spaolacci/murmur3"
)

var ErrZeroValue = errors.New("Got 0 length message")

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

func Marshal(pb proto.Message) ([]byte, error) {
	return proto.Marshal(pb)
}

func Unmarshal(buf []byte, pb proto.Message) error {
	if len(buf) == 0 {
		return ErrZeroValue
	}
	return proto.Unmarshal(buf, pb)
}
