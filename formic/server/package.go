// Package server provides a server implementation of a single node of
// a formic cluster.
package server

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/gogo/protobuf/proto"
	"github.com/spaolacci/murmur3"
)

func getID(fsid string, inode, block uint64) []byte {
	// TODO: Figure out what arrangement we want to use for the hash
	// TODO: This whole thing is a bit weird. It seems we generate these ids,
	// using murmur, then use the resulting []byte to generate the store
	// keypairs, using murmur again. Seems like we could cut out some steps.
	h := murmur3.New128()
	h.Write([]byte(fsid))
	binary.Write(h, binary.BigEndian, inode)
	binary.Write(h, binary.BigEndian, block)
	s1, s2 := h.Sum128()
	b := bytes.NewBuffer([]byte(""))
	binary.Write(b, binary.BigEndian, s1)
	binary.Write(b, binary.BigEndian, s2)
	return b.Bytes()
}

func getSystemID(fsid string, dir string) []byte {
	h := murmur3.New128()
	h.Write([]byte("/system/"))
	h.Write([]byte(fsid))
	h.Write([]byte(dir))
	s1, s2 := h.Sum128()
	b := bytes.NewBuffer([]byte(""))
	binary.Write(b, binary.BigEndian, s1)
	binary.Write(b, binary.BigEndian, s2)
	return b.Bytes()
}

func getDeletedID(fsid string) []byte {
	return getSystemID(fsid, "deleted")
}

func getDirtyID(fsid string) []byte {
	return getSystemID(fsid, "dirty")
}

func marshal(msg proto.Message) ([]byte, error) {
	return proto.Marshal(msg)
}

var errZeroValue = errors.New("got 0 length message")

func unmarshal(buf []byte, msg proto.Message) error {
	if len(buf) == 0 {
		return errZeroValue
	}
	return proto.Unmarshal(buf, msg)
}
