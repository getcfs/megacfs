package store

import (
	"math"
	"sync"
)

type groupMemBlock struct {
	store       *defaultGroupStore
	id          uint32
	fileID      uint32
	fileOffset  uint32
	toc         []byte
	values      []byte
	discardLock sync.RWMutex
}

func (memBlock *groupMemBlock) timestampnano() int64 {
	return math.MaxInt64
}

func (memBlock *groupMemBlock) read(keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampbits uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error) {
	memBlock.discardLock.RLock()
	timestampbits, id, offset, length := memBlock.store.locmap.Get(keyA, keyB, childKeyA, childKeyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 {
		memBlock.discardLock.RUnlock()
		return timestampbits, value, errNotFound
	}
	if id != memBlock.id {
		memBlock.discardLock.RUnlock()
		return memBlock.store.locBlock(id).read(keyA, keyB, childKeyA, childKeyB, timestampbits, offset, length, value)
	}
	value = append(value, memBlock.values[offset:offset+length]...)
	memBlock.discardLock.RUnlock()
	return timestampbits, value, nil
}

func (memBlock *groupMemBlock) close() error {
	return nil
}
