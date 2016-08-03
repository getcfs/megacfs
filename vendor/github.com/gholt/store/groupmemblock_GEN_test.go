package store

import (
	"math"
	"testing"
)

func TestGroupValuesMemRead(t *testing.T) {
	store, _ := newTestGroupStore(nil)
	memBlock1 := &groupMemBlock{id: 1, store: store, values: []byte("0123456789abcdef")}
	memBlock2 := &groupMemBlock{id: 2, store: store, values: []byte("fedcba9876543210")}
	store.locBlocks = []groupLocBlock{nil, memBlock1, memBlock2}
	tsn := memBlock1.timestampnano()
	if tsn != math.MaxInt64 {
		t.Fatal(tsn)
	}
	ts, v, err := memBlock1.read(1, 2, 0, 0, 0x100, 5, 6, nil)
	if !IsNotFound(err) {
		t.Fatal(err)
	}
	memBlock1.store.locmap.Set(1, 2, 0, 0, 0x100, memBlock1.id, 5, 6, false)
	ts, v, err = memBlock1.read(1, 2, 0, 0, 0x100, 5, 6, nil)
	if err != nil {
		a, b, c, d := memBlock1.store.locmap.Get(1, 2, 0, 0)
		t.Fatal(err, a, b, c, d)
	}
	if ts != 0x100 {
		t.Fatal(ts)
	}
	if string(v) != "56789a" {
		t.Fatal(string(v))
	}
	memBlock1.store.locmap.Set(1, 2, 0, 0, 0x100|_TSB_DELETION, memBlock1.id, 5, 6, false)
	ts, v, err = memBlock1.read(1, 2, 0, 0, 0x100, 5, 6, nil)
	if !IsNotFound(err) {
		t.Fatal(err)
	}
	memBlock1.store.locmap.Set(1, 2, 0, 0, 0x200, memBlock2.id, 5, 6, false)
	ts, v, err = memBlock1.read(1, 2, 0, 0, 0x100, 5, 6, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0x200 {
		t.Fatal(ts)
	}
	if string(v) != "a98765" {
		t.Fatal(string(v))
	}
}
