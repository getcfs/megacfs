package store

import (
	"bytes"
	"strings"
	"testing"
)

func TestGroupKTBloomFilterBasic(t *testing.T) {
	f := newGroupKTBloomFilter(10, 0.01, 0)
	if f.mayHave(1, 2, 3, 4, 5) {
		t.Fatal("")
	}
	f.add(1, 2, 3, 4, 5)
	if !f.mayHave(1, 2, 3, 4, 5) {
		t.Fatal("")
	}
	f.reset(0)
	if f.mayHave(1, 2, 3, 4, 5) {
		t.Fatal("")
	}
	s := f.String()
	if !strings.HasPrefix(s, "groupKTBloomFilter 0x") {
		t.Fatal(s)
	}
	if !strings.HasSuffix(s, " n=10 p=0.010000 salt=0 m=96 k=8 bytes=12") {
		t.Fatal(s)
	}
}

func TestGroupKTBloomFilterLots(t *testing.T) {
	f := newGroupKTBloomFilter(100, 0.001, 0)
	for i := uint64(0); i < 100; i++ {
		f.add(i, i, i, i, i)
	}
	for i := uint64(0); i < 100; i++ {
		if !f.mayHave(i, i, i, i, i) {
			t.Fatal(i)
		}
	}
	for i := uint64(0); i < 100; i++ {
		if f.mayHave(i, i, i, i, 101) {
			t.Fatal(i)
		}
	}
}

func TestGroupKTBloomFilterPersistence(t *testing.T) {
	f := newGroupKTBloomFilter(10, 0.01, 0)
	for i := uint64(0); i < 100; i++ {
		f.add(i, i, i, i, i)
	}
	m := &groupPullReplicationMsg{
		store:  nil,
		header: make([]byte, _GROUP_KT_BLOOM_FILTER_HEADER_BYTES+_GROUP_PULL_REPLICATION_MSG_HEADER_BYTES),
		body:   make([]byte, len(f.bits)),
	}
	f.toMsg(m, _GROUP_PULL_REPLICATION_MSG_HEADER_BYTES)
	f2 := newGroupKTBloomFilterFromMsg(m, _GROUP_PULL_REPLICATION_MSG_HEADER_BYTES)
	if f2.n != f.n {
		t.Fatal(f2.n)
	}
	if f2.p != f.p {
		t.Fatal(f2.p)
	}
	if f2.salt != f.salt {
		t.Fatal(f2.salt)
	}
	if f2.m != f.m {
		t.Fatal(f2.m)
	}
	if f2.kDiv4 != f.kDiv4 {
		t.Fatal(f2.kDiv4)
	}
	if !bytes.Equal(f2.bits, f.bits) {
		t.Fatal("")
	}
	for i := uint64(0); i < 100; i++ {
		if !f2.mayHave(i, i, i, i, i) {
			t.Fatal(i)
		}
	}
}
