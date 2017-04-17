package flother

import (
	"testing"
	"time"
)

func TestGetID(t *testing.T) {
	f := NewFlother(time.Now(), 1, DEFAULT_TIME_BITS, DEFAULT_NODE_BITS)
	id1 := f.NewID()
	id2 := f.NewID()
	if id1 != 1025 {
		t.Errorf("Unexpected first id %d", id1)
	}
	if id2 != (id1 + 1) {
		t.Errorf("Second id not incremented by one (%d, %d)", id1, id2)
	}
}

func BenchmarkGetID(b *testing.B) {
	f := NewFlother(time.Now(), 1, DEFAULT_TIME_BITS, DEFAULT_NODE_BITS)
	for i := 0; i < b.N; i++ {
		f.NewID()
	}
}
