package flother

import (
	"testing"
	"time"
)

func TestGetID(t *testing.T) {
	f := NewFlother(time.Now(), 1)
	id1 := f.GetID()
	id2 := f.GetID()
	if id1 != 1025 {
		t.Errorf("Unexpected first id %d", id1)
	}
	if id2 != (id1 + 1) {
		t.Errorf("Second id not incremented by one (%d, %d)", id1, id2)
	}
}

func BenchmarkGetID(b *testing.B) {
	f := NewFlother(time.Now(), 1)
	for i := 0; i < b.N; i++ {
		f.GetID()
	}
}
