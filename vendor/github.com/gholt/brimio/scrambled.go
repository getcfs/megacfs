package brimio

import (
	"math/rand"
	"time"
)

// Scrambled implements io.Reader by returning random data.
type Scrambled struct {
	r rand.Source
}

// NewScrambled returns a Scrambled with the random seed based on the current
// time.
func NewScrambled() *Scrambled {
	return NewSeededScrambled(time.Now().UnixNano())
}

// NewSeededScrambled returns a Scrambled with a specific random seed; useful
// for repeatable test/benchmark scenarios.
func NewSeededScrambled(seed int64) *Scrambled {
	return &Scrambled{r: rand.NewSource(seed)}
}

func (s *Scrambled) Read(bs []byte) {
	for i := len(bs) - 1; i >= 0; {
		v := s.r.Int63()
		for j := 6; i >= 0 && j >= 0; j-- {
			bs[i] = byte(v)
			i--
			v >>= 8
		}
	}
}
