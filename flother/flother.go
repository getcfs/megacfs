// Package flother is an implementation of Twitter's Snowflake 64-bit ID
// generation.
//
// See: https://blog.twitter.com/2010/announcing-snowflake
// or: http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram
package flother

import (
	"math"
	"sync/atomic"
	"time"
)

// DEFAULT_TIME_BITS is set to 41 bits to allow for almost 70 years of ids with
// an epoch.
const DEFAULT_TIME_BITS = 41

// DEFAULT_NODE_BITS is set to 13 bits to allow for 8192 nodes.
const DEFAULT_NODE_BITS = 13

// The above defaults would leave 10 bits for seqBits, allowing for 1024 IDs
// per millisecond.

type Flother struct {
	epoch    time.Time
	timeBits uint64
	nodeBits uint64
	seqBits  uint64
	counter  uint64
	node     uint64
}

// NewFlother returns a *Flother can be used to generate unique ids.
//
// epoch is the offset to use for the id generation.
//
// node is the unique node id for this server / application.
//
// timeBits are the number of bits to use to represent the time; recommended DEFAULT_TIME_BITS.
//
// nodeBits are the number of bits to use to represent the unique node; recommended DEFAULT_NODE_BITS.
func NewFlother(epoch time.Time, node uint64, timeBits uint64, nodeBits uint64) *Flother {
	return &Flother{
		timeBits: timeBits,
		nodeBits: nodeBits,
		seqBits:  64 - timeBits - nodeBits,
		epoch:    epoch,
		counter:  0,
		node:     node & uint64(math.Pow(2, float64(nodeBits))-1),
	}
}

// NewID returns a new unique identifier.
func (f *Flother) NewID() uint64 {
	milliseconds := uint64(time.Now().UnixNano()-f.epoch.UnixNano()) / 1000000
	id := milliseconds << (64 - f.timeBits)
	id |= f.node << (64 - f.timeBits - f.nodeBits)
	id |= atomic.AddUint64(&f.counter, 1) % (1 << f.seqBits)
	return id
}

// NodeID returns the node identifier given when creating this Flother.
func (f *Flother) NodeID() uint64 {
	return f.node
}
