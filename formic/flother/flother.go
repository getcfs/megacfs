// Flother is an implementation of Twitter's Snowflake 64-bit ID generation
// See: https://blog.twitter.com/2010/announcing-snowflake
//  or: http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram

package flother

import (
	"sync/atomic"
	"time"
)

type Flother struct {
	epoch    time.Time
	timeBits uint64
	nodeBits uint64
	seqBits  uint64
	counter  uint64
	node     uint64
}

// epoch is the offset to use for the id generation and node is the unique node id for this node.
func NewFlother(epoch time.Time, node uint64) *Flother {
	f := &Flother{
		timeBits: 41, // 41 bits should allow for almost 70 years of ids with an epoch
		nodeBits: 13, // 13 bits allows for 8192 nodes
		seqBits:  10, // 10 bits allows for 1024 IDs per milisecond
		epoch:    epoch,
		counter:  0,
		node:     node,
	}
	return f
}

// Get an ID
func (f *Flother) GetID() uint64 {
	ms := uint64(time.Now().UnixNano()-f.epoch.UnixNano()) / 1000000 // miliseconds
	id := ms << (64 - f.timeBits)
	id |= f.node << (64 - f.timeBits - f.nodeBits)
	atomic.AddUint64(&f.counter, 1)
	id |= f.counter % (1 << f.seqBits)
	return id
}
