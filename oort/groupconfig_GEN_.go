package oort

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/getcfs/megacfs/ftls"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// GroupStoreConfig defines the settings when creating new stores, such as NewReplGroupStore.
type GroupStoreConfig struct {
	// Logger defines where log output will go. If not set, the logger will
	// default to zap.New(zap.NewJSONEncoder()).
	Logger *zap.Logger
	// AddressIndex indicates which of the ring node addresses to use when
	// connecting to a node (see github.com/gholt/ring/Node.Address).
	AddressIndex int
	// ValueCap defines the maximum value size supported by the set of stores.
	// This defaults to 0xffffffff, or math.MaxUint32. In order to discover the
	// true value cap, all stores would have to be queried and then the lowest
	// cap used. However, that's probably not really necessary and configuring
	// a set value cap here is probably fine.
	ValueCap uint32
	// PoolSize sets how many store connections can be used per store endpoint.
	PoolSize int
	// ConcurrentRequestsPerStore defines the concurrent requests per
	// underlying connected store. Default: 1000
	ConcurrentRequestsPerStore int
	// FailedConnectRetryDelay defines how many seconds must pass before
	// retrying a failed connection. Default: 15 seconds
	FailedConnectRetryDelay int
	// StoreFTLSConfig is the ftls config you want use to build a tls.Config for
	// each grpc client used to communicate to the Store.
	StoreFTLSConfig *ftls.Config
	// GRPCOpts are any additional reusable options you'd like to pass to GRPC
	// when connecting to stores.
	GRPCOpts []grpc.DialOption
	// RingServer is the network address to use to connect to a ring server. An
	// empty string will use the default DNS method of determining the ring
	// server location.
	RingServer string
	// RingServerGRPCOpts are any additional options you'd like to pass to GRPC
	// when connecting to the ring server.
	RingServerGRPCOpts []grpc.DialOption
	// RingClientID is a unique identifier for this client, used when
	// registering with the RingServer. This allows the ring server to
	// proactively clean up stale connections should a reconnection be needed.
	RingClientID string
	// RingCachePath is the full location file name where you'd like persist
	// last received ring data, such as "/var/lib/myprog/ring/valuestore.ring".
	// An empty string will disable caching. The cacher will need permission to
	// create a new file with the path given plus a temporary suffix, and will
	// then move that temporary file into place using the exact path given.
	RingCachePath string
}

func resolveGroupStoreConfig(c *GroupStoreConfig) *GroupStoreConfig {
	cfg := &GroupStoreConfig{}
	if c != nil {
		*cfg = *c
	}
	if cfg.ValueCap == 0 {
		cfg.ValueCap = 0xffffffff
	}
	if cfg.PoolSize == 0 {
		cfg.PoolSize = 10
	}
	if cfg.PoolSize < 1 {
		cfg.PoolSize = 1
	}
	if cfg.ConcurrentRequestsPerStore == 0 {
		cfg.ConcurrentRequestsPerStore = 1000
	}
	if cfg.ConcurrentRequestsPerStore < 1 {
		cfg.ConcurrentRequestsPerStore = 1
	}
	if cfg.FailedConnectRetryDelay == 0 {
		cfg.FailedConnectRetryDelay = 15
	}
	if cfg.FailedConnectRetryDelay < 1 {
		cfg.FailedConnectRetryDelay = 1
	}
	if cfg.RingClientID == "" {
		// Try to generate a random UUID according to RFC 4122.
		uuid := make([]byte, 16)
		n, err := io.ReadFull(rand.Reader, uuid)
		if n != len(uuid) || err != nil {
			// If rand read gives error, just use current time, we don't need
			// true crypto really.
			i := uint64(time.Now().UnixNano())
			binary.BigEndian.PutUint64(uuid, i)
			binary.BigEndian.PutUint64(uuid[8:], i)
		}
		// Variant bits; see section 4.1.1.
		uuid[8] = uuid[8]&^0xc0 | 0x80
		// Version 4 (pseudo-random); see section 4.1.3.
		uuid[6] = uuid[6]&^0xf0 | 0x40
		cfg.RingClientID = fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:])
	}
	return cfg
}
