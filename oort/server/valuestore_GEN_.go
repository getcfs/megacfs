package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/getcfs/megacfs/ftls"
	"github.com/getcfs/megacfs/oort/proto"
	"github.com/getcfs/megacfs/oort/valueproto"
	"github.com/gholt/ring"
	"github.com/gholt/store"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type ValueStore struct {
	sync.RWMutex
	waitGroup         *sync.WaitGroup
	shutdownChan      chan struct{}
	started           bool
	valueStore        store.ValueStore
	valueStoreMsgRing *ring.TCPMsgRing
	grpcServer        *grpc.Server
	grpcAddressIndex  int
	grpcDefaultPort   int
	grpcCertFile      string
	grpcKeyFile       string
	replCertFile      string
	replKeyFile       string
	caFile            string
	logger            *zap.Logger
}

type ValueStoreConfig struct {
	GRPCAddressIndex int
	ReplAddressIndex int
	GRPCCertFile     string
	GRPCKeyFile      string
	ReplCertFile     string
	ReplKeyFile      string
	CAFile           string
	Path             string
	Scale            float64
	Ring             ring.Ring
	Logger           *zap.Logger
}

func resolveValueStoreConfig(c *ValueStoreConfig) *ValueStoreConfig {
	cfg := &ValueStoreConfig{}
	if c != nil {
		*cfg = *c
	}
	if cfg.Logger == nil {
		var err error
		cfg.Logger, err = zap.NewProduction()
		if err != nil {
			panic(err)
		}
	}
	return cfg
}

func NewValueStore(cfg *ValueStoreConfig) (*ValueStore, chan error, error) {
	cfg = resolveValueStoreConfig(cfg)
	s := &ValueStore{
		waitGroup:        &sync.WaitGroup{},
		grpcAddressIndex: cfg.GRPCAddressIndex,
		grpcCertFile:     cfg.GRPCCertFile,
		grpcKeyFile:      cfg.GRPCKeyFile,
		replCertFile:     cfg.ReplCertFile,
		replKeyFile:      cfg.ReplKeyFile,
		caFile:           cfg.CAFile,
		logger:           cfg.Logger,
	}
	var err error
	s.valueStoreMsgRing, err = ring.NewTCPMsgRing(&ring.TCPMsgRingConfig{
		AddressIndex: cfg.ReplAddressIndex,
		UseTLS:       true,
		MutualTLS:    true,
		CertFile:     s.replCertFile,
		KeyFile:      s.replKeyFile,
		CAFile:       s.caFile,
		DefaultPort:  12321,
	})
	if err != nil {
		return nil, nil, err
	}
	s.valueStoreMsgRing.SetRing(cfg.Ring)
	var valueStoreRestartChan chan error
	s.valueStore, valueStoreRestartChan = store.NewValueStore(&store.ValueStoreConfig{
		Scale:   cfg.Scale,
		Path:    cfg.Path,
		MsgRing: s.valueStoreMsgRing,
	})
	return s, valueStoreRestartChan, nil
}

func (s *ValueStore) Startup(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()
	if s.started {
		return nil
	}
	s.started = true
	s.shutdownChan = make(chan struct{})
	err := s.valueStore.Startup(ctx)
	if err != nil {
		return err
	}
	go func() {
		mRingChanges := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "RingChanges",
			Help:      "Number of received ring changes.",
		})
		mRingChangeCloses := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "RingChangeCloses",
			Help:      "Number of connections closed due to ring changes.",
		})
		mMsgToNodes := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgToNodes",
			Help:      "Number of times MsgToNode function has been called; single message to single node.",
		})
		mMsgToNodeNoRings := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgToNodeNoRings",
			Help:      "Number of times MsgToNode function has been called with no ring yet available.",
		})
		mMsgToNodeNoNodes := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgToNodeNoNodes",
			Help:      "Number of times MsgToNode function has been called with no matching node.",
		})
		mMsgToOtherReplicas := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgToOtherReplicas",
			Help:      "Number of times MsgToOtherReplicas function has been called; single message to all replicas, excluding the local replica if responsible.",
		})
		mMsgToOtherReplicasNoRings := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgToOtherReplicasNoRings",
			Help:      "Number of times MsgToOtherReplicas function has been called with no ring yet available.",
		})
		mListenErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "ListenErrors",
			Help:      "Number of errors trying to establish a TCP listener.",
		})
		mIncomingConnections := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "IncomingConnections",
			Help:      "Number of incoming TCP connections made.",
		})
		mDials := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "Dials",
			Help:      "Number of attempts to establish outgoing TCP connections.",
		})
		mDialErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "DialErrors",
			Help:      "Number of errors trying to establish outgoing TCP connections.",
		})
		mOutgoingConnections := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "OutgoingConnections",
			Help:      "Number of outgoing TCP connections established.",
		})
		mMsgChanCreations := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgChanCreations",
			Help:      "Number of internal message channels created.",
		})
		mMsgToAddrs := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "MsgToAddrsTCPMsgRing",
			Help:      "Number times internal function msgToAddr has been called.",
		})
		mMsgToAddrQueues := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgToAddrQueues",
			Help:      "Number of messages msgToAddr successfully queued.",
		})
		mMsgToAddrTimeoutDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgToAddrTimeoutDrops",
			Help:      "Number of messages msgToAddr dropped after timeout.",
		})
		mMsgToAddrShutdownDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgToAddrShutdownDrops",
			Help:      "Number of messages msgToAddr dropped due to a shutdown.",
		})
		mMsgReads := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgReads",
			Help:      "Number of incoming messages read.",
		})
		mMsgReadErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgReadErrors",
			Help:      "Number of errors reading incoming messages.",
		})
		mMsgWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgWrites",
			Help:      "Number of outgoing messages written.",
		})
		mMsgWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStoreTCPMsgRing",
			Name:      "MsgWriteErrors",
			Help:      "Number of errors writing outgoing messages.",
		})
		mValues := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ValueStore",
			Name:      "Values",
			Help:      "Current number of values stored.",
		})
		mValueBytes := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ValueStore",
			Name:      "ValueBytes",
			Help:      "Current number of bytes for the values stored.",
		})
		mLookups := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "Lookups",
			Help:      "Count of lookup requests executed.",
		})
		mLookupErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "LookupErrors",
			Help:      "Count of lookup requests executed resulting in errors.",
		})

		mReads := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "Reads",
			Help:      "Count of read requests executed.",
		})
		mReadErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "ReadErrors",
			Help:      "Count of read requests executed resulting in errors.",
		})

		mWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "Writes",
			Help:      "Count of write requests executed.",
		})
		mWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "WriteErrors",
			Help:      "Count of write requests executed resulting in errors.",
		})
		mWritesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "WritesOverridden",
			Help:      "Count of write requests that were outdated or repeated.",
		})
		mDeletes := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "Deletes",
			Help:      "Count of delete requests executed.",
		})
		mDeleteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "DeleteErrors",
			Help:      "Count of delete requests executed resulting in errors.",
		})
		mDeletesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "DeletesOverridden",
			Help:      "Count of delete requests that were outdated or repeated.",
		})
		mOutBulkSets := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "OutBulkSets",
			Help:      "Count of outgoing bulk-set messages in response to incoming pull replication messages.",
		})
		mOutBulkSetValues := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "OutBulkSetValues",
			Help:      "Count of values in outgoing bulk-set messages; these bulk-set messages are those in response to incoming pull-replication messages.",
		})
		mOutBulkSetPushes := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "OutBulkSetPushes",
			Help:      "Count of outgoing bulk-set messages due to push replication.",
		})
		mOutBulkSetPushValues := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "OutBulkSetPushValues",
			Help:      "Count of values in outgoing bulk-set messages; these bulk-set messages are those due to push replication.",
		})
		mOutPushReplicationSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ValueStore",
			Name:      "OutPushReplicationSeconds",
			Help:      "How long the last out push replication pass took.",
		})
		mInBulkSets := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InBulkSets",
			Help:      "Count of incoming bulk-set messages.",
		})
		mInBulkSetDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InBulkSetDrops",
			Help:      "Count of incoming bulk-set messages dropped due to the local system being overworked at the time.",
		})
		mInBulkSetInvalids := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InBulkSetInvalids",
			Help:      "Count of incoming bulk-set messages that couldn't be parsed.",
		})
		mInBulkSetWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InBulkSetWrites",
			Help:      "Count of writes due to incoming bulk-set messages.",
		})
		mInBulkSetWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InBulkSetWriteErrors",
			Help:      "Count of errors returned from writes due to incoming bulk-set messages.",
		})
		mInBulkSetWritesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InBulkSetWritesOverridden",
			Help:      "Count of writes from incoming bulk-set messages that result in no change.",
		})
		mOutBulkSetAcks := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "OutBulkSetAcks",
			Help:      "Count of outgoing bulk-set-ack messages.",
		})
		mInBulkSetAcks := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InBulkSetAcks",
			Help:      "Count of incoming bulk-set-ack messages.",
		})
		mInBulkSetAckDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InBulkSetAckDrops",
			Help:      "Count of incoming bulk-set-ack messages dropped due to the local system being overworked at the time.",
		})
		mInBulkSetAckInvalids := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InBulkSetAckInvalids",
			Help:      "Count of incoming bulk-set-ack messages that couldn't be parsed.",
		})
		mInBulkSetAckWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InBulkSetAckWrites",
			Help:      "Count of writes (for local removal) due to incoming bulk-set-ack messages.",
		})
		mInBulkSetAckWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InBulkSetAckWriteErrors",
			Help:      "Count of errors returned from writes due to incoming bulk-set-ack messages.",
		})
		mInBulkSetAckWritesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InBulkSetAckWritesOverridden",
			Help:      "Count of writes from incoming bulk-set-ack messages that result in no change.",
		})
		mOutPullReplications := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "OutPullReplications",
			Help:      "Count of outgoing pull-replication messages.",
		})
		mOutPullReplicationSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ValueStore",
			Name:      "OutPullReplicationSeconds",
			Help:      "How long the last out pull replication pass took.",
		})
		mInPullReplications := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InPullReplications",
			Help:      "Count of incoming pull-replication messages.",
		})
		mInPullReplicationDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InPullReplicationDrops",
			Help:      "Count of incoming pull-replication messages droppped due to the local system being overworked at the time.",
		})
		mInPullReplicationInvalids := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "InPullReplicationInvalids",
			Help:      "Count of incoming pull-replication messages that couldn't be parsed.",
		})
		mExpiredDeletions := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "ExpiredDeletions",
			Help:      "Count of recent deletes that have become old enough to be completely discarded.",
		})
		mTombstoneDiscardSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ValueStore",
			Name:      "TombstoneDiscardSeconds",
			Help:      "How long the last tombstone discard pass took.",
		})
		mCompactionSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ValueStore",
			Name:      "CompactionSeconds",
			Help:      "How long the last compaction pass took.",
		})
		mCompactions := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "Compactions",
			Help:      "Count of disk file sets compacted due to their contents exceeding a staleness threshold. For example, this happens when enough of the values have been overwritten or deleted in more recent operations.",
		})
		mSmallFileCompactions := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "ValueStore",
			Name:      "SmallFileCompactions",
			Help:      "Count of disk file sets compacted due to the entire file size being too small. For example, this may happen when the store is shutdown and restarted.",
		})
		mReadOnly := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ValueStore",
			Name:      "ReadOnly",
			Help:      "Indicates when the store has been put in read-only mode, whether by an operator or automatically by the watcher.",
		})
		mAuditSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "ValueStore",
			Name:      "AuditSeconds",
			Help:      "How long the last audit pass took.",
		})
		prometheus.Register(mRingChanges)
		prometheus.Register(mRingChangeCloses)
		prometheus.Register(mMsgToNodes)
		prometheus.Register(mMsgToNodeNoRings)
		prometheus.Register(mMsgToNodeNoNodes)
		prometheus.Register(mMsgToOtherReplicas)
		prometheus.Register(mMsgToOtherReplicasNoRings)
		prometheus.Register(mListenErrors)
		prometheus.Register(mIncomingConnections)
		prometheus.Register(mDials)
		prometheus.Register(mDialErrors)
		prometheus.Register(mOutgoingConnections)
		prometheus.Register(mMsgChanCreations)
		prometheus.Register(mMsgToAddrs)
		prometheus.Register(mMsgToAddrQueues)
		prometheus.Register(mMsgToAddrTimeoutDrops)
		prometheus.Register(mMsgToAddrShutdownDrops)
		prometheus.Register(mMsgReads)
		prometheus.Register(mMsgReadErrors)
		prometheus.Register(mMsgWrites)
		prometheus.Register(mMsgWriteErrors)
		prometheus.Register(mValues)
		prometheus.Register(mValueBytes)
		prometheus.Register(mLookups)
		prometheus.Register(mLookupErrors)

		prometheus.Register(mReads)
		prometheus.Register(mReadErrors)

		prometheus.Register(mWrites)
		prometheus.Register(mWriteErrors)
		prometheus.Register(mWritesOverridden)
		prometheus.Register(mDeletes)
		prometheus.Register(mDeleteErrors)
		prometheus.Register(mDeletesOverridden)
		prometheus.Register(mOutBulkSets)
		prometheus.Register(mOutBulkSetValues)
		prometheus.Register(mOutBulkSetPushes)
		prometheus.Register(mOutBulkSetPushValues)
		prometheus.Register(mOutPushReplicationSeconds)
		prometheus.Register(mInBulkSets)
		prometheus.Register(mInBulkSetDrops)
		prometheus.Register(mInBulkSetInvalids)
		prometheus.Register(mInBulkSetWrites)
		prometheus.Register(mInBulkSetWriteErrors)
		prometheus.Register(mInBulkSetWritesOverridden)
		prometheus.Register(mOutBulkSetAcks)
		prometheus.Register(mInBulkSetAcks)
		prometheus.Register(mInBulkSetAckDrops)
		prometheus.Register(mInBulkSetAckInvalids)
		prometheus.Register(mInBulkSetAckWrites)
		prometheus.Register(mInBulkSetAckWriteErrors)
		prometheus.Register(mInBulkSetAckWritesOverridden)
		prometheus.Register(mOutPullReplications)
		prometheus.Register(mOutPullReplicationSeconds)
		prometheus.Register(mInPullReplications)
		prometheus.Register(mInPullReplicationDrops)
		prometheus.Register(mInPullReplicationInvalids)
		prometheus.Register(mExpiredDeletions)
		prometheus.Register(mTombstoneDiscardSeconds)
		prometheus.Register(mCompactionSeconds)
		prometheus.Register(mCompactions)
		prometheus.Register(mSmallFileCompactions)
		prometheus.Register(mReadOnly)
		prometheus.Register(mAuditSeconds)
		tcpMsgRingStats := s.valueStoreMsgRing.Stats(false)
		for {
			select {
			case <-s.shutdownChan:
				return
			case <-time.After(time.Minute):
				tcpMsgRingStats = s.valueStoreMsgRing.Stats(false)
				mRingChanges.Add(float64(tcpMsgRingStats.RingChanges))
				mRingChangeCloses.Add(float64(tcpMsgRingStats.RingChangeCloses))
				mMsgToNodes.Add(float64(tcpMsgRingStats.MsgToNodes))
				mMsgToNodeNoRings.Add(float64(tcpMsgRingStats.MsgToNodeNoRings))
				mMsgToNodeNoNodes.Add(float64(tcpMsgRingStats.MsgToNodeNoNodes))
				mMsgToOtherReplicas.Add(float64(tcpMsgRingStats.MsgToOtherReplicas))
				mMsgToOtherReplicasNoRings.Add(float64(tcpMsgRingStats.MsgToOtherReplicasNoRings))
				mListenErrors.Add(float64(tcpMsgRingStats.ListenErrors))
				mIncomingConnections.Add(float64(tcpMsgRingStats.IncomingConnections))
				mDials.Add(float64(tcpMsgRingStats.Dials))
				mDialErrors.Add(float64(tcpMsgRingStats.DialErrors))
				mOutgoingConnections.Add(float64(tcpMsgRingStats.OutgoingConnections))
				mMsgChanCreations.Add(float64(tcpMsgRingStats.MsgChanCreations))
				mMsgToAddrs.Add(float64(tcpMsgRingStats.MsgToAddrs))
				mMsgToAddrQueues.Add(float64(tcpMsgRingStats.MsgToAddrQueues))
				mMsgToAddrTimeoutDrops.Add(float64(tcpMsgRingStats.MsgToAddrTimeoutDrops))
				mMsgToAddrShutdownDrops.Add(float64(tcpMsgRingStats.MsgToAddrShutdownDrops))
				mMsgReads.Add(float64(tcpMsgRingStats.MsgReads))
				mMsgReadErrors.Add(float64(tcpMsgRingStats.MsgReadErrors))
				mMsgWrites.Add(float64(tcpMsgRingStats.MsgWrites))
				mMsgWriteErrors.Add(float64(tcpMsgRingStats.MsgWriteErrors))
				stats, err := s.valueStore.Stats(context.Background(), false)
				if err != nil {
					s.logger.Debug("stats error", zap.Error(err))
				} else if sstats, ok := stats.(*store.ValueStoreStats); ok {
					mValues.Set(float64(sstats.Values))
					mValueBytes.Set(float64(sstats.ValueBytes))
					mLookups.Add(float64(sstats.Lookups))
					mLookupErrors.Add(float64(sstats.LookupErrors))

					mReads.Add(float64(sstats.Reads))
					mReadErrors.Add(float64(sstats.ReadErrors))

					mWrites.Add(float64(sstats.Writes))
					mWriteErrors.Add(float64(sstats.WriteErrors))
					mWritesOverridden.Add(float64(sstats.WritesOverridden))
					mDeletes.Add(float64(sstats.Deletes))
					mDeleteErrors.Add(float64(sstats.DeleteErrors))
					mDeletesOverridden.Add(float64(sstats.DeletesOverridden))
					mOutBulkSets.Add(float64(sstats.OutBulkSets))
					mOutBulkSetValues.Add(float64(sstats.OutBulkSetValues))
					mOutBulkSetPushes.Add(float64(sstats.OutBulkSetPushes))
					mOutBulkSetPushValues.Add(float64(sstats.OutBulkSetPushValues))
					mOutPushReplicationSeconds.Set(float64(sstats.OutPushReplicationNanoseconds) / 1000000000)
					mInBulkSets.Add(float64(sstats.InBulkSets))
					mInBulkSetDrops.Add(float64(sstats.InBulkSetDrops))
					mInBulkSetInvalids.Add(float64(sstats.InBulkSetInvalids))
					mInBulkSetWrites.Add(float64(sstats.InBulkSetWrites))
					mInBulkSetWriteErrors.Add(float64(sstats.InBulkSetWriteErrors))
					mInBulkSetWritesOverridden.Add(float64(sstats.InBulkSetWritesOverridden))
					mOutBulkSetAcks.Add(float64(sstats.OutBulkSetAcks))
					mInBulkSetAcks.Add(float64(sstats.InBulkSetAcks))
					mInBulkSetAckDrops.Add(float64(sstats.InBulkSetAckDrops))
					mInBulkSetAckInvalids.Add(float64(sstats.InBulkSetAckInvalids))
					mInBulkSetAckWrites.Add(float64(sstats.InBulkSetAckWrites))
					mInBulkSetAckWriteErrors.Add(float64(sstats.InBulkSetAckWriteErrors))
					mInBulkSetAckWritesOverridden.Add(float64(sstats.InBulkSetAckWritesOverridden))
					mOutPullReplications.Add(float64(sstats.OutPullReplications))
					mOutPullReplicationSeconds.Set(float64(sstats.OutPullReplicationNanoseconds) / 1000000000)
					mInPullReplications.Add(float64(sstats.InPullReplications))
					mInPullReplicationDrops.Add(float64(sstats.InPullReplicationDrops))
					mInPullReplicationInvalids.Add(float64(sstats.InPullReplicationInvalids))
					mExpiredDeletions.Add(float64(sstats.ExpiredDeletions))
					mTombstoneDiscardSeconds.Set(float64(sstats.TombstoneDiscardNanoseconds) / 1000000000)
					mCompactionSeconds.Set(float64(sstats.CompactionNanoseconds) / 1000000000)
					mCompactions.Add(float64(sstats.Compactions))
					mSmallFileCompactions.Add(float64(sstats.SmallFileCompactions))
					if sstats.ReadOnly {
						mReadOnly.Set(1)
					} else {
						mReadOnly.Set(0)
					}
					mAuditSeconds.Set(float64(sstats.AuditNanoseconds) / 1000000000)
				} else {
					s.logger.Debug("unknown stats type", zap.Any("stats", stats))
				}
			}
		}
	}()
	s.waitGroup.Add(1)
	go func() {
		s.valueStoreMsgRing.Listen()
		s.waitGroup.Done()
	}()
	s.waitGroup.Add(1)
	go func() {
		<-s.shutdownChan
		s.valueStoreMsgRing.Shutdown()
		s.waitGroup.Done()
	}()
	ln := s.valueStoreMsgRing.Ring().LocalNode()
	if ln == nil {
		return errors.New("no local node set")
	}
	grpcAddr := ln.Address(s.grpcAddressIndex)
	if grpcAddr == "" {
		return fmt.Errorf("no local node address index %d", s.grpcAddressIndex)
	}
	grpcHostPort, err := ring.CanonicalHostPort(grpcAddr, 12320)
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", grpcHostPort)
	if err != nil {
		return err
	}
	tlsCfg, err := ftls.NewServerTLSConfig(ftls.DefaultServerFTLSConf(s.grpcCertFile, s.grpcKeyFile, s.caFile))
	if err != nil {
		return err
	}
	s.grpcServer = grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsCfg)),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
	valueproto.RegisterValueStoreServer(s.grpcServer, s)
	grpc_prometheus.Register(s.grpcServer)

	s.waitGroup.Add(1)
	go func() {
		err := s.grpcServer.Serve(lis)
		if err != nil {
			s.logger.Debug("grpcServer.Serve error", zap.Error(err))
		}
		lis.Close()
		s.waitGroup.Done()
	}()
	s.waitGroup.Add(1)
	go func() {
		<-s.shutdownChan
		s.grpcServer.Stop()
		lis.Close()
		s.waitGroup.Done()
	}()
	return nil
}

func (s *ValueStore) Shutdown(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()
	if !s.started {
		return nil
	}
	close(s.shutdownChan)
	s.waitGroup.Wait()
	return s.valueStore.Shutdown(ctx)
}

func (s *ValueStore) Write(stream valueproto.ValueStore_WriteServer) error {
	var resp valueproto.WriteResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		resp.Rpcid = req.Rpcid
		resp.TimestampMicro, err = s.valueStore.Write(stream.Context(), req.KeyA, req.KeyB, req.TimestampMicro, req.Value)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *ValueStore) Read(stream valueproto.ValueStore_ReadServer) error {
	var resp valueproto.ReadResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		resp.Rpcid = req.Rpcid
		resp.TimestampMicro, resp.Value, err = s.valueStore.Read(stream.Context(), req.KeyA, req.KeyB, resp.Value)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *ValueStore) Lookup(stream valueproto.ValueStore_LookupServer) error {
	var resp valueproto.LookupResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		resp.Rpcid = req.Rpcid
		resp.TimestampMicro, resp.Length, err = s.valueStore.Lookup(stream.Context(), req.KeyA, req.KeyB)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *ValueStore) Delete(stream valueproto.ValueStore_DeleteServer) error {
	var resp valueproto.DeleteResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		resp.Rpcid = req.Rpcid
		resp.TimestampMicro, err = s.valueStore.Delete(stream.Context(), req.KeyA, req.KeyB, req.TimestampMicro)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *ValueStore) Stats() []byte {
	stats, err := s.valueStore.Stats(context.Background(), true)
	if err != nil {
		return nil
	}
	return []byte(stats.String())
}

// Wait isn't implemented yet, need graceful shutdowns in grpc
func (s *ValueStore) Wait() {}
