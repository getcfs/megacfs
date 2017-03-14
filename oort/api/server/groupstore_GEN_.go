package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/getcfs/megacfs/ftls"
	"github.com/getcfs/megacfs/oort/api/groupproto"
	"github.com/getcfs/megacfs/oort/api/proto"
	"github.com/gholt/ring"
	"github.com/gholt/store"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type GroupStore struct {
	sync.RWMutex
	waitGroup         *sync.WaitGroup
	shutdownChan      chan struct{}
	started           bool
	groupStore        store.GroupStore
	groupStoreMsgRing *ring.TCPMsgRing
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

type GroupStoreConfig struct {
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

func resolveGroupStoreConfig(c *GroupStoreConfig) *GroupStoreConfig {
	cfg := &GroupStoreConfig{}
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

func NewGroupStore(cfg *GroupStoreConfig) (*GroupStore, chan error, error) {
	cfg = resolveGroupStoreConfig(cfg)
	s := &GroupStore{
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
	s.groupStoreMsgRing, err = ring.NewTCPMsgRing(&ring.TCPMsgRingConfig{
		AddressIndex: cfg.ReplAddressIndex,
		UseTLS:       true,
		MutualTLS:    true,
		CertFile:     s.replCertFile,
		KeyFile:      s.replKeyFile,
		CAFile:       s.caFile,
		DefaultPort:  12311,
	})
	if err != nil {
		return nil, nil, err
	}
	s.groupStoreMsgRing.SetRing(cfg.Ring)
	var groupStoreRestartChan chan error
	s.groupStore, groupStoreRestartChan = store.NewGroupStore(&store.GroupStoreConfig{
		Scale:   cfg.Scale,
		Path:    cfg.Path,
		MsgRing: s.groupStoreMsgRing,
	})
	return s, groupStoreRestartChan, nil
}

func (s *GroupStore) Startup(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()
	if s.started {
		return nil
	}
	s.started = true
	s.shutdownChan = make(chan struct{})
	err := s.groupStore.Startup(ctx)
	if err != nil {
		return err
	}
	go func() {
		mRingChanges := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "RingChanges",
			Help:      "Number of received ring changes.",
		})
		mRingChangeCloses := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "RingChangeCloses",
			Help:      "Number of connections closed due to ring changes.",
		})
		mMsgToNodes := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgToNodes",
			Help:      "Number of times MsgToNode function has been called; single message to single node.",
		})
		mMsgToNodeNoRings := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgToNodeNoRings",
			Help:      "Number of times MsgToNode function has been called with no ring yet available.",
		})
		mMsgToNodeNoNodes := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgToNodeNoNodes",
			Help:      "Number of times MsgToNode function has been called with no matching node.",
		})
		mMsgToOtherReplicas := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgToOtherReplicas",
			Help:      "Number of times MsgToOtherReplicas function has been called; single message to all replicas, excluding the local replica if responsible.",
		})
		mMsgToOtherReplicasNoRings := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgToOtherReplicasNoRings",
			Help:      "Number of times MsgToOtherReplicas function has been called with no ring yet available.",
		})
		mListenErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "ListenErrors",
			Help:      "Number of errors trying to establish a TCP listener.",
		})
		mIncomingConnections := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "IncomingConnections",
			Help:      "Number of incoming TCP connections made.",
		})
		mDials := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "Dials",
			Help:      "Number of attempts to establish outgoing TCP connections.",
		})
		mDialErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "DialErrors",
			Help:      "Number of errors trying to establish outgoing TCP connections.",
		})
		mOutgoingConnections := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "OutgoingConnections",
			Help:      "Number of outgoing TCP connections established.",
		})
		mMsgChanCreations := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgChanCreations",
			Help:      "Number of internal message channels created.",
		})
		mMsgToAddrs := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "MsgToAddrsTCPMsgRing",
			Help:      "Number times internal function msgToAddr has been called.",
		})
		mMsgToAddrQueues := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgToAddrQueues",
			Help:      "Number of messages msgToAddr successfully queued.",
		})
		mMsgToAddrTimeoutDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgToAddrTimeoutDrops",
			Help:      "Number of messages msgToAddr dropped after timeout.",
		})
		mMsgToAddrShutdownDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgToAddrShutdownDrops",
			Help:      "Number of messages msgToAddr dropped due to a shutdown.",
		})
		mMsgReads := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgReads",
			Help:      "Number of incoming messages read.",
		})
		mMsgReadErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgReadErrors",
			Help:      "Number of errors reading incoming messages.",
		})
		mMsgWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgWrites",
			Help:      "Number of outgoing messages written.",
		})
		mMsgWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStoreTCPMsgRing",
			Name:      "MsgWriteErrors",
			Help:      "Number of errors writing outgoing messages.",
		})
		mValues := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "GroupStore",
			Name:      "Values",
			Help:      "Current number of values stored.",
		})
		mValueBytes := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "GroupStore",
			Name:      "ValueBytes",
			Help:      "Current number of bytes for the values stored.",
		})
		mLookups := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "Lookups",
			Help:      "Count of lookup requests executed.",
		})
		mLookupErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "LookupErrors",
			Help:      "Count of lookup requests executed resulting in errors.",
		})

		mLookupGroups := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "LookupGroups",
			Help:      "Count of lookup-group requests executed.",
		})
		mLookupGroupItems := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "LookupGroupItems",
			Help:      "Count of items lookup-group requests have returned.",
		})
		mLookupGroupErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "LookupGroupErrors",
			Help:      "Count of errors lookup-group requests have returned.",
		})

		mReads := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "Reads",
			Help:      "Count of read requests executed.",
		})
		mReadErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "ReadErrors",
			Help:      "Count of read requests executed resulting in errors.",
		})

		mReadGroups := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "ReadGroups",
			Help:      "Count of read-group requests executed.",
		})
		mReadGroupItems := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "ReadGroupItems",
			Help:      "Count of items read-group requests have returned.",
		})
		mReadGroupErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "ReadGroupErrors",
			Help:      "Count of errors read-group requests have returned.",
		})

		mWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "Writes",
			Help:      "Count of write requests executed.",
		})
		mWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "WriteErrors",
			Help:      "Count of write requests executed resulting in errors.",
		})
		mWritesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "WritesOverridden",
			Help:      "Count of write requests that were outdated or repeated.",
		})
		mDeletes := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "Deletes",
			Help:      "Count of delete requests executed.",
		})
		mDeleteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "DeleteErrors",
			Help:      "Count of delete requests executed resulting in errors.",
		})
		mDeletesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "DeletesOverridden",
			Help:      "Count of delete requests that were outdated or repeated.",
		})
		mOutBulkSets := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "OutBulkSets",
			Help:      "Count of outgoing bulk-set messages in response to incoming pull replication messages.",
		})
		mOutBulkSetValues := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "OutBulkSetValues",
			Help:      "Count of values in outgoing bulk-set messages; these bulk-set messages are those in response to incoming pull-replication messages.",
		})
		mOutBulkSetPushes := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "OutBulkSetPushes",
			Help:      "Count of outgoing bulk-set messages due to push replication.",
		})
		mOutBulkSetPushValues := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "OutBulkSetPushValues",
			Help:      "Count of values in outgoing bulk-set messages; these bulk-set messages are those due to push replication.",
		})
		mOutPushReplicationSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "GroupStore",
			Name:      "OutPushReplicationSeconds",
			Help:      "How long the last out push replication pass took.",
		})
		mInBulkSets := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InBulkSets",
			Help:      "Count of incoming bulk-set messages.",
		})
		mInBulkSetDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InBulkSetDrops",
			Help:      "Count of incoming bulk-set messages dropped due to the local system being overworked at the time.",
		})
		mInBulkSetInvalids := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InBulkSetInvalids",
			Help:      "Count of incoming bulk-set messages that couldn't be parsed.",
		})
		mInBulkSetWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InBulkSetWrites",
			Help:      "Count of writes due to incoming bulk-set messages.",
		})
		mInBulkSetWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InBulkSetWriteErrors",
			Help:      "Count of errors returned from writes due to incoming bulk-set messages.",
		})
		mInBulkSetWritesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InBulkSetWritesOverridden",
			Help:      "Count of writes from incoming bulk-set messages that result in no change.",
		})
		mOutBulkSetAcks := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "OutBulkSetAcks",
			Help:      "Count of outgoing bulk-set-ack messages.",
		})
		mInBulkSetAcks := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InBulkSetAcks",
			Help:      "Count of incoming bulk-set-ack messages.",
		})
		mInBulkSetAckDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InBulkSetAckDrops",
			Help:      "Count of incoming bulk-set-ack messages dropped due to the local system being overworked at the time.",
		})
		mInBulkSetAckInvalids := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InBulkSetAckInvalids",
			Help:      "Count of incoming bulk-set-ack messages that couldn't be parsed.",
		})
		mInBulkSetAckWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InBulkSetAckWrites",
			Help:      "Count of writes (for local removal) due to incoming bulk-set-ack messages.",
		})
		mInBulkSetAckWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InBulkSetAckWriteErrors",
			Help:      "Count of errors returned from writes due to incoming bulk-set-ack messages.",
		})
		mInBulkSetAckWritesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InBulkSetAckWritesOverridden",
			Help:      "Count of writes from incoming bulk-set-ack messages that result in no change.",
		})
		mOutPullReplications := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "OutPullReplications",
			Help:      "Count of outgoing pull-replication messages.",
		})
		mOutPullReplicationSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "GroupStore",
			Name:      "OutPullReplicationSeconds",
			Help:      "How long the last out pull replication pass took.",
		})
		mInPullReplications := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InPullReplications",
			Help:      "Count of incoming pull-replication messages.",
		})
		mInPullReplicationDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InPullReplicationDrops",
			Help:      "Count of incoming pull-replication messages droppped due to the local system being overworked at the time.",
		})
		mInPullReplicationInvalids := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "InPullReplicationInvalids",
			Help:      "Count of incoming pull-replication messages that couldn't be parsed.",
		})
		mExpiredDeletions := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "ExpiredDeletions",
			Help:      "Count of recent deletes that have become old enough to be completely discarded.",
		})
		mTombstoneDiscardSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "GroupStore",
			Name:      "TombstoneDiscardSeconds",
			Help:      "How long the last tombstone discard pass took.",
		})
		mCompactionSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "GroupStore",
			Name:      "CompactionSeconds",
			Help:      "How long the last compaction pass took.",
		})
		mCompactions := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "Compactions",
			Help:      "Count of disk file sets compacted due to their contents exceeding a staleness threshold. For example, this happens when enough of the values have been overwritten or deleted in more recent operations.",
		})
		mSmallFileCompactions := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "GroupStore",
			Name:      "SmallFileCompactions",
			Help:      "Count of disk file sets compacted due to the entire file size being too small. For example, this may happen when the store is shutdown and restarted.",
		})
		mReadOnly := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "GroupStore",
			Name:      "ReadOnly",
			Help:      "Indicates when the store has been put in read-only mode, whether by an operator or automatically by the watcher.",
		})
		mAuditSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "GroupStore",
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

		prometheus.Register(mLookupGroups)
		prometheus.Register(mLookupGroupItems)
		prometheus.Register(mLookupGroupErrors)

		prometheus.Register(mReads)
		prometheus.Register(mReadErrors)

		prometheus.Register(mReadGroups)
		prometheus.Register(mReadGroupItems)
		prometheus.Register(mReadGroupErrors)

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
		tcpMsgRingStats := s.groupStoreMsgRing.Stats(false)
		for {
			select {
			case <-s.shutdownChan:
				return
			case <-time.After(time.Minute):
				tcpMsgRingStats = s.groupStoreMsgRing.Stats(false)
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
				stats, err := s.groupStore.Stats(context.Background(), false)
				if err != nil {
					s.logger.Debug("stats error", zap.Error(err))
				} else if sstats, ok := stats.(*store.GroupStoreStats); ok {
					mValues.Set(float64(sstats.Values))
					mValueBytes.Set(float64(sstats.ValueBytes))
					mLookups.Add(float64(sstats.Lookups))
					mLookupErrors.Add(float64(sstats.LookupErrors))

					mLookupGroups.Add(float64(sstats.LookupGroups))
					mLookupGroupItems.Add(float64(sstats.LookupGroupItems))
					mLookupGroupErrors.Add(float64(sstats.LookupGroupErrors))

					mReads.Add(float64(sstats.Reads))
					mReadErrors.Add(float64(sstats.ReadErrors))

					mReadGroups.Add(float64(sstats.ReadGroups))
					mReadGroupItems.Add(float64(sstats.ReadGroupItems))
					mReadGroupErrors.Add(float64(sstats.ReadGroupErrors))

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
		s.groupStoreMsgRing.Listen()
		s.waitGroup.Done()
	}()
	s.waitGroup.Add(1)
	go func() {
		<-s.shutdownChan
		s.groupStoreMsgRing.Shutdown()
		s.waitGroup.Done()
	}()
	ln := s.groupStoreMsgRing.Ring().LocalNode()
	if ln == nil {
		return errors.New("no local node set")
	}
	grpcAddr := ln.Address(s.grpcAddressIndex)
	if grpcAddr == "" {
		return fmt.Errorf("no local node address index %d", s.grpcAddressIndex)
	}
	grpcHostPort, err := ring.CanonicalHostPort(grpcAddr, 12310)
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", grpcHostPort)
	if err != nil {
		return err
	}
	tlsCfg, err := ftls.NewServerTLSConfig(&ftls.Config{
		MutualTLS:          true,
		InsecureSkipVerify: false,
		CertFile:           s.grpcCertFile,
		KeyFile:            s.grpcKeyFile,
		CAFile:             s.caFile,
	})
	if err != nil {
		return err
	}
	s.grpcServer = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	groupproto.RegisterGroupStoreServer(s.grpcServer, s)

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

func (s *GroupStore) Shutdown(ctx context.Context) error {
	s.Lock()
	defer s.Unlock()
	if !s.started {
		return nil
	}
	close(s.shutdownChan)
	s.waitGroup.Wait()
	return s.groupStore.Shutdown(ctx)
}

func (s *GroupStore) Write(ctx context.Context, req *groupproto.WriteRequest) (*groupproto.WriteResponse, error) {
	resp := groupproto.WriteResponse{Rpcid: req.Rpcid}
	var err error
	resp.TimestampMicro, err = s.groupStore.Write(ctx, req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, req.TimestampMicro, req.Value)
	if err != nil {
		resp.Err = proto.TranslateError(err)
	}
	return &resp, nil
}

func (s *GroupStore) StreamWrite(stream groupproto.GroupStore_StreamWriteServer) error {
	var resp groupproto.WriteResponse
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
		resp.TimestampMicro, err = s.groupStore.Write(stream.Context(), req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, req.TimestampMicro, req.Value)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *GroupStore) Read(ctx context.Context, req *groupproto.ReadRequest) (*groupproto.ReadResponse, error) {
	resp := groupproto.ReadResponse{Rpcid: req.Rpcid}
	var err error
	resp.TimestampMicro, resp.Value, err = s.groupStore.Read(ctx, req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, resp.Value)
	if err != nil {
		resp.Err = proto.TranslateError(err)
	}
	return &resp, nil
}

func (s *GroupStore) StreamRead(stream groupproto.GroupStore_StreamReadServer) error {
	var resp groupproto.ReadResponse
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
		resp.TimestampMicro, resp.Value, err = s.groupStore.Read(stream.Context(), req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, resp.Value)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *GroupStore) Lookup(ctx context.Context, req *groupproto.LookupRequest) (*groupproto.LookupResponse, error) {
	resp := groupproto.LookupResponse{Rpcid: req.Rpcid}
	var err error
	resp.TimestampMicro, resp.Length, err = s.groupStore.Lookup(ctx, req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB)
	if err != nil {
		resp.Err = proto.TranslateError(err)
	}
	return &resp, nil
}

func (s *GroupStore) StreamLookup(stream groupproto.GroupStore_StreamLookupServer) error {
	var resp groupproto.LookupResponse
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
		resp.TimestampMicro, resp.Length, err = s.groupStore.Lookup(stream.Context(), req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *GroupStore) LookupGroup(ctx context.Context, req *groupproto.LookupGroupRequest) (*groupproto.LookupGroupResponse, error) {
	resp := &groupproto.LookupGroupResponse{Rpcid: req.Rpcid}
	items, err := s.groupStore.LookupGroup(ctx, req.KeyA, req.KeyB)
	if err != nil {
		resp.Err = proto.TranslateError(err)
	} else {
		for _, v := range items {
			g := groupproto.LookupGroupItem{}
			g.Length = v.Length
			g.ChildKeyA = v.ChildKeyA
			g.ChildKeyB = v.ChildKeyB
			g.TimestampMicro = v.TimestampMicro
			resp.Items = append(resp.Items, &g)
		}
	}
	return resp, nil
}

func (s *GroupStore) StreamLookupGroup(stream groupproto.GroupStore_StreamLookupGroupServer) error {
	var resp groupproto.LookupGroupResponse
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
		items, err := s.groupStore.LookupGroup(stream.Context(), req.KeyA, req.KeyB)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		} else {
			for _, v := range items {
				g := groupproto.LookupGroupItem{}
				g.Length = v.Length
				g.ChildKeyA = v.ChildKeyA
				g.ChildKeyB = v.ChildKeyB
				g.TimestampMicro = v.TimestampMicro
				resp.Items = append(resp.Items, &g)
			}
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *GroupStore) ReadGroup(ctx context.Context, req *groupproto.ReadGroupRequest) (*groupproto.ReadGroupResponse, error) {
	resp := groupproto.ReadGroupResponse{Rpcid: req.Rpcid}
	lgis, err := s.groupStore.LookupGroup(ctx, req.KeyA, req.KeyB)
	if err != nil {
		resp.Err = proto.TranslateError(err)
	} else {
		resp.Items = make([]*groupproto.ReadGroupItem, len(lgis))
		itemCount := 0
		var err error
		for i, lgi := range lgis {
			g := &groupproto.ReadGroupItem{}
			g.TimestampMicro, g.Value, err = s.groupStore.Read(ctx, req.KeyA, req.KeyB, lgi.ChildKeyA, lgi.ChildKeyB, nil)
			if err != nil {
				continue
			}
			g.ChildKeyA = lgi.ChildKeyA
			g.ChildKeyB = lgi.ChildKeyB
			resp.Items[i] = g
			itemCount++
		}
		resp.Items = resp.Items[:itemCount]
	}
	return &resp, nil
}

func (s *GroupStore) StreamReadGroup(stream groupproto.GroupStore_StreamReadGroupServer) error {
	var resp groupproto.ReadGroupResponse
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
		lgis, err := s.groupStore.LookupGroup(stream.Context(), req.KeyA, req.KeyB)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		} else {
			resp.Items = make([]*groupproto.ReadGroupItem, len(lgis))
			itemCount := 0
			for i, lgi := range lgis {
				g := groupproto.ReadGroupItem{}
				g.TimestampMicro, g.Value, err = s.groupStore.Read(stream.Context(), req.KeyA, req.KeyB, lgi.ChildKeyA, lgi.ChildKeyB, nil)
				if err != nil {
					continue
				}
				g.ChildKeyA = lgi.ChildKeyA
				g.ChildKeyB = lgi.ChildKeyB
				resp.Items[i] = &g
				itemCount++
			}
			resp.Items = resp.Items[:itemCount]
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *GroupStore) Delete(ctx context.Context, req *groupproto.DeleteRequest) (*groupproto.DeleteResponse, error) {
	resp := groupproto.DeleteResponse{Rpcid: req.Rpcid}
	var err error
	resp.TimestampMicro, err = s.groupStore.Delete(ctx, req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, req.TimestampMicro)
	if err != nil {
		resp.Err = proto.TranslateError(err)
	}
	return &resp, nil
}

func (s *GroupStore) StreamDelete(stream groupproto.GroupStore_StreamDeleteServer) error {
	var resp groupproto.DeleteResponse
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
		resp.TimestampMicro, err = s.groupStore.Delete(stream.Context(), req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, req.TimestampMicro)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *GroupStore) Stats() []byte {
	stats, err := s.groupStore.Stats(context.Background(), true)
	if err != nil {
		return nil
	}
	return []byte(stats.String())
}

// Wait isn't implemented yet, need graceful shutdowns in grpc
func (s *GroupStore) Wait() {}
