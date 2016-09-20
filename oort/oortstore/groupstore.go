package oortstore

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/getcfs/megacfs/ftls"
	"github.com/getcfs/megacfs/oort/api/groupproto"
	"github.com/getcfs/megacfs/oort/api/proto"
	"github.com/getcfs/megacfs/oort/oort"
	"github.com/gholt/ring"
	"github.com/gholt/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/uber-go/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type OortGroupStore struct {
	sync.RWMutex
	waitGroup        *sync.WaitGroup
	gs               store.GroupStore
	grpc             *grpc.Server
	grpcStopping     bool
	msgRing          *ring.TCPMsgRing
	oort             *oort.Server
	Config           *OortGroupConfig `toml:"OortGroupStoreConfig"` // load config using an explicit/different config header
	stopped          bool
	GroupStoreConfig store.GroupStoreConfig
	TCPMsgRingConfig ring.TCPMsgRingConfig
	serverTLSConfig  *tls.Config
	// Original logger given to NewValueStore
	logger zap.Logger
	// Value of "name" field used by this struct and things it creates
	loggerName string
	// Logger instance used directly by this struct; has "name" set already
	localLogger zap.Logger
}

type OortGroupConfig struct {
	Debug              bool
	Profile            bool
	ListenAddr         string `toml:"ListenAddress"` //another example
	MutualTLS          bool
	InsecureSkipVerify bool
	CertFile           string
	KeyFile            string
	CAFile             string
	MetricsAddr        string
	MetricsCollectors  string
}

func NewGroupStore(oort *oort.Server, logger zap.Logger, loggerName string) (*OortGroupStore, error) {
	s := &OortGroupStore{}
	s.Config = &OortGroupConfig{}
	s.waitGroup = &sync.WaitGroup{}
	s.oort = oort
	s.logger = logger
	s.loggerName = loggerName
	if s.loggerName == "" {
		s.loggerName = "groupstore"
	} else {
		s.loggerName += ".groupstore"
	}
	s.localLogger = s.logger.With(zap.String("name", s.loggerName))
	err := s.oort.LoadRingConfig(s)
	if err != nil {
		return s, err
	}
	if s.Config.Debug {
		// Sets for localLogger too
		s.logger.SetLevel(zap.DebugLevel)
	}
	if s.TCPMsgRingConfig.AddressIndex == 0 {
		s.TCPMsgRingConfig.AddressIndex = 1
	}
	if s.Config.MutualTLS && s.Config.InsecureSkipVerify {
		return s, fmt.Errorf("Option MutualTLS=true, and InsecureSkipVerify=true conflict")
	}
	s.serverTLSConfig, err = ftls.NewServerTLSConfig(&ftls.Config{
		MutualTLS:          s.Config.MutualTLS,
		InsecureSkipVerify: s.Config.InsecureSkipVerify,
		CertFile:           s.Config.CertFile,
		KeyFile:            s.Config.KeyFile,
		CAFile:             s.Config.CAFile,
	})
	if err != nil {
		return s, err
	}
	s.start()
	s.stopped = false
	return s, nil
}

func (s *OortGroupStore) start() {
	s.gs = nil
	runtime.GC()
	var err error
	s.msgRing, err = ring.NewTCPMsgRing(&s.TCPMsgRingConfig)
	if err != nil {
		panic(err)
	}
	s.GroupStoreConfig.MsgRing = s.msgRing
	s.msgRing.SetRing(s.oort.Ring())
	s.GroupStoreConfig.Logger = s.logger
	s.GroupStoreConfig.LoggerName = s.loggerName
	var restartChan chan error
	s.gs, restartChan = store.NewGroupStore(&s.GroupStoreConfig)
	// TODO: I'm guessing we'll want to do something more graceful here; but
	// this will work for now since Systemd (or another service manager) should
	// restart the service.
	go func(restartChan chan error) {
		if err := <-restartChan; err != nil {
			panic(err)
		}
	}(restartChan)
	if err := s.gs.Startup(context.Background()); err != nil {
		panic(err)
	}
	go func(t *ring.TCPMsgRing) {
		t.Listen()
	}(s.msgRing)
	go func(t *ring.TCPMsgRing) {
		mRingChanges := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "RingChanges",
			Help:      "Number of received ring changes.",
		})
		mRingChangeCloses := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "RingChangeCloses",
			Help:      "Number of connections closed due to ring changes.",
		})
		mMsgToNodes := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgToNodes",
			Help:      "Number of times MsgToNode function has been called; single message to single node.",
		})
		mMsgToNodeNoRings := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgToNodeNoRings",
			Help:      "Number of times MsgToNode function has been called with no ring yet available.",
		})
		mMsgToNodeNoNodes := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgToNodeNoNodes",
			Help:      "Number of times MsgToNode function has been called with no matching node.",
		})
		mMsgToOtherReplicas := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgToOtherReplicas",
			Help:      "Number of times MsgToOtherReplicas function has been called; single message to all replicas, excluding the local replica if responsible.",
		})
		mMsgToOtherReplicasNoRings := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgToOtherReplicasNoRings",
			Help:      "Number of times MsgToOtherReplicas function has been called with no ring yet available.",
		})
		mListenErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "ListenErrors",
			Help:      "Number of errors trying to establish a TCP listener.",
		})
		mIncomingConnections := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "IncomingConnections",
			Help:      "Number of incoming TCP connections made.",
		})
		mDials := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "Dials",
			Help:      "Number of attempts to establish outgoing TCP connections.",
		})
		mDialErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "DialErrors",
			Help:      "Number of errors trying to establish outgoing TCP connections.",
		})
		mOutgoingConnections := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "OutgoingConnections",
			Help:      "Number of outgoing TCP connections established.",
		})
		mMsgChanCreations := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgChanCreations",
			Help:      "Number of internal message channels created.",
		})
		mMsgToAddrs := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgToAddrs",
			Help:      "Number times internal function msgToAddr has been called.",
		})
		mMsgToAddrQueues := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgToAddrQueues",
			Help:      "Number of messages msgToAddr successfully queued.",
		})
		mMsgToAddrTimeoutDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgToAddrTimeoutDrops",
			Help:      "Number of messages msgToAddr dropped after timeout.",
		})
		mMsgToAddrShutdownDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgToAddrShutdownDrops",
			Help:      "Number of messages msgToAddr dropped due to a shutdown.",
		})
		mMsgReads := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgReads",
			Help:      "Number of incoming messages read.",
		})
		mMsgReadErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgReadErrors",
			Help:      "Number of errors reading incoming messages.",
		})
		mMsgWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgWrites",
			Help:      "Number of outgoing messages written.",
		})
		mMsgWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "TCPMsgRing",
			Name:      "MsgWriteErrors",
			Help:      "Number of errors writing outgoing messages.",
		})
		mValues := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "Store",
			Name:      "Values",
			Help:      "Current number of values stored.",
		})
		mValueBytes := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "Store",
			Name:      "ValueBytes",
			Help:      "Current number of bytes for the values stored.",
		})
		mLookups := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "Lookups",
			Help:      "Count of lookup requests executed.",
		})
		mLookupErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "LookupErrors",
			Help:      "Count of lookup requests executed resulting in errors.",
		})
		mLookupGroups := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "LookupGroups",
			Help:      "Count of lookup-group requests executed.",
		})
		mLookupGroupItems := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "LookupGroupItems",
			Help:      "Count of items lookup-group requests have returned.",
		})
		mLookupGroupErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "LookupGroupErrors",
			Help:      "Count of errors lookup-group requests have returned.",
		})
		mReads := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "Reads",
			Help:      "Count of read requests executed.",
		})
		mReadErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "ReadErrors",
			Help:      "Count of read requests executed resulting in errors.",
		})
		mReadGroups := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "ReadGroups",
			Help:      "Count of read-group requests executed.",
		})
		mReadGroupItems := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "ReadGroupItems",
			Help:      "Count of items read-group requests have returned.",
		})
		mReadGroupErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "ReadGroupErrors",
			Help:      "Count of errors read-group requests have returned.",
		})
		mWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "Writes",
			Help:      "Count of write requests executed.",
		})
		mWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "WriteErrors",
			Help:      "Count of write requests executed resulting in errors.",
		})
		mWritesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "WritesOverridden",
			Help:      "Count of write requests that were outdated or repeated.",
		})
		mDeletes := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "Deletes",
			Help:      "Count of delete requests executed.",
		})
		mDeleteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "DeleteErrors",
			Help:      "Count of delete requests executed resulting in errors.",
		})
		mDeletesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "DeletesOverridden",
			Help:      "Count of delete requests that were outdated or repeated.",
		})
		mOutBulkSets := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "OutBulkSets",
			Help:      "Count of outgoing bulk-set messages in response to incoming pull replication messages.",
		})
		mOutBulkSetValues := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "OutBulkSetValues",
			Help:      "Count of values in outgoing bulk-set messages; these bulk-set messages are those in response to incoming pull-replication messages.",
		})
		mOutBulkSetPushes := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "OutBulkSetPushes",
			Help:      "Count of outgoing bulk-set messages due to push replication.",
		})
		mOutBulkSetPushValues := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "OutBulkSetPushValues",
			Help:      "Count of values in outgoing bulk-set messages; these bulk-set messages are those due to push replication.",
		})
		mOutPushReplicationSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "Store",
			Name:      "OutPushReplicationSeconds",
			Help:      "How long the last out push replication pass took.",
		})
		mInBulkSets := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InBulkSets",
			Help:      "Count of incoming bulk-set messages.",
		})
		mInBulkSetDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InBulkSetDrops",
			Help:      "Count of incoming bulk-set messages dropped due to the local system being overworked at the time.",
		})
		mInBulkSetInvalids := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InBulkSetInvalids",
			Help:      "Count of incoming bulk-set messages that couldn't be parsed.",
		})
		mInBulkSetWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InBulkSetWrites",
			Help:      "Count of writes due to incoming bulk-set messages.",
		})
		mInBulkSetWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InBulkSetWriteErrors",
			Help:      "Count of errors returned from writes due to incoming bulk-set messages.",
		})
		mInBulkSetWritesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InBulkSetWritesOverridden",
			Help:      "Count of writes from incoming bulk-set messages that result in no change.",
		})
		mOutBulkSetAcks := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "OutBulkSetAcks",
			Help:      "Count of outgoing bulk-set-ack messages.",
		})
		mInBulkSetAcks := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InBulkSetAcks",
			Help:      "Count of incoming bulk-set-ack messages.",
		})
		mInBulkSetAckDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InBulkSetAckDrops",
			Help:      "Count of incoming bulk-set-ack messages dropped due to the local system being overworked at the time.",
		})
		mInBulkSetAckInvalids := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InBulkSetAckInvalids",
			Help:      "Count of incoming bulk-set-ack messages that couldn't be parsed.",
		})
		mInBulkSetAckWrites := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InBulkSetAckWrites",
			Help:      "Count of writes (for local removal) due to incoming bulk-set-ack messages.",
		})
		mInBulkSetAckWriteErrors := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InBulkSetAckWriteErrors",
			Help:      "Count of errors returned from writes due to incoming bulk-set-ack messages.",
		})
		mInBulkSetAckWritesOverridden := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InBulkSetAckWritesOverridden",
			Help:      "Count of writes from incoming bulk-set-ack messages that result in no change.",
		})
		mOutPullReplications := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "OutPullReplications",
			Help:      "Count of outgoing pull-replication messages.",
		})
		mOutPullReplicationSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "Store",
			Name:      "OutPullReplicationSeconds",
			Help:      "How long the last out pull replication pass took.",
		})
		mInPullReplications := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InPullReplications",
			Help:      "Count of incoming pull-replication messages.",
		})
		mInPullReplicationDrops := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InPullReplicationDrops",
			Help:      "Count of incoming pull-replication messages droppped due to the local system being overworked at the time.",
		})
		mInPullReplicationInvalids := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "InPullReplicationInvalids",
			Help:      "Count of incoming pull-replication messages that couldn't be parsed.",
		})
		mExpiredDeletions := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "ExpiredDeletions",
			Help:      "Count of recent deletes that have become old enough to be completely discarded.",
		})
		mCompactions := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "Compactions",
			Help:      "Count of disk file sets compacted due to their contents exceeding a staleness threshold. For example, this happens when enough of the values have been overwritten or deleted in more recent operations.",
		})
		mSmallFileCompactions := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Store",
			Name:      "SmallFileCompactions",
			Help:      "Count of disk file sets compacted due to the entire file size being too small. For example, this may happen when the store is shutdown and restarted.",
		})
		mReadOnly := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "Store",
			Name:      "ReadOnly",
			Help:      "Indicates when the store has been put in read-only mode, whether by an operator or automatically by the watcher.",
		})
		mCompactionSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "Store",
			Name:      "CompactionSeconds",
			Help:      "How long the last compaction pass took.",
		})
		mTombstoneDiscardSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "Store",
			Name:      "TombstoneDiscardSeconds",
			Help:      "How long the last tombstone discard pass took.",
		})
		mAuditSeconds := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "Store",
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
		prometheus.Register(mCompactions)
		prometheus.Register(mSmallFileCompactions)
		prometheus.Register(mCompactionSeconds)
		prometheus.Register(mTombstoneDiscardSeconds)
		prometheus.Register(mAuditSeconds)
		prometheus.Register(mReadOnly)
		tcpMsgRingStats := t.Stats(false)
		for !tcpMsgRingStats.Shutdown {
			time.Sleep(time.Minute)
			tcpMsgRingStats = t.Stats(false)
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
			stats, err := s.gs.Stats(context.Background(), false)
			if err != nil {
				s.localLogger.Error("stats error", zap.Error(err))
			} else if gstats, ok := stats.(*store.GroupStoreStats); ok {
				mValues.Set(float64(gstats.Values))
				mValueBytes.Set(float64(gstats.ValueBytes))
				mLookups.Add(float64(gstats.Lookups))
				mLookupErrors.Add(float64(gstats.LookupErrors))
				mLookupGroups.Add(float64(gstats.LookupGroups))
				mLookupGroupItems.Add(float64(gstats.LookupGroupItems))
				mLookupGroupErrors.Add(float64(gstats.LookupGroupErrors))
				mReads.Add(float64(gstats.Reads))
				mReadErrors.Add(float64(gstats.ReadErrors))
				mReadGroups.Add(float64(gstats.ReadGroups))
				mReadGroupItems.Add(float64(gstats.ReadGroupItems))
				mReadGroupErrors.Add(float64(gstats.ReadGroupErrors))
				mWrites.Add(float64(gstats.Writes))
				mWriteErrors.Add(float64(gstats.WriteErrors))
				mWritesOverridden.Add(float64(gstats.WritesOverridden))
				mDeletes.Add(float64(gstats.Deletes))
				mDeleteErrors.Add(float64(gstats.DeleteErrors))
				mDeletesOverridden.Add(float64(gstats.DeletesOverridden))
				mOutBulkSets.Add(float64(gstats.OutBulkSets))
				mOutBulkSetValues.Add(float64(gstats.OutBulkSetValues))
				mOutBulkSetPushes.Add(float64(gstats.OutBulkSetPushes))
				mOutBulkSetPushValues.Add(float64(gstats.OutBulkSetPushValues))
				mOutPushReplicationSeconds.Set(float64(gstats.OutPushReplicationNanoseconds) / 1000000000)
				mInBulkSets.Add(float64(gstats.InBulkSets))
				mInBulkSetDrops.Add(float64(gstats.InBulkSetDrops))
				mInBulkSetInvalids.Add(float64(gstats.InBulkSetInvalids))
				mInBulkSetWrites.Add(float64(gstats.InBulkSetWrites))
				mInBulkSetWriteErrors.Add(float64(gstats.InBulkSetWriteErrors))
				mInBulkSetWritesOverridden.Add(float64(gstats.InBulkSetWritesOverridden))
				mOutBulkSetAcks.Add(float64(gstats.OutBulkSetAcks))
				mInBulkSetAcks.Add(float64(gstats.InBulkSetAcks))
				mInBulkSetAckDrops.Add(float64(gstats.InBulkSetAckDrops))
				mInBulkSetAckInvalids.Add(float64(gstats.InBulkSetAckInvalids))
				mInBulkSetAckWrites.Add(float64(gstats.InBulkSetAckWrites))
				mInBulkSetAckWriteErrors.Add(float64(gstats.InBulkSetAckWriteErrors))
				mInBulkSetAckWritesOverridden.Add(float64(gstats.InBulkSetAckWritesOverridden))
				mOutPullReplications.Add(float64(gstats.OutPullReplications))
				mOutPullReplicationSeconds.Set(float64(gstats.OutPullReplicationNanoseconds) / 1000000000)
				mInPullReplications.Add(float64(gstats.InPullReplications))
				mInPullReplicationDrops.Add(float64(gstats.InPullReplicationDrops))
				mInPullReplicationInvalids.Add(float64(gstats.InPullReplicationInvalids))
				mExpiredDeletions.Add(float64(gstats.ExpiredDeletions))
				mCompactions.Add(float64(gstats.Compactions))
				mSmallFileCompactions.Add(float64(gstats.SmallFileCompactions))
				mCompactionSeconds.Set(float64(gstats.CompactionNanoseconds) / 1000000000)
				mTombstoneDiscardSeconds.Set(float64(gstats.TombstoneDiscardNanoseconds) / 1000000000)
				mAuditSeconds.Set(float64(gstats.AuditNanoseconds) / 1000000000)
				if gstats.ReadOnly {
					mReadOnly.Set(1)
				} else {
					mReadOnly.Set(0)
				}
			} else {
				s.localLogger.Error("unknown stats type", zap.Stringer("stats", stats))
			}
		}
		prometheus.Unregister(mRingChanges)
		prometheus.Unregister(mRingChangeCloses)
		prometheus.Unregister(mMsgToNodes)
		prometheus.Unregister(mMsgToNodeNoRings)
		prometheus.Unregister(mMsgToNodeNoNodes)
		prometheus.Unregister(mMsgToOtherReplicas)
		prometheus.Unregister(mMsgToOtherReplicasNoRings)
		prometheus.Unregister(mListenErrors)
		prometheus.Unregister(mIncomingConnections)
		prometheus.Unregister(mDials)
		prometheus.Unregister(mDialErrors)
		prometheus.Unregister(mOutgoingConnections)
		prometheus.Unregister(mMsgChanCreations)
		prometheus.Unregister(mMsgToAddrs)
		prometheus.Unregister(mMsgToAddrQueues)
		prometheus.Unregister(mMsgToAddrTimeoutDrops)
		prometheus.Unregister(mMsgToAddrShutdownDrops)
		prometheus.Unregister(mMsgReads)
		prometheus.Unregister(mMsgReadErrors)
		prometheus.Unregister(mMsgWrites)
		prometheus.Unregister(mMsgWriteErrors)
		prometheus.Unregister(mValues)
		prometheus.Unregister(mValueBytes)
		prometheus.Unregister(mLookups)
		prometheus.Unregister(mLookupErrors)
		prometheus.Unregister(mLookupGroups)
		prometheus.Unregister(mLookupGroupItems)
		prometheus.Unregister(mLookupGroupErrors)
		prometheus.Unregister(mReads)
		prometheus.Unregister(mReadErrors)
		prometheus.Unregister(mReadGroups)
		prometheus.Unregister(mReadGroupItems)
		prometheus.Unregister(mReadGroupErrors)
		prometheus.Unregister(mWrites)
		prometheus.Unregister(mWriteErrors)
		prometheus.Unregister(mWritesOverridden)
		prometheus.Unregister(mDeletes)
		prometheus.Unregister(mDeleteErrors)
		prometheus.Unregister(mDeletesOverridden)
		prometheus.Unregister(mOutBulkSets)
		prometheus.Unregister(mOutBulkSetValues)
		prometheus.Unregister(mOutBulkSetPushes)
		prometheus.Unregister(mOutBulkSetPushValues)
		prometheus.Unregister(mInBulkSets)
		prometheus.Unregister(mInBulkSetDrops)
		prometheus.Unregister(mInBulkSetInvalids)
		prometheus.Unregister(mInBulkSetWrites)
		prometheus.Unregister(mInBulkSetWriteErrors)
		prometheus.Unregister(mInBulkSetWritesOverridden)
		prometheus.Unregister(mOutBulkSetAcks)
		prometheus.Unregister(mInBulkSetAcks)
		prometheus.Unregister(mInBulkSetAckDrops)
		prometheus.Unregister(mInBulkSetAckInvalids)
		prometheus.Unregister(mInBulkSetAckWrites)
		prometheus.Unregister(mInBulkSetAckWriteErrors)
		prometheus.Unregister(mInBulkSetAckWritesOverridden)
		prometheus.Unregister(mOutPullReplications)
		prometheus.Unregister(mOutPullReplicationSeconds)
		prometheus.Unregister(mInPullReplications)
		prometheus.Unregister(mInPullReplicationDrops)
		prometheus.Unregister(mInPullReplicationInvalids)
		prometheus.Unregister(mExpiredDeletions)
		prometheus.Unregister(mCompactions)
		prometheus.Unregister(mSmallFileCompactions)
		prometheus.Unregister(mReadOnly)
	}(s.msgRing)
}

func (s *OortGroupStore) UpdateRing(ring ring.Ring) {
	s.Lock()
	s.msgRing.SetRing(ring)
	s.Unlock()
}

func (s *OortGroupStore) Write(ctx context.Context, req *groupproto.WriteRequest) (*groupproto.WriteResponse, error) {
	resp := groupproto.WriteResponse{Rpcid: req.Rpcid}
	var err error
	resp.TimestampMicro, err = s.gs.Write(ctx, req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, req.TimestampMicro, req.Value)
	if err != nil {
		resp.Err = proto.TranslateError(err)
	}
	return &resp, nil
}

func (s *OortGroupStore) StreamWrite(stream groupproto.GroupStore_StreamWriteServer) error {
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
		resp.TimestampMicro, err = s.gs.Write(stream.Context(), req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, req.TimestampMicro, req.Value)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortGroupStore) Read(ctx context.Context, req *groupproto.ReadRequest) (*groupproto.ReadResponse, error) {
	resp := groupproto.ReadResponse{Rpcid: req.Rpcid}
	var err error
	resp.TimestampMicro, resp.Value, err = s.gs.Read(ctx, req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, resp.Value)
	if err != nil {
		resp.Err = proto.TranslateError(err)
	}
	return &resp, nil
}

func (s *OortGroupStore) StreamRead(stream groupproto.GroupStore_StreamReadServer) error {
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
		resp.TimestampMicro, resp.Value, err = s.gs.Read(stream.Context(), req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, resp.Value)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortGroupStore) Lookup(ctx context.Context, req *groupproto.LookupRequest) (*groupproto.LookupResponse, error) {
	resp := groupproto.LookupResponse{Rpcid: req.Rpcid}
	var err error
	resp.TimestampMicro, resp.Length, err = s.gs.Lookup(ctx, req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB)
	if err != nil {
		resp.Err = proto.TranslateError(err)
	}
	return &resp, nil
}

func (s *OortGroupStore) StreamLookup(stream groupproto.GroupStore_StreamLookupServer) error {
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
		resp.TimestampMicro, resp.Length, err = s.gs.Lookup(stream.Context(), req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortGroupStore) LookupGroup(ctx context.Context, req *groupproto.LookupGroupRequest) (*groupproto.LookupGroupResponse, error) {
	resp := &groupproto.LookupGroupResponse{Rpcid: req.Rpcid}
	items, err := s.gs.LookupGroup(ctx, req.KeyA, req.KeyB)
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

func (s *OortGroupStore) StreamLookupGroup(stream groupproto.GroupStore_StreamLookupGroupServer) error {
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
		items, err := s.gs.LookupGroup(stream.Context(), req.KeyA, req.KeyB)
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

func (s *OortGroupStore) ReadGroup(ctx context.Context, req *groupproto.ReadGroupRequest) (*groupproto.ReadGroupResponse, error) {
	resp := groupproto.ReadGroupResponse{Rpcid: req.Rpcid}
	lgis, err := s.gs.LookupGroup(ctx, req.KeyA, req.KeyB)
	if err != nil {
		resp.Err = proto.TranslateError(err)
	} else {
		resp.Items = make([]*groupproto.ReadGroupItem, len(lgis))
		itemCount := 0
		var err error
		for i, lgi := range lgis {
			g := &groupproto.ReadGroupItem{}
			g.TimestampMicro, g.Value, err = s.gs.Read(ctx, req.KeyA, req.KeyB, lgi.ChildKeyA, lgi.ChildKeyB, nil)
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

func (s *OortGroupStore) StreamReadGroup(stream groupproto.GroupStore_StreamReadGroupServer) error {
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
		lgis, err := s.gs.LookupGroup(stream.Context(), req.KeyA, req.KeyB)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		} else {
			resp.Items = make([]*groupproto.ReadGroupItem, len(lgis))
			itemCount := 0
			for i, lgi := range lgis {
				g := groupproto.ReadGroupItem{}
				g.TimestampMicro, g.Value, err = s.gs.Read(stream.Context(), req.KeyA, req.KeyB, lgi.ChildKeyA, lgi.ChildKeyB, nil)
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

func (s *OortGroupStore) Delete(ctx context.Context, req *groupproto.DeleteRequest) (*groupproto.DeleteResponse, error) {
	resp := groupproto.DeleteResponse{Rpcid: req.Rpcid}
	var err error
	resp.TimestampMicro, err = s.gs.Delete(ctx, req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, req.TimestampMicro)
	if err != nil {
		resp.Err = proto.TranslateError(err)
	}
	return &resp, nil
}

func (s *OortGroupStore) StreamDelete(stream groupproto.GroupStore_StreamDeleteServer) error {
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
		resp.TimestampMicro, err = s.gs.Delete(stream.Context(), req.KeyA, req.KeyB, req.ChildKeyA, req.ChildKeyB, req.TimestampMicro)
		if err != nil {
			resp.Err = proto.TranslateError(err)
		}
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (s *OortGroupStore) Start() {
	s.Lock()
	if !s.stopped {
		s.Unlock()
		return
	}
	s.start()
	s.stopped = false
	s.Unlock()
}

func (s *OortGroupStore) Stop() {
	s.Lock()
	if s.stopped {
		s.Unlock()
		return
	}
	s.gs.Shutdown(context.Background())
	s.msgRing.Shutdown()
	s.stopped = true
	s.Unlock()
}

func (s *OortGroupStore) Stats() []byte {
	stats, err := s.gs.Stats(context.Background(), true)
	if err != nil {
		return nil
	}
	return []byte(stats.String())
}

func (s *OortGroupStore) ListenAndServe() {
	go func(s *OortGroupStore) {
		s.grpcStopping = false
		for {
			var err error
			listenAddr := s.oort.GetListenAddr()
			if listenAddr == "" {
				s.localLogger.Fatal("No listen address specified in ring at address2")
			}
			l, err := net.Listen("tcp", listenAddr)
			if err != nil {
				s.localLogger.Fatal("Unable to bind to address:", zap.Error(err))
			}
			var opts []grpc.ServerOption
			creds := credentials.NewTLS(s.serverTLSConfig)
			opts = []grpc.ServerOption{grpc.Creds(creds)}
			s.grpc = grpc.NewServer(opts...)
			groupproto.RegisterGroupStoreServer(s.grpc, s)
			err = s.grpc.Serve(l)
			if err != nil && !s.grpcStopping {
				s.localLogger.Error("GroupStore Serve encountered error will attempt to restart", zap.Error(err))
			} else if err != nil && s.grpcStopping {
				s.localLogger.Warn("GroupStore got error but halt is in progress", zap.Error(err))
				l.Close()
				break
			} else {
				l.Close()
				break
			}
		}
	}(s)
}

func (s *OortGroupStore) StopListenAndServe() {
	s.grpcStopping = true
	s.grpc.Stop()
}

// Wait isn't implemented yet, need graceful shutdowns in grpc
func (s *OortGroupStore) Wait() {}
