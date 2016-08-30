package store

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gholt/brimtext"
	"golang.org/x/net/context"
)

type ValueStoreStats struct {
	// Values is the number of values in the ValueStore.
	Values uint64
	// ValuesBytes is the number of bytes of the values in the ValueStore.
	ValueBytes uint64
	// Lookups is the number of calls to Lookup.
	Lookups int32
	// LookupErrors is the number of errors returned by Lookup; not-found
	// errors do not count here.
	LookupErrors int32

	// Reads is the number of calls to Read.
	Reads int32
	// ReadErrors is the number of errors returned by Read; not-found errors do
	// not count here.
	ReadErrors int32

	// Writes is the number of calls to Write.
	Writes int32
	// WriteErrors is the number of errors returned by Write.
	WriteErrors int32
	// WritesOverridden is the number of calls to Write that resulted in no
	// change.
	WritesOverridden int32
	// Deletes is the number of calls to Delete.
	Deletes int32
	// DeleteErrors is the number of errors returned by Delete.
	DeleteErrors int32
	// DeletesOverridden is the number of calls to Delete that resulted in no
	// change.
	DeletesOverridden int32
	// OutBulkSets is the number of outgoing bulk-set messages in response to
	// incoming pull replication messages.
	OutBulkSets int32
	// OutBulkSetValues is the number of values in outgoing bulk-set messages;
	// these bulk-set messages are those in response to incoming
	// pull-replication messages.
	OutBulkSetValues int32
	// OutBulkSetPushes is the number of outgoing bulk-set messages due to push
	// replication.
	OutBulkSetPushes int32
	// OutBulkSetPushValues is the number of values in outgoing bulk-set
	// messages; these bulk-set messages are those due to push replication.
	OutBulkSetPushValues int32
	// OutPushReplicationNanoseconds is how long the last out push replication
	// pass took.
	OutPushReplicationNanoseconds int64
	// InBulkSets is the number of incoming bulk-set messages.
	InBulkSets int32
	// InBulkSetDrops is the number of incoming bulk-set messages dropped due
	// to the local system being overworked at the time.
	InBulkSetDrops int32
	// InBulkSetInvalids is the number of incoming bulk-set messages that
	// couldn't be parsed.
	InBulkSetInvalids int32
	// InBulkSetWrites is the number of writes due to incoming bulk-set
	// messages.
	InBulkSetWrites int32
	// InBulkSetWriteErrors is the number of errors returned from writes due to
	// incoming bulk-set messages.
	InBulkSetWriteErrors int32
	// InBulkSetWritesOverridden is the number of writes from incoming bulk-set
	// messages that result in no change.
	InBulkSetWritesOverridden int32
	// OutBulkSetAcks is the number of outgoing bulk-set-ack messages.
	OutBulkSetAcks int32
	// InBulkSetAcks is the number of incoming bulk-set-ack messages.
	InBulkSetAcks int32
	// InBulkSetAckDrops is the number of incoming bulk-set-ack messages
	// dropped due to the local system being overworked at the time.
	InBulkSetAckDrops int32
	// InBulkSetAckInvalids is the number of incoming bulk-set-ack messages
	// that couldn't be parsed.
	InBulkSetAckInvalids int32
	// InBulkSetAckWrites is the number of writes (for local removal) due to
	// incoming bulk-set-ack messages.
	InBulkSetAckWrites int32
	// InBulkSetAckWriteErrors is the number of errors returned from writes due
	// to incoming bulk-set-ack messages.
	InBulkSetAckWriteErrors int32
	// InBulkSetAckWritesOverridden is the number of writes from incoming
	// bulk-set-ack messages that result in no change.
	InBulkSetAckWritesOverridden int32
	// OutPullReplications is the number of outgoing pull-replication messages.
	OutPullReplications int32
	// OutPullReplicationNanoseconds is how long the last out pull replication
	// pass took.
	OutPullReplicationNanoseconds int64
	// InPullReplications is the number of incoming pull-replication messages.
	InPullReplications int32
	// InPullReplicationDrops is the number of incoming pull-replication
	// messages droppped due to the local system being overworked at the time.
	InPullReplicationDrops int32
	// InPullReplicationInvalids is the number of incoming pull-replication
	// messages that couldn't be parsed.
	InPullReplicationInvalids int32
	// ExpiredDeletions is the number of recent deletes that have become old
	// enough to be completely discarded.
	ExpiredDeletions int32
	// TombstoneDiscardNanoseconds is how long the last tombstone discard pass
	// took.
	TombstoneDiscardNanoseconds int64
	// CompactionNanoseconds is how long the last compaction pass took.
	CompactionNanoseconds int64
	// Compactions is the number of disk file sets compacted due to their
	// contents exceeding a staleness threshold. For example, this happens when
	// enough of the values have been overwritten or deleted in more recent
	// operations.
	Compactions int32
	// SmallFileCompactions is the number of disk file sets compacted due to
	// the entire file size being too small. For example, this may happen when
	// the store is shutdown and restarted.
	SmallFileCompactions int32
	// DiskFree is the number of bytes free on the device containing the
	// Config.Path for the defaultValueStore.
	DiskFree uint64
	// DiskUsed is the number of bytes used on the device containing the
	// Config.Path for the defaultValueStore.
	DiskUsed uint64
	// DiskSize is the size in bytes of the device containing the Config.Path
	// for the defaultValueStore.
	DiskSize uint64
	// DiskFreeTOC is the number of bytes free on the device containing the
	// Config.PathTOC for the defaultValueStore.
	DiskFreeTOC uint64
	// DiskUsedTOC is the number of bytes used on the device containing the
	// Config.PathTOC for the defaultValueStore.
	DiskUsedTOC uint64
	// DiskSizeTOC is the size in bytes of the device containing the
	// Config.PathTOC for the defaultValueStore.
	DiskSizeTOC uint64
	// MemFree is the number of bytes of free memory.
	MemFree uint64
	// MemUsed is the number of bytes of used memory.
	MemUsed uint64
	// MemSize is the size in bytes of total memory on the system.
	MemSize uint64
	// ReadOnly indicates when the system has been put in read-only mode,
	// whether by DisableWrites or automatically by the watcher.
	ReadOnly bool
	// AuditNanoseconds is how long the last audit pass took.
	AuditNanoseconds int64

	debug                      bool
	freeableMemBlockChansCap   int
	freeableMemBlockChansIn    int
	freeMemBlockChanCap        int
	freeMemBlockChanIn         int
	freeWriteReqChans          int
	freeWriteReqChansCap       int
	freeWriteReqChansIn        int
	pendingWriteReqChans       int
	pendingWriteReqChansCap    int
	pendingWriteReqChansIn     int
	fileMemBlockChanCap        int
	fileMemBlockChanIn         int
	freeTOCBlockChanCap        int
	freeTOCBlockChanIn         int
	pendingTOCBlockChanCap     int
	pendingTOCBlockChanIn      int
	maxLocBlockID              uint64
	path                       string
	pathtoc                    string
	workers                    int
	tombstoneDiscardInterval   int
	outPullReplicationWorkers  uint64
	outPullReplicationInterval int
	pushReplicationWorkers     int
	pushReplicationInterval    int
	valueCap                   uint32
	pageSize                   uint32
	minValueAlloc              int
	writePagesPerWorker        int
	tombstoneAge               int
	fileCap                    uint32
	fileReaders                int
	checksumInterval           uint32
	replicationIgnoreRecent    int
	locmapDebugInfo            fmt.Stringer
}

func (store *defaultValueStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	store.statsLock.Lock()
	stats := &ValueStoreStats{
		Lookups:      atomic.LoadInt32(&store.lookups),
		LookupErrors: atomic.LoadInt32(&store.lookupErrors),

		Reads:      atomic.LoadInt32(&store.reads),
		ReadErrors: atomic.LoadInt32(&store.readErrors),

		Writes:                        atomic.LoadInt32(&store.writes),
		WriteErrors:                   atomic.LoadInt32(&store.writeErrors),
		WritesOverridden:              atomic.LoadInt32(&store.writesOverridden),
		Deletes:                       atomic.LoadInt32(&store.deletes),
		DeleteErrors:                  atomic.LoadInt32(&store.deleteErrors),
		DeletesOverridden:             atomic.LoadInt32(&store.deletesOverridden),
		OutBulkSets:                   atomic.LoadInt32(&store.outBulkSets),
		OutBulkSetValues:              atomic.LoadInt32(&store.outBulkSetValues),
		OutBulkSetPushes:              atomic.LoadInt32(&store.outBulkSetPushes),
		OutBulkSetPushValues:          atomic.LoadInt32(&store.outBulkSetPushValues),
		OutPushReplicationNanoseconds: atomic.LoadInt64(&store.outPushReplicationNanoseconds),
		InBulkSets:                    atomic.LoadInt32(&store.inBulkSets),
		InBulkSetDrops:                atomic.LoadInt32(&store.inBulkSetDrops),
		InBulkSetInvalids:             atomic.LoadInt32(&store.inBulkSetInvalids),
		InBulkSetWrites:               atomic.LoadInt32(&store.inBulkSetWrites),
		InBulkSetWriteErrors:          atomic.LoadInt32(&store.inBulkSetWriteErrors),
		InBulkSetWritesOverridden:     atomic.LoadInt32(&store.inBulkSetWritesOverridden),
		OutBulkSetAcks:                atomic.LoadInt32(&store.outBulkSetAcks),
		InBulkSetAcks:                 atomic.LoadInt32(&store.inBulkSetAcks),
		InBulkSetAckDrops:             atomic.LoadInt32(&store.inBulkSetAckDrops),
		InBulkSetAckInvalids:          atomic.LoadInt32(&store.inBulkSetAckInvalids),
		InBulkSetAckWrites:            atomic.LoadInt32(&store.inBulkSetAckWrites),
		InBulkSetAckWriteErrors:       atomic.LoadInt32(&store.inBulkSetAckWriteErrors),
		InBulkSetAckWritesOverridden:  atomic.LoadInt32(&store.inBulkSetAckWritesOverridden),
		OutPullReplications:           atomic.LoadInt32(&store.outPullReplications),
		OutPullReplicationNanoseconds: atomic.LoadInt64(&store.outPullReplicationNanoseconds),
		InPullReplications:            atomic.LoadInt32(&store.inPullReplications),
		InPullReplicationDrops:        atomic.LoadInt32(&store.inPullReplicationDrops),
		InPullReplicationInvalids:     atomic.LoadInt32(&store.inPullReplicationInvalids),
		ExpiredDeletions:              atomic.LoadInt32(&store.expiredDeletions),
		TombstoneDiscardNanoseconds:   atomic.LoadInt64(&store.tombstoneDiscardNanoseconds),
		CompactionNanoseconds:         atomic.LoadInt64(&store.compactionNanoseconds),
		Compactions:                   atomic.LoadInt32(&store.compactions),
		SmallFileCompactions:          atomic.LoadInt32(&store.smallFileCompactions),
		DiskFree:                      atomic.LoadUint64(&store.watcherState.diskFree),
		DiskUsed:                      atomic.LoadUint64(&store.watcherState.diskUsed),
		DiskSize:                      atomic.LoadUint64(&store.watcherState.diskSize),
		DiskFreeTOC:                   atomic.LoadUint64(&store.watcherState.diskFreeTOC),
		DiskUsedTOC:                   atomic.LoadUint64(&store.watcherState.diskUsedTOC),
		DiskSizeTOC:                   atomic.LoadUint64(&store.watcherState.diskSizeTOC),
		MemFree:                       atomic.LoadUint64(&store.watcherState.memFree),
		MemUsed:                       atomic.LoadUint64(&store.watcherState.memUsed),
		MemSize:                       atomic.LoadUint64(&store.watcherState.memSize),
		AuditNanoseconds:              atomic.LoadInt64(&store.auditNanoseconds),
	}
	store.disableEnableWritesLock.Lock()
	stats.ReadOnly = store.readOnly
	store.disableEnableWritesLock.Unlock()
	atomic.AddInt32(&store.lookups, -stats.Lookups)
	atomic.AddInt32(&store.lookupErrors, -stats.LookupErrors)

	atomic.AddInt32(&store.reads, -stats.Reads)
	atomic.AddInt32(&store.readErrors, -stats.ReadErrors)

	atomic.AddInt32(&store.writes, -stats.Writes)
	atomic.AddInt32(&store.writeErrors, -stats.WriteErrors)
	atomic.AddInt32(&store.writesOverridden, -stats.WritesOverridden)
	atomic.AddInt32(&store.deletes, -stats.Deletes)
	atomic.AddInt32(&store.deleteErrors, -stats.DeleteErrors)
	atomic.AddInt32(&store.deletesOverridden, -stats.DeletesOverridden)
	atomic.AddInt32(&store.outBulkSets, -stats.OutBulkSets)
	atomic.AddInt32(&store.outBulkSetValues, -stats.OutBulkSetValues)
	atomic.AddInt32(&store.outBulkSetPushes, -stats.OutBulkSetPushes)
	atomic.AddInt32(&store.outBulkSetPushValues, -stats.OutBulkSetPushValues)
	atomic.AddInt32(&store.inBulkSets, -stats.InBulkSets)
	atomic.AddInt32(&store.inBulkSetDrops, -stats.InBulkSetDrops)
	atomic.AddInt32(&store.inBulkSetInvalids, -stats.InBulkSetInvalids)
	atomic.AddInt32(&store.inBulkSetWrites, -stats.InBulkSetWrites)
	atomic.AddInt32(&store.inBulkSetWriteErrors, -stats.InBulkSetWriteErrors)
	atomic.AddInt32(&store.inBulkSetWritesOverridden, -stats.InBulkSetWritesOverridden)
	atomic.AddInt32(&store.outBulkSetAcks, -stats.OutBulkSetAcks)
	atomic.AddInt32(&store.inBulkSetAcks, -stats.InBulkSetAcks)
	atomic.AddInt32(&store.inBulkSetAckDrops, -stats.InBulkSetAckDrops)
	atomic.AddInt32(&store.inBulkSetAckInvalids, -stats.InBulkSetAckInvalids)
	atomic.AddInt32(&store.inBulkSetAckWrites, -stats.InBulkSetAckWrites)
	atomic.AddInt32(&store.inBulkSetAckWriteErrors, -stats.InBulkSetAckWriteErrors)
	atomic.AddInt32(&store.inBulkSetAckWritesOverridden, -stats.InBulkSetAckWritesOverridden)
	atomic.AddInt32(&store.outPullReplications, -stats.OutPullReplications)
	atomic.AddInt32(&store.inPullReplications, -stats.InPullReplications)
	atomic.AddInt32(&store.inPullReplicationDrops, -stats.InPullReplicationDrops)
	atomic.AddInt32(&store.inPullReplicationInvalids, -stats.InPullReplicationInvalids)
	atomic.AddInt32(&store.expiredDeletions, -stats.ExpiredDeletions)
	atomic.AddInt32(&store.compactions, -stats.Compactions)
	atomic.AddInt32(&store.smallFileCompactions, -stats.SmallFileCompactions)
	store.statsLock.Unlock()
	if !debug {
		locmapStats := store.locmap.Stats(false)
		stats.Values = locmapStats.ActiveCount
		stats.ValueBytes = locmapStats.ActiveBytes
		stats.locmapDebugInfo = locmapStats
	} else {
		stats.debug = debug
		for i := 0; i < len(store.freeableMemBlockChans); i++ {
			stats.freeableMemBlockChansCap += cap(store.freeableMemBlockChans[i])
			stats.freeableMemBlockChansIn += len(store.freeableMemBlockChans[i])
		}
		stats.freeMemBlockChanCap = cap(store.freeMemBlockChan)
		stats.freeMemBlockChanIn = len(store.freeMemBlockChan)
		stats.freeWriteReqChans = len(store.freeWriteReqChans)
		for i := 0; i < len(store.freeWriteReqChans); i++ {
			stats.freeWriteReqChansCap += cap(store.freeWriteReqChans[i])
			stats.freeWriteReqChansIn += len(store.freeWriteReqChans[i])
		}
		stats.pendingWriteReqChans = len(store.pendingWriteReqChans)
		for i := 0; i < len(store.pendingWriteReqChans); i++ {
			stats.pendingWriteReqChansCap += cap(store.pendingWriteReqChans[i])
			stats.pendingWriteReqChansIn += len(store.pendingWriteReqChans[i])
		}
		stats.fileMemBlockChanCap = cap(store.fileMemBlockChan)
		stats.fileMemBlockChanIn = len(store.fileMemBlockChan)
		stats.freeTOCBlockChanCap = cap(store.freeTOCBlockChan)
		stats.freeTOCBlockChanIn = len(store.freeTOCBlockChan)
		stats.pendingTOCBlockChanCap = cap(store.pendingTOCBlockChan)
		stats.pendingTOCBlockChanIn = len(store.pendingTOCBlockChan)
		stats.maxLocBlockID = atomic.LoadUint64(&store.locBlockIDer)
		stats.path = store.path
		stats.pathtoc = store.pathtoc
		stats.workers = store.workers
		stats.tombstoneDiscardInterval = store.tombstoneDiscardState.interval
		stats.outPullReplicationWorkers = store.pullReplicationState.outWorkers
		stats.outPullReplicationInterval = store.pullReplicationState.outInterval
		stats.pushReplicationWorkers = store.pushReplicationState.workers
		stats.pushReplicationInterval = store.pushReplicationState.interval
		stats.valueCap = store.valueCap
		stats.pageSize = store.pageSize
		stats.minValueAlloc = store.minValueAlloc
		stats.writePagesPerWorker = store.writePagesPerWorker
		stats.tombstoneAge = int((store.tombstoneDiscardState.age >> _TSB_UTIL_BITS) * 1000 / uint64(time.Second))
		stats.fileCap = store.fileCap
		stats.fileReaders = store.fileReaders
		stats.checksumInterval = store.checksumInterval
		stats.replicationIgnoreRecent = int(store.replicationIgnoreRecent / uint64(time.Second))
		locmapStats := store.locmap.Stats(true)
		stats.Values = locmapStats.ActiveCount
		stats.ValueBytes = locmapStats.ActiveBytes
		stats.locmapDebugInfo = locmapStats
	}
	return stats, nil
}

func (stats *ValueStoreStats) String() string {
	report := [][]string{
		{"Values", fmt.Sprintf("%d", stats.Values)},
		{"ValueBytes", fmt.Sprintf("%d", stats.ValueBytes)},
		{"Lookups", fmt.Sprintf("%d", stats.Lookups)},
		{"LookupErrors", fmt.Sprintf("%d", stats.LookupErrors)},

		{"Reads", fmt.Sprintf("%d", stats.Reads)},
		{"ReadErrors", fmt.Sprintf("%d", stats.ReadErrors)},

		{"Writes", fmt.Sprintf("%d", stats.Writes)},
		{"WriteErrors", fmt.Sprintf("%d", stats.WriteErrors)},
		{"WritesOverridden", fmt.Sprintf("%d", stats.WritesOverridden)},
		{"Deletes", fmt.Sprintf("%d", stats.Deletes)},
		{"DeleteErrors", fmt.Sprintf("%d", stats.DeleteErrors)},
		{"DeletesOverridden", fmt.Sprintf("%d", stats.DeletesOverridden)},
		{"OutBulkSets", fmt.Sprintf("%d", stats.OutBulkSets)},
		{"OutBulkSetValues", fmt.Sprintf("%d", stats.OutBulkSetValues)},
		{"OutBulkSetPushes", fmt.Sprintf("%d", stats.OutBulkSetPushes)},
		{"OutBulkSetPushValues", fmt.Sprintf("%d", stats.OutBulkSetPushValues)},
		{"OutPushReplicationNanoseconds", fmt.Sprintf("%d", stats.OutPushReplicationNanoseconds)},
		{"InBulkSets", fmt.Sprintf("%d", stats.InBulkSets)},
		{"InBulkSetDrops", fmt.Sprintf("%d", stats.InBulkSetDrops)},
		{"InBulkSetInvalids", fmt.Sprintf("%d", stats.InBulkSetInvalids)},
		{"InBulkSetWrites", fmt.Sprintf("%d", stats.InBulkSetWrites)},
		{"InBulkSetWriteErrors", fmt.Sprintf("%d", stats.InBulkSetWriteErrors)},
		{"InBulkSetWritesOverridden", fmt.Sprintf("%d", stats.InBulkSetWritesOverridden)},
		{"OutBulkSetAcks", fmt.Sprintf("%d", stats.OutBulkSetAcks)},
		{"InBulkSetAcks", fmt.Sprintf("%d", stats.InBulkSetAcks)},
		{"InBulkSetAckDrops", fmt.Sprintf("%d", stats.InBulkSetAckDrops)},
		{"InBulkSetAckInvalids", fmt.Sprintf("%d", stats.InBulkSetAckInvalids)},
		{"InBulkSetAckWrites", fmt.Sprintf("%d", stats.InBulkSetAckWrites)},
		{"InBulkSetAckWriteErrors", fmt.Sprintf("%d", stats.InBulkSetAckWriteErrors)},
		{"InBulkSetAckWritesOverridden", fmt.Sprintf("%d", stats.InBulkSetAckWritesOverridden)},
		{"OutPullReplications", fmt.Sprintf("%d", stats.OutPullReplications)},
		{"OutPullReplicationNanoseconds", fmt.Sprintf("%d", stats.OutPullReplicationNanoseconds)},
		{"InPullReplications", fmt.Sprintf("%d", stats.InPullReplications)},
		{"InPullReplicationDrops", fmt.Sprintf("%d", stats.InPullReplicationDrops)},
		{"InPullReplicationInvalids", fmt.Sprintf("%d", stats.InPullReplicationInvalids)},
		{"ExpiredDeletions", fmt.Sprintf("%d", stats.ExpiredDeletions)},
		{"TombstoneDiscardNanoseconds", fmt.Sprintf("%d", stats.TombstoneDiscardNanoseconds)},
		{"CompactionNanoseconds", fmt.Sprintf("%d", stats.CompactionNanoseconds)},
		{"Compactions", fmt.Sprintf("%d", stats.Compactions)},
		{"SmallFileCompactions", fmt.Sprintf("%d", stats.SmallFileCompactions)},
		{"DiskFree", fmt.Sprintf("%d", stats.DiskFree)},
		{"DiskUsed", fmt.Sprintf("%d", stats.DiskUsed)},
		{"DiskSize", fmt.Sprintf("%d", stats.DiskSize)},
		{"DiskFreeTOC", fmt.Sprintf("%d", stats.DiskFreeTOC)},
		{"DiskUsedTOC", fmt.Sprintf("%d", stats.DiskUsedTOC)},
		{"DiskSizeTOC", fmt.Sprintf("%d", stats.DiskSizeTOC)},
		{"MemFree", fmt.Sprintf("%d", stats.MemFree)},
		{"MemUsed", fmt.Sprintf("%d", stats.MemUsed)},
		{"MemSize", fmt.Sprintf("%d", stats.MemSize)},
		{"AuditNanoseconds", fmt.Sprintf("%d", stats.AuditNanoseconds)},
	}
	if stats.debug {
		report = append(report, [][]string{
			nil,
			{"freeableMemBlockChansCap", fmt.Sprintf("%d", stats.freeableMemBlockChansCap)},
			{"freeableMemBlockChansIn", fmt.Sprintf("%d", stats.freeableMemBlockChansIn)},
			{"freeMemBlockChanCap", fmt.Sprintf("%d", stats.freeMemBlockChanCap)},
			{"freeMemBlockChanIn", fmt.Sprintf("%d", stats.freeMemBlockChanIn)},
			{"freeWriteReqChans", fmt.Sprintf("%d", stats.freeWriteReqChans)},
			{"freeWriteReqChansCap", fmt.Sprintf("%d", stats.freeWriteReqChansCap)},
			{"freeWriteReqChansIn", fmt.Sprintf("%d", stats.freeWriteReqChansIn)},
			{"pendingWriteReqChans", fmt.Sprintf("%d", stats.pendingWriteReqChans)},
			{"pendingWriteReqChansCap", fmt.Sprintf("%d", stats.pendingWriteReqChansCap)},
			{"pendingWriteReqChansIn", fmt.Sprintf("%d", stats.pendingWriteReqChansIn)},
			{"fileMemBlockChanCap", fmt.Sprintf("%d", stats.fileMemBlockChanCap)},
			{"fileMemBlockChanIn", fmt.Sprintf("%d", stats.fileMemBlockChanIn)},
			{"freeTOCBlockChanCap", fmt.Sprintf("%d", stats.freeTOCBlockChanCap)},
			{"freeTOCBlockChanIn", fmt.Sprintf("%d", stats.freeTOCBlockChanIn)},
			{"pendingTOCBlockChanCap", fmt.Sprintf("%d", stats.pendingTOCBlockChanCap)},
			{"pendingTOCBlockChanIn", fmt.Sprintf("%d", stats.pendingTOCBlockChanIn)},
			{"maxLocBlockID", fmt.Sprintf("%d", stats.maxLocBlockID)},
			{"path", stats.path},
			{"pathtoc", stats.pathtoc},
			{"workers", fmt.Sprintf("%d", stats.workers)},
			{"tombstoneDiscardInterval", fmt.Sprintf("%d", stats.tombstoneDiscardInterval)},
			{"outPullReplicationWorkers", fmt.Sprintf("%d", stats.outPullReplicationWorkers)},
			{"outPullReplicationInterval", fmt.Sprintf("%d", stats.outPullReplicationInterval)},
			{"pushReplicationWorkers", fmt.Sprintf("%d", stats.pushReplicationWorkers)},
			{"pushReplicationInterval", fmt.Sprintf("%d", stats.pushReplicationInterval)},
			{"valueCap", fmt.Sprintf("%d", stats.valueCap)},
			{"pageSize", fmt.Sprintf("%d", stats.pageSize)},
			{"minValueAlloc", fmt.Sprintf("%d", stats.minValueAlloc)},
			{"writePagesPerWorker", fmt.Sprintf("%d", stats.writePagesPerWorker)},
			{"tombstoneAge", fmt.Sprintf("%d", stats.tombstoneAge)},
			{"fileCap", fmt.Sprintf("%d", stats.fileCap)},
			{"fileReaders", fmt.Sprintf("%d", stats.fileReaders)},
			{"checksumInterval", fmt.Sprintf("%d", stats.checksumInterval)},
			{"replicationIgnoreRecent", fmt.Sprintf("%d", stats.replicationIgnoreRecent)},
			{"locmapDebugInfo", stats.locmapDebugInfo.String()},
		}...)
	}
	return brimtext.Align(report, nil)
}
