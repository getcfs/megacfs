package store

import (
	"io"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/gholt/locmap"
	"github.com/gholt/ring"
	"go.uber.org/zap"
)

// Absolute minimum: timestampnano:8 leader plus at least one TOC entry
const _GROUP_PAGE_SIZE_MIN = 8 + _GROUP_FILE_ENTRY_SIZE

// GroupStoreConfig represents the set of values for configuring a
// GroupStore. Note that changing the values (shallow changes) in this
// structure will have no effect on existing GroupStores.
type GroupStoreConfig struct {
	// Scale sets how much to scale the default values by; this can reduce
	// memory usage for systems where the store isn't the only thing running on
	// the hardware. Note that this will *not* scale explictly set values, just
	// the values if they fallback to the defaults.
	Scale float64
	// Logger defines where log output will go. This library has minimal log
	// output (most meta data is sent via metrics) but some errors and debug
	// logging does exist. If not set, the logger will default to
	// zap.NewProduction()
	Logger *zap.Logger
	// LoggerName is used as a prefix when setting the "name" field with log
	// messages. For example, given a LoggerName of "mystoreapp", this library
	// would log under names such as "mystoreapp.compaction" and
	// "mystoreapp.recovery".
	LoggerName string
	// Rand sets the rand.Rand to use as a random data source. Defaults to a
	// new randomizer based on the current time.
	Rand *rand.Rand
	// Path sets the path where group files will be written; grouptoc files
	// will also be written here unless overridden with PathTOC. Defaults to
	// the current working directory.
	Path string
	// PathTOC sets the path where grouptoc files will be written. Defaults to
	// the Path value.
	PathTOC string
	// ValueCap indicates the maximum number of bytes any given value may be.
	// Defaults to 1,048,576 bytes.
	ValueCap int
	// Workers indicates how many goroutines may be used for various tasks
	// (processing incoming writes and batching them to disk, background tasks,
	// etc.). This will also have an impact on memory usage. Defaults to
	// GOMAXPROCS.
	Workers int
	// ChecksumInterval indicates how many bytes are output to a file before a
	// 4-byte checksum is also output. Defaults to 65,532 bytes.
	ChecksumInterval int
	// PageSize controls the size of each chunk of memory allocated. Defaults
	// to 4,194,304 bytes.
	PageSize int
	// WritePagesPerWorker controls how many pages are created per worker for
	// caching recently written values. Defaults to 3.
	WritePagesPerWorker int
	// GroupLocMap allows overriding the default GroupLocMap, an interface
	// used by GroupStore for tracking the mappings from keys to the locations
	// of their values. Defaults to
	// github.com/gholt/locmap.NewGroupLocMap().
	GroupLocMap locmap.GroupLocMap
	// MsgRing sets the ring.MsgRing to use for determining the key ranges the
	// GroupStore is responsible for as well as providing methods to send
	// messages to other nodes.
	MsgRing ring.MsgRing
	// MsgCap indicates the maximum bytes for outgoing messages. Defaults to
	// 16,777,216 bytes.
	MsgCap int
	// MsgTimeout indicates the maximum milliseconds a message can be pending
	// before just discarding it. Defaults to 250 milliseconds.
	MsgTimeout int
	// FileCap indicates how large a file can be before closing it and opening
	// a new one. Defaults to 4,294,967,295 bytes.
	FileCap int
	// FileReaders indicates how many open file descriptors are allowed per
	// file for reading. Defaults to Workers.
	FileReaders int
	// RecoveryBatchSize indicates how many keys to set in a batch while
	// performing recovery (initial start up). Defaults to 1,048,576 keys.
	RecoveryBatchSize int
	// TombstoneDiscardInterval indicates the minimum number of seconds between
	// the starts of background passes to discard expired tombstones [deletion
	// markers]. If set to 300 seconds and a pass takes 100 seconds to run, it
	// will wait 200 seconds (with a small amount of randomization) before
	// starting the next pass. Default: 300 seconds
	TombstoneDiscardInterval int
	// TombstoneDiscardBatchSize indicates how many items to queue up before
	// pausing a scan, issuing the actual discards, and resuming the scan
	// again. Defaults to 1,048,576 items.
	TombstoneDiscardBatchSize int
	// TombstoneAge indicates how many seconds old a deletion marker may be
	// before it is permanently removed. Defaults to 14,400 seconds (4 hours).
	TombstoneAge int
	// TombstoneDiscardWorkers indicates how many goroutines may be used for
	// discard passes (discarding expired tombstones [deletion markers]).
	// Defaults to 1 worker.
	TombstoneDiscardWorkers int
	// ReplicationIgnoreRecent indicates how many seconds old a value should be
	// before it is included in replication processing. Defaults to 60 seconds.
	ReplicationIgnoreRecent int
	// OutPullReplicationInterval is much like TombstoneDiscardInterval but for
	// outgoing pull replication passes. Default: 60 seconds
	OutPullReplicationInterval int
	// OutPullReplicationWorkers indicates how many goroutines may be used for
	// an outgoing pull replication pass. Defaults to Workers / 10 with a
	// minimum of 1.
	OutPullReplicationWorkers int
	// OutPullReplicationMsgs indicates how many outgoing pull-replication
	// messages can be buffered before blocking on creating more. Defaults to
	// OutPullReplicationWorkers * 4.
	OutPullReplicationMsgs int
	// OutPullReplicationBloomN indicates the N-factor for the outgoing
	// pull-replication bloom filters. This indicates how many keys the bloom
	// filter can reasonably hold and, in combination with the P-factor,
	// affects memory usage. Defaults to 1,000,000.
	OutPullReplicationBloomN int
	// OutPullReplicationBloomP indicates the P-factor for the outgoing
	// pull-replication bloom filters. This indicates the desired percentage
	// chance of a collision within the bloom filter and, in combination with
	// the N-factor, affects memory usage. Defaults to 0.001.
	OutPullReplicationBloomP float64
	// OutPullReplicationMsgTimeout indicates the maximum milliseconds an
	// outgoing pull replication message can be pending before just discarding
	// it. Defaults to MsgTimeout.
	OutPullReplicationMsgTimeout int
	// InPullReplicationWorkers indicates how many incoming pull-replication
	// messages can be processed at the same time. Defaults to Workers.
	InPullReplicationWorkers int
	// InPullReplicationMsgs indicates how many incoming pull-replication
	// messages can be buffered before dropping additional ones. Defaults to
	// InPullReplicationWorkers * 4.
	InPullReplicationMsgs int
	// InPullReplicationResponseMsgTimeout indicates the maximum milliseconds
	// an outgoing response message to an incoming pull replication message can
	// be pending before just discarding it. Defaults to MsgTimeout.
	InPullReplicationResponseMsgTimeout int
	// PushReplicationInterval is much like TombstoneDiscardInterval but for
	// outgoing push replication passes. Default: 60 seconds
	PushReplicationInterval int
	// PushReplicationWorkers indicates how many goroutines may be used for an
	// outgoing push replication pass. Defaults to Workers / 10 with minimum of
	// 1.
	PushReplicationWorkers int
	// PushReplicationMsgTimeout indicates the maximum milliseconds an
	// outgoing push replication message can be pending before just discarding
	// it. Defaults to MsgTimeout.
	PushReplicationMsgTimeout int
	// BulkSetMsgCap indicates the maximum bytes for bulk-set messages.
	// Defaults to MsgCap.
	BulkSetMsgCap int
	// OutBulkSetMsgs indicates how many outgoing bulk-set messages can be
	// buffered before blocking on creating more. Defaults to
	// PushReplicationWorkers * 4.
	OutBulkSetMsgs int
	// InBulkSetWorkers indicates how many incoming bulk-set messages can be
	// processed at the same time. Defaults to Workers.
	InBulkSetWorkers int
	// InBulkSetMsgs indicates how many incoming bulk-set messages can be
	// buffered before dropping additional ones. Defaults to InBulkSetWorkers *
	// 4.
	InBulkSetMsgs int
	// InBulkSetResponseMsgTimeout indicates the maximum milliseconds a
	// response message to an incoming bulk-set message can be pending before
	// just discarding it. Defaults to MsgTimeout.
	InBulkSetResponseMsgTimeout int
	// BulkSetAckMsgCap indicates the maximum bytes for bulk-set-ack messages.
	// Defaults to MsgCap.
	BulkSetAckMsgCap int
	// InBulkSetAckWorkers indicates how many incoming bulk-set-ack messages
	// can be processed at the same time. Defaults to Workers.
	InBulkSetAckWorkers int
	// InBulkSetAckMsgs indicates how many incoming bulk-set-ack messages can
	// be buffered before dropping additional ones. Defaults to
	// InBulkSetAckWorkers * 4.
	InBulkSetAckMsgs int
	// OutBulkSetAckMsgs indicates how many outgoing bulk-set-ack messages can
	// be buffered before blocking on creating more. Defaults to
	// InBulkSetWorkers * 4.
	OutBulkSetAckMsgs int
	// CompactionInterval is much like TombstoneDiscardInterval but for
	// compaction passes. Default: 600 seconds
	CompactionInterval int
	// CompactionWorkers indicates how much concurrency is allowed for
	// compaction. Defaults to 1.
	CompactionWorkers int
	// CompactionThreshold indicates how much waste a given file may have
	// before it is compacted. Defaults to 0.10 (10%).
	CompactionThreshold float64
	// CompactionAgeThreshold indicates how old a given file must be before it
	// is considered for compaction. Defaults to 300 seconds.
	CompactionAgeThreshold int
	// DiskFreeDisableThreshold controls when to automatically disable writes;
	// the number is in bytes. If the number of free bytes on either the Path
	// or TOCPath device falls below this threshold, writes will be
	// automatically disabled.
	// 0 will use the default; 1 will disable the check.
	// default: 8,589,934,592 (8G)
	DiskFreeDisableThreshold uint64
	// DiskFreeReenableThreshold controls when to automatically re-enable
	// writes; the number is in bytes. If writes are automatically disabled and
	// the number of free bytes on each of the Path or TOCPath devices rises
	// above this threshold, writes will be automatically re-enabled. A
	// negative value will turn off this check.
	// 0 will use the default; 1 will disable the check.
	// default: 17,179,869,184 (16G)
	DiskFreeReenableThreshold uint64
	// DiskUsageDisableThreshold controls when to automatically disable writes;
	// the number is a percentage (1 == 100%). If the percentage used on either
	// the Path or TOCPath device grows above this threshold, writes will be
	// automatically disabled.
	// 0 will use the default; a negative value will disable the check.
	// default: 0.94 (94%)
	DiskUsageDisableThreshold float64
	// DiskUsageReenableThreshold controls when to automatically re-enable
	// writes; the number is a percentage (1 == 100%). If writes are
	// automatically disabled and the percentage used on each of the Path or
	// TOCPath devices falls below this threshold, writes will be automatically
	// re-enabled.
	// 0 will use the default; a negative value will disable the check.
	// default: 0.90 (90%)
	DiskUsageReenableThreshold float64
	// FlusherThreshold sets the number of to-disk modifications controlling
	// the once-a-minute automatic flusher. If there are less than this
	// setting's number of modifications for a minute, the store's Flush method
	// will be called. This is so relatively idle stores won't just leave the
	// last few modifications sitting only in memory for too long, but without
	// penalizing active stores that will already be sending recent
	// modifications to disk as ever more recent modifications occur.
	// 0 will use the default; a negative value will disable the check.
	// default is the number of entries that are guaranteed to fill a memory
	// page; this depends on the PageSize value and the internal struct size
	// for the store.
	FlusherThreshold int32
	// AuditInterval is much like TombstoneDiscardInterval but for audit
	// passes. Default: 604,800 seconds (1 week).
	AuditInterval int
	// AuditAgeThreshold indicates how old a given file must be before it
	// is considered for an audit. Defaults to 604,800 seconds (1 week).
	AuditAgeThreshold int
	// MemFreeDisableThreshold controls when to automatically disable writes;
	// the number is in bytes. If the number of free bytes of memory falls
	// below this threshold, writes will be automatically disabled.
	// 0 will use the default; 1 will disable the check.
	// default: 134,217,728 (128M)
	MemFreeDisableThreshold uint64
	// MemFreeReenableThreshold controls when to automatically re-enable
	// writes; the number is in bytes. If writes are automatically disabled and
	// the number of free bytes of memory rises above this threshold, writes
	// will be automatically re-enabled. A negative value will turn off this
	// check.
	// 0 will use the default; 1 will disable the check.
	// default: 268,435,456 (256M)
	MemFreeReenableThreshold uint64
	// MemUsageDisableThreshold controls when to automatically disable writes;
	// the number is a percentage (1 == 100%). If the percentage of used memory
	// grows above this threshold, writes will be automatically disabled.
	// 0 will use the default; a negative value will disable the check.
	// default: 0.94 (94%)
	MemUsageDisableThreshold float64
	// MemUsageReenableThreshold controls when to automatically re-enable
	// writes; the number is a percentage (1 == 100%). If writes are
	// automatically disabled and the percentage of used memory falls below
	// this threshold, writes will be automatically re-enabled.
	// 0 will use the default; a negative value will disable the check.
	// default: 0.90 (90%)
	MemUsageReenableThreshold float64

	OpenReadSeeker    func(fullPath string) (io.ReadSeeker, error)
	OpenWriteSeeker   func(fullPath string) (io.WriteSeeker, error)
	Readdirnames      func(fullPath string) ([]string, error)
	CreateWriteCloser func(fullPath string) (io.WriteCloser, error)
	Stat              func(fullPath string) (os.FileInfo, error)
	Remove            func(fullPath string) error
	Rename            func(oldFullPath string, newFullPath string) error
	IsNotExist        func(err error) bool

	minValueAlloc int
}

func resolveGroupStoreConfig(c *GroupStoreConfig) *GroupStoreConfig {
	cfg := &GroupStoreConfig{}
	if c != nil {
		*cfg = *c
	}
	if env := os.Getenv("GROUPSTORE_SCALE"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			cfg.Scale = val
		}
	}
	if cfg.Scale <= 0 || cfg.Scale > 1 {
		cfg.Scale = 1
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if env := os.Getenv("GROUPSTORE_PATH"); env != "" {
		cfg.Path = env
	}
	if cfg.Path == "" {
		cfg.Path = "."
	}
	if env := os.Getenv("GROUPSTORE_PATH_TOC"); env != "" {
		cfg.PathTOC = env
	}
	if cfg.PathTOC == "" {
		cfg.PathTOC = cfg.Path
	}
	if env := os.Getenv("GROUPSTORE_VALUE_CAP"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.ValueCap = val
		}
	}
	if cfg.ValueCap == 0 {
		cfg.ValueCap = 1048576
	}
	if cfg.ValueCap < 0 {
		cfg.ValueCap = 0
	}
	if cfg.ValueCap > 1048576 {
		cfg.ValueCap = 1048576
	}
	if env := os.Getenv("GROUPSTORE_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.Workers = val
		}
	}
	if cfg.Workers == 0 {
		cfg.Workers = int(float64(runtime.GOMAXPROCS(0)) * cfg.Scale)
	}
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if env := os.Getenv("GROUPSTORE_CHECKSUM_INTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.ChecksumInterval = val
		}
	}
	if cfg.ChecksumInterval == 0 {
		cfg.ChecksumInterval = 64*1024 - 4
	}
	if cfg.ChecksumInterval < _GROUP_FILE_HEADER_SIZE {
		cfg.ChecksumInterval = _GROUP_FILE_HEADER_SIZE
	}
	if env := os.Getenv("GROUPSTORE_PAGE_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.PageSize = val
		}
	}
	if cfg.PageSize == 0 {
		cfg.PageSize = int(4 * 1024 * 1024 * cfg.Scale)
	}
	// Ensure each page will have at least ChecksumInterval worth of data in it
	// so that each page written will at least flush the previous page's data.
	if cfg.PageSize < cfg.ValueCap+cfg.ChecksumInterval {
		cfg.PageSize = cfg.ValueCap + cfg.ChecksumInterval
	}
	// Absolute minimum: timestampnano leader plus at least one TOC entry
	if cfg.PageSize < _GROUP_PAGE_SIZE_MIN {
		cfg.PageSize = _GROUP_PAGE_SIZE_MIN
	}
	// The max is MaxUint32-1 because we use MaxUint32 to indicate push
	// replication local removal.
	if cfg.PageSize > math.MaxUint32-1 {
		cfg.PageSize = math.MaxUint32 - 1
	}
	// Ensure a full TOC page will have an associated data page of at least
	// checksumInterval in size, again so that each page written will at least
	// flush the previous page's data.
	cfg.minValueAlloc = cfg.ChecksumInterval/(cfg.PageSize/_GROUP_FILE_ENTRY_SIZE+1) + 1
	if env := os.Getenv("GROUPSTORE_WRITE_PAGES_PER_WORKER"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.WritePagesPerWorker = val
		}
	}
	if cfg.WritePagesPerWorker == 0 {
		cfg.WritePagesPerWorker = int(3 * cfg.Scale)
	}
	if cfg.WritePagesPerWorker < 2 {
		cfg.WritePagesPerWorker = 2
	}
	if env := os.Getenv("GROUPSTORE_MSG_CAP"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.MsgCap = val
		}
	}
	if cfg.MsgCap == 0 {
		cfg.MsgCap = int(16 * 1024 * 1024 * cfg.Scale)
	}
	// NOTE: This minimum needs to be the largest minimum size of all the
	// message types; 1024 "should" be enough.
	if cfg.MsgCap < 1024 {
		cfg.MsgCap = 1024
	}
	if env := os.Getenv("GROUPSTORE_MSG_TIMEOUT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.MsgTimeout = val
		}
	}
	if cfg.MsgTimeout == 0 {
		cfg.MsgTimeout = 250
	}
	if cfg.MsgTimeout < 1 {
		cfg.MsgTimeout = 250
	}
	if env := os.Getenv("GROUPSTORE_FILE_CAP"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.FileCap = val
		}
	}
	if cfg.FileCap == 0 {
		cfg.FileCap = math.MaxUint32
	}
	if cfg.FileCap < _GROUP_FILE_HEADER_SIZE+_GROUP_FILE_TRAILER_SIZE+cfg.ValueCap { // header value trailer
		cfg.FileCap = _GROUP_FILE_HEADER_SIZE + _GROUP_FILE_TRAILER_SIZE + cfg.ValueCap
	}
	if cfg.FileCap > math.MaxUint32 {
		cfg.FileCap = math.MaxUint32
	}
	if env := os.Getenv("GROUPSTORE_FILE_READERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.FileReaders = val
		}
	}
	if cfg.FileReaders == 0 {
		cfg.FileReaders = cfg.Workers
	}
	if cfg.FileReaders < 1 {
		cfg.FileReaders = 1
	}
	if env := os.Getenv("GROUPSTORE_RECOVERY_BATCH_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.RecoveryBatchSize = val
		}
	}
	if cfg.RecoveryBatchSize == 0 {
		cfg.RecoveryBatchSize = int(1024 * 1024 * cfg.Scale)
	}
	if cfg.RecoveryBatchSize < 1 {
		cfg.RecoveryBatchSize = 1
	}
	if env := os.Getenv("GROUPSTORE_TOMBSTONE_DISCARD_INTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.TombstoneDiscardInterval = val
		}
	}
	if cfg.TombstoneDiscardInterval == 0 {
		cfg.TombstoneDiscardInterval = 300
	}
	if cfg.TombstoneDiscardInterval < 1 {
		cfg.TombstoneDiscardInterval = 1
	}
	if env := os.Getenv("GROUPSTORE_TOMBSTONE_DISCARD_BATCH_SIZE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.TombstoneDiscardBatchSize = val
		}
	}
	if cfg.TombstoneDiscardBatchSize == 0 {
		cfg.TombstoneDiscardBatchSize = int(1024 * 1024 * cfg.Scale)
	}
	if cfg.TombstoneDiscardBatchSize < 1 {
		cfg.TombstoneDiscardBatchSize = 1
	}
	if env := os.Getenv("GROUPSTORE_TOMBSTONE_AGE"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.TombstoneAge = val
		}
	}
	if cfg.TombstoneAge == 0 {
		cfg.TombstoneAge = 4 * 60 * 60
	}
	if cfg.TombstoneAge < 0 {
		cfg.TombstoneAge = 0
	}
	if env := os.Getenv("GROUPSTORE_TOMBSTONE_DISCARD_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.TombstoneDiscardWorkers = val
		}
	}
	if cfg.TombstoneDiscardWorkers == 0 {
		cfg.TombstoneDiscardWorkers = cfg.Workers
	}
	if cfg.TombstoneDiscardWorkers < 1 {
		cfg.TombstoneDiscardWorkers = 1
	}
	if env := os.Getenv("GROUPSTORE_REPLICATION_IGNORE_RECENT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.ReplicationIgnoreRecent = val
		}
	}
	if cfg.ReplicationIgnoreRecent == 0 {
		cfg.ReplicationIgnoreRecent = 60
	}
	if cfg.ReplicationIgnoreRecent < 0 {
		cfg.ReplicationIgnoreRecent = 0
	}
	if env := os.Getenv("GROUPSTORE_OUT_PULL_REPLICATION_INTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.OutPullReplicationInterval = val
		}
	}
	if cfg.OutPullReplicationInterval == 0 {
		cfg.OutPullReplicationInterval = 60
	}
	if cfg.OutPullReplicationInterval < 1 {
		cfg.OutPullReplicationInterval = 1
	}
	if env := os.Getenv("GROUPSTORE_OUT_PULL_REPLICATION_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.OutPullReplicationWorkers = val
		}
	}
	if cfg.OutPullReplicationWorkers == 0 {
		cfg.OutPullReplicationWorkers = cfg.Workers / 10
	}
	if cfg.OutPullReplicationWorkers < 1 {
		cfg.OutPullReplicationWorkers = 1
	}
	if env := os.Getenv("GROUPSTORE_OUT_PULL_REPLICATION_MSGS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.OutPullReplicationMsgs = val
		}
	}
	if cfg.OutPullReplicationMsgs == 0 {
		cfg.OutPullReplicationMsgs = int(float64(cfg.OutPullReplicationWorkers) * 4 * cfg.Scale)
	}
	if cfg.OutPullReplicationMsgs < 1 {
		cfg.OutPullReplicationMsgs = 1
	}
	if env := os.Getenv("GROUPSTORE_OUT_PULL_REPLICATION_BLOOM_N"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.OutPullReplicationBloomN = val
		}
	}
	if cfg.OutPullReplicationBloomN == 0 {
		cfg.OutPullReplicationBloomN = int(1000000 * cfg.Scale)
	}
	if cfg.OutPullReplicationBloomN < 1 {
		cfg.OutPullReplicationBloomN = 1
	}
	if env := os.Getenv("GROUPSTORE_OUT_PULL_REPLICATION_BLOOM_P"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			cfg.OutPullReplicationBloomP = val
		}
	}
	if cfg.OutPullReplicationBloomP == 0.0 {
		cfg.OutPullReplicationBloomP = 0.001
	}
	if cfg.OutPullReplicationBloomP < 0.000001 {
		cfg.OutPullReplicationBloomP = 0.000001
	}
	if env := os.Getenv("GROUPSTORE_OUT_PULL_REPLICATION_MSG_TIMEOUT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.OutPullReplicationMsgTimeout = val
		}
	}
	if cfg.OutPullReplicationMsgTimeout == 0 {
		cfg.OutPullReplicationMsgTimeout = cfg.MsgTimeout
	}
	if cfg.OutPullReplicationMsgTimeout < 1 {
		cfg.OutPullReplicationMsgTimeout = 250
	}
	if env := os.Getenv("GROUPSTORE_IN_PULL_REPLICATION_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.InPullReplicationWorkers = val
		}
	}
	if cfg.InPullReplicationWorkers == 0 {
		cfg.InPullReplicationWorkers = cfg.Workers
	}
	if cfg.InPullReplicationWorkers < 1 {
		cfg.InPullReplicationWorkers = 1
	}
	if env := os.Getenv("GROUPSTORE_IN_PULL_REPLICATION_MSGS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.InPullReplicationMsgs = val
		}
	}
	if cfg.InPullReplicationMsgs == 0 {
		cfg.InPullReplicationMsgs = int(float64(cfg.InPullReplicationWorkers) * 4 * cfg.Scale)
	}
	if cfg.InPullReplicationMsgs < 1 {
		cfg.InPullReplicationMsgs = 1
	}
	if env := os.Getenv("GROUPSTORE_IN_PULL_REPLICATION_RESPONSE_MSG_TIMEOUT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.InPullReplicationResponseMsgTimeout = val
		}
	}
	if cfg.InPullReplicationResponseMsgTimeout == 0 {
		cfg.InPullReplicationResponseMsgTimeout = cfg.MsgTimeout
	}
	if cfg.InPullReplicationResponseMsgTimeout < 1 {
		cfg.InPullReplicationResponseMsgTimeout = 250
	}
	if env := os.Getenv("GROUPSTORE_PUSH_REPLICATION_INTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.PushReplicationInterval = val
		}
	}
	if cfg.PushReplicationInterval == 0 {
		cfg.PushReplicationInterval = 60
	}
	if cfg.PushReplicationInterval < 1 {
		cfg.PushReplicationInterval = 1
	}
	if env := os.Getenv("GROUPSTORE_PUSH_REPLICATION_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.PushReplicationWorkers = val
		}
	}
	if cfg.PushReplicationWorkers == 0 {
		cfg.PushReplicationWorkers = cfg.Workers / 10
	}
	if cfg.PushReplicationWorkers < 1 {
		cfg.PushReplicationWorkers = 1
	}
	if env := os.Getenv("GROUPSTORE_PUSH_REPLICATION_MSG_TIMEOUT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.PushReplicationMsgTimeout = val
		}
	}
	if cfg.PushReplicationMsgTimeout == 0 {
		cfg.PushReplicationMsgTimeout = cfg.MsgTimeout
	}
	if cfg.PushReplicationMsgTimeout < 1 {
		cfg.PushReplicationMsgTimeout = 250
	}
	if env := os.Getenv("GROUPSTORE_BULK_SET_MSG_CAP"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.BulkSetMsgCap = val
		}
	}
	if cfg.BulkSetMsgCap == 0 {
		cfg.BulkSetMsgCap = cfg.MsgCap
	}
	if cfg.BulkSetMsgCap < 1 {
		cfg.BulkSetMsgCap = 1
	}
	if env := os.Getenv("GROUPSTORE_OUT_BULK_SET_MSGS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.OutBulkSetMsgs = val
		}
	}
	if cfg.OutBulkSetMsgs == 0 {
		cfg.OutBulkSetMsgs = int(float64(cfg.PushReplicationWorkers) * 4 * cfg.Scale)
	}
	if cfg.OutBulkSetMsgs < 1 {
		cfg.OutBulkSetMsgs = 1
	}
	if env := os.Getenv("GROUPSTORE_IN_BULK_SET_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.InBulkSetWorkers = val
		}
	}
	if cfg.InBulkSetWorkers == 0 {
		cfg.InBulkSetWorkers = cfg.Workers
	}
	if cfg.InBulkSetWorkers < 1 {
		cfg.InBulkSetWorkers = 1
	}
	if env := os.Getenv("GROUPSTORE_IN_BULK_SET_MSGS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.InBulkSetMsgs = val
		}
	}
	if cfg.InBulkSetMsgs == 0 {
		cfg.InBulkSetMsgs = int(float64(cfg.InBulkSetWorkers) * 4 * cfg.Scale)
	}
	if cfg.InBulkSetMsgs < 1 {
		cfg.InBulkSetMsgs = 1
	}
	if env := os.Getenv("GROUPSTORE_IN_BULK_SET_RESPONSE_MSG_TIMEOUT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.InBulkSetResponseMsgTimeout = val
		}
	}
	if cfg.InBulkSetResponseMsgTimeout == 0 {
		cfg.InBulkSetResponseMsgTimeout = cfg.MsgTimeout
	}
	if cfg.InBulkSetResponseMsgTimeout < 1 {
		cfg.InBulkSetResponseMsgTimeout = 250
	}
	if env := os.Getenv("GROUPSTORE_OUT_BULK_SET_ACK_MSG_CAP"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.BulkSetAckMsgCap = val
		}
	}
	if cfg.BulkSetAckMsgCap == 0 {
		cfg.BulkSetAckMsgCap = cfg.MsgCap
	}
	if cfg.BulkSetAckMsgCap < 1 {
		cfg.BulkSetAckMsgCap = 1
	}
	if env := os.Getenv("GROUPSTORE_IN_BULK_SET_ACK_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.InBulkSetAckWorkers = val
		}
	}
	if cfg.InBulkSetAckWorkers == 0 {
		cfg.InBulkSetAckWorkers = cfg.Workers
	}
	if cfg.InBulkSetAckWorkers < 1 {
		cfg.InBulkSetAckWorkers = 1
	}
	if env := os.Getenv("GROUPSTORE_IN_BULK_SET_ACK_MSGS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.InBulkSetAckMsgs = val
		}
	}
	if cfg.InBulkSetAckMsgs == 0 {
		cfg.InBulkSetAckMsgs = int(float64(cfg.InBulkSetAckWorkers) * 4 * cfg.Scale)
	}
	if cfg.InBulkSetAckMsgs < 1 {
		cfg.InBulkSetAckMsgs = 1
	}
	if env := os.Getenv("GROUPSTORE_OUT_BULK_SET_ACK_MSGS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.OutBulkSetAckMsgs = val
		}
	}
	if cfg.OutBulkSetAckMsgs == 0 {
		cfg.OutBulkSetAckMsgs = int(float64(cfg.InBulkSetAckWorkers) * 4 * cfg.Scale)
	}
	if cfg.OutBulkSetAckMsgs < 1 {
		cfg.OutBulkSetAckMsgs = 1
	}
	if env := os.Getenv("GROUPSTORE_COMPACTION_INTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.CompactionInterval = val
		}
	}
	if cfg.CompactionInterval == 0 {
		cfg.CompactionInterval = 600
	}
	if cfg.CompactionInterval < 1 {
		cfg.CompactionInterval = 1
	}
	if env := os.Getenv("GROUPSTORE_COMPACTION_WORKERS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.CompactionWorkers = val
		}
	}
	if cfg.CompactionWorkers == 0 {
		cfg.CompactionWorkers = cfg.Workers
	}
	if cfg.CompactionWorkers < 1 {
		cfg.CompactionWorkers = 1
	}
	if env := os.Getenv("GROUPSTORE_COMPACTION_THRESHOLD"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			cfg.CompactionThreshold = val
		}
	}
	if cfg.CompactionThreshold == 0.0 {
		cfg.CompactionThreshold = 0.10
	}
	if cfg.CompactionThreshold >= 1.0 || cfg.CompactionThreshold <= 0.01 {
		cfg.CompactionThreshold = 0.10
	}
	if env := os.Getenv("GROUPSTORE_COMPACTION_AGE_THRESHOLD"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.CompactionAgeThreshold = val
		}
	}
	if cfg.CompactionAgeThreshold == 0 {
		cfg.CompactionAgeThreshold = 300
	}
	if cfg.CompactionAgeThreshold < 1 {
		cfg.CompactionAgeThreshold = 1
	}
	if env := os.Getenv("GROUPSTORE_DISK_FREE_DISABLE_THRESHOLD"); env != "" {
		if val, err := strconv.ParseUint(env, 10, 64); err == nil {
			cfg.DiskFreeDisableThreshold = val
		}
	}
	// NOTE: If the value is 1, that will disable the check
	if cfg.DiskFreeDisableThreshold == 0 {
		cfg.DiskFreeDisableThreshold = 8589934592
	}
	if env := os.Getenv("GROUPSTORE_DISK_FREE_REENABLE_THRESHOLD"); env != "" {
		if val, err := strconv.ParseUint(env, 10, 64); err == nil {
			cfg.DiskFreeReenableThreshold = val
		}
	}
	// NOTE: If the value is 1, that will disable the check
	if cfg.DiskFreeReenableThreshold == 0 {
		cfg.DiskFreeReenableThreshold = 17179869184
	}
	if env := os.Getenv("GROUPSTORE_DISK_USAGE_DISABLE_THRESHOLD"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			cfg.DiskUsageDisableThreshold = val
		}
	}
	if cfg.DiskUsageDisableThreshold == 0 {
		cfg.DiskUsageDisableThreshold = 0.94
	}
	if cfg.DiskUsageDisableThreshold < 0 {
		cfg.DiskUsageDisableThreshold = 0
	}
	if env := os.Getenv("GROUPSTORE_DISK_USAGE_REENABLE_THRESHOLD"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			cfg.DiskUsageReenableThreshold = val
		}
	}
	if cfg.DiskUsageReenableThreshold == 0 {
		cfg.DiskUsageReenableThreshold = 0.90
	}
	if cfg.DiskUsageReenableThreshold < 0 {
		cfg.DiskUsageReenableThreshold = 0
	}
	if env := os.Getenv("GROUPSTORE_FLUSHER_THRESHOLD"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.FlusherThreshold = int32(val)
		}
	}
	if cfg.FlusherThreshold == 0 {
		cfg.FlusherThreshold = int32(cfg.PageSize) / _GROUP_FILE_ENTRY_SIZE
	}
	if cfg.FlusherThreshold < 0 {
		cfg.FlusherThreshold = 0
	}
	if env := os.Getenv("GROUPSTORE_AUDIT_INTERVAL"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.AuditInterval = val
		}
	}
	if cfg.AuditInterval == 0 {
		cfg.AuditInterval = 604800
	}
	if cfg.AuditInterval < 1 {
		cfg.AuditInterval = 1
	}
	if env := os.Getenv("GROUPSTORE_AUDIT_AGE_THRESHOLD"); env != "" {
		if val, err := strconv.Atoi(env); err == nil {
			cfg.AuditAgeThreshold = val
		}
	}
	if cfg.AuditAgeThreshold == 0 {
		cfg.AuditAgeThreshold = 604800
	}
	if cfg.AuditAgeThreshold < 1 {
		cfg.AuditAgeThreshold = 1
	}
	if env := os.Getenv("GROUPSTORE_MEM_FREE_DISABLE_THRESHOLD"); env != "" {
		if val, err := strconv.ParseUint(env, 10, 64); err == nil {
			cfg.MemFreeDisableThreshold = val
		}
	}
	// NOTE: If the value is 1, that will disable the check
	if cfg.MemFreeDisableThreshold == 0 {
		cfg.MemFreeDisableThreshold = 134217728
	}
	if env := os.Getenv("GROUPSTORE_MEM_FREE_REENABLE_THRESHOLD"); env != "" {
		if val, err := strconv.ParseUint(env, 10, 64); err == nil {
			cfg.MemFreeReenableThreshold = val
		}
	}
	// NOTE: If the value is 1, that will disable the check
	if cfg.MemFreeReenableThreshold == 0 {
		cfg.MemFreeReenableThreshold = 268435456
	}
	if env := os.Getenv("GROUPSTORE_MEM_USAGE_DISABLE_THRESHOLD"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			cfg.MemUsageDisableThreshold = val
		}
	}
	if cfg.MemUsageDisableThreshold == 0 {
		cfg.MemUsageDisableThreshold = 0.94
	}
	if cfg.MemUsageDisableThreshold < 0 {
		cfg.MemUsageDisableThreshold = 0
	}
	if env := os.Getenv("GROUPSTORE_MEM_USAGE_REENABLE_THRESHOLD"); env != "" {
		if val, err := strconv.ParseFloat(env, 64); err == nil {
			cfg.MemUsageReenableThreshold = val
		}
	}
	if cfg.MemUsageReenableThreshold == 0 {
		cfg.MemUsageReenableThreshold = 0.90
	}
	if cfg.MemUsageReenableThreshold < 0 {
		cfg.MemUsageReenableThreshold = 0
	}

	if cfg.OpenReadSeeker == nil {
		cfg.OpenReadSeeker = osOpenReadSeeker
	}
	if cfg.OpenWriteSeeker == nil {
		cfg.OpenWriteSeeker = osOpenWriteSeeker
	}
	if cfg.Readdirnames == nil {
		cfg.Readdirnames = osReaddirnames
	}
	if cfg.CreateWriteCloser == nil {
		cfg.CreateWriteCloser = osCreateWriteCloser
	}
	if cfg.Stat == nil {
		cfg.Stat = os.Stat
	}
	if cfg.Remove == nil {
		cfg.Remove = os.Remove
	}
	if cfg.Rename == nil {
		cfg.Rename = os.Rename
	}
	if cfg.IsNotExist == nil {
		cfg.IsNotExist = os.IsNotExist
	}
	return cfg
}
