package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gholt/brimio"
	"github.com/gholt/locmap"
	"github.com/gholt/ring"
	"github.com/spaolacci/murmur3"
	"github.com/uber-go/zap"
	"golang.org/x/net/context"
)

// defaultGroupStore instances are created with NewGroupStore.
type defaultGroupStore struct {
	runningLock sync.Mutex
	// 0 = not running, 1 = running, 2 = can't run due to previous error
	running int

	logger                  zap.Logger
	loggerPrefix            string
	randMutex               sync.Mutex
	rand                    *rand.Rand
	freeableMemBlockChans   []chan *groupMemBlock
	freeMemBlockChan        chan *groupMemBlock
	freeWriteReqChans       []chan *groupWriteReq
	pendingWriteReqChans    []chan *groupWriteReq
	fileMemBlockChan        chan *groupMemBlock
	freeTOCBlockChan        chan *groupTOCBlock
	pendingTOCBlockChan     chan *groupTOCBlock
	activeTOCA              uint64
	activeTOCB              uint64
	flushedChan             chan struct{}
	shutdownChan            chan struct{}
	locBlocks               []groupLocBlock
	locBlockIDer            uint64
	path                    string
	pathtoc                 string
	locmap                  locmap.GroupLocMap
	workers                 int
	recoveryBatchSize       int
	valueCap                uint32
	pageSize                uint32
	minValueAlloc           int
	writePagesPerWorker     int
	fileCap                 uint32
	fileReaders             int
	checksumInterval        uint32
	msgRing                 ring.MsgRing
	tombstoneDiscardState   groupTombstoneDiscardState
	auditState              groupAuditState
	replicationIgnoreRecent uint64
	pullReplicationState    groupPullReplicationState
	pushReplicationState    groupPushReplicationState
	compactionState         groupCompactionState
	bulkSetState            groupBulkSetState
	bulkSetAckState         groupBulkSetAckState
	disableEnableWritesLock sync.Mutex
	readOnly                bool
	userDisabled            bool
	flusherState            groupFlusherState
	watcherState            groupWatcherState
	restartChan             chan error

	statsLock    sync.Mutex
	lookups      int32
	lookupErrors int32

	lookupGroups      int32
	lookupGroupItems  int32
	lookupGroupErrors int32

	reads      int32
	readErrors int32

	readGroups      int32
	readGroupItems  int32
	readGroupErrors int32

	writes                        int32
	writeErrors                   int32
	writesOverridden              int32
	deletes                       int32
	deleteErrors                  int32
	deletesOverridden             int32
	outBulkSets                   int32
	outBulkSetValues              int32
	outBulkSetPushes              int32
	outBulkSetPushValues          int32
	outPushReplicationNanoseconds int64
	inBulkSets                    int32
	inBulkSetDrops                int32
	inBulkSetInvalids             int32
	inBulkSetWrites               int32
	inBulkSetWriteErrors          int32
	inBulkSetWritesOverridden     int32
	outBulkSetAcks                int32
	inBulkSetAcks                 int32
	inBulkSetAckDrops             int32
	inBulkSetAckInvalids          int32
	inBulkSetAckWrites            int32
	inBulkSetAckWriteErrors       int32
	inBulkSetAckWritesOverridden  int32
	outPullReplications           int32
	outPullReplicationNanoseconds int64
	inPullReplications            int32
	inPullReplicationDrops        int32
	inPullReplicationInvalids     int32
	expiredDeletions              int32
	tombstoneDiscardNanoseconds   int64
	compactionNanoseconds         int64
	compactions                   int32
	smallFileCompactions          int32
	auditNanoseconds              int64

	// Used by the flusher only
	modifications int32

	openReadSeeker    func(fullPath string) (io.ReadSeeker, error)
	openWriteSeeker   func(fullPath string) (io.WriteSeeker, error)
	readdirnames      func(fullPath string) ([]string, error)
	createWriteCloser func(fullPath string) (io.WriteCloser, error)
	stat              func(fullPath string) (os.FileInfo, error)
	remove            func(fullPath string) error
	rename            func(oldFullPath string, newFullPath string) error
	isNotExist        func(err error) bool
}

type groupWriteReq struct {
	keyA uint64
	keyB uint64

	childKeyA uint64
	childKeyB uint64

	timestampbits uint64
	value         []byte
	errChan       chan error
	internal      bool
}

var enableGroupWriteReq *groupWriteReq = &groupWriteReq{}
var disableGroupWriteReq *groupWriteReq = &groupWriteReq{}
var flushGroupWriteReq *groupWriteReq = &groupWriteReq{}
var flushGroupMemBlock *groupMemBlock = &groupMemBlock{}
var flushGroupTOCBlock *groupTOCBlock = &groupTOCBlock{}
var shutdownGroupWriteReq *groupWriteReq = &groupWriteReq{}
var shutdownGroupMemBlock *groupMemBlock = &groupMemBlock{}
var shutdownGroupTOCBlock *groupTOCBlock = &groupTOCBlock{}

type groupTOCBlock struct {
	data []byte
}

type groupLocBlock interface {
	timestampnano() int64
	read(keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampbits uint64, offset uint32, length uint32, value []byte) (uint64, []byte, error)
	close() error
}

// NewGroupStore creates a GroupStore for use in storing []byte values
// referenced by 128 bit keys; the store, restart channel (chan error), or any
// error during construction is returned.
//
// The restart channel (chan error) should be read from continually during the
// life of the store and, upon any error from the channel, the store should be
// restarted with Shutdown and Startup. This restart procedure is needed when
// data on disk is detected as corrupted and cannot be easily recovered from; a
// restart will cause only good entries to be loaded therefore discarding any
// bad entries due to the corruption. A restart may also be requested if the
// store reaches an unrecoverable state, such as no longer being able to open
// new files.
//
// Note that a lot of buffering, multiple cores, and background processes can
// be in use and therefore Shutdown should be called prior to the process
// exiting to ensure all processing is done and the buffers are flushed.
func NewGroupStore(c *GroupStoreConfig) (GroupStore, chan error) {
	cfg := resolveGroupStoreConfig(c)
	_ = os.MkdirAll(cfg.Path, 0755)
	_ = os.MkdirAll(cfg.PathTOC, 0755)
	lcmap := cfg.GroupLocMap
	if lcmap == nil {
		lcmap = locmap.NewGroupLocMap(nil)
	}
	lcmap.SetInactiveMask(_TSB_INACTIVE)
	store := &defaultGroupStore{
		logger:                  cfg.Logger,
		loggerPrefix:            cfg.LoggerName, // may add "." below
		rand:                    cfg.Rand,
		path:                    cfg.Path,
		pathtoc:                 cfg.PathTOC,
		locmap:                  lcmap,
		workers:                 cfg.Workers,
		recoveryBatchSize:       cfg.RecoveryBatchSize,
		replicationIgnoreRecent: (uint64(cfg.ReplicationIgnoreRecent) * uint64(time.Second) / 1000) << _TSB_UTIL_BITS,
		valueCap:                uint32(cfg.ValueCap),
		pageSize:                uint32(cfg.PageSize),
		minValueAlloc:           cfg.minValueAlloc,
		writePagesPerWorker:     cfg.WritePagesPerWorker,
		fileCap:                 uint32(cfg.FileCap),
		fileReaders:             cfg.FileReaders,
		checksumInterval:        uint32(cfg.ChecksumInterval),
		msgRing:                 cfg.MsgRing,
		restartChan:             make(chan error),
		openReadSeeker:          cfg.OpenReadSeeker,
		openWriteSeeker:         cfg.OpenWriteSeeker,
		readdirnames:            cfg.Readdirnames,
		createWriteCloser:       cfg.CreateWriteCloser,
		stat:                    cfg.Stat,
		remove:                  cfg.Remove,
		rename:                  cfg.Rename,
		isNotExist:              cfg.IsNotExist,
	}
	if store.logger == nil {
		store.logger = zap.New(zap.NewJSONEncoder())
	}
	if store.loggerPrefix != "" {
		store.loggerPrefix += "."
	}
	store.tombstoneDiscardConfig(cfg)
	store.compactionConfig(cfg)
	store.auditConfig(cfg)
	store.pullReplicationConfig(cfg)
	store.pushReplicationConfig(cfg)
	store.bulkSetConfig(cfg)
	store.bulkSetAckConfig(cfg)
	store.flusherConfig(cfg)
	store.watcherConfig(cfg)
	return store, store.restartChan
}

func (store *defaultGroupStore) ValueCap(ctx context.Context) (uint32, error) {
	return store.valueCap, nil
}

func (store *defaultGroupStore) Startup(ctx context.Context) error {
	store.runningLock.Lock()
	switch store.running {
	case 0: // not running
	case 1: // running
		store.runningLock.Unlock()
		return nil
	case 2: // can't run due to previous error
		store.runningLock.Unlock()
		return errors.New("can't Startup due to previous Startup error")
	}
	store.locBlocks = make([]groupLocBlock, math.MaxUint16)
	store.locBlockIDer = 0
	// freeableMemBlockChans is a slice of channels so that the individual
	// memClearers can be communicated with later (flushes, etc.)
	store.freeableMemBlockChans = make([]chan *groupMemBlock, store.workers)
	for i := 0; i < cap(store.freeableMemBlockChans); i++ {
		store.freeableMemBlockChans[i] = make(chan *groupMemBlock, store.workers)
	}
	store.freeMemBlockChan = make(chan *groupMemBlock, store.workers*store.writePagesPerWorker)
	store.freeWriteReqChans = make([]chan *groupWriteReq, store.workers)
	store.pendingWriteReqChans = make([]chan *groupWriteReq, store.workers)
	store.fileMemBlockChan = make(chan *groupMemBlock, store.workers)
	store.freeTOCBlockChan = make(chan *groupTOCBlock, store.workers*2)
	store.pendingTOCBlockChan = make(chan *groupTOCBlock, store.workers)
	store.activeTOCA = 0
	store.activeTOCB = 0
	store.flushedChan = make(chan struct{}, 1)
	store.shutdownChan = make(chan struct{}, 1)
	for i := 0; i < cap(store.freeMemBlockChan); i++ {
		memBlock := &groupMemBlock{
			store:  store,
			toc:    make([]byte, 0, store.pageSize),
			values: make([]byte, 0, store.pageSize),
		}
		var err error
		memBlock.id, err = store.addLocBlock(memBlock)
		if err != nil {
			store.running = 2 // can't run due to previous error
			store.runningLock.Unlock()
			return err
		}
		store.freeMemBlockChan <- memBlock
	}
	for i := 0; i < len(store.freeWriteReqChans); i++ {
		store.freeWriteReqChans[i] = make(chan *groupWriteReq, store.workers*2)
		for j := 0; j < store.workers*2; j++ {
			store.freeWriteReqChans[i] <- &groupWriteReq{errChan: make(chan error, 1)}
		}
	}
	for i := 0; i < len(store.pendingWriteReqChans); i++ {
		store.pendingWriteReqChans[i] = make(chan *groupWriteReq)
	}
	for i := 0; i < cap(store.freeTOCBlockChan); i++ {
		store.freeTOCBlockChan <- &groupTOCBlock{make([]byte, 0, store.pageSize)}
	}
	go store.tocWriter()
	go store.fileWriter()
	for i := 0; i < len(store.freeableMemBlockChans); i++ {
		go store.memClearer(store.freeableMemBlockChans[i])
	}
	for i := 0; i < len(store.pendingWriteReqChans); i++ {
		go store.memWriter(store.pendingWriteReqChans[i])
	}
	err := store.recovery()
	if err != nil {
		store.running = 2 // can't run due to previous error
		store.runningLock.Unlock()
		return err
	}
	wg := &sync.WaitGroup{}
	for i, f := range []func(){
		store.auditStartup,
		store.bulkSetStartup,
		store.bulkSetAckStartup,
		store.compactionStartup,
		store.watcherStartup,
		store.flusherStartup,
		store.pullReplicationStartup,
		store.pushReplicationStartup,
		store.tombstoneDiscardStartup,
	} {
		wg.Add(1)
		go func(ii int, ff func()) {
			ff()
			wg.Done()
		}(i, f)
	}
	wg.Wait()
	store.EnableWrites(ctx)
	store.running = 1 // running
	store.runningLock.Unlock()
	return nil
}

func (store *defaultGroupStore) Shutdown(ctx context.Context) error {
	store.runningLock.Lock()
	if store.running != 1 { // running
		store.runningLock.Unlock()
		return nil
	}
	wg := &sync.WaitGroup{}
	for i, f := range []func(){
		store.auditShutdown,
		store.bulkSetShutdown,
		store.bulkSetAckShutdown,
		store.compactionShutdown,
		store.watcherShutdown,
		store.flusherShutdown,
		store.pullReplicationShutdown,
		store.pushReplicationShutdown,
		store.tombstoneDiscardShutdown,
	} {
		wg.Add(1)
		go func(ii int, ff func()) {
			ff()
			wg.Done()
		}(i, f)
	}
	wg.Wait()
	store.DisableWrites(ctx)
	for _, c := range store.pendingWriteReqChans {
		c <- shutdownGroupWriteReq
	}
	<-store.shutdownChan
	store.locmap.Clear()
	store.locBlocks = nil
	store.freeableMemBlockChans = nil
	store.freeMemBlockChan = nil
	store.freeWriteReqChans = nil
	store.pendingWriteReqChans = nil
	store.fileMemBlockChan = nil
	store.freeTOCBlockChan = nil
	store.pendingTOCBlockChan = nil
	store.flushedChan = nil
	store.shutdownChan = nil
	store.running = 0 // not running
	store.runningLock.Unlock()
	return nil
}

func (store *defaultGroupStore) EnableWrites(ctx context.Context) error {
	store.enableWrites(true)
	return nil
}

func (store *defaultGroupStore) enableWrites(userCall bool) {
	store.disableEnableWritesLock.Lock()
	store.readOnly = false
	if userCall || !store.userDisabled {
		store.userDisabled = false
		for _, c := range store.pendingWriteReqChans {
			c <- enableGroupWriteReq
		}
	}
	store.disableEnableWritesLock.Unlock()
}

func (store *defaultGroupStore) DisableWrites(ctx context.Context) error {
	store.disableWrites(true)
	return nil
}

func (store *defaultGroupStore) disableWrites(userCall bool) {
	store.disableEnableWritesLock.Lock()
	store.readOnly = true
	if userCall {
		store.userDisabled = true
	}
	for _, c := range store.pendingWriteReqChans {
		c <- disableGroupWriteReq
	}
	store.disableEnableWritesLock.Unlock()
}

func (store *defaultGroupStore) Flush(ctx context.Context) error {
	for _, c := range store.pendingWriteReqChans {
		c <- flushGroupWriteReq
	}
	<-store.flushedChan
	return nil
}

func (store *defaultGroupStore) Lookup(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64) (int64, uint32, error) {
	atomic.AddInt32(&store.lookups, 1)
	timestampbits, _, length, err := store.lookup(keyA, keyB, childKeyA, childKeyB)
	if err != nil && err != errNotFound {
		atomic.AddInt32(&store.lookupErrors, 1)
	}
	return int64(timestampbits >> _TSB_UTIL_BITS), length, err
}

func (store *defaultGroupStore) lookup(keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64) (uint64, uint32, uint32, error) {
	timestampbits, id, _, length := store.locmap.Get(keyA, keyB, childKeyA, childKeyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 {
		return timestampbits, id, 0, errNotFound
	}
	return timestampbits, id, length, nil
}

func (store *defaultGroupStore) LookupGroup(ctx context.Context, keyA uint64, keyB uint64) ([]LookupGroupItem, error) {
	// Returned []LookupGroupItem is not a []* for less garbage collection and
	// is likely fine most use cases.
	atomic.AddInt32(&store.lookupGroups, 1)
	items := store.locmap.GetGroup(keyA, keyB)
	if len(items) == 0 {
		return nil, nil
	}
	rv := make([]LookupGroupItem, len(items))
	i := 0
	for _, item := range items {
		if item.Timestamp&_TSB_DELETION == 0 {
			rv[i].ChildKeyA = item.ChildKeyA
			rv[i].ChildKeyB = item.ChildKeyB
			rv[i].TimestampMicro = int64(item.Timestamp >> _TSB_UTIL_BITS)
			rv[i].Length = item.Length
			i++
		}
	}
	atomic.AddInt32(&store.lookupGroupItems, int32(i))
	return rv[:i], nil
}

func (store *defaultGroupStore) ReadGroup(ctx context.Context, keyA uint64, keyB uint64) ([]ReadGroupItem, error) {
	// Returned []ReadGroupItem is not a []* for less garbage collection and
	// is likely fine most use cases.
	atomic.AddInt32(&store.readGroups, 1)
	items := store.locmap.GetGroup(keyA, keyB)
	if len(items) == 0 {
		return nil, nil
	}
	rv := make([]ReadGroupItem, len(items))
	i := 0
	for _, item := range items {
		timestampMicro, value, err := store.read(keyA, keyB, item.ChildKeyA, item.ChildKeyB, nil)
		if err != nil && err != errNotFound {
			atomic.AddInt32(&store.readGroupErrors, 1)
		}
		if err == nil && timestampMicro&_TSB_DELETION == 0 {
			rv[i].ChildKeyA = item.ChildKeyA
			rv[i].ChildKeyB = item.ChildKeyB
			rv[i].TimestampMicro = int64(timestampMicro >> _TSB_UTIL_BITS)
			rv[i].Value = value
			i++
		}
	}
	atomic.AddInt32(&store.readGroupItems, int32(i))
	return rv[:i], nil
}

func (store *defaultGroupStore) Read(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, value []byte) (int64, []byte, error) {
	atomic.AddInt32(&store.reads, 1)
	timestampbits, value, err := store.read(keyA, keyB, childKeyA, childKeyB, value)
	if err != nil && err != errNotFound {
		atomic.AddInt32(&store.readErrors, 1)
	}
	return int64(timestampbits >> _TSB_UTIL_BITS), value, err
}

func (store *defaultGroupStore) read(keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, value []byte) (uint64, []byte, error) {
	timestampbits, id, offset, length := store.locmap.Get(keyA, keyB, childKeyA, childKeyB)
	if id == 0 || timestampbits&_TSB_DELETION != 0 || timestampbits&_TSB_LOCAL_REMOVAL != 0 {
		return timestampbits, value, errNotFound
	}
	return store.locBlock(id).read(keyA, keyB, childKeyA, childKeyB, timestampbits, offset, length, value)
}

func (store *defaultGroupStore) Write(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampmicro int64, value []byte) (int64, error) {
	atomic.AddInt32(&store.writes, 1)
	if timestampmicro < TIMESTAMPMICRO_MIN {
		atomic.AddInt32(&store.writeErrors, 1)
		return 0, fmt.Errorf("timestamp %d < %d", timestampmicro, TIMESTAMPMICRO_MIN)
	}
	if timestampmicro > TIMESTAMPMICRO_MAX {
		atomic.AddInt32(&store.writeErrors, 1)
		return 0, fmt.Errorf("timestamp %d > %d", timestampmicro, TIMESTAMPMICRO_MAX)
	}
	timestampbits, err := store.write(keyA, keyB, childKeyA, childKeyB, uint64(timestampmicro)<<_TSB_UTIL_BITS, value, false)
	if err != nil {
		atomic.AddInt32(&store.writeErrors, 1)
	} else if timestampmicro <= int64(timestampbits>>_TSB_UTIL_BITS) {
		atomic.AddInt32(&store.writesOverridden, 1)
	}
	return int64(timestampbits >> _TSB_UTIL_BITS), err
}

func (store *defaultGroupStore) write(keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampbits uint64, value []byte, internal bool) (uint64, error) {
	i := int(keyA>>1) % len(store.freeWriteReqChans)
	writeReq := <-store.freeWriteReqChans[i]
	writeReq.keyA = keyA
	writeReq.keyB = keyB

	writeReq.childKeyA = childKeyA
	writeReq.childKeyB = childKeyB

	writeReq.timestampbits = timestampbits
	writeReq.value = value
	writeReq.internal = internal
	store.pendingWriteReqChans[i] <- writeReq
	err := <-writeReq.errChan
	ptimestampbits := writeReq.timestampbits
	writeReq.value = nil
	store.freeWriteReqChans[i] <- writeReq
	// This is for the flusher
	if err == nil && ptimestampbits < timestampbits {
		atomic.AddInt32(&store.modifications, 1)
	}
	return ptimestampbits, err
}

func (store *defaultGroupStore) Delete(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampmicro int64) (int64, error) {
	atomic.AddInt32(&store.deletes, 1)
	if timestampmicro < TIMESTAMPMICRO_MIN {
		atomic.AddInt32(&store.deleteErrors, 1)
		return 0, fmt.Errorf("timestamp %d < %d", timestampmicro, TIMESTAMPMICRO_MIN)
	}
	if timestampmicro > TIMESTAMPMICRO_MAX {
		atomic.AddInt32(&store.deleteErrors, 1)
		return 0, fmt.Errorf("timestamp %d > %d", timestampmicro, TIMESTAMPMICRO_MAX)
	}
	ptimestampbits, err := store.write(keyA, keyB, childKeyA, childKeyB, (uint64(timestampmicro)<<_TSB_UTIL_BITS)|_TSB_DELETION, nil, true)
	if err != nil {
		atomic.AddInt32(&store.deleteErrors, 1)
	} else if timestampmicro <= int64(ptimestampbits>>_TSB_UTIL_BITS) {
		atomic.AddInt32(&store.deletesOverridden, 1)
	}
	return int64(ptimestampbits >> _TSB_UTIL_BITS), err
}

func (store *defaultGroupStore) locBlock(locBlockID uint32) groupLocBlock {
	return store.locBlocks[locBlockID]
}

func (store *defaultGroupStore) addLocBlock(block groupLocBlock) (uint32, error) {
	id := atomic.AddUint64(&store.locBlockIDer, 1)
	// TODO: We should probably issue a restart request if
	// id >= math.MaxUint32 / 2 since it's almost certainly not the case that
	// there are too many on-disk files, just that the process has been running
	// long enough to chew through ids. Issuing the restart at half the true
	// max would all but guarantee a restart occurs before reaching the true
	// max.
	if id >= math.MaxUint32 {
		return 0, errors.New("too many loc blocks")
	}
	store.locBlocks[id] = block
	return uint32(id), nil
}

func (store *defaultGroupStore) locBlockIDFromTimestampnano(tsn int64) uint32 {
	for i := 1; i <= len(store.locBlocks); i++ {
		if store.locBlocks[i] == nil {
			return 0
		} else {
			if tsn == store.locBlocks[i].timestampnano() {
				return uint32(i)
			}
		}
	}
	return 0
}

func (store *defaultGroupStore) closeLocBlock(locBlockID uint32) error {
	return store.locBlocks[locBlockID].close()
}

func (store *defaultGroupStore) memClearer(freeableMemBlockChan chan *groupMemBlock) {
	var tb *groupTOCBlock
	var tbTS int64
	var tbOffset int
	for {
		memBlock := <-freeableMemBlockChan
		if memBlock == flushGroupMemBlock || memBlock == shutdownGroupMemBlock {
			if tb != nil {
				store.pendingTOCBlockChan <- tb
				tb = nil
			}
			if memBlock == flushGroupMemBlock {
				store.pendingTOCBlockChan <- flushGroupTOCBlock
				continue
			}
			store.pendingTOCBlockChan <- shutdownGroupTOCBlock
			break
		}
		fl := store.locBlock(memBlock.fileID)
		if tb != nil && tbTS != fl.timestampnano() {
			store.pendingTOCBlockChan <- tb
			tb = nil
		}
		for memBlockTOCOffset := 0; memBlockTOCOffset < len(memBlock.toc); memBlockTOCOffset += _GROUP_FILE_ENTRY_SIZE {

			keyA := binary.BigEndian.Uint64(memBlock.toc[memBlockTOCOffset:])
			keyB := binary.BigEndian.Uint64(memBlock.toc[memBlockTOCOffset+8:])
			childKeyA := binary.BigEndian.Uint64(memBlock.toc[memBlockTOCOffset+16:])
			childKeyB := binary.BigEndian.Uint64(memBlock.toc[memBlockTOCOffset+24:])
			timestampbits := binary.BigEndian.Uint64(memBlock.toc[memBlockTOCOffset+32:])

			var blockID uint32
			var offset uint32
			var length uint32
			if timestampbits&_TSB_LOCAL_REMOVAL == 0 {
				blockID = memBlock.fileID

				offset = memBlock.fileOffset + binary.BigEndian.Uint32(memBlock.toc[memBlockTOCOffset+40:])
				length = binary.BigEndian.Uint32(memBlock.toc[memBlockTOCOffset+44:])

			}
			if store.locmap.Set(keyA, keyB, childKeyA, childKeyB, timestampbits, blockID, offset, length, true) > timestampbits {
				continue
			}
			if tb != nil && tbOffset+_GROUP_FILE_ENTRY_SIZE > cap(tb.data) {
				store.pendingTOCBlockChan <- tb
				tb = nil
			}
			if tb == nil {
				tb = <-store.freeTOCBlockChan
				tbTS = fl.timestampnano()
				tb.data = tb.data[:8]
				binary.BigEndian.PutUint64(tb.data, uint64(tbTS))
				tbOffset = 8
			}
			tb.data = tb.data[:tbOffset+_GROUP_FILE_ENTRY_SIZE]
			tbd := tb.data[tbOffset : tbOffset+_GROUP_FILE_ENTRY_SIZE]

			binary.BigEndian.PutUint64(tbd, keyA)
			binary.BigEndian.PutUint64(tbd[8:], keyB)
			binary.BigEndian.PutUint64(tbd[16:], childKeyA)
			binary.BigEndian.PutUint64(tbd[24:], childKeyB)
			binary.BigEndian.PutUint64(tbd[32:], timestampbits)
			binary.BigEndian.PutUint32(tbd[40:], offset)
			binary.BigEndian.PutUint32(tbd[44:], length)

			tbOffset += _GROUP_FILE_ENTRY_SIZE
		}
		memBlock.discardLock.Lock()
		memBlock.fileID = 0
		memBlock.fileOffset = 0
		memBlock.toc = memBlock.toc[:0]
		memBlock.values = memBlock.values[:0]
		memBlock.discardLock.Unlock()
		store.freeMemBlockChan <- memBlock
	}
}

func (store *defaultGroupStore) memWriter(pendingWriteReqChan chan *groupWriteReq) {
	var enabled bool
	var memBlock *groupMemBlock
	var memBlockTOCOffset int
	var memBlockMemOffset int
	for {
		writeReq := <-pendingWriteReqChan
		if writeReq == enableGroupWriteReq {
			enabled = true
			continue
		}
		if writeReq == disableGroupWriteReq {
			enabled = false
			continue
		}
		if writeReq == flushGroupWriteReq || writeReq == shutdownGroupWriteReq {
			if memBlock != nil && len(memBlock.toc) > 0 {
				store.fileMemBlockChan <- memBlock
				memBlock = nil
			}
			if writeReq == flushGroupWriteReq {
				store.fileMemBlockChan <- flushGroupMemBlock
				continue
			}
			store.fileMemBlockChan <- shutdownGroupMemBlock
			break
		}
		if !enabled && !writeReq.internal {
			writeReq.errChan <- errDisabled
			continue
		}
		length := len(writeReq.value)
		if length > int(store.valueCap) {
			writeReq.errChan <- fmt.Errorf("value length of %d > %d", length, store.valueCap)
			continue
		}
		alloc := length
		if alloc < store.minValueAlloc {
			alloc = store.minValueAlloc
		}
		if memBlock != nil && (memBlockTOCOffset+_GROUP_FILE_ENTRY_SIZE > cap(memBlock.toc) || memBlockMemOffset+alloc > cap(memBlock.values)) {
			store.fileMemBlockChan <- memBlock
			memBlock = nil
		}
		if memBlock == nil {
			memBlock = <-store.freeMemBlockChan
			memBlockTOCOffset = 0
			memBlockMemOffset = 0
		}
		memBlock.discardLock.Lock()
		memBlock.values = memBlock.values[:memBlockMemOffset+alloc]
		memBlock.discardLock.Unlock()
		copy(memBlock.values[memBlockMemOffset:], writeReq.value)
		if alloc > length {
			for i, j := memBlockMemOffset+length, memBlockMemOffset+alloc; i < j; i++ {
				memBlock.values[i] = 0
			}
		}
		ptimestampbits := store.locmap.Set(writeReq.keyA, writeReq.keyB, writeReq.childKeyA, writeReq.childKeyB, writeReq.timestampbits & ^uint64(_TSB_COMPACTION_REWRITE), memBlock.id, uint32(memBlockMemOffset), uint32(length), writeReq.timestampbits&_TSB_COMPACTION_REWRITE != 0)
		if ptimestampbits < writeReq.timestampbits {
			memBlock.toc = memBlock.toc[:memBlockTOCOffset+_GROUP_FILE_ENTRY_SIZE]

			binary.BigEndian.PutUint64(memBlock.toc[memBlockTOCOffset:], writeReq.keyA)
			binary.BigEndian.PutUint64(memBlock.toc[memBlockTOCOffset+8:], writeReq.keyB)
			binary.BigEndian.PutUint64(memBlock.toc[memBlockTOCOffset+16:], writeReq.childKeyA)
			binary.BigEndian.PutUint64(memBlock.toc[memBlockTOCOffset+24:], writeReq.childKeyB)
			binary.BigEndian.PutUint64(memBlock.toc[memBlockTOCOffset+32:], writeReq.timestampbits & ^uint64(_TSB_COMPACTION_REWRITE))
			binary.BigEndian.PutUint32(memBlock.toc[memBlockTOCOffset+40:], uint32(memBlockMemOffset))
			binary.BigEndian.PutUint32(memBlock.toc[memBlockTOCOffset+44:], uint32(length))

			memBlockTOCOffset += _GROUP_FILE_ENTRY_SIZE
			memBlockMemOffset += alloc
		} else {
			memBlock.discardLock.Lock()
			memBlock.values = memBlock.values[:memBlockMemOffset]
			memBlock.discardLock.Unlock()
		}
		writeReq.timestampbits = ptimestampbits
		writeReq.errChan <- nil
	}
}

func (store *defaultGroupStore) fileWriter() {
	var fl *groupStoreFile
	memWritersFlushLeft := len(store.pendingWriteReqChans)
	memWritersShutdownLeft := len(store.pendingWriteReqChans)
	var tocLen uint64
	var valueLen uint64
	var disabledDueToError error
	freeableMemBlockChanIndex := 0
	var disabledDueToErrorLogTime time.Time
	for {
		memBlock := <-store.fileMemBlockChan
		if memBlock == flushGroupMemBlock || memBlock == shutdownGroupMemBlock {
			if memBlock == flushGroupMemBlock {
				memWritersFlushLeft--
				if memWritersFlushLeft > 0 {
					continue
				}
			} else {
				memWritersShutdownLeft--
				if memWritersShutdownLeft > 0 {
					continue
				}
			}
			if fl != nil {
				err := fl.closeWriting()
				if err != nil {
					// TODO: Trigger an audit based on this file being in an
					// unknown state.
					store.logger.Warn("error closing", zap.String("name", store.loggerPrefix+"fileWriter"), zap.String("path", fl.fullPath), zap.Error(err))
				}
				fl = nil
			}
			if memBlock == flushGroupMemBlock {
				for i := 0; i < len(store.freeableMemBlockChans); i++ {
					store.freeableMemBlockChans[i] <- flushGroupMemBlock
				}
				memWritersFlushLeft = len(store.pendingWriteReqChans)
				continue
			}
			// This loop is reversed so there isn't a race condition in the
			// loop check; if you use the usual loop of 0 through len(x), the
			// use of x in the len will race.
			for i := len(store.freeableMemBlockChans) - 1; i >= 0; i-- {
				store.freeableMemBlockChans[i] <- shutdownGroupMemBlock
			}
			break
		}
		if disabledDueToError != nil {
			if disabledDueToErrorLogTime.Before(time.Now()) {
				store.logger.Warn("disabled due to previous critical error", zap.String("name", store.loggerPrefix+"fileWriter"), zap.Error(disabledDueToError))
				disabledDueToErrorLogTime = time.Now().Add(5 * time.Minute)
			}
			store.freeableMemBlockChans[freeableMemBlockChanIndex] <- memBlock
			freeableMemBlockChanIndex++
			if freeableMemBlockChanIndex >= len(store.freeableMemBlockChans) {
				freeableMemBlockChanIndex = 0
			}
			continue
		}
		if fl != nil && (tocLen+uint64(len(memBlock.toc)) >= uint64(store.fileCap) || valueLen+uint64(len(memBlock.values)) > uint64(store.fileCap)) {
			err := fl.closeWriting()
			if err != nil {
				// TODO: Trigger an audit based on this file being in an
				// unknown state.
				store.logger.Warn("error closing", zap.String("name", store.loggerPrefix+"fileWriter"), zap.String("path", fl.fullPath), zap.Error(err))
			}
			fl = nil
		}
		if fl == nil {
			var err error
			fl, err = store.createGroupReadWriteFile()
			if err != nil {
				store.logger.Error("must shutdown because no new files can be opened", zap.String("name", store.loggerPrefix+"fileWriter"), zap.Error(err))
				disabledDueToError = err
				disabledDueToErrorLogTime = time.Now().Add(5 * time.Minute)
				go func() {
					store.Shutdown(context.Background())
					store.restartChan <- errors.New("no new files can be opened")
				}()
			}
			tocLen = _GROUP_FILE_HEADER_SIZE
			valueLen = _GROUP_FILE_HEADER_SIZE
		}
		fl.write(memBlock)
		tocLen += uint64(len(memBlock.toc))
		valueLen += uint64(len(memBlock.values))
	}
}

func (store *defaultGroupStore) tocWriter() {
	memClearersFlushLeft := len(store.freeableMemBlockChans)
	memClearersShutdownLeft := len(store.freeableMemBlockChans)
	// writerA is the current toc file while writerB is the previously active
	// toc writerB is kept around in case a "late" key arrives to be flushed
	// whom's value is actually in the previous value file.
	var writerA io.WriteCloser
	var offsetA uint64
	var writerB io.WriteCloser
	var offsetB uint64
	var err error
	head := []byte("GROUPSTORETOC v0                ")
	binary.BigEndian.PutUint32(head[28:], uint32(store.checksumInterval))
	// Make sure any trailing data is covered by a checksum by writing an
	// additional block of zeros (entry offsets of zero are ignored on
	// recovery).
	term := make([]byte, store.checksumInterval)
	copy(term[len(term)-8:], []byte("TERM v0 "))
	disabled := false
	fatal := func(point int, err error) {
		store.logger.Error("error while writing toc contents", zap.String("name", store.loggerPrefix+"tocWriter"), zap.Int("point", point), zap.Error(err))
		disabled = true
		go func() {
			store.Shutdown(context.Background())
			store.restartChan <- errors.New("tocWriter encountered a fatal error; restart required")
		}()
	}
OuterLoop:
	for {
		t := <-store.pendingTOCBlockChan
		if t == flushGroupTOCBlock || t == shutdownGroupTOCBlock {
			if t == flushGroupTOCBlock {
				memClearersFlushLeft--
				if memClearersFlushLeft > 0 {
					continue OuterLoop
				}
			} else {
				memClearersShutdownLeft--
				if memClearersShutdownLeft > 0 {
					continue OuterLoop
				}
			}
			if writerB != nil {
				if _, err = writerB.Write(term); err != nil {
					fatal(1, err)
					continue OuterLoop
				}
				if err = writerB.Close(); err != nil {
					fatal(2, err)
					continue OuterLoop
				}
				writerB = nil
				atomic.StoreUint64(&store.activeTOCB, 0)
				offsetB = 0
			}
			if writerA != nil {
				if _, err = writerA.Write(term); err != nil {
					fatal(3, err)
					continue OuterLoop
				}
				if err = writerA.Close(); err != nil {
					fatal(4, err)
					continue OuterLoop
				}
				writerA = nil
				atomic.StoreUint64(&store.activeTOCA, 0)
				offsetA = 0
			}
			if t == flushGroupTOCBlock {
				store.flushedChan <- struct{}{}
				memClearersFlushLeft = len(store.freeableMemBlockChans)
				continue OuterLoop
			}
			store.shutdownChan <- struct{}{}
			break
		}
		if disabled {
			store.freeTOCBlockChan <- t
			continue OuterLoop
		}
		if len(t.data) > 8 {
			bts := binary.BigEndian.Uint64(t.data)
			switch bts {
			case atomic.LoadUint64(&store.activeTOCA):
				if _, err = writerA.Write(t.data[8:]); err != nil {
					fatal(5, err)
					continue OuterLoop
				}
				offsetA += uint64(len(t.data) - 8)
			case atomic.LoadUint64(&store.activeTOCB):
				if _, err = writerB.Write(t.data[8:]); err != nil {
					fatal(6, err)
					continue OuterLoop
				}
				offsetB += uint64(len(t.data) - 8)
			default:
				// An assumption is made here: If the timestampnano for this
				// toc block doesn't match the last two seen timestampnanos
				// then we expect no more toc blocks for the oldest
				// timestampnano and can close that toc file.
				if writerB != nil {
					if _, err = writerB.Write(term); err != nil {
						fatal(7, err)
						continue OuterLoop
					}
					if err = writerB.Close(); err != nil {
						fatal(8, err)
						continue OuterLoop
					}
				}
				atomic.StoreUint64(&store.activeTOCB, atomic.LoadUint64(&store.activeTOCA))
				writerB = writerA
				offsetB = offsetA
				atomic.StoreUint64(&store.activeTOCA, bts)
				var fp io.WriteCloser
				fp, err = store.createWriteCloser(path.Join(store.pathtoc, fmt.Sprintf("%d.grouptoc", bts)))
				if err != nil {
					fatal(9, err)
					continue OuterLoop
				}
				writerA = brimio.NewMultiCoreChecksummedWriter(fp, int(store.checksumInterval), murmur3.New32, store.workers)
				if _, err = writerA.Write(head); err != nil {
					fatal(10, err)
					continue OuterLoop
				}
				if _, err = writerA.Write(t.data[8:]); err != nil {
					fatal(11, err)
					continue OuterLoop
				}
				offsetA = _GROUP_FILE_HEADER_SIZE + uint64(len(t.data)-8)
			}
		}
		store.freeTOCBlockChan <- t
	}
	if writerA != nil {
		writerA.Close()
	}
	if writerB != nil {
		writerB.Close()
	}
}

func (store *defaultGroupStore) recovery() error {
	start := time.Now()
	causedChangeCount := int64(0)
	workers := uint64(store.workers)
	pendingBatchChans := make([]chan []groupTOCEntry, workers)
	freeBatchChans := make([]chan []groupTOCEntry, len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		pendingBatchChans[i] = make(chan []groupTOCEntry, 3)
		freeBatchChans[i] = make(chan []groupTOCEntry, cap(pendingBatchChans[i]))
		for j := 0; j < cap(freeBatchChans[i]); j++ {
			freeBatchChans[i] <- make([]groupTOCEntry, store.recoveryBatchSize)
		}
	}
	var encounteredValues int64
	wg := &sync.WaitGroup{}
	wg.Add(len(pendingBatchChans))
	for i := 0; i < len(pendingBatchChans); i++ {
		go func(pendingBatchChan chan []groupTOCEntry, freeBatchChan chan []groupTOCEntry) {
			for {
				batch := <-pendingBatchChan
				if batch == nil {
					break
				}
				for j := 0; j < len(batch); j++ {
					wr := &batch[j]
					if wr.TimestampBits&_TSB_LOCAL_REMOVAL != 0 {
						wr.BlockID = 0
					}
					atomic.AddInt64(&encounteredValues, 1)
					if cm := store.logger.Check(zap.DebugLevel, "debug?"); cm.OK() {
						if store.locmap.Set(wr.KeyA, wr.KeyB, wr.ChildKeyA, wr.ChildKeyB, wr.TimestampBits, wr.BlockID, wr.Offset, wr.Length, true) < wr.TimestampBits {
							atomic.AddInt64(&causedChangeCount, 1)
						}
					} else {
						store.locmap.Set(wr.KeyA, wr.KeyB, wr.ChildKeyA, wr.ChildKeyB, wr.TimestampBits, wr.BlockID, wr.Offset, wr.Length, true)
					}
				}
				freeBatchChan <- batch
			}
			wg.Done()
		}(pendingBatchChans[i], freeBatchChans[i])
	}
	spindown := func() {
		for i := 0; i < len(pendingBatchChans); i++ {
			pendingBatchChans[i] <- nil
		}
		wg.Wait()
	}
	names, err := store.readdirnames(store.pathtoc)
	if err != nil {
		spindown()
		return err
	}
	sort.Strings(names)
	fromDiskCount := 0
	var compactNames []string
	var compactBlockIDs []uint32
	for i := 0; i < len(names); i++ {
		if !strings.HasSuffix(names[i], ".grouptoc") {
			continue
		}
		namets := int64(0)
		if namets, err = strconv.ParseInt(names[i][:len(names[i])-len(".grouptoc")], 10, 64); err != nil {
			store.logger.Warn("bad timestamp in name", zap.String("name", store.loggerPrefix+"recovery"), zap.String("filename", names[i]))
			continue
		}
		if namets == 0 {
			store.logger.Warn("bad timestamp in name", zap.String("name", store.loggerPrefix+"recovery"), zap.String("filename", names[i]))
			continue
		}
		fpr, err := store.openReadSeeker(path.Join(store.pathtoc, names[i]))
		if err != nil {
			store.logger.Warn("error opening", zap.String("name", store.loggerPrefix+"recovery"), zap.String("filename", names[i]), zap.Error(err))
			continue
		}
		fl, err := store.newGroupReadFile(namets)
		if err != nil {
			store.logger.Warn("error opening", zap.String("name", store.loggerPrefix+"recovery"), zap.String("filename", names[i][:len(names[i])-3]), zap.Error(err))
			closeIfCloser(fpr)
			continue
		}
		fdc, errs := groupReadTOCEntriesBatched(fpr, fl.id, freeBatchChans, pendingBatchChans, make(chan struct{}))
		fromDiskCount += fdc
		for _, err := range errs {
			store.logger.Warn("error performing ReadTOCEntriesBatched", zap.String("name", store.loggerPrefix+"recovery"), zap.String("filename", names[i]), zap.Error(err))
			// TODO: The auditor should catch this eventually, but we should be
			// proactive and notify the auditor of the issue here.
		}
		if len(errs) != 0 {
			compactNames = append(compactNames, names[i])
			compactBlockIDs = append(compactBlockIDs, fl.id)
		}
		closeIfCloser(fpr)
	}
	spindown()
	if cm := store.logger.Check(zap.DebugLevel, "stats"); cm.OK() {
		dur := time.Now().Sub(start)
		stringerStats, err := store.Stats(context.Background(), false)
		if err != nil {
			store.logger.Warn("stats error", zap.String("name", store.loggerPrefix+"recovery"), zap.Error(err))
		} else {
			stats := stringerStats.(*GroupStoreStats)
			cm.Write(zap.String("name", store.loggerPrefix+"recovery"), zap.Int("keyLocationsLoaded", fromDiskCount), zap.Duration("duration", dur), zap.Float64("perSecond", float64(fromDiskCount)/(float64(dur)/float64(time.Second))), zap.Int64("causedChange", causedChangeCount), zap.Uint64("resultingLocations", stats.Values), zap.Uint64("resultingBytesReferenced", stats.ValueBytes))
		}
	}
	if len(compactNames) > 0 {
		store.logger.Debug("secondary recovery started", zap.String("name", store.loggerPrefix+"recovery"), zap.Int("fileCount", len(compactNames)))
		for i, name := range compactNames {
			store.compactFile(name, compactBlockIDs[i], make(chan struct{}), "recovery")
		}
		store.logger.Debug("secondary recovery completed", zap.String("name", store.loggerPrefix+"recovery"))
	}
	store.logger.Debug("recovery complete", zap.Int64("encounteredValues", encounteredValues))
	return nil
}
