package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// bsm: senderNodeID:8 entries:n
// bsm entry: keyA:8, keyB:8, childKeyA:8, childKeyB:8, timestampbits:8, length:4, value:n
const _GROUP_BULK_SET_MSG_TYPE = 0xbe53367e1994c262
const _GROUP_BULK_SET_MSG_HEADER_LENGTH = 8
const _GROUP_BULK_SET_MSG_ENTRY_HEADER_LENGTH = 44
const _GROUP_BULK_SET_MSG_MIN_ENTRY_LENGTH = 44

type groupBulkSetState struct {
	msgCap               int
	inWorkers            int
	inResponseMsgTimeout time.Duration
	inBulkSetMsgs        int
	outBulkSetMsgs       int

	startupShutdownLock sync.Mutex
	inNotifyChan        chan *bgNotification
	inMsgChan           chan *groupBulkSetMsg
	inFreeMsgChan       chan *groupBulkSetMsg
	outFreeMsgChan      chan *groupBulkSetMsg
}

type groupBulkSetMsg struct {
	store  *defaultGroupStore
	header []byte
	body   []byte
}

func (store *defaultGroupStore) bulkSetConfig(cfg *GroupStoreConfig) {
	store.bulkSetState.msgCap = cfg.BulkSetMsgCap
	store.bulkSetState.inWorkers = cfg.InBulkSetWorkers
	store.bulkSetState.inResponseMsgTimeout = time.Duration(cfg.InBulkSetResponseMsgTimeout) * time.Millisecond
	store.bulkSetState.inBulkSetMsgs = cfg.InBulkSetMsgs
	store.bulkSetState.outBulkSetMsgs = cfg.OutBulkSetMsgs
	if store.msgRing != nil {
		store.msgRing.SetMsgHandler(_GROUP_BULK_SET_MSG_TYPE, store.newInBulkSetMsg)
	}
}

func (store *defaultGroupStore) bulkSetStartup() {
	store.bulkSetState.startupShutdownLock.Lock()
	if store.bulkSetState.inNotifyChan == nil {
		store.bulkSetState.inNotifyChan = make(chan *bgNotification, 1)
		store.bulkSetState.inMsgChan = make(chan *groupBulkSetMsg, store.bulkSetState.inBulkSetMsgs)
		store.bulkSetState.inFreeMsgChan = make(chan *groupBulkSetMsg, store.bulkSetState.inBulkSetMsgs)
		for i := 0; i < cap(store.bulkSetState.inFreeMsgChan); i++ {
			store.bulkSetState.inFreeMsgChan <- &groupBulkSetMsg{
				store:  store,
				header: make([]byte, _GROUP_BULK_SET_MSG_HEADER_LENGTH),
				body:   make([]byte, store.bulkSetState.msgCap),
			}
		}
		store.bulkSetState.outFreeMsgChan = make(chan *groupBulkSetMsg, store.bulkSetState.outBulkSetMsgs)
		for i := 0; i < cap(store.bulkSetState.outFreeMsgChan); i++ {
			store.bulkSetState.outFreeMsgChan <- &groupBulkSetMsg{
				store:  store,
				header: make([]byte, _GROUP_BULK_SET_MSG_HEADER_LENGTH),
				body:   make([]byte, store.bulkSetState.msgCap),
			}
		}
		go store.inBulkSetLauncher(store.bulkSetState.inNotifyChan)
	}
	store.bulkSetState.startupShutdownLock.Unlock()
}

func (store *defaultGroupStore) bulkSetShutdown() {
	store.bulkSetState.startupShutdownLock.Lock()
	if store.bulkSetState.inNotifyChan != nil {
		c := make(chan struct{}, 1)
		store.bulkSetState.inNotifyChan <- &bgNotification{
			action:   _BG_DISABLE,
			doneChan: c,
		}
		<-c
		store.bulkSetState.inNotifyChan = nil
		store.bulkSetState.inMsgChan = nil
		store.bulkSetState.inFreeMsgChan = nil
		store.bulkSetState.outFreeMsgChan = nil
	}
	store.bulkSetState.startupShutdownLock.Unlock()
}

func (store *defaultGroupStore) inBulkSetLauncher(notifyChan chan *bgNotification) {
	wg := &sync.WaitGroup{}
	wg.Add(store.bulkSetState.inWorkers)
	for i := 0; i < store.bulkSetState.inWorkers; i++ {
		go store.inBulkSet(wg)
	}
	var notification *bgNotification
	running := true
	for running {
		notification = <-notifyChan
		if notification.action == _BG_DISABLE {
			for i := 0; i < store.bulkSetState.inWorkers; i++ {
				store.bulkSetState.inMsgChan <- nil
			}
			wg.Wait()
			running = false
		} else {
			store.logCritical("inBulkSet: invalid action requested: %d", notification.action)
		}
		notification.doneChan <- struct{}{}
	}
}

// newInBulkSetMsg reads bulk-set messages from the MsgRing and puts them on
// the inMsgChan for the inBulkSet workers to work on.
func (store *defaultGroupStore) newInBulkSetMsg(r io.Reader, l uint64) (uint64, error) {
	var bsm *groupBulkSetMsg
	select {
	case bsm = <-store.bulkSetState.inFreeMsgChan:
	default:
		// If there isn't a free groupBulkSetMsg, just read and discard the
		// incoming bulk-set message.
		left := l
		var sn int
		var err error
		for left > 0 {
			t := toss
			if left < uint64(len(t)) {
				t = t[:left]
			}
			sn, err = r.Read(t)
			left -= uint64(sn)
			if err != nil {
				atomic.AddInt32(&store.inBulkSetInvalids, 1)
				return l - left, err
			}
		}
		atomic.AddInt32(&store.inBulkSetDrops, 1)
		return l, nil
	}
	// If the message is obviously too short, just throw it away.
	if l < _GROUP_BULK_SET_MSG_HEADER_LENGTH+_GROUP_BULK_SET_MSG_MIN_ENTRY_LENGTH {
		store.bulkSetState.inFreeMsgChan <- bsm
		left := l
		var sn int
		var err error
		for left > 0 {
			t := toss
			if left < uint64(len(t)) {
				t = t[:left]
			}
			sn, err = r.Read(t)
			left -= uint64(sn)
			if err != nil {
				atomic.AddInt32(&store.inBulkSetInvalids, 1)
				return l - left, err
			}
		}
		atomic.AddInt32(&store.inBulkSetInvalids, 1)
		return l, nil
	}
	var n int
	var sn int
	var err error
	for n != len(bsm.header) {
		sn, err = r.Read(bsm.header[n:])
		n += sn
		if err != nil {
			store.bulkSetState.inFreeMsgChan <- bsm
			atomic.AddInt32(&store.inBulkSetInvalids, 1)
			return uint64(n), err
		}
	}
	l -= uint64(len(bsm.header))
	// TODO: I think we should cap the body size to store.bulkSetState.msgCap
	// but that also means that the inBulkSet worker will need to handle the
	// likely trailing truncated entry. Once all this is done, the overall
	// cluster should work even if the caps are set differently from node to
	// node (definitely not recommended though), as the bulk-set messages would
	// eventually start falling under the minimum cap as the front-end data is
	// tranferred and acknowledged. Anyway, I think this is needed in case
	// someone accidentally screws up the cap on one node, making it way too
	// big. Rather just have that one node abuse/run-out-of memory instead of
	// it causing every other node it sends bulk-set messages to also have
	// memory issues.
	if l > uint64(cap(bsm.body)) {
		bsm.body = make([]byte, l)
	}
	bsm.body = bsm.body[:l]
	n = 0
	for n != len(bsm.body) {
		sn, err = r.Read(bsm.body[n:])
		n += sn
		if err != nil {
			store.bulkSetState.inFreeMsgChan <- bsm
			atomic.AddInt32(&store.inBulkSetInvalids, 1)
			return uint64(len(bsm.header)) + uint64(n), err
		}
	}
	store.bulkSetState.inMsgChan <- bsm
	atomic.AddInt32(&store.inBulkSets, 1)
	return uint64(len(bsm.header)) + l, nil
}

// inBulkSet actually processes incoming bulk-set messages; there may be more
// than one of these workers.
func (store *defaultGroupStore) inBulkSet(wg *sync.WaitGroup) {
	for {
		bsm := <-store.bulkSetState.inMsgChan
		if bsm == nil {
			break
		}
		body := bsm.body
		var err error
		ring := store.msgRing.Ring()
		var rightwardPartitionShift uint64
		var bsam *groupBulkSetAckMsg
		var ptimestampbits uint64
		if ring != nil {
			rightwardPartitionShift = 64 - uint64(ring.PartitionBitCount())
			// Only ack if there is someone to ack to.
			if bsm.nodeID() != 0 {
				bsam = store.newOutBulkSetAckMsg()
			}
		}
		for len(body) > _GROUP_BULK_SET_MSG_ENTRY_HEADER_LENGTH {

			keyA := binary.BigEndian.Uint64(body)
			keyB := binary.BigEndian.Uint64(body[8:])
			childKeyA := binary.BigEndian.Uint64(body[16:])
			childKeyB := binary.BigEndian.Uint64(body[24:])
			timestampbits := binary.BigEndian.Uint64(body[32:])
			l := binary.BigEndian.Uint32(body[40:])

			atomic.AddInt32(&store.inBulkSetWrites, 1)
			/*
			   // REMOVEME logging when we get zero-length values.
			   if l == 0 && timestampbits&_TSB_DELETION == 0 {
			       fmt.Printf("REMOVEME inbulkset got a zero-length value, %x %x %x %x %x\n", keyA, keyB, childKeyA, childKeyB, timestampbits)
			   }
			*/
			// Attempt to store everything received...
			// Note that deletions are acted upon as internal requests (work
			// even if writes are disabled due to disk fullness) and new data
			// writes are not.
			ptimestampbits, err = store.write(keyA, keyB, childKeyA, childKeyB, timestampbits, body[_GROUP_BULK_SET_MSG_ENTRY_HEADER_LENGTH:_GROUP_BULK_SET_MSG_ENTRY_HEADER_LENGTH+l], timestampbits&_TSB_DELETION != 0)
			if err != nil {
				atomic.AddInt32(&store.inBulkSetWriteErrors, 1)
			} else if ptimestampbits >= timestampbits {
				atomic.AddInt32(&store.inBulkSetWritesOverridden, 1)
			}
			// But only ack on success, there is someone to ack to, and the
			// local node is responsible for the data.
			if err == nil && bsam != nil && ring != nil && ring.Responsible(uint32(keyA>>rightwardPartitionShift)) {
				bsam.add(keyA, keyB, childKeyA, childKeyB, timestampbits)
			}
			body = body[_GROUP_BULK_SET_MSG_ENTRY_HEADER_LENGTH+l:]
		}
		if bsam != nil {
			atomic.AddInt32(&store.outBulkSetAcks, 1)
			store.msgRing.MsgToNode(bsam, bsm.nodeID(), store.bulkSetState.inResponseMsgTimeout)
		}
		store.bulkSetState.inFreeMsgChan <- bsm
	}
	wg.Done()
}

// newOutBulkSetMsg gives an initialized groupBulkSetMsg for filling out and
// eventually sending using the MsgRing. The MsgRing (or someone else if the
// message doesn't end up with the MsgRing) will call groupBulkSetMsg.Free
// eventually and the groupBulkSetMsg will be requeued for reuse later. There
// is a fixed number of outgoing groupBulkSetMsg instances that can exist at
// any given time, capping memory usage. Once the limit is reached, this method
// will block until a groupBulkSetMsg is available to return.
func (store *defaultGroupStore) newOutBulkSetMsg() *groupBulkSetMsg {
	bsm := <-store.bulkSetState.outFreeMsgChan
	if store.msgRing != nil {
		if r := store.msgRing.Ring(); r != nil {
			if n := r.LocalNode(); n != nil {
				binary.BigEndian.PutUint64(bsm.header, n.ID())
			}
		}
	}
	bsm.body = bsm.body[:0]
	return bsm
}

func (bsm *groupBulkSetMsg) MsgType() uint64 {
	return _GROUP_BULK_SET_MSG_TYPE
}

func (bsm *groupBulkSetMsg) MsgLength() uint64 {
	return uint64(len(bsm.header) + len(bsm.body))
}

func (bsm *groupBulkSetMsg) WriteContent(w io.Writer) (uint64, error) {
	n, err := w.Write(bsm.header)
	if err != nil {
		return uint64(n), err
	}
	n, err = w.Write(bsm.body)
	return uint64(len(bsm.header)) + uint64(n), err
}

func (bsm *groupBulkSetMsg) Free(successes int, failures int) {
	bsm.store.bulkSetState.outFreeMsgChan <- bsm
}

func (bsm *groupBulkSetMsg) nodeID() uint64 {
	return binary.BigEndian.Uint64(bsm.header)
}

func (bsm *groupBulkSetMsg) add(keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampbits uint64, value []byte) bool {
	// REMOVEME double-checking
	dctimestampbits, dcblockid, dclength, dcerr := bsm.store.lookup(keyA, keyB, childKeyA, childKeyB)
	if int(dclength) != len(value) {
		fmt.Printf("REMOVEME bulkset.add double-check error: ka:%x kb:%x cka:%x ckb:%x ts:%x l:%d dcts:%x dcbi:%d dcl:%d dce:%s\n", keyA, keyB, childKeyA, childKeyB, timestampbits, len(value), dctimestampbits, dcblockid, dclength, dcerr)
		panic("REMOVEME double-check error")
	}
	o := len(bsm.body)
	if o+_GROUP_BULK_SET_MSG_ENTRY_HEADER_LENGTH+len(value) >= cap(bsm.body) {
		return false
	}
	bsm.body = bsm.body[:o+_GROUP_BULK_SET_MSG_ENTRY_HEADER_LENGTH+len(value)]

	binary.BigEndian.PutUint64(bsm.body[o:], keyA)
	binary.BigEndian.PutUint64(bsm.body[o+8:], keyB)
	binary.BigEndian.PutUint64(bsm.body[o+16:], childKeyA)
	binary.BigEndian.PutUint64(bsm.body[o+24:], childKeyB)
	binary.BigEndian.PutUint64(bsm.body[o+32:], timestampbits)
	binary.BigEndian.PutUint32(bsm.body[o+40:], uint32(len(value)))

	copy(bsm.body[o+_GROUP_BULK_SET_MSG_ENTRY_HEADER_LENGTH:], value)
	return true
}
