package store

import (
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

// bsam: entries:n
// bsam entry: keyA:8, keyB:8, timestampbits:8

const _GROUP_BULK_SET_ACK_MSG_TYPE = 0xec3577cc6dbb75bb
const _GROUP_BULK_SET_ACK_MSG_ENTRY_LENGTH = 40

type groupBulkSetAckState struct {
	msgCap            int
	inWorkers         int
	inBulkSetAckMsgs  int
	outBulkSetAckMsgs int

	startupShutdownLock sync.Mutex
	inNotifyChan        chan *bgNotification
	inMsgChan           chan *groupBulkSetAckMsg
	inFreeMsgChan       chan *groupBulkSetAckMsg
	outFreeMsgChan      chan *groupBulkSetAckMsg
}

type groupBulkSetAckMsg struct {
	store *defaultGroupStore
	body  []byte
}

func (store *defaultGroupStore) bulkSetAckConfig(cfg *GroupStoreConfig) {
	store.bulkSetAckState.msgCap = cfg.BulkSetAckMsgCap
	store.bulkSetAckState.inWorkers = cfg.InBulkSetAckWorkers
	store.bulkSetAckState.inBulkSetAckMsgs = cfg.InBulkSetAckMsgs
	store.bulkSetAckState.outBulkSetAckMsgs = cfg.OutBulkSetAckMsgs
	if store.msgRing != nil {
		store.msgRing.SetMsgHandler(_GROUP_BULK_SET_ACK_MSG_TYPE, store.newInBulkSetAckMsg)
	}
}

func (store *defaultGroupStore) bulkSetAckStartup() {
	store.bulkSetAckState.startupShutdownLock.Lock()
	if store.bulkSetAckState.inNotifyChan == nil {
		store.bulkSetAckState.inNotifyChan = make(chan *bgNotification, 1)
		store.bulkSetAckState.inMsgChan = make(chan *groupBulkSetAckMsg, store.bulkSetAckState.inBulkSetAckMsgs)
		store.bulkSetAckState.inFreeMsgChan = make(chan *groupBulkSetAckMsg, store.bulkSetAckState.inBulkSetAckMsgs)
		for i := 0; i < cap(store.bulkSetAckState.inFreeMsgChan); i++ {
			store.bulkSetAckState.inFreeMsgChan <- &groupBulkSetAckMsg{
				store: store,
				body:  make([]byte, store.bulkSetAckState.msgCap),
			}
		}
		store.bulkSetAckState.outFreeMsgChan = make(chan *groupBulkSetAckMsg, store.bulkSetAckState.outBulkSetAckMsgs)
		for i := 0; i < cap(store.bulkSetAckState.outFreeMsgChan); i++ {
			store.bulkSetAckState.outFreeMsgChan <- &groupBulkSetAckMsg{
				store: store,
				body:  make([]byte, store.bulkSetAckState.msgCap),
			}
		}
		go store.inBulkSetAckLauncher(store.bulkSetAckState.inNotifyChan)
	}
	store.bulkSetAckState.startupShutdownLock.Unlock()
}

func (store *defaultGroupStore) bulkSetAckShutdown() {
	store.bulkSetAckState.startupShutdownLock.Lock()
	if store.bulkSetAckState.inNotifyChan != nil {
		c := make(chan struct{}, 1)
		store.bulkSetAckState.inNotifyChan <- &bgNotification{
			action:   _BG_DISABLE,
			doneChan: c,
		}
		<-c
		store.bulkSetAckState.inNotifyChan = nil
		store.bulkSetAckState.inMsgChan = nil
		store.bulkSetAckState.inFreeMsgChan = nil
		store.bulkSetAckState.outFreeMsgChan = nil
	}
	store.bulkSetAckState.startupShutdownLock.Unlock()
}

func (store *defaultGroupStore) inBulkSetAckLauncher(notifyChan chan *bgNotification) {
	wg := &sync.WaitGroup{}
	wg.Add(store.bulkSetAckState.inWorkers)
	for i := 0; i < store.bulkSetAckState.inWorkers; i++ {
		go store.inBulkSetAck(wg)
	}
	var notification *bgNotification
	running := true
	for running {
		notification = <-notifyChan
		if notification.action == _BG_DISABLE {
			for i := 0; i < store.bulkSetAckState.inWorkers; i++ {
				store.bulkSetAckState.inMsgChan <- nil
			}
			wg.Wait()
			running = false
		} else {
			store.logger.Error("invalid action requested", zap.String("name", store.loggerPrefix+"inBulkSetAck"), zap.Int("action", int(notification.action)))
		}
		notification.doneChan <- struct{}{}
	}
}

// newInBulkSetAckMsg reads bulk-set-ack messages from the MsgRing and puts
// them on the inMsgChan for the inBulkSetAck workers to work on.
func (store *defaultGroupStore) newInBulkSetAckMsg(r io.Reader, l uint64) (uint64, error) {
	var bsam *groupBulkSetAckMsg
	select {
	case bsam = <-store.bulkSetAckState.inFreeMsgChan:
	default:
		// If there isn't a free groupBulkSetAckMsg, just read and discard the
		// incoming bulk-set-ack message.
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
				atomic.AddInt32(&store.inBulkSetAckInvalids, 1)
				return l - left, err
			}
		}
		atomic.AddInt32(&store.inBulkSetAckDrops, 1)
		return l, nil
	}
	var n int
	var sn int
	var err error
	// TODO: Need to read up the actual msg cap and toss rest.
	if l > uint64(cap(bsam.body)) {
		bsam.body = make([]byte, l)
	}
	bsam.body = bsam.body[:l]
	n = 0
	for n != len(bsam.body) {
		sn, err = r.Read(bsam.body[n:])
		n += sn
		if err != nil {
			store.bulkSetAckState.inFreeMsgChan <- bsam
			atomic.AddInt32(&store.inBulkSetAckInvalids, 1)
			return uint64(n), err
		}
	}
	store.bulkSetAckState.inMsgChan <- bsam
	atomic.AddInt32(&store.inBulkSetAcks, 1)
	return l, nil
}

// inBulkSetAck actually processes incoming bulk-set-ack messages; there may be
// more than one of these workers.
func (store *defaultGroupStore) inBulkSetAck(wg *sync.WaitGroup) {
	for {
		bsam := <-store.bulkSetAckState.inMsgChan
		if bsam == nil {
			break
		}
		ring := store.msgRing.Ring()
		var rightwardPartitionShift uint64
		if ring != nil {
			rightwardPartitionShift = 64 - uint64(ring.PartitionBitCount())
		}
		b := bsam.body
		// div mul just ensures any trailing bytes are dropped
		l := len(b) / _GROUP_BULK_SET_ACK_MSG_ENTRY_LENGTH * _GROUP_BULK_SET_ACK_MSG_ENTRY_LENGTH
		for o := 0; o < l; o += _GROUP_BULK_SET_ACK_MSG_ENTRY_LENGTH {
			keyA := binary.BigEndian.Uint64(b[o:])
			if ring != nil && !ring.Responsible(uint32(keyA>>rightwardPartitionShift)) {
				atomic.AddInt32(&store.inBulkSetAckWrites, 1)
				timestampbits := binary.BigEndian.Uint64(b[o+32:]) | _TSB_LOCAL_REMOVAL
				ptimestampbits, err := store.write(keyA, binary.BigEndian.Uint64(b[o+8:]), binary.BigEndian.Uint64(b[o+16:]), binary.BigEndian.Uint64(b[o+24:]), timestampbits, nil, true)
				if err != nil {
					atomic.AddInt32(&store.inBulkSetAckWriteErrors, 1)
				} else if ptimestampbits >= timestampbits {
					atomic.AddInt32(&store.inBulkSetAckWritesOverridden, 1)
				}
			}
		}
		store.bulkSetAckState.inFreeMsgChan <- bsam
	}
	wg.Done()
}

// newOutBulkSetAckMsg gives an initialized groupBulkSetAckMsg for filling out
// and eventually sending using the MsgRing. The MsgRing (or someone else if
// the message doesn't end up with the MsgRing) will call
// groupBulkSetAckMsg.Free eventually and the groupBulkSetAckMsg will be
// requeued for reuse later. There is a fixed number of outgoing
// groupBulkSetAckMsg instances that can exist at any given time, capping
// memory usage. Once the limit is reached, this method will block until a
// groupBulkSetAckMsg is available to return.
func (store *defaultGroupStore) newOutBulkSetAckMsg() *groupBulkSetAckMsg {
	bsam := <-store.bulkSetAckState.outFreeMsgChan
	bsam.body = bsam.body[:0]
	return bsam
}

func (bsam *groupBulkSetAckMsg) MsgType() uint64 {
	return _GROUP_BULK_SET_ACK_MSG_TYPE
}

func (bsam *groupBulkSetAckMsg) MsgLength() uint64 {
	return uint64(len(bsam.body))
}

func (bsam *groupBulkSetAckMsg) WriteContent(w io.Writer) (uint64, error) {
	n, err := w.Write(bsam.body)
	return uint64(n), err
}

func (bsam *groupBulkSetAckMsg) Free(successes int, failures int) {
	bsam.store.bulkSetAckState.outFreeMsgChan <- bsam
}

func (bsam *groupBulkSetAckMsg) add(keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampbits uint64) bool {
	o := len(bsam.body)
	if o+_GROUP_BULK_SET_ACK_MSG_ENTRY_LENGTH >= cap(bsam.body) {
		return false
	}
	bsam.body = bsam.body[:o+_GROUP_BULK_SET_ACK_MSG_ENTRY_LENGTH]

	binary.BigEndian.PutUint64(bsam.body[o:], keyA)
	binary.BigEndian.PutUint64(bsam.body[o+8:], keyB)
	binary.BigEndian.PutUint64(bsam.body[o+16:], childKeyA)
	binary.BigEndian.PutUint64(bsam.body[o+24:], childKeyB)
	binary.BigEndian.PutUint64(bsam.body[o+32:], timestampbits)

	return true
}
