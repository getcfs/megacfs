package store

import (
	"bytes"
	"io"
	"testing"

	"github.com/gholt/ring"
	"golang.org/x/net/context"
)

func TestValueBulkSetAckRead(t *testing.T) {
	cfg := newTestValueStoreConfig()
	cfg.MsgRing = &msgRingPlaceholder{}
	store, _ := newTestValueStore(cfg)
	if err := store.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown(context.Background())
	imc := store.bulkSetAckState.inMsgChan
	ifmc := store.bulkSetAckState.inFreeMsgChan
	store.bulkSetAckShutdown()
	store.bulkSetAckState.inMsgChan = imc
	store.bulkSetAckState.inFreeMsgChan = ifmc
	n, err := store.newInBulkSetAckMsg(bytes.NewBuffer(make([]byte, 100)), 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatal(n)
	}
	<-store.bulkSetAckState.inMsgChan
	// Once again, but with an error in the body.
	n, err = store.newInBulkSetAckMsg(bytes.NewBuffer(make([]byte, 10)), 100)
	if err != io.EOF {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatal(n)
	}
	select {
	case bsam := <-store.bulkSetAckState.inMsgChan:
		t.Fatal(bsam)
	default:
	}
}

func TestValueBulkSetAckReadLowSendCap(t *testing.T) {
	cfg := newTestValueStoreConfig()
	cfg.MsgRing = &msgRingPlaceholder{}
	cfg.BulkSetAckMsgCap = 1
	store, _ := newTestValueStore(cfg)
	if err := store.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown(context.Background())
	imc := store.bulkSetAckState.inMsgChan
	ifmc := store.bulkSetAckState.inFreeMsgChan
	store.bulkSetAckShutdown()
	store.bulkSetAckState.inMsgChan = imc
	store.bulkSetAckState.inFreeMsgChan = ifmc
	n, err := store.newInBulkSetAckMsg(bytes.NewBuffer(make([]byte, 100)), 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatal(n)
	}
	<-store.bulkSetAckState.inMsgChan
}

func TestValueBulkSetAckMsgIncoming(t *testing.T) {
	b := ring.NewBuilder(64)
	n, err := b.AddNode(true, 1, nil, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	r := b.Ring()
	r.SetLocalNode(n.ID() + 1) // so we're not responsible for anything
	m := &msgRingPlaceholder{ring: r}
	cfg := newTestValueStoreConfig()
	cfg.MsgRing = m
	cfg.InBulkSetAckWorkers = 1
	cfg.InBulkSetAckMsgs = 1
	store, _ := newTestValueStore(cfg)
	if err := store.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown(context.Background())
	ts, err := store.write(1, 2, 0x500, []byte("testing"), true)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0 {
		t.Fatal(ts)
	}
	// just double check the item is there
	ts2, v, err := store.read(1, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts2 != 0x500 {
		t.Fatal(ts2)
	}
	if string(v) != "testing" {
		t.Fatal(string(v))
	}
	bsam := <-store.bulkSetAckState.inFreeMsgChan
	bsam.body = bsam.body[:0]
	if !bsam.add(1, 2, 0x500) {
		t.Fatal("")
	}
	store.bulkSetAckState.inMsgChan <- bsam
	// only one of these, so if we get it back we know the previous data was
	// processed
	<-store.bulkSetAckState.inFreeMsgChan
	// Make sure the item is gone
	ts2, v, err = store.read(1, 2, nil)
	if !IsNotFound(err) {
		t.Fatal(err)
	}
	if ts2 != 0x500|_TSB_LOCAL_REMOVAL {
		t.Fatal(ts2)
	}
	if string(v) != "" {
		t.Fatal(string(v))
	}
}

func TestValueBulkSetAckMsgIncomingNoRing(t *testing.T) {
	m := &msgRingPlaceholder{}
	cfg := newTestValueStoreConfig()
	cfg.MsgRing = m
	cfg.InBulkSetAckWorkers = 1
	cfg.InBulkSetAckMsgs = 1
	store, _ := newTestValueStore(cfg)
	if err := store.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown(context.Background())
	ts, err := store.write(1, 2, 0x500, []byte("testing"), true)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0 {
		t.Fatal(ts)
	}
	// just double check the item is there
	ts2, v, err := store.read(1, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts2 != 0x500 {
		t.Fatal(ts2)
	}
	if string(v) != "testing" {
		t.Fatal(string(v))
	}
	bsam := <-store.bulkSetAckState.inFreeMsgChan
	bsam.body = bsam.body[:0]
	if !bsam.add(1, 2, 0x500) {
		t.Fatal("")
	}
	store.bulkSetAckState.inMsgChan <- bsam
	// only one of these, so if we get it back we know the previous data was
	// processed
	<-store.bulkSetAckState.inFreeMsgChan
	// Make sure the item is not gone since we don't know if we're responsible
	// or not since we don't have a ring
	ts2, v, err = store.read(1, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts2 != 0x500 {
		t.Fatal(ts2)
	}
	if string(v) != "testing" {
		t.Fatal(string(v))
	}
}

func TestValueBulkSetAckMsgOut(t *testing.T) {
	cfg := newTestValueStoreConfig()
	cfg.MsgRing = &msgRingPlaceholder{}
	store, _ := newTestValueStore(cfg)
	if err := store.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown(context.Background())
	bsam := store.newOutBulkSetAckMsg()
	if bsam.MsgType() != _VALUE_BULK_SET_ACK_MSG_TYPE {
		t.Fatal(bsam.MsgType())
	}
	if bsam.MsgLength() != 0 {
		t.Fatal(bsam.MsgLength())
	}
	buf := bytes.NewBuffer(nil)
	n, err := bsam.WriteContent(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal(n)
	}
	if !bytes.Equal(buf.Bytes(), []byte{}) {
		t.Fatal(buf.Bytes())
	}
	bsam.Free(0, 0)
	bsam = store.newOutBulkSetAckMsg()
	bsam.add(1, 2, 0x500)
	bsam.add(6, 7, 0xa00)
	if bsam.MsgType() != _VALUE_BULK_SET_ACK_MSG_TYPE {
		t.Fatal(bsam.MsgType())
	}
	if bsam.MsgLength() != _VALUE_BULK_SET_ACK_MSG_ENTRY_LENGTH+_VALUE_BULK_SET_ACK_MSG_ENTRY_LENGTH {
		t.Fatal(bsam.MsgLength())
	}
	buf = bytes.NewBuffer(nil)
	n, err = bsam.WriteContent(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != _VALUE_BULK_SET_ACK_MSG_ENTRY_LENGTH+_VALUE_BULK_SET_ACK_MSG_ENTRY_LENGTH {
		t.Fatal(n)
	}
	if !bytes.Equal(buf.Bytes(), []byte{
		0, 0, 0, 0, 0, 0, 0, 1, // keyA
		0, 0, 0, 0, 0, 0, 0, 2, // keyB

		0, 0, 0, 0, 0, 0, 5, 0, // timestamp
		0, 0, 0, 0, 0, 0, 0, 6, // keyA
		0, 0, 0, 0, 0, 0, 0, 7, // keyB

		0, 0, 0, 0, 0, 0, 10, 0, // timestamp
	}) {
		t.Fatal(buf.Bytes())
	}
	bsam.Free(0, 0)
}

func TestValueBulkSetAckMsgOutWriteError(t *testing.T) {
	cfg := newTestValueStoreConfig()
	cfg.MsgRing = &msgRingPlaceholder{}
	store, _ := newTestValueStore(cfg)
	if err := store.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown(context.Background())
	bsam := store.newOutBulkSetAckMsg()
	bsam.add(1, 2, 0x500)
	_, err := bsam.WriteContent(&testErrorWriter{})
	if err == nil {
		t.Fatal(err)
	}
	bsam.Free(0, 0)
}

func TestValueBulkSetAckMsgOutHitCap(t *testing.T) {
	cfg := newTestValueStoreConfig()
	cfg.MsgRing = &msgRingPlaceholder{}
	cfg.BulkSetAckMsgCap = _VALUE_BULK_SET_ACK_MSG_ENTRY_LENGTH + 3
	store, _ := newTestValueStore(cfg)
	if err := store.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown(context.Background())
	bsam := store.newOutBulkSetAckMsg()
	if !bsam.add(1, 2, 0x500) {
		t.Fatal("")
	}
	if bsam.add(6, 7, 0xa00) {
		t.Fatal("")
	}
}
