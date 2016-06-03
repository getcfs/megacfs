package store

import (
	"sync"
	"testing"
	"time"

	"github.com/gholt/ring"
	"golang.org/x/net/context"
)

type msgRingGroupPullReplicationTester struct {
	ring               ring.Ring
	lock               sync.Mutex
	msgToNodeIDs       []uint64
	headerToPartitions [][]byte
	bodyToPartitions   [][]byte
}

func (m *msgRingGroupPullReplicationTester) Ring() ring.Ring {
	return m.ring
}

func (m *msgRingGroupPullReplicationTester) MaxMsgLength() uint64 {
	return 65536
}

func (m *msgRingGroupPullReplicationTester) SetMsgHandler(msgType uint64, handler ring.MsgUnmarshaller) {
}

func (m *msgRingGroupPullReplicationTester) MsgToNode(msg ring.Msg, nodeID uint64, timeout time.Duration) {
	m.lock.Lock()
	m.msgToNodeIDs = append(m.msgToNodeIDs, nodeID)
	m.lock.Unlock()
	msg.Free(0, 0)
}

func (m *msgRingGroupPullReplicationTester) MsgToOtherReplicas(msg ring.Msg, partition uint32, timeout time.Duration) {
	prm, ok := msg.(*groupPullReplicationMsg)
	if ok {
		m.lock.Lock()
		h := make([]byte, len(prm.header))
		copy(h, prm.header)
		m.headerToPartitions = append(m.headerToPartitions, h)
		b := make([]byte, len(prm.body))
		copy(b, prm.body)
		m.bodyToPartitions = append(m.bodyToPartitions, b)
		m.lock.Unlock()
	}
	msg.Free(0, 0)
}

func TestGroupPullReplicationSimple(t *testing.T) {
	b := ring.NewBuilder(64)
	b.SetReplicaCount(2)
	n, err := b.AddNode(true, 1, nil, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.AddNode(true, 1, nil, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	r := b.Ring()
	r.SetLocalNode(n.ID())
	m := &msgRingGroupPullReplicationTester{ring: r}
	cfg := newTestGroupStoreConfig()
	cfg.MsgRing = m
	store, _ := newTestGroupStore(cfg)
	if err := store.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown(context.Background())
	_, err = store.write(1, 2, 3, 4, 0x500, []byte("testing"), false)
	if err != nil {
		t.Fatal(err)
	}
	store.OutPullReplicationPass()
	m.lock.Lock()
	v := len(m.headerToPartitions)
	m.lock.Unlock()
	if v == 0 {
		t.Fatal(v)
	}
	mayHave := false
	m.lock.Lock()
	for i := 0; i < len(m.headerToPartitions); i++ {
		prm := &groupPullReplicationMsg{store: store, header: m.headerToPartitions[i], body: m.bodyToPartitions[i]}
		bf := prm.ktBloomFilter()
		if bf.mayHave(1, 2, 3, 4, 0x500) {
			mayHave = true
		}
	}
	m.lock.Unlock()
	if !mayHave {
		t.Fatal("")
	}
}
