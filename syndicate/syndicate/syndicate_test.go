package syndicate

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/getcfs/megacfs/syndicate/api/proto"
	"github.com/gholt/ring"
	"github.com/uber-go/zap"
	"golang.org/x/net/context"
)

func invert(a bool) bool {
	if a {
		return false
	}
	return true
}

type MockRingBuilderThings struct {
	builderPath       string
	ringPath          string
	persistBuilderErr error
	persistRingErr    error
	bytesLoaderData   []byte
	bytesLoaderErr    error
	ring              ring.Ring
	ringbytes         *[]byte
	builder           *ring.Builder
	builderbytes      *[]byte
	buildererr        error
	managedNodes      map[uint64]ManagedNode
	slaves            []*RingSlave
	changeChan        chan *changeMsg
}

func (f *MockRingBuilderThings) BytesLoader(path string) ([]byte, error) {
	return f.bytesLoaderData, f.bytesLoaderErr
}

func (f *MockRingBuilderThings) Persist(c *RingChange, renameMaster bool) (error, error) {
	log.Println("Persist called with", c, renameMaster)
	return f.persistBuilderErr, f.persistRingErr
}

func (f *MockRingBuilderThings) GetBuilder(path string) (*ring.Builder, error) {
	return f.builder, f.buildererr
}

func newTestServerWithDefaults() (*Server, *MockRingBuilderThings) {
	b := ring.NewBuilder(64)
	b.SetReplicaCount(3)
	b.AddNode(true, 1, []string{"server1", "zone1"}, []string{"1.2.3.4:56789"}, "server1|meta one", []byte("Conf Thing1"))
	b.AddNode(true, 1, []string{"dummy1", "zone42"}, []string{"1.42.42.42:56789"}, "dummy1|meta one", []byte("Dummy Conf"))
	ring := b.Ring()

	rbytes := []byte("imnotaring")
	bbytes := []byte("imnotbuilder")

	mock := &MockRingBuilderThings{
		builderPath:  "/tmp/test.builder",
		ringPath:     "/tmp/test.ring",
		ring:         ring,
		ringbytes:    &rbytes,
		builder:      b,
		builderbytes: &bbytes,
		managedNodes: make(map[uint64]ManagedNode, 0),
		slaves:       make([]*RingSlave, 0),
		changeChan:   make(chan *changeMsg, 1),
	}
	baseLogger := zap.New(zap.NewJSONEncoder())
	baseLogger.SetLevel(zap.InfoLevel)
	ctxlog := baseLogger.With(zap.String("service", "test"))
	s := newTestServer(&Config{}, "test", ctxlog, mock)
	_, netblock, _ := net.ParseCIDR("10.0.0.0/24")
	s.netlimits = append(s.netlimits, netblock)
	_, netblock, _ = net.ParseCIDR("1.2.3.0/24")
	s.netlimits = append(s.netlimits, netblock)
	return s, mock
}

func newTestServer(cfg *Config, servicename string, logger zap.Logger, mockinfo *MockRingBuilderThings) *Server {
	s := &Server{}
	s.cfg = cfg
	s.servicename = servicename
	s.logger = logger
	s.rbPersistFn = mockinfo.Persist
	s.rbLoaderFn = mockinfo.BytesLoader
	s.getBuilderFn = mockinfo.GetBuilder
	s.b = mockinfo.builder
	s.r = mockinfo.ring
	s.rb = mockinfo.ringbytes
	s.bb = mockinfo.builderbytes
	s.managedNodes = make(map[uint64]ManagedNode, 0)
	s.slaves = mockinfo.slaves
	s.changeChan = mockinfo.changeChan
	s.metrics = metricsInit(s.servicename)
	return s
}

func TestWithRingBuilderPersister(t *testing.T) {
}

func TestWithRingBuilderBytesLoader(t *testing.T) {
}

func TestNewServer(t *testing.T) {
}

func TestServer_AddNode(t *testing.T) {
	s, m := newTestServerWithDefaults()
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Millisecond)

	origVersion := s.r.Version()
	n := &pb.Node{}
	n.Active = true
	n.Addresses = []string{"10.1.1.1:4242"}
	n.Capacity = 100
	n.Conf = []byte("test conf")
	n.Meta = "newtestnode"
	n.Tiers = []string{"newtestnode"}
	r, err := s.AddNode(ctx, n)
	if err != nil {
		t.Error(err)
	}
	if r.Version == origVersion {
		t.Error("Ring change failed")
	}

	//failures
	origVersion = s.r.Version()
	m.buildererr = fmt.Errorf("Can't even")
	n = &pb.Node{}
	n.Active = true
	n.Addresses = []string{"10.1.1.2:4242"}
	n.Capacity = 100
	n.Conf = []byte("test conf")
	n.Meta = "newtestnode2"
	n.Tiers = []string{"newtestnode2"}
	r, err = s.AddNode(ctx, n)
	if err == nil {
		t.Errorf("Should have returned error")
	}
	if r.Version != 0 {
		if r.Version != origVersion {
			t.Errorf("Ring version should not have changed")
		}
	}

	m.buildererr = nil

	origVersion = s.r.Version()
	m.persistBuilderErr = fmt.Errorf("Can't even")
	n = &pb.Node{}
	n.Active = true
	n.Addresses = []string{"10.1.1.2:4242"}
	n.Capacity = 100
	n.Conf = []byte("test conf")
	n.Meta = "newtestnode2"
	n.Tiers = []string{"newtestnode2"}
	r, err = s.AddNode(ctx, n)
	if err == nil {
		t.Errorf("Should have returned error")
	}
	if r.Version != origVersion {
		t.Errorf("Ring version should not have changed")
	}
	m.persistBuilderErr = nil
}

func TestServer_ReplaceAddresses(t *testing.T) {
	s, m := newTestServerWithDefaults()
	ctx := context.Background()

	id := s.r.Nodes()[1].ID()
	origVersion := s.r.Version()
	var addrs []string

	addrs = append(addrs, fmt.Sprintf("%d", time.Now().UnixNano()))
	n := &pb.Node{Id: id, Addresses: addrs}
	r, err := s.ReplaceAddresses(ctx, n)
	if err != nil {
		t.Errorf("ReplaceAddresses(ctx, %#v) should not have returned error: %s", n, err.Error())
	}
	if r.Status != true || r.Version == origVersion {
		t.Errorf("ReplaceAddresses(ctx, %#v) SHOULD have changed ring: %#v", n, r)
	}
	if !reflect.DeepEqual(addrs, s.r.Node(id).Addresses()) {
		t.Errorf("ReplaceAddresses(ctx, %#v) should have increased capacity", n)
	}

	//one of the addresses already exists in another node
	n.Reset()
	addrs = []string{}
	origVersion = s.r.Version()
	n.Id = id
	addrs = append(addrs, fmt.Sprintf("%d", time.Now().UnixNano()))
	addrs = append(addrs, s.r.Nodes()[0].Address(0))
	n.Addresses = addrs
	r, err = s.ReplaceAddresses(ctx, n)
	if err == nil {
		t.Errorf("ReplaceAddresses(ctx, %#v) SHOULD  have returned error: %s", n, err.Error())
	}
	if r.Status || r.Version != origVersion {
		t.Errorf("ReplaceAddresses(ctx, %#v) should not have changed ring: %#v", n, r)
	}

	//empty addrs
	n.Reset()
	addrs = []string{}
	origVersion = s.r.Version()
	n.Id = id
	n.Addresses = addrs
	r, err = s.ReplaceAddresses(ctx, n)
	if err == nil {
		t.Errorf("ReplaceAddresses(ctx, %#v) SHOULD  have returned error: %s", n, err.Error())
	}
	if r.Status || r.Version != origVersion {
		t.Errorf("ReplaceAddresses(ctx, %#v) should not have changed ring: %#v", n, r)
	}

	//nonexistent node
	n.Reset()
	addrs = append(addrs, fmt.Sprintf("%d", time.Now().UnixNano()))
	origVersion = s.r.Version()
	n.Id = 42
	n.Addresses = addrs
	r, err = s.ReplaceAddresses(ctx, n)
	if err == nil {
		t.Errorf("ReplaceAddresses(ctx, %#v) SHOULD  have returned error: %s", n, err.Error())
	}
	if r.Status || r.Version != origVersion {
		t.Errorf("ReplaceAddresses(ctx, %#v) should not have changed ring: %#v", n, r)
	}

	//failures
	n.Reset()
	n.Id = id
	addrs = append(addrs, fmt.Sprintf("%d", time.Now().UnixNano()))
	n.Addresses = addrs
	m.buildererr = fmt.Errorf("Can't even")
	origVersion = s.r.Version()
	r, err = s.ReplaceAddresses(ctx, n)
	if err == nil {
		t.Errorf("ReplaceAddresses(ctx, %#v) SHOULD have failed", n)
	}
	m.buildererr = nil

	m.persistRingErr = fmt.Errorf("Can't even")
	n.Reset()
	n.Id = id
	addrs = append(addrs, fmt.Sprintf("%d", time.Now().UnixNano()))
	n.Addresses = addrs
	origVersion = s.r.Version()
	r, err = s.ReplaceAddresses(ctx, n)
	if err == nil {
		t.Errorf("ReplaceAddresses(ctx, %#v) should have failed: %s", n, err.Error())
	} else {
		if r.Version != origVersion || r.Status == true {
			t.Errorf("ReplaceAddresses(ctx, %#v) SHOULD not have resulted in ring change: %#v", n, r)
		}
	}
	m.persistRingErr = nil
}

func TestServer_RemoveNode(t *testing.T) {
	s, m := newTestServerWithDefaults()
	ctx := context.Background()

	origVersion := s.r.Version()

	nonExistent := &pb.Node{Id: 4242}

	r, err := s.RemoveNode(ctx, nonExistent)
	if err == nil {
		t.Errorf("RemoveNode(%#v) should have returned error (%s): %#v", nonExistent, err.Error(), r)
	}
	if err != nil {
		if r.Status {
			t.Errorf("RemoveNode(%#v) should have not returned true status: %#v", nonExistent, r)
		}
		if r.Version != origVersion {
			t.Errorf("RemoveNode(%#v) should have not returned new ring version: %#v", nonExistent, r)
		}
	}

	nodes, err := m.builder.Nodes().Filter([]string{"meta~=dummy1.*"})
	legitNode := &pb.Node{Id: nodes[0].ID()}
	r, err = s.RemoveNode(ctx, legitNode)
	if err != nil {
		t.Errorf("RemoveNode(%#v) should not have returned error (%s): %#v", legitNode, err.Error(), r)
	}
	if r.Version == origVersion || r.Status == false {
		t.Errorf("RemoveNode(%#v) should have returned new version and true status (%s): %#v", legitNode, err.Error(), r)
	}
	nodes, err = m.builder.Nodes().Filter([]string{"meta~=dummy1.*"})
	if len(nodes) != 0 {
		t.Errorf("RemoveNode(%#v) should have modified the builder but didn't", legitNode)
	}

	if len(m.builder.Nodes()) == 0 {
		t.Errorf("SHOULD NOT HAVE REMOVED ALL")
	}

	m.buildererr = fmt.Errorf("Can't even")
	origVersion = s.r.Version()
	legitNode = &pb.Node{Id: m.builder.Nodes()[0].ID()}
	r, err = s.RemoveNode(ctx, legitNode)
	if err == nil {
		t.Errorf("RemoveNode(ctx, %#v) SHOULD have returned error: %s", legitNode, err.Error())
	}
	if r.Version != origVersion || r.Status == true {
		log.Println(origVersion)
		t.Errorf("RemoveNode(ctx, %#v) should not have modified ring: %#v", legitNode, r)
	}
}

func TestServer_ModNode(t *testing.T) {
}

func TestServer_SetConf(t *testing.T) {
	s, m := newTestServerWithDefaults()
	ctx := context.Background()

	oldVersion := s.r.Version()
	newConf := &pb.Conf{}
	newConf.Conf = []byte("new config")
	r, err := s.SetConf(ctx, newConf)
	if err != nil {
		t.Errorf("SetConf(ctx, %#v) should not have failed: %s", newConf, err.Error())
	}
	if r.Version == oldVersion || r.Status == false {
		t.Errorf("SetConf(ctx, %#v) should have resulted in ring change: %#v", newConf, r)
	}
	if !bytes.Equal(s.r.Config(), newConf.Conf) {
		t.Errorf("SetConf(ctx, %#v) failed to actually update conf bytes", newConf)
	}

	m.buildererr = fmt.Errorf("Can't even")
	oldVersion = s.r.Version()
	r, err = s.SetConf(ctx, newConf)
	if err == nil {
		t.Errorf("SetConf(ctx, %#v) SHOULD have failed", newConf)
	}
	m.buildererr = nil

	m.persistRingErr = fmt.Errorf("Can't even")
	oldVersion = s.r.Version()
	r, err = s.SetConf(ctx, newConf)
	if err == nil {
		t.Errorf("SetConf(ctx, %#v) should have failed: %s", newConf, err.Error())
	} else {
		if r.Version != oldVersion || r.Status == true {
			t.Errorf("SetConf(ctx, %#v) SHOULD not have resulted in ring change: %#v", newConf, r)
		}
	}
	m.persistRingErr = nil

}

func TestServer_SetActive(t *testing.T) {
	s, m := newTestServerWithDefaults()
	ctx := context.Background()

	id := s.r.Nodes()[1].ID()
	origVersion := s.r.Version()

	//disable node
	n := &pb.Node{Id: id, Active: false}
	r, err := s.SetActive(ctx, n)
	if err != nil {
		t.Errorf("SetActive(ctx, %#v) should not have returned error: %s", n, err.Error())
	}
	if r.Status != true || r.Version == origVersion {
		t.Errorf("SetActive(ctx, %#v) SHOULD have changed ring: %#v", n, r)
	}
	if s.r.Node(id).Active() {
		t.Errorf("SetActive(ctx, %#v) should be inactive now but wasn't", n)
	}

	//enable node
	origVersion = s.r.Version()
	n.Active = true
	r, err = s.SetActive(ctx, n)
	if err != nil {
		t.Errorf("SetActive(ctx, %#v) should not have returned error: %s", n, err.Error())
	}
	if r.Status != true || r.Version == origVersion {
		t.Errorf("SetActive(ctx, %#v) SHOULD have changed ring: %#v", n, r)
	}
	if !s.r.Node(id).Active() {
		t.Errorf("SetActive(ctx, %#v) should be active now but wasn't", n)
	}

	//nonexistent node
	n.Reset()
	origVersion = s.r.Version()
	n.Id = 42
	n.Active = true
	r, err = s.SetActive(ctx, n)
	if err == nil {
		t.Errorf("SetActive(ctx, %#v) SHOULD  have returned error: %s", n, err.Error())
	}
	if r.Status || r.Version != origVersion {
		t.Errorf("SetActive(ctx, %#v) should not have changed ring: %#v", n, r)
	}

	//failures
	n.Reset()
	n.Id = id
	n.Active = invert(s.r.Node(id).Active())
	m.buildererr = fmt.Errorf("Can't even")
	origVersion = s.r.Version()
	r, err = s.SetActive(ctx, n)
	if err == nil {
		t.Errorf("SetActive(ctx, %#v) SHOULD have failed", n)
	}
	m.buildererr = nil

	m.persistRingErr = fmt.Errorf("Can't even")
	n.Reset()
	n.Id = id
	n.Active = invert(s.r.Node(id).Active())
	origVersion = s.r.Version()
	r, err = s.SetActive(ctx, n)
	if err == nil {
		t.Errorf("SetActive(ctx, %#v) should have failed: %s", n, err.Error())
	} else {
		if r.Version != origVersion || r.Status == true {
			t.Errorf("SetActive(ctx, %#v) SHOULD not have resulted in ring change: %#v", n, r)
		}
	}
	m.persistRingErr = nil

}

func TestServer_SetCapacity(t *testing.T) {
	s, m := newTestServerWithDefaults()
	ctx := context.Background()

	id := s.r.Nodes()[1].ID()
	origVersion := s.r.Version()
	var cap uint32

	cap = s.r.Node(id).Capacity() + 1
	n := &pb.Node{Id: id, Capacity: cap}
	r, err := s.SetCapacity(ctx, n)
	if err != nil {
		t.Errorf("SetCapacity(ctx, %#v) should not have returned error: %s", n, err.Error())
	}
	if r.Status != true || r.Version == origVersion {
		t.Errorf("SetCapacity(ctx, %#v) SHOULD have changed ring: %#v", n, r)
	}
	if s.r.Node(id).Capacity() != cap {
		t.Errorf("SetCapacity(ctx, %#v) should have increased capacity", n)
	}

	//nonexistent node
	n.Reset()
	origVersion = s.r.Version()
	n.Id = 42
	n.Capacity = 1
	r, err = s.SetCapacity(ctx, n)
	if err == nil {
		t.Errorf("SetCapacity(ctx, %#v) SHOULD  have returned error: %s", n, err.Error())
	}
	if r.Status || r.Version != origVersion {
		t.Errorf("SetCapacity(ctx, %#v) should not have changed ring: %#v", n, r)
	}

	//failures
	n.Reset()
	n.Id = id
	cap = s.r.Node(id).Capacity() + 1
	n.Capacity = cap
	m.buildererr = fmt.Errorf("Can't even")
	origVersion = s.r.Version()
	r, err = s.SetCapacity(ctx, n)
	if err == nil {
		t.Errorf("SetCapacity(ctx, %#v) SHOULD have failed", n)
	}
	m.buildererr = nil

	m.persistRingErr = fmt.Errorf("Can't even")
	n.Reset()
	n.Id = id
	cap = s.r.Node(id).Capacity() + 1
	n.Capacity = cap
	origVersion = s.r.Version()
	r, err = s.SetCapacity(ctx, n)
	if err == nil {
		t.Errorf("SetCapacity(ctx, %#v) should have failed: %s", n, err.Error())
	} else {
		if r.Version != origVersion || r.Status == true {
			t.Errorf("SetCapacity(ctx, %#v) SHOULD not have resulted in ring change: %#v", n, r)
		}
	}
	m.persistRingErr = nil
}

func TestServer_ReplaceTiers(t *testing.T) {
	s, m := newTestServerWithDefaults()
	ctx := context.Background()

	id := s.r.Nodes()[1].ID()
	origVersion := s.r.Version()
	var tiers []string

	tiers = append(tiers, fmt.Sprintf("%d", time.Now().UnixNano()))
	n := &pb.Node{Id: id, Tiers: tiers}
	r, err := s.ReplaceTiers(ctx, n)
	if err != nil {
		t.Errorf("ReplaceTiers(ctx, %#v) should not have returned error: %s", n, err.Error())
	}
	if r.Status != true || r.Version == origVersion {
		t.Errorf("ReplaceTiers(ctx, %#v) SHOULD have changed ring: %#v", n, r)
	}
	if !reflect.DeepEqual(tiers, s.r.Node(id).Tiers()) {
		fmt.Printf("T: %#v\n", tiers)
		fmt.Printf("T: %#v\n", s.r.Node(id).Tiers())
		t.Errorf("ReplaceTiers(ctx, %#v) should have increased capacity", n)
	}

	//empty tiers
	n.Reset()
	tiers = []string{}
	origVersion = s.r.Version()
	n.Id = id
	n.Tiers = tiers
	r, err = s.ReplaceTiers(ctx, n)
	if err == nil {
		t.Errorf("ReplaceTiers(ctx, %#v) SHOULD  have returned error: %s", n, err.Error())
	}
	if r.Status || r.Version != origVersion {
		t.Errorf("ReplaceTiers(ctx, %#v) should not have changed ring: %#v", n, r)
	}

	//nonexistent node
	n.Reset()
	tiers = append(tiers, fmt.Sprintf("%d", time.Now().UnixNano()))
	origVersion = s.r.Version()
	n.Id = 42
	n.Tiers = tiers
	r, err = s.ReplaceTiers(ctx, n)
	if err == nil {
		t.Errorf("ReplaceTiers(ctx, %#v) SHOULD  have returned error: %s", n, err.Error())
	}
	if r.Status || r.Version != origVersion {
		t.Errorf("ReplaceTiers(ctx, %#v) should not have changed ring: %#v", n, r)
	}

	//failures
	n.Reset()
	n.Id = id
	tiers = append(tiers, fmt.Sprintf("%d", time.Now().UnixNano()))
	n.Tiers = tiers
	m.buildererr = fmt.Errorf("Can't even")
	origVersion = s.r.Version()
	r, err = s.ReplaceTiers(ctx, n)
	if err == nil {
		t.Errorf("ReplaceTiers(ctx, %#v) SHOULD have failed", n)
	}
	m.buildererr = nil

	m.persistRingErr = fmt.Errorf("Can't even")
	n.Reset()
	n.Id = id
	tiers = append(tiers, fmt.Sprintf("%d", time.Now().UnixNano()))
	n.Tiers = tiers
	origVersion = s.r.Version()
	r, err = s.ReplaceTiers(ctx, n)
	if err == nil {
		t.Errorf("ReplaceTiers(ctx, %#v) should have failed: %s", n, err.Error())
	} else {
		if r.Version != origVersion || r.Status == true {
			t.Errorf("ReplaceTiers(ctx, %#v) SHOULD not have resulted in ring change: %#v", n, r)
		}
	}
	m.persistRingErr = nil
}

func TestServer_GetVersion(t *testing.T) {
	s, m := newTestServerWithDefaults()
	ctx := context.Background()
	empty := &pb.EmptyMsg{}

	r, err := s.GetVersion(ctx, empty)
	if err != nil {
		t.Errorf("GetVersion() should not have returned an error: %s", err.Error())
	}

	if r.Version != m.ring.Version() {
		t.Errorf("GetVersion() returned ring version %d, expected %d", r.Version, m.ring.Version())
	}
}

func TestServer_GetGlobalConfig(t *testing.T) {
	s, m := newTestServerWithDefaults()
	ctx := context.Background()
	empty := &pb.EmptyMsg{}

	r, err := s.GetGlobalConfig(ctx, empty)
	if err != nil {
		t.Errorf("GetGlobalConfig() should not have rerturned an error: %s", err.Error())
	}
	if !bytes.Equal(r.Conf.Conf, m.builder.Config()) {
		t.Errorf("GetGlobalConfig() returned config: %#v expected %#v", r.Conf.Conf, m.builder.Config())
	}
}

func TestServer_SearchNodes(t *testing.T) {
	s, _ := newTestServerWithDefaults()
	ctx := context.Background()
	r, err := s.SearchNodes(ctx, &pb.Node{})
	if err != nil {
		t.Errorf("SearchNodes() should not have failed: %s", err.Error())
	}

	targetID := s.r.Nodes()[0].ID()
	n := &pb.Node{}

	n.Reset()
	n.Meta = "server1"
	r, err = s.SearchNodes(ctx, n)
	if err != nil {
		t.Errorf("SearchNodes(ctx, %#v) should not have failed: %s", n, err.Error())
	}
	if len(r.Nodes) != 1 {
		if len(r.Nodes) == 0 {
			t.Errorf("SearchNodes(ctx, %#v) failed to return any results.", n)
		} else {
			t.Errorf("SearchNodes(ctx, %#v) returned to many results: %+v", n, r.Nodes)
		}
	} else {
		if r.Nodes[0].Id != targetID {
			t.Errorf("SearchNodes(ctx, %#v) failed to return expected ring entry got: %+v", n, r.Nodes)
		}
	}

	n.Reset()
	n.Tiers = append(n.Tiers, s.r.Nodes()[0].Tier(0))
	r, err = s.SearchNodes(ctx, n)
	if err != nil {
		t.Errorf("SearchNodes(ctx, %#v) should not have failed: %s", n, err.Error())
	}
	if len(r.Nodes) != 1 {
		if len(r.Nodes) == 0 {
			t.Errorf("SearchNodes(ctx, %#v) failed to return any results.", n)
		} else {
			t.Errorf("SearchNodes(ctx, %#v) returned to many results: %+v", n, r.Nodes)
		}
	} else {
		if r.Nodes[0].Id != targetID {
			t.Errorf("SearchNodes(ctx, %#v) failed to return expected ring entry got: %+v", n, r.Nodes)
		}
	}

	n.Reset()
	n.Addresses = append(n.Addresses, s.r.Nodes()[0].Address(0))
	r, err = s.SearchNodes(ctx, n)
	if err != nil {
		t.Errorf("SearchNodes(ctx, %#v) should not have failed: %s", n, err.Error())
	}
	if len(r.Nodes) != 1 {
		if len(r.Nodes) == 0 {
			t.Errorf("SearchNodes(ctx, %#v) failed to return any results.", n)
		} else {
			t.Errorf("SearchNodes(ctx, %#v) returned to many results: %+v", n, r.Nodes)
		}
	} else {
		if r.Nodes[0].Id != targetID {
			t.Errorf("SearchNodes(ctx, %#v) failed to return expected ring entry got: %+v", n, r.Nodes)
		}
	}

	//this should return two hits:
	n.Reset()
	n.Meta = "dummy1.*|server1.*"
	r, err = s.SearchNodes(ctx, n)
	if err != nil {
		t.Errorf("SearchNodes(ctx, %#v) should not have failed: %s", n, err.Error())
	}
	if len(r.Nodes) != 2 {
		t.Errorf("SearchNodes(ctx, %#v) failed to return 2 results, got: %+v", n, r.Nodes)
	}

	//this should return 0 hits
	n.Reset()
	n.Id = 42
	n.Meta = "nope"
	r, err = s.SearchNodes(ctx, n)
	if err != nil {
		t.Errorf("SearchNodes(ctx, %#v) should not have failed: %s", n, err.Error())
	}
	if len(r.Nodes) != 0 {
		t.Errorf("SearchNodes(ctx, %#v) should have returned any results but got: %+v", n, r.Nodes)
	}
}

func TestServer_GetNodeConfig(t *testing.T) {
	s, _ := newTestServerWithDefaults()
	ctx := context.Background()

	n := &pb.Node{Id: 42}
	_, err := s.GetNodeConfig(ctx, n)
	if err == nil {
		t.Errorf("GetNodeConfig(ctx, %#v) should have failed as not found.", n)
	}

	n = &pb.Node{Id: s.r.Nodes()[0].ID()}
	r, err := s.GetNodeConfig(ctx, n)
	if err != nil {
		t.Errorf("GetNodeConfig(ctx, %#v) should not have failed: %s", n, err.Error())
	}
	if !bytes.Equal(r.Conf.Conf, s.r.Nodes()[0].Config()) {
		t.Errorf("GetNodeConfig(ctx, %#v) returned wrong config: %#v", n, r.Conf)
	}
}

func TestServer_GetRing(t *testing.T) {
	s, m := newTestServerWithDefaults()
	ctx := context.Background()
	empty := &pb.EmptyMsg{}
	rmsg, err := s.GetRing(ctx, empty)
	if rmsg.Version != m.ring.Version() {
		t.Errorf("GetRing() ring version was %d, expected %d", rmsg.Version, m.ring.Version())
	}
	if err != nil {
		t.Errorf("GetRing() returned unexpected error: %s", err.Error())
	}
	if !bytes.Equal(rmsg.Ring, *m.ringbytes) {
		log.Printf("%#v", rmsg)
		log.Printf("%d, %#v", m.ring.Version(), *m.ringbytes)
		t.Errorf("GetRing() returned ring bytes don't match expected")
	}
}

func TestServer_RegisterNode(t *testing.T) {
	s, m := newTestServerWithDefaults()
	ctx := context.Background()

	okHwProfile := &pb.HardwareProfile{
		Cpus:     0,
		Memfree:  1000,
		Memtotal: 1000,
		Disks:    []*pb.Disk{&pb.Disk{Path: "/data", Size_: 10000000000}},
	}

	badRequests := map[string]*pb.RegisterRequest{
		"Bad Network Interface": &pb.RegisterRequest{
			Hostname: "badnetiface.test.com",
			Addrs:    []string{"127.0.0.1/32", "192.168.2.2/32"},
			Tiers:    []string{"badnetiface.test.com", "zone1"},
			Hardware: okHwProfile,
		},
		"Duplicate server name and addr": &pb.RegisterRequest{
			Hostname: "server1",
			Addrs:    []string{"1.2.3.4/32", "127.0.0.1/32", "192.168.2.2/32"},
			Tiers:    []string{"server1", "zone1"},
			Hardware: okHwProfile,
		},
		"Bad tier": &pb.RegisterRequest{
			Hostname: "server42",
			Addrs:    []string{"10.0.0.42/32", "127.0.0.1/32", "192.168.2.2/32"},
			Tiers:    []string{""},
			Hardware: okHwProfile,
		},
	}

	for k, _ := range badRequests {
		r, err := s.RegisterNode(ctx, badRequests[k])
		if err == nil {
			t.Errorf("RegisterNode(ctx, %#v) (%#v, %s) should have returned error because %s", badRequests[k], r, err.Error(), k)
		}
	}

	//now add a valid entry with the default strategy
	validRequest := &pb.RegisterRequest{
		Hostname: "server2",
		Addrs:    []string{"10.0.0.2/32", "127.0.0.1/32"},
		Tiers:    []string{"server2", "zone2"},
		Hardware: okHwProfile,
	}
	r, err := s.RegisterNode(ctx, validRequest)
	if err != nil {
		t.Errorf("RegisterNode(ctx, %#v) (%#v, %s) should have succeeded", validRequest, r, err.Error())
	}
	nodesbyaddr, _ := s.b.Nodes().Filter([]string{"address~=10.0.0.2"})
	nodesbymeta, _ := s.b.Nodes().Filter([]string{"meta~=server2.*"})
	if len(nodesbyaddr) != 1 || len(nodesbyaddr) != 1 {
		t.Errorf("RegisterNode(ctx, %#v) (%#v, %s) should have resulted with new ring entry. by addr (%v), by meta (%v)", validRequest, r, err.Error(), nodesbyaddr, nodesbymeta)
	}
	if len(nodesbyaddr) == 1 && len(nodesbyaddr) == 1 {
		if r.Localid != nodesbyaddr[0].ID() || r.Localid != nodesbymeta[0].ID() {
			t.Errorf("RegisterNode(ctx, %#v), (%#v, %s) localid should have matched id's in ring. by addr (%d), by meta (%d)", validRequest, r, err.Error(), nodesbyaddr[0].ID(), nodesbymeta[0].ID())
		}
	} else {
		t.Errorf("RegisterNode(ctx, %#v), (%#v, %s) missing a entry by addr (%v) or meta (%v)", validRequest, r, err.Error(), nodesbyaddr, nodesbymeta)
	}
	//verify default weight assignment
	if nodesbyaddr[0].Active() == true {
		t.Errorf("RegisterNodes weight assignment strategy is default but found node with capacity (%d) and active (%v)", nodesbyaddr[0].Capacity(), nodesbyaddr[0].Active())
	}

	//test node already in ring
	validRequest.Reset()
	validRequest = &pb.RegisterRequest{
		Hostname: "server2",
		Addrs:    []string{"10.0.0.2/32", "127.0.0.1/32"},
		Tiers:    []string{"server2", "zone2"},
	}
	r, err = s.RegisterNode(ctx, validRequest)
	if err != nil {
		t.Errorf("RegisterNode(ctx, %#v) (%#v, %s) should have succeeded", validRequest, r, err.Error())
	}
	nodesbyaddr, _ = s.b.Nodes().Filter([]string{"address~=10.0.0.2"})
	nodesbymeta, _ = s.b.Nodes().Filter([]string{"meta~=server2.*"})
	if len(nodesbyaddr) != 1 || len(nodesbyaddr) != 1 {
		t.Errorf("RegisterNode(ctx, %#v) (%#v, %s) should not have resulted with new ring entry. by addr (%v), by meta (%v)", validRequest, r, err.Error(), nodesbyaddr, nodesbymeta)
	}
	if len(nodesbyaddr) == 1 && len(nodesbyaddr) == 1 {
		if r.Localid != nodesbyaddr[0].ID() || r.Localid != nodesbymeta[0].ID() {
			t.Errorf("RegisterNode(ctx, %#v), (%#v, %s) localid should have matched id's in ring. by addr (%d), by meta (%d)", validRequest, r, err.Error(), nodesbyaddr[0].ID(), nodesbymeta[0].ID())
		} else {
			//verify default weight assignment
			if nodesbyaddr[0].Active() {
				t.Errorf("RegisterNodes weight assignment strategy is default but found node with capacity (%d) and active (%v)", nodesbyaddr[0].Capacity(), nodesbyaddr[0].Active())
			}
		}
	} else {
		t.Errorf("RegisterNode(ctx, %#v), (%#v, %s) missing a entry by addr (%v) or meta (%v)", validRequest, r, err.Error(), nodesbyaddr, nodesbymeta)
	}

	//test fixed
	s.cfg.WeightAssignment = "fixed"
	validRequest.Reset()
	validRequest = &pb.RegisterRequest{
		Hostname: "server3",
		Addrs:    []string{"10.0.0.3/32", "127.0.0.1/32"},
		Tiers:    []string{"server3", "zone3"},
	}
	r, err = s.RegisterNode(ctx, validRequest)
	if err != nil {
		t.Errorf("RegisterNode(ctx, %#v) (%#v, %s) should have succeeded", validRequest, r, err.Error())
	}
	nodesbyaddr, _ = s.b.Nodes().Filter([]string{"address~=10.0.0.3"})
	nodesbymeta, _ = s.b.Nodes().Filter([]string{"meta~=server3.*"})
	if len(nodesbyaddr) != 1 || len(nodesbyaddr) != 1 {
		t.Errorf("RegisterNode(ctx, %#v) (%#v, %s) should not have resulted with new ring entry. by addr (%v), by meta (%v)", validRequest, r, err.Error(), nodesbyaddr, nodesbymeta)
	}
	if len(nodesbyaddr) == 1 && len(nodesbyaddr) == 1 {
		if r.Localid != nodesbyaddr[0].ID() || r.Localid != nodesbymeta[0].ID() {
			t.Errorf("RegisterNode(ctx, %#v), (%#v, %s) localid should have matched id's in ring. by addr (%d), by meta (%d)", validRequest, r, err.Error(), nodesbyaddr[0].ID(), nodesbymeta[0].ID())
		} else {
			//verify default weight assignment
			if nodesbyaddr[0].Capacity() != 1000 || nodesbyaddr[0].Active() != true {
				t.Errorf("RegisterNodes weight assignment strategy is fixed but found node with capacity (%d) and active (%v)", nodesbyaddr[0].Capacity(), nodesbyaddr[0].Active())
			}
		}
	} else {
		t.Errorf("RegisterNode(ctx, %#v), (%#v, %s) missing a entry by addr (%v) or meta (%v)", validRequest, r, err.Error(), nodesbyaddr, nodesbymeta)
	}

	// test self
	s.cfg.WeightAssignment = "self"
	validRequest.Reset()
	validRequest = &pb.RegisterRequest{
		Hostname: "server4",
		Addrs:    []string{"10.0.0.4/32", "127.0.0.1/32"},
		Tiers:    []string{"server4", "zone4"},
		Hardware: okHwProfile,
	}
	r, err = s.RegisterNode(ctx, validRequest)
	if err != nil {
		t.Errorf("RegisterNode(ctx, %#v) (%#v, %s) should have succeeded", validRequest, r, err.Error())
	}
	nodesbyaddr, _ = s.b.Nodes().Filter([]string{"address~=10.0.0.4"})
	nodesbymeta, _ = s.b.Nodes().Filter([]string{"meta~=server4.*"})
	if len(nodesbyaddr) != 1 || len(nodesbyaddr) != 1 {
		t.Errorf("RegisterNode(ctx, %#v) (%#v, %s) should not have resulted with new ring entry. by addr (%v), by meta (%v)", validRequest, r, err.Error(), nodesbyaddr, nodesbymeta)
	}
	if len(nodesbyaddr) == 1 && len(nodesbyaddr) == 1 {
		if r.Localid != nodesbyaddr[0].ID() || r.Localid != nodesbymeta[0].ID() {
			t.Errorf("RegisterNode(ctx, %#v), (%#v, %s) localid should have matched id's in ring. by addr (%d), by meta (%d)", validRequest, r, err.Error(), nodesbyaddr[0].ID(), nodesbymeta[0].ID())
		} else {
			//verify default weight assignment
			if nodesbyaddr[0].Capacity() != uint32(okHwProfile.Disks[0].Size_/1024/1024/1024) || nodesbyaddr[0].Active() == false {
				t.Errorf("RegisterNodes weight assignment strategy is self but found node with capacity (%d) and active (%v)", nodesbyaddr[0].Capacity(), nodesbyaddr[0].Active())
			}
		}
	} else {
		t.Errorf("RegisterNode(ctx, %#v), (%#v, %s) missing a entry by addr (%v) or meta (%v)", validRequest, r, err.Error(), nodesbyaddr, nodesbymeta)
	}

	//test manual
	s.cfg.WeightAssignment = "manual"
	validRequest.Reset()
	validRequest = &pb.RegisterRequest{
		Hostname: "server5",
		Addrs:    []string{"10.0.0.5/32", "127.0.0.1/32"},
		Tiers:    []string{"server5", "zone5"},
		Hardware: okHwProfile,
	}
	r, err = s.RegisterNode(ctx, validRequest)
	if err != nil {
		t.Errorf("RegisterNode(ctx, %#v) (%#v, %s) should have succeeded", validRequest, r, err.Error())
	}
	nodesbyaddr, _ = s.b.Nodes().Filter([]string{"address~=10.0.0.5"})
	nodesbymeta, _ = s.b.Nodes().Filter([]string{"meta~=server5.*"})
	if len(nodesbyaddr) != 1 || len(nodesbyaddr) != 1 {
		t.Errorf("RegisterNode(ctx, %#v) (%#v, %s) should not have resulted with new ring entry. by addr (%v), by meta (%v)", validRequest, r, err.Error(), nodesbyaddr, nodesbymeta)
	}
	if len(nodesbyaddr) == 1 && len(nodesbyaddr) == 1 {
		if r.Localid != nodesbyaddr[0].ID() || r.Localid != nodesbymeta[0].ID() {
			t.Errorf("RegisterNode(ctx, %#v), (%#v, %s) localid should have matched id's in ring. by addr (%d), by meta (%d)", validRequest, r, err.Error(), nodesbyaddr[0].ID(), nodesbymeta[0].ID())
		} else {
			//verify default weight assignment
			if nodesbyaddr[0].Capacity() != uint32(okHwProfile.Disks[0].Size_/1024/1024/1024) || nodesbyaddr[0].Active() != false {
				t.Errorf("RegisterNodes weight assignment strategy is manual but found node with capacity (%d) and active (%v)", nodesbyaddr[0].Capacity(), nodesbyaddr[0].Active())
			}
		}
	} else {
		t.Errorf("RegisterNode(ctx, %#v), (%#v, %s) missing a entry by addr (%v) or meta (%v)", validRequest, r, err.Error(), nodesbyaddr, nodesbymeta)
	}

	//load builder failure
	m.buildererr = fmt.Errorf("Can't even")
	validRequest.Reset()
	validRequest = &pb.RegisterRequest{
		Hostname: "server6",
		Addrs:    []string{"10.0.0.6/32", "127.0.0.1/32"},
		Tiers:    []string{"server6", "zone6"},
	}
	_, err = s.RegisterNode(ctx, validRequest)
	if err == nil {
		t.Error("RegisterNode should have failed due to load builder error")
	}
	m.buildererr = nil

	//persist failure
	m.persistBuilderErr = fmt.Errorf("Can't even")
	validRequest.Reset()
	validRequest = &pb.RegisterRequest{
		Hostname: "server7",
		Addrs:    []string{"10.0.0.7/32", "127.0.0.1/32"},
		Tiers:    []string{"server7", "zone7"},
	}
	oldVersion := s.r.Version()
	r, err = s.RegisterNode(ctx, validRequest)
	if err == nil {
		t.Error("RegisterNode should have failed due to persist builder error")
	}
	if s.r.Version() != oldVersion {
		t.Errorf("RegisterNode should have failed due to persist builder error. Ring was modified")
	}

}

func TestParseSlaveAddrs(t *testing.T) {
	slaves := []string{"1.1.1.1:8000", "2.2.2.2:8000"}
	rslaves := parseSlaveAddrs(slaves)
	if len(rslaves) != 2 {
		t.Errorf("parseSlaveAddrs(%#v), should have returned 2 RingSlaves but got: %#v", slaves, rslaves)
	}
}

func TestServer_ParseConfig(t *testing.T) {
	b := ring.NewBuilder(64)
	b.SetReplicaCount(3)
	b.AddNode(true, 1, []string{"server1", "zone1"}, []string{"1.2.3.4:56789"}, "server1|meta one", []byte("Conf Thing1"))
	b.AddNode(true, 1, []string{"dummy1", "zone42"}, []string{"1.42.42.42:56789"}, "dummy1|meta one", []byte("Dummy Conf"))
	ring := b.Ring()

	rbytes := []byte("imnotaring")
	bbytes := []byte("imnotbuilder")
	mock := &MockRingBuilderThings{
		builderPath:  "/tmp/test.builder",
		ringPath:     "/tmp/test.ring",
		ring:         ring,
		ringbytes:    &rbytes,
		builder:      b,
		builderbytes: &bbytes,
		managedNodes: make(map[uint64]ManagedNode, 0),
		slaves:       make([]*RingSlave, 0),
		changeChan:   make(chan *changeMsg, 1),
	}
	baseLogger := zap.New(zap.NewJSONEncoder())
	baseLogger.SetLevel(zap.InfoLevel)
	ctxlog := baseLogger.With(zap.String("service", "test"))
	s := newTestServer(&Config{}, "test", ctxlog, mock)
	s.parseConfig()

	if s.cfg.NetFilter == nil {
		t.Errorf("Failed to set default NetFilter")
	}
	if s.cfg.TierFilter == nil {
		t.Errorf("Failed to set default TierFilter")
	}
	if s.cfg.Port != DefaultPort {
		t.Errorf("Failed to set default Port: %#v", s.cfg.Port)
	}
	if s.cfg.MsgRingPort != DefaultMsgRingPort {
		t.Errorf("Failed to set default MsgRingPort: %#v", s.cfg.MsgRingPort)
	}
	if s.cfg.CmdCtrlPort != DefaultCmdCtrlPort {
		t.Errorf("Failed to set default CmdCtrlPort: %#v", s.cfg.CmdCtrlPort)
	}
	if s.cfg.RingDir != filepath.Join(DefaultRingDir, s.servicename) {
		t.Errorf("Failed to set default RingDir: %#v", s.cfg.RingDir)
	}
	if s.cfg.CertFile != DefaultCertFile {
		t.Errorf("Failed to set default CertFile: %#v", s.cfg.CertFile)
	}
	if s.cfg.KeyFile != DefaultCertKey {
		t.Errorf("Failed to set default KeyFile: %#v", s.cfg.KeyFile)
	}
}

func TestServer_LoadRingBuilderBytes(t *testing.T) {
}

func TestServer_RingBuilderPersisterFn(t *testing.T) {
	s, m := newTestServerWithDefaults()
	m.builder.SetConfig([]byte("persisttest"))

	change := &RingChange{
		r: m.builder.Ring(),
		b: m.builder,
	}
	change.v = change.r.Version()
	tmpdir, err := ioutil.TempDir("", "rbpfntest")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	s.cfg.RingDir = tmpdir
	berr, rerr := s.ringBuilderPersisterFn(change, false)
	if berr != nil || rerr != nil {
		t.Errorf("ringBuilderPersisterFn(%v, false), should not have errored, got: %v, %v", change, berr, rerr)
	}
	berr, rerr = s.ringBuilderPersisterFn(change, true)
	if berr != nil || rerr != nil {
		t.Errorf("ringBuilderPersisterFn(%v, true), should not have errored, got: %v, %v", change, berr, rerr)
	}
	entries, err := ioutil.ReadDir(tmpdir)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	bnameExpected := fmt.Sprintf("%d-%s.builder", change.v, s.servicename)
	rnameExpected := fmt.Sprintf("%d-%s.ring", change.v, s.servicename)

	var bfound bool
	var rfound bool

	for _, entry := range entries {
		if entry.Name() == bnameExpected {
			bfound = true
		}
		if entry.Name() == rnameExpected {
			rfound = true
		}
	}

	if !bfound || !rfound {
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Errorf("Couldn't find %s or %s file in tmpdir (%s): %#v", bnameExpected, rnameExpected, tmpdir, names)
	}

	bnameExpected = fmt.Sprintf("%s.builder", s.servicename)
	rnameExpected = fmt.Sprintf("%s.ring", s.servicename)

	bfound = false
	rfound = false

	for _, entry := range entries {
		if entry.Name() == bnameExpected {
			bfound = true
		}
		if entry.Name() == rnameExpected {
			rfound = true
		}
	}

	if !bfound || !rfound {
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Errorf("Couldn't find %s or %s file in tmpdir (%s): %#v", bnameExpected, rnameExpected, tmpdir, names)
	}

	err = os.RemoveAll(tmpdir)
	if err != nil {
		t.Errorf(err.Error())
	}

}

func TestServer_ApplyRingChange(t *testing.T) {

	s, m := newTestServerWithDefaults()

	m.builder.SetConfig([]byte("persisttest"))

	change := &RingChange{
		r: m.builder.Ring(),
		b: m.builder,
	}
	change.v = change.r.Version()

	err := s.applyRingChange(change)
	if err != nil {
		t.Errorf("applyRingChange(%v), should not have errored, got: %v", change, err)
	}

	m.persistBuilderErr = fmt.Errorf("persist builder oops")
	err = s.applyRingChange(change)
	if err == nil {
		t.Errorf("applyRingChange(%v), should have errored because of failed builder persist", change)
	}
	m.persistBuilderErr = nil

	m.persistRingErr = fmt.Errorf("persist ring oops")
	err = s.applyRingChange(change)
	if err == nil {
		t.Errorf("applyRingChange(%v), should have errored because of failed ring persist", change)
	}
	m.persistRingErr = nil

	m.bytesLoaderErr = fmt.Errorf("loader oops")
	err = s.applyRingChange(change)
	if err == nil {
		t.Errorf("applyRingChange(%v), should have failed because of loader err.", change)
	}

}

func TestServer_ValidNodeIP(t *testing.T) {
	s, _ := newTestServerWithDefaults()

	_, netblock, _ := net.ParseCIDR("10.0.0.0/24")
	s.netlimits = append(s.netlimits, netblock)

	loopback := "127.0.0.1/32"
	multicast := "224.0.0.1/32"
	inlimit := "10.0.0.1/32"
	badips := []string{
		loopback,
		multicast,
		"2.2.2.2/32",
	}
	for _, v := range badips {
		i, _, _ := net.ParseCIDR(v)
		if s.validNodeIP(i) {
			t.Errorf("validNodeIP(%s) should have been false but was true", v)
		}
	}
	log.Println(inlimit)
	okip, _, err := net.ParseCIDR(inlimit)
	if err != nil {
		panic(err)
	}
	if !s.validNodeIP(okip) {
		t.Errorf("validNodeIP(%s) should have been true but was false", okip)
	}
}

func TestServer_ValidTiers(t *testing.T) {
	s, _ := newTestServerWithDefaults()
	s.tierlimits = append(s.tierlimits, "*.zone")

	oktiers := []string{"localhost", "zone1"}
	exists := []string{"server1"}
	notiers := []string{}

	if !s.validTiers(oktiers) {
		t.Errorf("validTiers(%#v), should have been true", oktiers)
	}

	if s.validTiers(exists) {
		t.Errorf("validTiers(%#v), should have been false because host already exists", exists)
	}

	if s.validTiers(notiers) {
		t.Errorf("validTiers(%#v), should have been false because not enough tiers", notiers)
	}

}

func TestServer_NodeInRing(t *testing.T) {
	s, _ := newTestServerWithDefaults()

	if !s.nodeInRing("server1", []string{"1.2.3.4:56789"}) {
		t.Errorf("nodeInRing(server1, 1.2.3.4:56789), should have been true because node is in ring")
	}

	if !s.nodeInRing("server2", []string{"1.2.3.4:56789"}) {
		a := strings.Join([]string{s.r.Nodes()[0].Address(0)}, "|")
		log.Printf(".%s.", a)
		r, err := s.r.Nodes().Filter([]string{fmt.Sprintf("address~=%s", a)})
		log.Println(r)
		log.Println(err)
		t.Errorf("nodeInRing(server2, 1.2.3.4:56789), should have been true because ip is already in ring")
	}

	if s.nodeInRing("server2", []string{"1.2.3.5:56789"}) {
		t.Errorf("nodeInRing(server2, 1.2.3.5:56789), should have been false because node is not in ring")
	}
}
