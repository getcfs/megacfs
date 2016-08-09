package syndicate

import (
	"testing"

	"google.golang.org/grpc"

	"github.com/Sirupsen/logrus"
	"github.com/gholt/ring"
)

func TestParseManagedNodeAddress(t *testing.T) {
	addr := "something:5555"
	port := 4444
	expected := "something:4444"
	resp, err := ParseManagedNodeAddress(addr, port)
	if resp != expected || err != nil {
		t.Errorf("ParseManagedNodeAddress(%s, %d) == %q, want %q", addr, port, resp, expected)
	}
	if err != nil {
		t.Errorf("ParseManagedNodeAddress(%q, %q) returned error: %q", addr, port, err)
	}

	_, err = ParseManagedNodeAddress("", 4444)
	if err == nil {
		t.Errorf("ParseManagedNodeAddress(%s, %d) expected error.", "", 4444)
	}

	_, err = ParseManagedNodeAddress("nope", 4444)
	if err == nil {
		t.Errorf("ParseManagedNodeAddress(%s, %d) expected error.", "nope", 4444)
	}
}

func getTestRing() (*ring.Builder, ring.Ring) {
	b := ring.NewBuilder(64)
	b.SetReplicaCount(3)
	b.AddNode(true, 1, []string{"server1", "zone1"}, []string{"1.2.3.4:56789"}, "Meta One", []byte("Conf"))
	b.AddNode(true, 1, []string{"server2", "zone1"}, []string{"1.2.3.5:56789", "1.2.3.5:9876"}, "Meta Four", []byte("Conf"))
	b.AddNode(false, 0, []string{"server3", "zone1"}, []string{"1.2.3.6:56789"}, "Meta Three", []byte("Conf"))
	return b, b.Ring()
}

func TestBootstrapManagedNodes(t *testing.T) {
	b, ring := getTestRing()
	port := 3333
	ctxlog := logrus.WithField("service", "testservice")
	n := bootstrapManagedNodes(ring, port, ctxlog, []grpc.DialOption{})
	if len(n) != 3 {
		t.Errorf("Should have had 3 bootstrapped nodes but got: %d", len(n))
	}

	b.AddNode(true, 0, []string{"server4"}, []string{"1.1.1.1"}, "", []byte("conf"))

	n = bootstrapManagedNodes(b.Ring(), port, ctxlog, []grpc.DialOption{})
	if len(n) == 4 {
		t.Errorf("Should only have had 3 node but had 4. The 4th node should have been skipped")
	}
}

func TestNewManagedNode(t *testing.T) {
	_, err := NewManagedNode(&ManagedNodeOpts{Address: ""})
	if err == nil {
		t.Errorf("Should have thrown error on empty address")
	}
}
