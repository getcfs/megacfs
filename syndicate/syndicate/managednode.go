package syndicate

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gholt/ring"
	cc "github.com/pandemicsyn/cmdctrl/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	_FH_STOP_NODE_TIMEOUT = 180
)

var (
	DEFAULT_CTX_TIMEOUT = 10 * time.Second
	RESTART_CTX_TIMEOUT = 180 * time.Second
)

func ParseManagedNodeAddress(addr string, port int) (string, error) {
	if addr == "" {
		return "", fmt.Errorf("address missing")
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, port), nil
}

func bootstrapManagedNodes(ring ring.Ring, ccport int, ctxlog *log.Entry, gopts []grpc.DialOption) map[uint64]ManagedNode {
	nodes := ring.Nodes()
	m := make(map[uint64]ManagedNode, len(nodes))
	for _, node := range nodes {
		addr, err := ParseManagedNodeAddress(node.Address(0), ccport)
		if err != nil {
			ctxlog.WithFields(log.Fields{
				"node":    node.ID(),
				"address": node.Address(0),
			}).Info("Can't split address in node (skipped node)")
			continue
		}
		m[node.ID()], err = NewManagedNode(&ManagedNodeOpts{Address: addr, GrpcOpts: gopts})
		if err != nil {
			ctxlog.WithFields(log.Fields{
				"node":    node.ID(),
				"address": node.Address(0),
				"err":     err,
			}).Warning("Unable to bootstrap node")
		} else {
			ctxlog.WithFields(log.Fields{
				"node":    node.ID(),
				"address": node.Address(0),
			}).Debug("Added node")
		}
	}
	return m
}

type ManagedNode interface {
	Connect() error
	Disconnect() error
	Ping() (bool, string, error)
	Stop() error
	Restart() (bool, string, error)
	RingUpdate(*[]byte, int64) (bool, error)
	Lock()
	Unlock()
	RLock()
	RUnlock()
	Address() string
	GetSoftwareVersion() (string, error)
	UpgradeSoftwareVersion(string) (bool, error)
}

type managedNode struct {
	sync.RWMutex
	failcount   int64
	ringversion int64
	active      bool
	conn        *grpc.ClientConn
	client      cc.CmdCtrlClient
	address     string
	grpcOpts    []grpc.DialOption
}

type ManagedNodeOpts struct {
	Address  string
	GrpcOpts []grpc.DialOption
}

func NewManagedNode(o *ManagedNodeOpts) (ManagedNode, error) {
	var err error
	node := &managedNode{}
	if o.Address == "" {
		return &managedNode{}, fmt.Errorf("Invalid Address supplied")
	}
	node.address = o.Address
	node.grpcOpts = o.GrpcOpts

	node.conn, err = grpc.Dial(node.address, node.grpcOpts...)
	if err != nil {
		return &managedNode{}, fmt.Errorf("Failed to dial cmdctrl server for node %s: %v", node.address, err)
	}
	node.client = cc.NewCmdCtrlClient(node.conn)
	node.active = false
	node.failcount = 0
	return node, nil
}

func (n *managedNode) Address() string {
	n.RLock()
	defer n.RUnlock()
	return n.address
}

// Connect sets up a grpc connection for the node.
// Note that this will overwrite an existing conn.
func (n *managedNode) Connect() error {
	n.Lock()
	defer n.Unlock()
	var opts []grpc.DialOption
	var creds credentials.TransportCredentials
	var err error
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	n.conn, err = grpc.Dial(n.address, opts...)
	if err != nil {
		return fmt.Errorf("Failed to dial ring server for config: %v", err)
	}
	n.client = cc.NewCmdCtrlClient(n.conn)
	return nil
}

// Disconnect lets you disconnect a managed nodes grpc conn.
func (n *managedNode) Disconnect() error {
	n.Lock()
	defer n.Unlock()
	return n.conn.Close()
}

// Ping verifies a node as actually still alive.
func (n *managedNode) Ping() (bool, string, error) {
	n.RLock()
	defer n.RUnlock()
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	status, err := n.client.HealthCheck(ctx, &cc.EmptyMsg{})
	if err != nil {
		return false, "", err
	}
	return status.Status, status.Msg, err
}

// Stop a remote node
func (n *managedNode) Stop() error {
	n.Lock()
	defer n.Unlock()
	ctx, _ := context.WithTimeout(context.Background(), _FH_STOP_NODE_TIMEOUT*time.Second)
	status, err := n.client.Stop(ctx, &cc.EmptyMsg{})
	if err != nil {
		return err
	}
	n.active = status.Status
	return nil
}

// Restart a remote node
func (n *managedNode) Restart() (bool, string, error) {
	n.Lock()
	defer n.Unlock()
	ctx, _ := context.WithTimeout(context.Background(), RESTART_CTX_TIMEOUT*time.Second)
	status, err := n.client.Restart(ctx, &cc.EmptyMsg{})
	if err != nil {
		return false, "", err
	}
	return status.Status, status.Msg, nil
}

// RingUpdate lets you push a ring update to a remote node
// If the underlying grpc conn is not ready it will wait for it to become
// available. If the underlying conn is shutdown (like we caught an update
// while in the processes of removing a managed node), no update is performed.
func (n *managedNode) RingUpdate(r *[]byte, version int64) (bool, error) {
	n.Lock()
	defer n.Unlock()
	if n.ringversion == version {
		return false, nil
	}
	ctx, _ := context.WithTimeout(context.Background(), DEFAULT_CTX_TIMEOUT)
	ru := &cc.Ring{
		Ring:    *r,
		Version: version,
	}
	status, err := n.client.RingUpdate(ctx, ru)
	if err != nil {
		if status != nil {
			if status.Newversion == version {
				return true, err
			}
		}
		return false, err
	}
	n.ringversion = status.Newversion
	if n.ringversion != ru.Version {
		return false, fmt.Errorf("Ring update failed. Expected: %d, but node reports: %d\n", ru.Version, status.Newversion)
	}
	return true, nil
}

// GetSoftwareVersion retrieves a managed nodes running version
func (n *managedNode) GetSoftwareVersion() (string, error) {
	n.RLock()
	defer n.RUnlock()
	ctx, _ := context.WithTimeout(context.Background(), DEFAULT_CTX_TIMEOUT)
	version, err := n.client.SoftwareVersion(ctx, &cc.EmptyMsg{})
	return version.Version, err
}

// UpgradeSoftwareVersion asks a managed node to download and replace the running software
func (n *managedNode) UpgradeSoftwareVersion(version string) (bool, error) {
	n.Lock()
	defer n.Unlock()
	ctx, _ := context.WithTimeout(context.Background(), DEFAULT_CTX_TIMEOUT)
	status, err := n.client.SelfUpgrade(ctx, &cc.SelfUpgradeMsg{Version: version})
	return status.Status, err
}

// TODO: if disconnect encounters an error we just log it and remove the node anyway
func (s *Server) removeManagedNodes(nodes []uint64) {
	for _, nodeid := range nodes {
		if node, ok := s.managedNodes[nodeid]; ok {
			err := node.Disconnect()
			if err != nil {
				s.ctxlog.WithFields(log.Fields{"nodeid": nodeid, "err": err}).Warning("error disconnecting node")
			}
			delete(s.managedNodes, nodeid)
			s.metrics.managedNodes.Dec()
		}
	}
	return
}
