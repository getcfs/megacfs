package syndicate

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"path/filepath"
	"strings"
	"sync"

	"github.com/getcfs/megacfs/ftls"
	pb "github.com/getcfs/megacfs/syndicate/api/proto"
	"github.com/gholt/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/uber-go/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	_SYN_REGISTER_TIMEOUT = 4
	_SYN_DIAL_TIMEOUT     = 2
	DefaultPort           = 8443                        //The default port to use for the main backend service
	DefaultCmdCtrlPort    = 4443                        //The default port to use for cmdctrl (address0)
	DefaultMsgRingPort    = 8001                        //The default port the TCPMsgRing should use (address1)
	DefaultStorePort      = 6379                        //The default port the Store's should use (address2)
	DefaultRingDir        = "/etc/syndicate/ring"       //The default directory where to store the rings
	DefaultCertFile       = "/etc/syndicate/server.crt" //The default SSL Cert
	DefaultCertKey        = "/etc/syndicate/server.key" //The default SSL Key
	DefaultCAFile         = "/etc/syndicate/ca.pem"     //The default SSL CA
	DefaultMutualTLS      = true                        //The default mutual tls auth setting
)

var (
	DefaultNetFilter  = []string{"10.0.0.0/8", "192.168.0.0/16"} //Default the netfilters to private networks
	DefaultTierFilter = []string{".*"}                           //Default to ...anything

	ErrInvalidTiers = errors.New("Tier0 already present in ring")
	ErrInvalidAddrs = errors.New("No valid addresses provided")
)

//Config options for syndicate manager
type Config struct {
	Master           bool
	Debug            bool
	Slaves           []string
	NetFilter        []string
	TierFilter       []string
	Port             int
	MsgRingPort      int
	CmdCtrlPort      int
	CmdCtrlIndex     int
	StorePort        int
	RingDir          string
	CertFile         string
	KeyFile          string
	CAFile           string
	MutualTLS        bool
	WeightAssignment string
	logger           zap.Logger
}

//Logger returns the zap.Logger instance
func (c *Config) Logger() zap.Logger {
	return c.logger
}

func parseSlaveAddrs(slaveAddrs []string) []*RingSlave {
	slaves := make([]*RingSlave, len(slaveAddrs))
	for i, v := range slaveAddrs {
		slaves[i] = &RingSlave{
			status: false,
			addr:   v,
		}
	}
	return slaves
}

type syndicateMetrics struct {
	managedNodes    prometheus.Gauge
	subscriberNodes prometheus.Gauge
}

func metricsInit(servicename string) *syndicateMetrics {
	m := syndicateMetrics{}
	m.managedNodes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "ManagedNodes",
		Help:        "Current number of nodes managed.",
		ConstLabels: prometheus.Labels{"servicename": servicename},
	})
	m.subscriberNodes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "SubscriberNodes",
		Help:        "Current number of unmanaged nodes subscribed for ring changes.",
		ConstLabels: prometheus.Labels{"servicename": servicename},
	})
	prometheus.Register(m.managedNodes)
	prometheus.Register(m.subscriberNodes)
	return &m
}

//Server is the syndicate manager instance
type Server struct {
	sync.RWMutex
	servicename    string
	cfg            *Config
	logger         zap.Logger
	metrics        *syndicateMetrics
	r              ring.Ring
	b              *ring.Builder
	slaves         []*RingSlave
	localAddress   string
	rb             *[]byte // even a 1000 node ring is reasonably small (17k) so just keep the current ring in mem
	bb             *[]byte
	netlimits      []*net.IPNet
	tierlimits     []string
	managedNodes   map[uint64]ManagedNode
	cOpts          []grpc.DialOption //managed node client dial options
	changeChan     chan *changeMsg
	ringSubs       *RingSubscribers
	subsChangeChan chan *changeMsg
	// mostly just present to aid mocking
	rbLoaderFn   func(path string) ([]byte, error)
	rbPersistFn  func(c *RingChange, renameMaster bool) (error, error)
	getBuilderFn func(path string) (*ring.Builder, error)
}

//MockOpt is just used for testing
type MockOpt func(*Server)

//WithRingBuilderPersister is used for testing/mocking
func WithRingBuilderPersister(p func(c *RingChange, renameMaster bool) (error, error)) MockOpt {
	return func(s *Server) {
		s.rbPersistFn = p
	}
}

//WithRingBuilderBytesLoader is used for testing/mocking
func WithRingBuilderBytesLoader(l func(path string) ([]byte, error)) MockOpt {
	return func(s *Server) {
		s.rbLoaderFn = l
	}
}

//WithGetBuilderFn is used for testing/mocking
func WithGetBuilderFn(l func(path string) (*ring.Builder, error)) MockOpt {
	return func(s *Server) {
		s.getBuilderFn = l
	}
}

//NewServer returns a new instance of an up and running syndicate mangement node
func NewServer(cfg *Config, servicename string, logger zap.Logger, opts ...MockOpt) (*Server, error) {
	var err error
	s := new(Server)
	s.cfg = cfg
	s.servicename = servicename

	if s.cfg.Debug {
		logger.SetLevel(zap.DebugLevel)
	} else {
		logger.SetLevel(zap.InfoLevel)
	}
	s.logger = logger.With(zap.String("service", servicename))
	s.metrics = metricsInit(s.servicename)

	s.parseConfig()

	for _, opt := range opts {
		opt(s)
	}
	if s.rbPersistFn == nil {
		s.rbPersistFn = s.ringBuilderPersisterFn
	}
	if s.rbLoaderFn == nil {
		s.rbLoaderFn = func(path string) ([]byte, error) {
			return ioutil.ReadFile(path)
		}
	}
	if s.getBuilderFn == nil {
		s.getBuilderFn = s.getBuilder
	}

	bfile, rfile, err := getRingPaths(cfg, s.servicename)
	if err != nil {
		panic(err)
	}

	_, s.b, err = ring.RingOrBuilder(bfile)
	FatalIf(err, fmt.Sprintf("Builder file (%s) load failed:", bfile))
	s.r, _, err = ring.RingOrBuilder(rfile)
	FatalIf(err, fmt.Sprintf("Ring file (%s) load failed:", rfile))
	//TODO: verify ring version in bytes matches what we expect
	s.rb, s.bb, err = s.loadRingBuilderBytes(s.r.Version())
	FatalIf(err, "Attempting to load ring/builder bytes")

	for _, v := range cfg.NetFilter {
		_, n, err := net.ParseCIDR(v)
		if err != nil {
			FatalIf(err, "Invalid network range provided")
		}
		s.netlimits = append(s.netlimits, n)
	}
	s.tierlimits = cfg.TierFilter
	tlsConf := &ftls.Config{
		MutualTLS: cfg.MutualTLS,
		CertFile:  cfg.CertFile,
		KeyFile:   cfg.KeyFile,
		CAFile:    cfg.CAFile,
	}
	s.cOpts = make([]grpc.DialOption, 0)
	tlsOpts, err := ftls.NewGRPCClientDialOpt(tlsConf)
	if err != nil {
		return s, fmt.Errorf("Err setting up client ssl certs: %s", err.Error())
	}
	s.cOpts = append(s.cOpts, tlsOpts)
	s.managedNodes = bootstrapManagedNodes(s.r, s.cfg.CmdCtrlPort, s.logger, s.cOpts)
	s.metrics.managedNodes.Set(float64(len(s.managedNodes)))
	s.changeChan = make(chan *changeMsg, 1)
	s.subsChangeChan = make(chan *changeMsg, 1)
	go s.RingChangeManager()
	s.ringSubs = &RingSubscribers{
		subs: make(map[string]chan *pb.Ring),
	}
	go s.ringSubscribersNotify()
	s.slaves = parseSlaveAddrs(cfg.Slaves)
	if len(s.slaves) == 0 {
		s.logger.Debug("running without slaves")
		return s, nil
	}

	failcount := 0
	for _, slave := range s.slaves {
		if err = s.RegisterSlave(slave); err != nil {
			s.logger.Warn(
				"error registering slave",
				zap.String("slave", slave.addr),
				zap.Bool("status", slave.status),
				zap.Error(err),
			)
			failcount++
		}
	}
	if failcount > (len(s.slaves) / 2) {
		return s, fmt.Errorf("more than half of the ring slaves failed to respond. Exiting.")
	}
	return s, nil
}

func (s *Server) parseConfig() {
	if s.cfg.NetFilter == nil {
		s.cfg.NetFilter = DefaultNetFilter
		s.logger.Debug(fmt.Sprintf("Config didn't specify netfilter, using default - %s", DefaultNetFilter))
	}
	if s.cfg.TierFilter == nil {
		s.cfg.TierFilter = DefaultTierFilter
		s.logger.Debug(fmt.Sprintf("Config didn't specify tierfilter, using default - %s", DefaultTierFilter))
	}
	if s.cfg.Port == 0 {
		s.logger.Debug("Config didn't specify port, using default", zap.Int("default", DefaultPort))
		s.cfg.Port = DefaultPort
	}
	if s.cfg.MsgRingPort == 0 {
		s.logger.Debug("Config didn't specify msg ring port, using default", zap.Int("default", DefaultMsgRingPort))
		s.cfg.MsgRingPort = DefaultMsgRingPort
	}
	if s.cfg.StorePort == 0 {
		s.logger.Debug("Config didn't specify store port, using default", zap.Int("default", DefaultStorePort))
		s.cfg.StorePort = DefaultStorePort
	}
	if s.cfg.CmdCtrlPort == 0 {
		s.logger.Debug("Config didn't specify cmdctrl port, using default", zap.Int("default", DefaultCmdCtrlPort))
		s.cfg.CmdCtrlPort = DefaultCmdCtrlPort
	}
	if s.cfg.CmdCtrlIndex == 0 {
		s.logger.Debug("Config didn't specify cmdctrl index, using default", zap.Int("default", 0))
	}
	if s.cfg.RingDir == "" {
		s.cfg.RingDir = filepath.Join(DefaultRingDir, s.servicename)
		s.logger.Debug("Config didn't specify ringdir, using default", zap.String("default", s.cfg.RingDir))
	}
	if s.cfg.CertFile == "" {
		s.logger.Debug("Config didn't specify certfile, using default", zap.String("default", DefaultCertFile))
		s.cfg.CertFile = DefaultCertFile
	}
	if s.cfg.KeyFile == "" {
		s.logger.Debug("Config didn't specify keyfile, using default", zap.String("default", DefaultCertKey))
		s.cfg.KeyFile = DefaultCertKey
	}
	if s.cfg.CAFile == "" {
		s.logger.Debug("Config didn't specify keyfile, using default", zap.String("default", DefaultCAFile))
		s.cfg.CAFile = DefaultCAFile
	}
}

func (s *Server) loadRingBuilderBytes(version int64) (ring, builder *[]byte, err error) {
	b, err := s.rbLoaderFn(fmt.Sprintf("%s/%d-%s.builder", s.cfg.RingDir, version, s.servicename))
	if err != nil {
		return ring, builder, err
	}
	r, err := s.rbLoaderFn(fmt.Sprintf("%s/%d-%s.ring", s.cfg.RingDir, version, s.servicename))
	if err != nil {
		return ring, builder, err
	}
	return &r, &b, nil
}

type RingChange struct {
	b            *ring.Builder
	r            ring.Ring
	v            int64
	removedNodes []uint64
}

//ringBuilderPersisterFn is the default ring & builder persistence method used when a ring change is triggered.
// It writes out first the builder file THEN the ring file. If the write of the builder file fails it immediately
// returns an error. By default it writes changes to version-servicename.{builder|ring}. If renameMaster is true
// it will instead write directly to servicename.{builder|ring}
// TODO: if renameMaster is true we should just write to a tmp file and mv in place or the like
func (s *Server) ringBuilderPersisterFn(c *RingChange, renameMaster bool) (error, error) {
	//Write Ring/Builder out to versioned file names
	if !renameMaster {
		if err := ring.PersistRingOrBuilder(nil, c.b, fmt.Sprintf("%s/%d-%s.builder", s.cfg.RingDir, c.v, s.servicename)); err != nil {
			return err, nil
		}
		if err := ring.PersistRingOrBuilder(c.r, nil, fmt.Sprintf("%s/%d-%s.ring", s.cfg.RingDir, c.v, s.servicename)); err != nil {
			return nil, err
		}
		return nil, nil
	}
	//Write Ring/Builder out to plain servicename.ring and servicename.builder files
	if err := ring.PersistRingOrBuilder(nil, c.b, fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename)); err != nil {
		return err, nil
	}
	if err := ring.PersistRingOrBuilder(c.r, nil, fmt.Sprintf("%s/%s.ring", s.cfg.RingDir, s.servicename)); err != nil {
		return nil, err
	}
	return nil, nil
}

//applyRingChange attempts to actually apply and persist the disk the given ring change.
func (s *Server) applyRingChange(c *RingChange) error {
	builderErr, ringErr := s.rbPersistFn(c, false)
	if builderErr != nil {
		s.logger.Warn(
			"unable to persist builder",
			zap.String("path", fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename)),
			zap.Int64("ringver", c.v),
			zap.Error(builderErr),
		)
		return builderErr
	}
	if ringErr != nil {
		s.logger.Warn(
			"unable to persist ring",
			zap.String("path", fmt.Sprintf("%s/%s.ring", s.cfg.RingDir, s.servicename)),
			zap.Int64("ringver", c.v),
			zap.Error(ringErr),
		)
		return ringErr
	}
	newRB, newBB, err := s.loadRingBuilderBytes(c.v)
	if err != nil {
		return fmt.Errorf("failed to load new ring/builder bytes: %s", err)
	}
	err = s.replicateRing(c.r, newRB, newBB)
	if err != nil {
		return fmt.Errorf("ring replicate failed: %s", err)
	}
	//now update the current working ring
	builderErr, ringErr = s.rbPersistFn(c, true)
	s.rb = newRB
	s.bb = newBB
	s.b = c.b
	s.r = c.r
	if len(c.removedNodes) != 0 {
		s.removeManagedNodes(c.removedNodes)
	}
	go s.NotifyNodes()
	return nil
}

//AddNode not currently used by anything.
//TODO: Need field/value error checks
func (s *Server) AddNode(c context.Context, e *pb.Node) (*pb.RingStatus, error) {
	s.Lock()
	defer s.Unlock()
	s.logger.Debug("got AddNode request")
	b, err := s.getBuilderFn(fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename))
	if err != nil {
		s.logger.Warn(
			"unable to load builder for change",
			zap.String("path", fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename)),
			zap.Error(err),
		)
		return &pb.RingStatus{}, err
	}
	n, err := b.AddNode(e.Active, e.Capacity, e.Tiers, e.Addresses, e.Meta, e.Conf)
	if err != nil {
		return &pb.RingStatus{}, err
	}
	s.logger.Debug("proposed ring entry", zap.Object("node", n))
	newRing := b.Ring()
	s.logger.Debug("attempting to apply ring version", zap.Int64("proposed-ringver", newRing.Version()))
	err = s.applyRingChange(&RingChange{b: b, r: newRing, v: newRing.Version()})
	if err != nil {
		s.logger.Warn("failed to apply ring change",
			zap.Int64("proposed-ringver", newRing.Version()),
			zap.Int64("ringver", s.r.Version()),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	s.logger.Info("updated ring", zap.Int64("ringver", s.r.Version()))
	return &pb.RingStatus{Status: true, Version: s.r.Version()}, nil
}

//getBuilder loads a builder from disk
func (s *Server) getBuilder(path string) (*ring.Builder, error) {
	_, b, err := ring.RingOrBuilder(path)
	return b, err
}

//getRing loads a ring from disk
func (s *Server) getRing(path string) (ring.Ring, error) {
	r, _, err := ring.RingOrBuilder(path)
	return r, err
}

//RemoveNode removes a node given node to the ring. If any errors are encountered
//the ring change is discarded. The response RingStatus message should only have True
//Status if the ring change succeeded. The active Ring Version at the end of the call
//is always returned.
func (s *Server) RemoveNode(c context.Context, n *pb.Node) (*pb.RingStatus, error) {
	s.Lock()
	defer s.Unlock()
	s.logger.Debug("got RemoveNode request")
	b, err := s.getBuilderFn(fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename))
	if err != nil {
		s.logger.Warn("unable to load builder for change",
			zap.String("path", fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename)),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	node := b.Node(n.Id)
	if node == nil {
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, fmt.Errorf("node ID not found")
	}
	b.RemoveNode(n.Id)
	newRing := b.Ring()
	change := RingChange{
		b:            b,
		r:            newRing,
		v:            newRing.Version(),
		removedNodes: []uint64{n.Id},
	}
	s.logger.Debug("attempting to apply ring version", zap.Int64("proposed-ringver", newRing.Version()))
	err = s.applyRingChange(&change)
	if err != nil {
		s.logger.Warn("failed to apply ring change",
			zap.Int64("proposed-ringver", newRing.Version()),
			zap.Int64("ringver", s.r.Version()),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	s.logger.Info("updated ring", zap.Int64("ringver", s.r.Version()))
	return &pb.RingStatus{Status: true, Version: s.r.Version()}, nil
}

func (s *Server) ModNode(c context.Context, n *pb.ModifyMsg) (*pb.RingStatus, error) {
	return &pb.RingStatus{}, nil
}

//SetConf sets the Ring global config to the provided bytes. If any errors are encountered
//the ring change is discarded. The response RingStatus message should only have True
//Status if the ring change succeeded. The active Ring Version at the end of the call
//is always returned.
func (s *Server) SetConf(c context.Context, conf *pb.Conf) (*pb.RingStatus, error) {
	s.Lock()
	defer s.Unlock()
	b, err := s.getBuilderFn(fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename))
	if err != nil {
		s.logger.Warn("unable to load builder for change",
			zap.String("path", fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename)),
			zap.Error(err),
		)
		return &pb.RingStatus{}, err
	}
	b.SetConfig(conf.Conf)
	newRing := b.Ring()
	s.logger.Debug("attempting to apply ring version", zap.Int64("proposed-ringver", newRing.Version()))
	err = s.applyRingChange(&RingChange{b: b, r: newRing, v: newRing.Version()})
	if err != nil {
		s.logger.Warn("failed to apply ring change",
			zap.Int64("proposed-ringver", newRing.Version()),
			zap.Int64("ringver", s.r.Version()),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	s.logger.Info("updated ring", zap.Int64("ringver", s.r.Version()))
	return &pb.RingStatus{Status: true, Version: s.r.Version()}, nil
}

func (s *Server) SetActive(c context.Context, n *pb.Node) (*pb.RingStatus, error) {
	s.Lock()
	defer s.Unlock()
	b, err := s.getBuilderFn(fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename))
	if err != nil {
		s.logger.Warn("unable to load builder for change",
			zap.String("path", fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename)),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	node := b.Node(n.Id)
	if node == nil {
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, fmt.Errorf("node not found")
	}
	node.SetActive(n.Active)
	newRing := b.Ring()
	s.logger.Debug("attempting to apply ring version", zap.Int64("proposed-ringver", newRing.Version()))
	err = s.applyRingChange(&RingChange{b: b, r: newRing, v: newRing.Version()})
	if err != nil {
		s.logger.Warn("failed to apply ring change",
			zap.Int64("proposed-ringver", newRing.Version()),
			zap.Int64("ringver", s.r.Version()),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	s.logger.Info("updated ring", zap.Int64("ringver", s.r.Version()))
	return &pb.RingStatus{Status: true, Version: s.r.Version()}, nil
}

func (s *Server) SetReplicas(c context.Context, n *pb.RingOpts) (*pb.RingStatus, error) {
	s.Lock()
	defer s.Unlock()
	b, err := s.getBuilderFn(fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename))
	if err != nil {
		s.logger.Warn("unable to load builder for change",
			zap.String("path", fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename)),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	b.SetReplicaCount(int(n.Replicas))
	newRing := b.Ring()
	s.logger.Debug("attempting to apply ring version", zap.Int64("proposed-ringver", newRing.Version()))
	err = s.applyRingChange(&RingChange{b: b, r: newRing, v: newRing.Version()})
	if err != nil {
		s.logger.Warn("failed to apply ring change",
			zap.Int64("proposed-ringver", newRing.Version()),
			zap.Int64("ringver", s.r.Version()),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	s.logger.Info("updated ring", zap.Int64("ringver", s.r.Version()))
	return &pb.RingStatus{Status: true, Version: s.r.Version()}, nil
}

func (s *Server) SetCapacity(c context.Context, n *pb.Node) (*pb.RingStatus, error) {
	s.Lock()
	defer s.Unlock()
	b, err := s.getBuilderFn(fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename))
	if err != nil {
		s.logger.Warn("unable to load builder for change",
			zap.String("path", fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename)),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	node := b.Node(n.Id)
	if node == nil {
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, fmt.Errorf("Node not found")
	}
	node.SetCapacity(n.Capacity)
	newRing := b.Ring()
	s.logger.Debug("attempting to apply ring version", zap.Int64("proposed-ringver", newRing.Version()))
	err = s.applyRingChange(&RingChange{b: b, r: newRing, v: newRing.Version()})
	if err != nil {
		s.logger.Warn("failed to apply ring change",
			zap.Int64("proposed-ringver", newRing.Version()),
			zap.Int64("ringver", s.r.Version()),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	s.logger.Info("updated ring", zap.Int64("ringver", s.r.Version()))
	return &pb.RingStatus{Status: true, Version: s.r.Version()}, nil
}

//ReplaceTiers explicitly sets a node to the provided tiers. NO validation is performed
// on the tiers provided and the address is NOT checked against the TierFilter list.
func (s *Server) ReplaceTiers(c context.Context, n *pb.Node) (*pb.RingStatus, error) {
	s.Lock()
	defer s.Unlock()

	b, err := s.getBuilderFn(fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename))
	if err != nil {
		s.logger.Warn("unable to load builder for change",
			zap.String("path", fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename)),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	node := b.Node(n.Id)
	if node == nil {
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, fmt.Errorf("node not found")
	}
	if len(n.Tiers) == 0 {
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, fmt.Errorf("no tiers provided")
	}
	node.ReplaceTiers(n.Tiers)
	newRing := b.Ring()
	s.logger.Debug("attempting to apply ring version", zap.Int64("proposed-ringver", newRing.Version()))
	err = s.applyRingChange(&RingChange{b: b, r: newRing, v: newRing.Version()})
	if err != nil {
		s.logger.Warn("failed to apply ring change",
			zap.Int64("proposed-ringver", newRing.Version()),
			zap.Int64("ringver", s.r.Version()),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	s.logger.Info("updated ring", zap.Int64("ringver", s.r.Version()))
	return &pb.RingStatus{Status: true, Version: s.r.Version()}, nil
}

//ReplaceAddresses explicitly sets a node to the provided addresses. NO validation is performed
// on the addresses provided and the address is NOT checked against the NetFilter list.
// The only check performed is to verify that the address(s) are not in use on another ring entry.
func (s *Server) ReplaceAddresses(c context.Context, n *pb.Node) (*pb.RingStatus, error) {
	s.Lock()
	defer s.Unlock()

	if len(n.Addresses) == 0 {
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, fmt.Errorf("no addrs provided")
	}
	a := strings.Join(n.Addresses, "|")
	addrnodes, _ := s.r.Nodes().Filter([]string{fmt.Sprintf("address~=%s", a)})

	if len(addrnodes) != 0 {
		if len(addrnodes) > 1 {
			return &pb.RingStatus{Status: false, Version: s.r.Version()}, fmt.Errorf("address already in ring/unable to verify ID (too many matches)")
		}
		if addrnodes[0].ID() != n.Id {
			return &pb.RingStatus{Status: false, Version: s.r.Version()}, fmt.Errorf("address already in ring for other ID")
		}
	}

	b, err := s.getBuilderFn(fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename))
	if err != nil {
		s.logger.Warn("unable to load builder for change",
			zap.String("path", fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename)),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	node := b.Node(n.Id)
	if node == nil {
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, fmt.Errorf("Node not found")
	}
	node.ReplaceAddresses(n.Addresses)
	newRing := b.Ring()
	s.logger.Debug("attempting to apply ring version", zap.Int64("proposed-ringver", newRing.Version()))
	err = s.applyRingChange(&RingChange{b: b, r: newRing, v: newRing.Version()})
	if err != nil {
		s.logger.Warn("failed to apply ring change",
			zap.Int64("proposed-ringver", newRing.Version()),
			zap.Int64("ringver", s.r.Version()),
			zap.Error(err),
		)
		return &pb.RingStatus{Status: false, Version: s.r.Version()}, err
	}
	s.logger.Info("updated ring", zap.Int64("ringver", s.r.Version()))
	return &pb.RingStatus{Status: true, Version: s.r.Version()}, nil
}

func (s *Server) GetVersion(c context.Context, n *pb.EmptyMsg) (*pb.RingStatus, error) {
	s.RLock()
	defer s.RUnlock()
	return &pb.RingStatus{Status: true, Version: s.r.Version()}, nil
}

//GetGlobalConfig retrieves the current global config []bytes present in the ring
func (s *Server) GetGlobalConfig(c context.Context, n *pb.EmptyMsg) (*pb.RingConf, error) {
	s.RLock()
	defer s.RUnlock()
	config := &pb.RingConf{
		Status: &pb.RingStatus{Status: true, Version: s.r.Version()},
		Conf:   &pb.Conf{Conf: s.r.Config(), RestartRequired: false},
	}
	return config, nil
}

//SearchNodes uses the ring's node Filter() method return all nodes
//matching the provided filters. The filter options are currently limited too:
//id~=, meta~=, tier~=, address~=
func (s *Server) SearchNodes(c context.Context, n *pb.Node) (*pb.SearchResult, error) {
	s.RLock()
	defer s.RUnlock()

	var filter []string
	if n.Id != 0 {
		filter = append(filter, fmt.Sprintf("id=%d", n.Id))
	}
	if n.Meta != "" {
		filter = append(filter, fmt.Sprintf("meta~=%s", n.Meta))
	}
	if len(n.Tiers) > 0 {
		for _, v := range n.Tiers {
			filter = append(filter, fmt.Sprintf("tier~=%s", v))
		}
	}
	if len(n.Addresses) > 0 {
		for _, v := range n.Addresses {
			filter = append(filter, fmt.Sprintf("address~=%s", v))
		}
	}
	nodes, err := s.r.Nodes().Filter(filter)
	res := make([]*pb.Node, len(nodes))
	if err != nil {
		return &pb.SearchResult{Nodes: res}, err
	}
	for i, n := range nodes {
		if n == nil {
			continue
		}
		res[i] = &pb.Node{
			Id:        n.ID(),
			Active:    n.Active(),
			Capacity:  n.Capacity(),
			Tiers:     n.Tiers(),
			Addresses: n.Addresses(),
			Meta:      n.Meta(),
			Conf:      n.Config(),
		}
	}
	return &pb.SearchResult{Nodes: res}, nil
}

//GetNodeConfig retrieves a specific nodes ring config []bytes or an error if the node is not found.
func (s *Server) GetNodeConfig(c context.Context, n *pb.Node) (*pb.RingConf, error) {
	s.RLock()
	defer s.RUnlock()
	node := s.r.Node(n.Id)
	if node == nil {
		return &pb.RingConf{}, fmt.Errorf("node %d not found", n.Id)
	}

	config := &pb.RingConf{
		Status: &pb.RingStatus{Status: true, Version: s.r.Version()},
		Conf:   &pb.Conf{Conf: node.Config(), RestartRequired: false},
	}
	return config, nil
}

//GetRing returns the current ring bytes and version
func (s *Server) GetRing(c context.Context, e *pb.EmptyMsg) (*pb.Ring, error) {
	s.RLock()
	defer s.RUnlock()
	return &pb.Ring{Version: s.r.Version(), Ring: *s.rb}, nil
}

//GetRingStream return a stream of rings as they become available
func (s *Server) GetRingStream(req *pb.SubscriberID, stream pb.Syndicate_GetRingStreamServer) error {
	s.RLock()
	ringChange := s.addRingSubscriber(req.Id)
	streamFinished := false
	if err := stream.Send(&pb.Ring{Version: s.r.Version(), Ring: *s.rb}); err != nil {
		s.RUnlock()
		s.logger.Warn("error GetRingStream initial send", zap.Error(err))
		streamFinished = true
		s.removeRingSubscriber(req.Id)
		return nil
	}
	s.RUnlock()
	for ring := range ringChange {
		if err := stream.Send(ring); err != nil {
			s.logger.Warn("error GetRingStream send", zap.Error(err))
			streamFinished = true
			break
		}
	}
	s.logger.Debug("closing ring sub stream")
	//our chan got closed before expected
	if !streamFinished {
		s.removeRingSubscriber(req.Id)
		return fmt.Errorf("ring change chan closed")
	}
	s.removeRingSubscriber(req.Id)
	return nil
}

//validNodeIP verifies that the provided ip is not a loopback or multicast address
//and checks whether the ip is in the configured network limits range.
func (s *Server) validNodeIP(i net.IP) bool {
	switch {
	case i.IsLoopback():
		return false
	case i.IsMulticast():
		return false
	}
	inRange := false
	for _, n := range s.netlimits {
		if n.Contains(i) {
			inRange = true
		}
	}
	return inRange
}

//tier0 must never already exist as a tier0 entry in the ring
func (s *Server) validTiers(t []string) bool {
	if len(t) == 0 {
		return false
	}
	r, err := s.r.Nodes().Filter([]string{fmt.Sprintf("tier0=%s", t[0])})
	if len(r) != 0 || err != nil {
		return false
	}
	/*
		//we're not using multiple tiers anymore
		for i := 1; i <= len(t); i++ {
			for _, v := range s.tierlimits {
				matched, err := regexp.MatchString(v, t[i])
				if err != nil {
					return false
				}
				if matched {
					return true
				}
			}
		}
	*/
	return true
}

//nodeInRing just checks to see if the hostname or addresses appear
//in any existing entries meta or address fields.
func (s *Server) nodeInRing(hostname string, addrs []string) bool {
	a := strings.Join(addrs, "|")
	r, _ := s.r.Nodes().Filter([]string{fmt.Sprintf("meta~=%s.*", hostname)})
	if len(r) != 0 {
		return true
	}
	r, _ = s.r.Nodes().Filter([]string{fmt.Sprintf("address~=%s", a)})
	if len(r) != 0 {
		return true
	}
	return false
}

func (s *Server) RestartNode(c context.Context, n *pb.Node) (*pb.NodeStatus, error) {
	s.Lock()
	defer s.Unlock()
	var err error
	node := s.r.Node(n.Id)
	if node == nil {
		return &pb.NodeStatus{Status: false}, fmt.Errorf("Node not found")
	}
	result := &pb.NodeStatus{}
	result.Status, result.Msg, err = s.managedNodes[n.Id].Restart()
	return result, err
}

func (s *Server) RegisterNode(c context.Context, r *pb.RegisterRequest) (*pb.NodeConfig, error) {
	s.Lock()
	defer s.Unlock()
	s.logger.Debug("got Register request", zap.Object("request", r))
	b, err := s.getBuilderFn(fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename))
	if err != nil {
		s.logger.Warn("unable to load builder for change",
			zap.String("path", fmt.Sprintf("%s/%s.builder", s.cfg.RingDir, s.servicename)),
			zap.Error(err),
		)
		return &pb.NodeConfig{}, err
	}
	var addrs []string
	for _, v := range r.Addrs {
		i, _, err := net.ParseCIDR(v)
		if err != nil {
			s.logger.Warn("unkown network addr received during registration", zap.String("addr", v), zap.Error(err))
			continue
		}
		if s.validNodeIP(i) {
			addrs = append(addrs, fmt.Sprintf("%s:%d", i.String(), s.cfg.CmdCtrlPort))
			addrs = append(addrs, fmt.Sprintf("%s:%d", i.String(), s.cfg.MsgRingPort))
			addrs = append(addrs, fmt.Sprintf("%s:%d", i.String(), s.cfg.StorePort))
		}
	}
	switch {
	case len(addrs) == 0:
		return &pb.NodeConfig{}, ErrInvalidAddrs
	case s.nodeInRing(r.Hostname, addrs):
		a := strings.Join(addrs, "|")
		metanodes, _ := s.r.Nodes().Filter([]string{fmt.Sprintf("meta~=%s.*", r.Hostname)})
		if len(metanodes) > 1 {
			s.logger.Warn("error registering node",
				zap.Int64("ringver", s.r.Version()),
				zap.String("addrs-search", a),
				zap.String("meta-search", r.Hostname),
				zap.Error(fmt.Errorf("more than one meta match when search for node ID")),
			)
			return &pb.NodeConfig{}, fmt.Errorf("Node already in ring/unable to obtain ID (too many matches)")
		}
		addrnodes, _ := s.r.Nodes().Filter([]string{fmt.Sprintf("address~=%s", a)})
		if len(addrnodes) > 1 {
			s.logger.Warn("error registering node",
				zap.Int64("ringver", s.r.Version()),
				zap.String("addrs-search", a),
				zap.String("meta-search", r.Hostname),
				zap.Error(fmt.Errorf("more than one addr match when search for node ID")),
			)
			return &pb.NodeConfig{}, fmt.Errorf("Node already in ring/unable to obtain ID (too many matches)")
		}
		var metaid uint64
		if len(metanodes) == 1 {
			metaid = metanodes[0].ID()
		}
		var addrid uint64
		if len(addrnodes) == 1 {
			addrid = addrnodes[0].ID()
		}
		if metaid != addrid {
			s.logger.Warn("error registering node",
				zap.Int64("ringver", s.r.Version()),
				zap.Uint64("addrid", addrid),
				zap.String("addrs-search", a),
				zap.Uint64("metaid", metaid),
				zap.String("meta-search", r.Hostname),
				zap.Error(fmt.Errorf("addrid and metaid conflict (are not the same)")),
			)
			return &pb.NodeConfig{}, fmt.Errorf("Node already in ring, unable to obtain ID (id by addr and id by meta do not match")
		}
		s.logger.Info("reregistered existing node", zap.Uint64("id", addrid))
		return &pb.NodeConfig{Localid: addrid, Ring: *s.rb}, nil
	case len(r.Tiers) == 0:
		return &pb.NodeConfig{}, fmt.Errorf("No tier0 provided")
	case len(r.Tiers) > 0:
		if !s.validTiers(r.Tiers) {
			return &pb.NodeConfig{}, ErrInvalidTiers
		}
	}

	var weight uint32
	nodeEnabled := false

	switch s.cfg.WeightAssignment {
	case "fixed":
		weight = 1000
		nodeEnabled = true
	case "self":
		if r.Hardware == nil {
			return &pb.NodeConfig{}, fmt.Errorf("No hardware profile provided but required")
		}
		if len(r.Hardware.Disks) == 0 {
			return &pb.NodeConfig{}, fmt.Errorf("No disks in hardware profile")
		}
		weight = ExtractCapacity("/data", r.Hardware.Disks)
		if weight == 0 {
			nodeEnabled = false
		} else {
			nodeEnabled = true
		}
	case "manual":
		if r.Hardware == nil {
			return &pb.NodeConfig{}, fmt.Errorf("No hardware profile provided but required")
		}
		if len(r.Hardware.Disks) == 0 {
			return &pb.NodeConfig{}, fmt.Errorf("No disks in hardware profile")
		}
		weight = ExtractCapacity("/data", r.Hardware.Disks)
		nodeEnabled = false
	default:
		s.logger.Debug("No weight assignment strategy specified, adding unconfigured node!")
		if r.Hardware == nil {
			return &pb.NodeConfig{}, fmt.Errorf("No hardware profile provided but required")
		}
		if len(r.Hardware.Disks) == 0 {
			return &pb.NodeConfig{}, fmt.Errorf("No disks in hardware profile")
		}
		weight = ExtractCapacity("/data", r.Hardware.Disks)
		nodeEnabled = false
	}
	n, err := b.AddNode(nodeEnabled, weight, r.Tiers, addrs, r.Hostname, []byte(""))
	if err != nil {
		return &pb.NodeConfig{}, err
	}
	s.logger.Debug("proposed ring entry", zap.Object("node", n))
	newRing := b.Ring()
	s.logger.Debug("attempting to apply ring version", zap.Int64("proposed-ringver", newRing.Version()))
	err = s.applyRingChange(&RingChange{b: b, r: newRing, v: newRing.Version()})
	if err != nil {
		s.logger.Warn("failed to apply ring change",
			zap.Int64("proposed-ringver", newRing.Version()),
			zap.Int64("ringver", s.r.Version()),
			zap.Error(err),
		)
		return &pb.NodeConfig{}, fmt.Errorf("Unable to apply ring change during registration")
	}
	s.logger.Info("updated ring", zap.Int64("ringver", s.r.Version()))
	s.managedNodes[n.ID()], err = NewManagedNode(&ManagedNodeOpts{Address: n.Address(s.cfg.CmdCtrlIndex), GrpcOpts: s.cOpts})
	if err != nil {
		s.logger.Warn("failed to add new managed node",
			zap.Uint64("id", n.ID()),
			zap.String("address", n.Address(0)),
			zap.Error(err),
		)
	}
	s.metrics.managedNodes.Inc()
	s.logger.Info("added managed node", zap.Uint64("id", n.ID()))
	return &pb.NodeConfig{Localid: n.ID(), Ring: *s.rb}, nil
}
