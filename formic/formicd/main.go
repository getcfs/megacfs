package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/getcfs/megacfs/formic/proto"
	"github.com/getcfs/megacfs/ftls"
	"github.com/getcfs/megacfs/oort/api"
	"github.com/uber-go/zap"

	"net"

	"github.com/getcfs/megacfs/syndicate/utils/sysmetrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	printVersionInfo = flag.Bool("version", false, "print version/build info")
)

var formicdVersion string
var buildDate string
var commitVersion string
var goVersion string

func setupMetrics(listenAddr, enabledCollectors string) error {
	if enabledCollectors == "" {
		enabledCollectors = sysmetrics.FilterAvailableCollectors(sysmetrics.DefaultCollectors)
	}
	collectors, err := sysmetrics.LoadCollectors(enabledCollectors)
	if err != nil {
		return err
	}
	nodeCollector := sysmetrics.New(collectors)
	prometheus.MustRegister(nodeCollector)
	http.Handle("/metrics", prometheus.Handler())
	go http.ListenAndServe(listenAddr, nil)
	return nil
}

func main() {
	flag.Parse()
	if *printVersionInfo {
		fmt.Println("formicd:", formicdVersion)
		fmt.Println("commit:", commitVersion)
		fmt.Println("build date:", buildDate)
		fmt.Println("go version:", goVersion)
		return
	}

	cfg := resolveConfig(nil)

	// Setup logging
	baseLogger := zap.New(zap.NewJSONEncoder())
	if cfg.debug {
		fmt.Println("DEBUG!")
		baseLogger.SetLevel(zap.DebugLevel)
	} else {
		baseLogger.SetLevel(zap.InfoLevel)
	}
	logger := baseLogger.With(zap.String("name", "formicd"))

	err := setupMetrics(cfg.metricsAddr, cfg.metricsCollectors)
	if err != nil {
		logger.Fatal("Couldn't load collectors", zap.Error(err))
	}

	var opts []grpc.ServerOption
	creds, err := credentials.NewServerTLSFromFile(path.Join(cfg.path, "server.crt"), path.Join(cfg.path, "server.key"))
	if err != nil {
		logger.Fatal("Couldn't load cert from file", zap.Error(err))
	}
	opts = []grpc.ServerOption{grpc.Creds(creds)}
	s := grpc.NewServer(opts...)

	var vcOpts []grpc.DialOption
	vtlsConfig := &ftls.Config{
		MutualTLS:          !cfg.skipMutualTLS,
		InsecureSkipVerify: cfg.insecureSkipVerify,
		CertFile:           path.Join(cfg.path, "client.crt"),
		KeyFile:            path.Join(cfg.path, "client.key"),
		CAFile:             path.Join(cfg.path, "ca.pem"),
	}
	vrOpts, err := ftls.NewGRPCClientDialOpt(&ftls.Config{
		MutualTLS:          false,
		InsecureSkipVerify: cfg.insecureSkipVerify,
		CAFile:             path.Join(cfg.path, "ca.pem"),
	})
	if err != nil {
		logger.Fatal("Cannot setup value store tls config for synd client", zap.Error(err))
	}

	var gcOpts []grpc.DialOption
	gtlsConfig := &ftls.Config{
		MutualTLS:          !cfg.skipMutualTLS,
		InsecureSkipVerify: cfg.insecureSkipVerify,
		CertFile:           path.Join(cfg.path, "client.crt"),
		KeyFile:            path.Join(cfg.path, "client.key"),
		CAFile:             path.Join(cfg.path, "ca.pem"),
	}
	grOpts, err := ftls.NewGRPCClientDialOpt(&ftls.Config{
		MutualTLS:          false,
		InsecureSkipVerify: cfg.insecureSkipVerify,
		CAFile:             path.Join(cfg.path, "ca.pem"),
	})
	if err != nil {
		logger.Fatal("Cannot setup group store tls config for synd client", zap.Error(err))
	}

	clientID, _ := os.Hostname()
	if clientID != "" {
		clientID += "/formicd"
	}

	oortLogger := baseLogger.With(zap.String("name", "formicd.oort"))
	vstore := api.NewReplValueStore(&api.ValueStoreConfig{
		Logger:                     oortLogger,
		AddressIndex:               2,
		StoreFTLSConfig:            vtlsConfig,
		GRPCOpts:                   vcOpts,
		RingServer:                 cfg.oortValueSyndicate,
		RingCachePath:              path.Join(cfg.path, "ring/valuestore.ring"),
		RingServerGRPCOpts:         []grpc.DialOption{vrOpts},
		RingClientID:               clientID,
		PoolSize:                   cfg.poolSize,
		ConcurrentRequestsPerStore: cfg.concurrentRequestsPerStore,
	})
	if verr := vstore.Startup(context.Background()); verr != nil {
		logger.Fatal("Cannot start valuestore connector:", zap.Error(err))
	}

	gstore := api.NewReplGroupStore(&api.GroupStoreConfig{
		Logger:                     oortLogger,
		AddressIndex:               2,
		StoreFTLSConfig:            gtlsConfig,
		GRPCOpts:                   gcOpts,
		RingServer:                 cfg.oortGroupSyndicate,
		RingCachePath:              path.Join(cfg.path, "ring/groupstore.ring"),
		RingServerGRPCOpts:         []grpc.DialOption{grOpts},
		RingClientID:               clientID,
		PoolSize:                   cfg.poolSize,
		ConcurrentRequestsPerStore: cfg.concurrentRequestsPerStore,
	})
	if gerr := gstore.Startup(context.Background()); gerr != nil {
		logger.Fatal("Cannot start groupstore connector:", zap.Error(err))
	}

	// starting up formicd
	comms, err := NewStoreComms(vstore, gstore, logger)
	if err != nil {
		logger.Fatal("Error setting up comms", zap.Error(err))
	}
	// TODO: How big should the chan be, or should we have another in memory queue that feeds the chan?
	deleteChan := make(chan *DeleteItem, 1000)
	fs := NewOortFS(comms, logger, deleteChan)
	deletes := newDeletinator(deleteChan, fs, comms, baseLogger.With(zap.String("name", "formicd.deletinator")))
	go deletes.run()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.port))
	if err != nil {
		logger.Fatal("Failed to bind formicd to port", zap.Error(err))
	}
	pb.RegisterFileSystemAPIServer(s, NewFileSystemAPIServer(gstore, baseLogger.With(zap.String("name", "formicd.fs"))))
	pb.RegisterApiServer(s, NewApiServer(fs, cfg.nodeId, comms, logger))
	logger.Info("Starting formic and the filesystem API", zap.Int("port", cfg.port))
	s.Serve(l)
}
