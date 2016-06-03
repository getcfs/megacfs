package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	pb "github.com/getcfs/megacfs/formic/proto"
	"github.com/getcfs/megacfs/ftls"
	"github.com/getcfs/megacfs/oort/api"

	"net"

	"github.com/getcfs/megacfs/syndicate/utils/sysmetrics"
	"github.com/prometheus/client_golang/prometheus"
)

// FatalIf is just a lazy log/panic on error func
func FatalIf(err error, msg string) {
	if err != nil {
		grpclog.Fatalf("%s: %v", msg, err)
	}
}

var (
	printVersionInfo = flag.Bool("version", false, "print version/build info")
)

var formicdVersion string
var buildDate string
var goVersion string

func setupMetrics(listenAddr, enabledCollectors string) {
	if enabledCollectors == "" {
		enabledCollectors = sysmetrics.FilterAvailableCollectors(sysmetrics.DefaultCollectors)
	}
	collectors, err := sysmetrics.LoadCollectors(enabledCollectors)
	if err != nil {
		log.Fatalf("Couldn't load collectors: %s", err)
	}
	nodeCollector := sysmetrics.New(collectors)
	prometheus.MustRegister(nodeCollector)
	http.Handle("/metrics", prometheus.Handler())
	go http.ListenAndServe(listenAddr, nil)
}

func main() {
	flag.Parse()
	if *printVersionInfo {
		fmt.Println("formicd version:", formicdVersion)
		fmt.Println("build date:", buildDate)
		fmt.Println("go version:", goVersion)
		return
	}

	cfg := resolveConfig(nil)
	var logDebug func(formt string, args ...interface{})
	if cfg.debug {
		logDebug = func(formt string, args ...interface{}) {
			if formt != "" && formt[len(formt)-1] == '\n' {
				formt = "DEBUG: " + formt
			} else {
				formt = "DEBUG: " + formt + "\n"
			}
			fmt.Printf(formt, args...)
		}
	}

	setupMetrics(cfg.metricsAddr, cfg.metricsCollectors)

	var opts []grpc.ServerOption
	creds, err := credentials.NewServerTLSFromFile(path.Join(cfg.path, "server.crt"), path.Join(cfg.path, "server.key"))
	FatalIf(err, "Couldn't load cert from file")
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
		grpclog.Fatalln("Cannot setup value store tls config for synd client:", err)
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
		grpclog.Fatalln("Cannot setup group store tls config for synd client:", err)
	}

	clientID, _ := os.Hostname()
	if clientID != "" {
		clientID += "/formicd"
	}

	vstore := api.NewReplValueStore(&api.ReplValueStoreConfig{
		LogDebug:                   logDebug,
		AddressIndex:               2,
		StoreFTLSConfig:            vtlsConfig,
		GRPCOpts:                   vcOpts,
		RingServer:                 cfg.oortValueSyndicate,
		RingCachePath:              path.Join(cfg.path, "ring/valuestore.ring"),
		RingServerGRPCOpts:         []grpc.DialOption{vrOpts},
		RingClientID:               clientID,
		ConcurrentRequestsPerStore: cfg.concurrentRequestsPerStore,
	})
	if verr := vstore.Startup(context.Background()); verr != nil {
		grpclog.Fatalln("Cannot start valuestore connector:", verr)
	}

	gstore := api.NewReplGroupStore(&api.ReplGroupStoreConfig{
		LogDebug:                   logDebug,
		AddressIndex:               2,
		StoreFTLSConfig:            gtlsConfig,
		GRPCOpts:                   gcOpts,
		RingServer:                 cfg.oortGroupSyndicate,
		RingCachePath:              path.Join(cfg.path, "ring/groupstore.ring"),
		RingServerGRPCOpts:         []grpc.DialOption{grOpts},
		RingClientID:               clientID,
		ConcurrentRequestsPerStore: cfg.concurrentRequestsPerStore,
	})
	if gerr := gstore.Startup(context.Background()); gerr != nil {
		grpclog.Fatalln("Cannot start groupstore connector:", gerr)
	}

	// starting up formicd
	comms, err := NewStoreComms(vstore, gstore)
	if err != nil {
		grpclog.Fatalln(err)
	}
	fs := NewOortFS(comms)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.port))
	FatalIf(err, "Failed to bind formicd to port")
	pb.RegisterFileSystemAPIServer(s, NewFileSystemAPIServer(gstore))
	pb.RegisterApiServer(s, NewApiServer(fs, cfg.nodeId, comms))
	grpclog.Printf("Starting up formic and the file system api on %d...\n", cfg.port)
	s.Serve(l)
}
