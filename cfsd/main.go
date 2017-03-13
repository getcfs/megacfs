package main

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/getcfs/megacfs/formic"
	"github.com/getcfs/megacfs/oort/api/server"
	"github.com/gholt/brimtext"
	"github.com/gholt/ring"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
)

var logger *zap.Logger

type redirectGRPCLogger struct {
	sugaredLogger *zap.SugaredLogger
}

func (logger *redirectGRPCLogger) Fatal(args ...interface{}) {
	logger.sugaredLogger.Fatal(args)
}

func (logger *redirectGRPCLogger) Fatalf(format string, args ...interface{}) {
	logger.sugaredLogger.Fatalf(format, args)
}

func (logger *redirectGRPCLogger) Fatalln(args ...interface{}) {
	logger.sugaredLogger.Fatal(args)
}

func (logger *redirectGRPCLogger) Print(args ...interface{}) {
	logger.sugaredLogger.Debug(args)
}

func (logger *redirectGRPCLogger) Printf(format string, args ...interface{}) {
	logger.sugaredLogger.Debugf(format, args)
}

func (logger *redirectGRPCLogger) Println(args ...interface{}) {
	logger.sugaredLogger.Debug(args)
}

var redirectGRPCLoggerV redirectGRPCLogger

func init() {
	debug := brimtext.TrueString(os.Getenv("DEBUG"))
	for _, arg := range os.Args[1:] {
		switch arg {
		case "debug", "--debug":
			debug = true
		}
	}
	var baseLogger *zap.Logger
	var err error
	if debug {
		baseLogger, err = zap.NewDevelopmentConfig().Build()
		baseLogger.Debug("Logging in developer mode.")
	} else {
		baseLogger, err = zap.NewProduction()
	}
	if err != nil {
		panic(err)
	}
	logger = baseLogger.With(zap.String("name", "cfsd"))
	redirectGRPCLoggerV.sugaredLogger = baseLogger.With(zap.String("name", "cfsd")).Sugar()
	grpclog.SetLogger(&redirectGRPCLoggerV)
}

const (
	ADDR_PROMETHEUS = iota
	ADDR_FORMIC
	ADDR_GROUP_GRPC
	ADDR_GROUP_REPL
	ADDR_VALUE_GRPC
	ADDR_VALUE_REPL
)

func main() {
	ringPath := "/etc/cfsd/cfs.ring"
	caPath := "/etc/cfsd/ca.pem"
	dataPath := "/var/lib/cfsd"
	var prometheusIP string
	var formicIP string
	var grpcGroupIP string
	var replGroupIP string
	var grpcValueIP string
	var replValueIP string
	var prometheusCertPath string
	var prometheusKeyPath string
	var formicCertPath string
	var formicKeyPath string
	var grpcGroupCertPath string
	var grpcGroupKeyPath string
	var replGroupCertPath string
	var replGroupKeyPath string
	var grpcValueCertPath string
	var grpcValueKeyPath string
	var replValueCertPath string
	var replValueKeyPath string

	fp, err := os.Open(ringPath)
	if err != nil {
		logger.Fatal("Couldn't open ring file", zap.Error(err))
	}
	oneRing, err := ring.LoadRing(fp)
	if err != nil {
		logger.Fatal("Error loading ring", zap.Error(err))
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		logger.Fatal("Couldn't find network interfaces", zap.Error(err))
	}
FIND_LOCAL_NODE:
	for _, addrObj := range addrs {
		if ipNet, ok := addrObj.(*net.IPNet); ok {
			for _, node := range oneRing.Nodes() {
				for _, nodeAddr := range node.Addresses() {
					hostPort, err := ring.CanonicalHostPort(nodeAddr, 1)
					if err != nil {
						continue
					}
					host, _, err := net.SplitHostPort(hostPort)
					if err != nil {
						continue
					}
					// TODO: I guess we should update this incase they put a
					// host name instead of a direct IP.
					nodeIP := net.ParseIP(host)
					if ipNet.IP.Equal(nodeIP) {
						oneRing.SetLocalNode(node.ID())
						nodeAddr = node.Address(ADDR_PROMETHEUS)
						i := strings.LastIndex(nodeAddr, ":")
						if i >= 0 {
							prometheusIP = nodeAddr[:i]
						}
						nodeAddr = node.Address(ADDR_FORMIC)
						i = strings.LastIndex(nodeAddr, ":")
						if i >= 0 {
							formicIP = nodeAddr[:i]
						}
						nodeAddr = node.Address(ADDR_GROUP_GRPC)
						i = strings.LastIndex(nodeAddr, ":")
						if i >= 0 {
							grpcGroupIP = nodeAddr[:i]
						}
						nodeAddr = node.Address(ADDR_GROUP_REPL)
						i = strings.LastIndex(nodeAddr, ":")
						if i >= 0 {
							replGroupIP = nodeAddr[:i]
						}
						nodeAddr = node.Address(ADDR_VALUE_GRPC)
						i = strings.LastIndex(nodeAddr, ":")
						if i >= 0 {
							grpcValueIP = nodeAddr[:i]
						}
						nodeAddr = node.Address(ADDR_VALUE_REPL)
						i = strings.LastIndex(nodeAddr, ":")
						if i >= 0 {
							replValueIP = nodeAddr[:i]
						}
						break FIND_LOCAL_NODE
					}
				}
			}
		}
	}
	if oneRing.LocalNode() == nil {
		logger.Fatal("No local IP match within ring.")
	}
	if prometheusIP == "" {
		logger.Fatal("No prometheusIP in ring.")
	}
	if formicIP == "" {
		logger.Fatal("No formicIP in ring.")
	}
	if grpcGroupIP == "" {
		logger.Fatal("No grpcGroupIP in ring.")
	}
	if replGroupIP == "" {
		logger.Fatal("No replGroupIP in ring.")
	}
	if grpcValueIP == "" {
		logger.Fatal("No grpcValueIP in ring.")
	}
	if replValueIP == "" {
		logger.Fatal("No replValueIP in ring.")
	}
	prometheusCertPath = "/etc/cfsd/" + prometheusIP + ".pem"
	prometheusKeyPath = "/etc/cfsd/" + prometheusIP + "-key.pem"
	formicCertPath = "/etc/cfsd/" + formicIP + ".pem"
	formicKeyPath = "/etc/cfsd/" + formicIP + "-key.pem"
	grpcGroupCertPath = "/etc/cfsd/" + grpcGroupIP + ".pem"
	grpcGroupKeyPath = "/etc/cfsd/" + grpcGroupIP + "-key.pem"
	replGroupCertPath = "/etc/cfsd/" + replGroupIP + ".pem"
	replGroupKeyPath = "/etc/cfsd/" + replGroupIP + "-key.pem"
	grpcValueCertPath = "/etc/cfsd/" + grpcValueIP + ".pem"
	grpcValueKeyPath = "/etc/cfsd/" + grpcValueIP + "-key.pem"
	replValueCertPath = "/etc/cfsd/" + replValueIP + ".pem"
	replValueKeyPath = "/etc/cfsd/" + replValueIP + "-key.pem"

	hostPort, err := ring.CanonicalHostPort(oneRing.LocalNode().Address(ADDR_PROMETHEUS), 9100)
	if err != nil {
		logger.Fatal("Erroring translating configured prometheus address.", zap.Error(err))
	}
	logger.Warn("Need to switch Prometheus to using TLS", zap.String("cert", prometheusCertPath), zap.String("key", prometheusKeyPath))
	http.Handle("/metrics", prometheus.Handler())
	go http.ListenAndServe(hostPort, nil)

	waitGroup := &sync.WaitGroup{}
	shutdownChan := make(chan struct{})

	groupStore, groupStoreRestartChan, err := server.NewGroupStore(&server.GroupStoreConfig{
		GRPCAddressIndex: ADDR_GROUP_GRPC,
		ReplAddressIndex: ADDR_GROUP_REPL,
		GRPCCertFile:     grpcGroupCertPath,
		GRPCKeyFile:      grpcGroupKeyPath,
		ReplCertFile:     replGroupCertPath,
		ReplKeyFile:      replGroupKeyPath,
		CAFile:           caPath,
		Scale:            0.4,
		Path:             dataPath,
		Ring:             oneRing,
	})
	if err != nil {
		logger.Fatal("Error initializing group store", zap.Error(err))
	}
	waitGroup.Add(1)
	go func() {
		for {
			select {
			case <-groupStoreRestartChan:
				ctx, _ := context.WithTimeout(context.Background(), time.Minute)
				groupStore.Shutdown(ctx)
				ctx, _ = context.WithTimeout(context.Background(), time.Minute)
				groupStore.Startup(ctx)
			case <-shutdownChan:
				ctx, _ := context.WithTimeout(context.Background(), time.Minute)
				groupStore.Shutdown(ctx)
				waitGroup.Done()
				return
			}
		}
	}()
	ctx, _ := context.WithTimeout(context.Background(), time.Minute)
	if err = groupStore.Startup(ctx); err != nil {
		ctx, _ = context.WithTimeout(context.Background(), time.Minute)
		groupStore.Shutdown(ctx)
		logger.Fatal("Error starting group store", zap.Error(err))
	}

	valueStore, valueStoreRestartChan, err := server.NewValueStore(&server.ValueStoreConfig{
		GRPCAddressIndex: ADDR_VALUE_GRPC,
		ReplAddressIndex: ADDR_VALUE_REPL,
		GRPCCertFile:     grpcValueCertPath,
		GRPCKeyFile:      grpcValueKeyPath,
		ReplCertFile:     replValueCertPath,
		ReplKeyFile:      replValueKeyPath,
		CAFile:           caPath,
		Scale:            0.4,
		Path:             dataPath,
		Ring:             oneRing,
	})
	if err != nil {
		logger.Fatal("Error initializing value store", zap.Error(err))
	}
	waitGroup.Add(1)
	go func() {
		for {
			select {
			case <-valueStoreRestartChan:
				ctx, _ := context.WithTimeout(context.Background(), time.Minute)
				valueStore.Shutdown(ctx)
				ctx, _ = context.WithTimeout(context.Background(), time.Minute)
				valueStore.Startup(ctx)
			case <-shutdownChan:
				ctx, _ := context.WithTimeout(context.Background(), time.Minute)
				valueStore.Shutdown(ctx)
				waitGroup.Done()
				return
			}
		}
	}()
	ctx, _ = context.WithTimeout(context.Background(), time.Minute)
	if err = valueStore.Startup(ctx); err != nil {
		ctx, _ = context.WithTimeout(context.Background(), time.Minute)
		valueStore.Shutdown(ctx)
		logger.Fatal("Error starting value store", zap.Error(err))
	}

	// Startup formic
	formicCfg := formic.NewConfig()
	formicCfg.FormicAddressIndex = ADDR_FORMIC
	formicCfg.ValueAddressIndex = ADDR_VALUE_GRPC
	formicCfg.GroupAddressIndex = ADDR_GROUP_GRPC
	formicCfg.CertFile = formicCertPath
	formicCfg.KeyFile = formicKeyPath
	formicCfg.CAFile = caPath
	formicCfg.Ring = oneRing
	formicCfg.RingPath = ringPath
	formicCfg.IpAddr = formicIP
	formicCfg.AuthUrl = "http://localhost:5000"
	formicCfg.AuthUser = "admin"
	formicCfg.AuthPassword = "admin"
	err = formic.NewFormicServer(formicCfg, logger)
	if err != nil {
		logger.Fatal("Couldn't start up formic", zap.Error(err))
	}

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	waitGroup.Add(1)
	go func() {
		for {
			select {
			case <-ch:
				close(shutdownChan)
				waitGroup.Done()
				return
			case <-shutdownChan:
				waitGroup.Done()
				return
			}
		}
	}()

	logger.Debug("Done launching components; ready for action.")
	waitGroup.Wait()
}
