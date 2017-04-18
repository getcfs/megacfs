// Package main builds a 'cfsd' executable that is a single node server
// application within a CFS cluster.
package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	formicserver "github.com/getcfs/megacfs/formic/server"
	oortserver "github.com/getcfs/megacfs/oort/server"
	"github.com/gholt/brimtext"
	"github.com/gholt/ring"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
)

var version = flag.Bool("version", false, "omit version information and exit")
var debug = flag.Bool("debug", false, "omit debug information while running")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")
var startFormic = flag.Bool("formic", false, "run formic service and not other services unless also specified")
var startGroup = flag.Bool("group", false, "run group service and not other services unless also specified")
var startValue = flag.Bool("value", false, "run value service and not other services unless also specified")

var logger *zap.Logger
var loggerConfig zap.Config

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

var cfsdVersion string
var buildDate string
var commitVersion string
var goVersion string

func init() {
	if brimtext.TrueString(os.Getenv("DEBUG")) {
		*debug = true
	}
	flag.Parse()
	if *debug {
		loggerConfig = zap.NewDevelopmentConfig()
	} else {
		loggerConfig = zap.NewProductionConfig()
	}
    baseLogger, err := loggerConfig.Build()
	if err != nil {
		panic(err)
	}
	if *debug {
		baseLogger.Debug("Logging in developer mode.")
    }
	logger = baseLogger.Named("cfsd")
	redirectGRPCLoggerV.sugaredLogger = baseLogger.Named("grpc").Sugar()
	grpclog.SetLogger(&redirectGRPCLoggerV)
}

const (
	ADDR_PROMETHEUS = iota
	ADDR_FORMIC_GRPC
	ADDR_GROUP_GRPC
	ADDR_GROUP_REPL
	ADDR_VALUE_GRPC
	ADDR_VALUE_REPL
)

func main() {
	if !*startFormic && !*startGroup && !*startValue {
		*startFormic = true
		*startGroup = true
		*startValue = true
	}
	var err error
	var scale float64 = 1 / 3
	s := os.Getenv("SCALE")
	if s != "" {
		scale, err = strconv.ParseFloat(s, 64)
		if err != nil {
			logger.Fatal("Could not parse env SCALE", zap.String("scale", s))
		}
	}
	if *version {
		fmt.Println("version:", cfsdVersion)
		fmt.Println("commit:", commitVersion)
		fmt.Println("build date:", buildDate)
		fmt.Println("go version:", goVersion)
		return
	}
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			logger.Fatal("Couldn't open cpuprofile file", zap.String("filename", *cpuprofile), zap.Error(err))
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		defer func() {
			f, err := os.Create(*memprofile)
			if err != nil {
				logger.Fatal("Couldn't open memprofile file", zap.String("filename", *memprofile), zap.Error(err))
			}
			runtime.GC()
			if err := pprof.WriteHeapProfile(f); err != nil {
				logger.Fatal("Couldn't write memprofile", zap.String("filename", *memprofile), zap.Error(err))
			}
			f.Close()
		}()
	}
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
	var grpcFormicCertPath string
	var grpcFormicKeyPath string
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
						nodeAddr = node.Address(ADDR_FORMIC_GRPC)
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
	grpcFormicCertPath = "/etc/cfsd/" + formicIP + ".pem"
	grpcFormicKeyPath = "/etc/cfsd/" + formicIP + "-key.pem"
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
		logger.Fatal("Error translating configured prometheus address.", zap.Error(err))
	}
	logger.Debug("Need to switch Prometheus to using TLS; or at least allow it.", zap.String("cert", prometheusCertPath), zap.String("key", prometheusKeyPath))
	http.Handle("/metrics", prometheus.Handler())
    http.Handle("/log/level", loggerConfig.Level)
	go http.ListenAndServe(hostPort, nil)

	waitGroup := &sync.WaitGroup{}
	shutdownChan := make(chan struct{})

	if *startGroup {
		groupStore, groupStoreRestartChan, err := oortserver.NewGroupStore(&oortserver.GroupStoreConfig{
			GRPCAddressIndex: ADDR_GROUP_GRPC,
			ReplAddressIndex: ADDR_GROUP_REPL,
			GRPCCertFile:     grpcGroupCertPath,
			GRPCKeyFile:      grpcGroupKeyPath,
			ReplCertFile:     replGroupCertPath,
			ReplKeyFile:      replGroupKeyPath,
			CAFile:           caPath,
			Scale:            scale,
			Path:             dataPath,
			Ring:             oneRing,
			Logger:           logger.Named("groupstore"),
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
	}

	if *startValue {
		valueStore, valueStoreRestartChan, err := oortserver.NewValueStore(&oortserver.ValueStoreConfig{
			GRPCAddressIndex: ADDR_VALUE_GRPC,
			ReplAddressIndex: ADDR_VALUE_REPL,
			GRPCCertFile:     grpcValueCertPath,
			GRPCKeyFile:      grpcValueKeyPath,
			ReplCertFile:     replValueCertPath,
			ReplKeyFile:      replValueKeyPath,
			CAFile:           caPath,
			Scale:            scale,
			Path:             dataPath,
			Ring:             oneRing,
			Logger:           logger.Named("valuestore"),
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
		ctx, _ := context.WithTimeout(context.Background(), time.Minute)
		if err = valueStore.Startup(ctx); err != nil {
			ctx, _ = context.WithTimeout(context.Background(), time.Minute)
			valueStore.Shutdown(ctx)
			logger.Fatal("Error starting value store", zap.Error(err))
		}
	}

	if *startFormic {
		authURL := os.Getenv("AUTH_URL")
		if authURL == "" {
			authURL = "http://localhost:5000"
		}
		authUser := os.Getenv("AUTH_USER")
		if authUser == "" {
			authUser = "admin"
		}
		authPassword := os.Getenv("AUTH_PASSWORD")
		if authPassword == "" {
			authPassword = "admin"
		}
		newFormic, err := formicserver.NewFormic(&formicserver.FormicConfig{
			GRPCAddressIndex:      ADDR_FORMIC_GRPC,
			ValueGRPCAddressIndex: ADDR_VALUE_GRPC,
			GroupGRPCAddressIndex: ADDR_GROUP_GRPC,
			GRPCCertFile:          grpcFormicCertPath,
			GRPCKeyFile:           grpcFormicKeyPath,
			CAFile:                caPath,
			Scale:                 scale,
			Ring:                  oneRing,
			RingPath:              ringPath,
			SkipAuth:              false,
			AuthURL:               authURL,
			AuthUser:              authUser,
			AuthPassword:          authPassword,
			Logger:                logger.Named("formic"),
		})
		if err != nil {
			logger.Fatal("Error initializing formic", zap.Error(err))
		}
		waitGroup.Add(1)
		go func() {
			<-shutdownChan
			ctx, _ := context.WithTimeout(context.Background(), time.Minute)
			newFormic.Shutdown(ctx)
			waitGroup.Done()
		}()
		ctx, _ := context.WithTimeout(context.Background(), time.Minute)
		if err = newFormic.Startup(ctx); err != nil {
			ctx, _ = context.WithTimeout(context.Background(), time.Minute)
			newFormic.Shutdown(ctx)
			logger.Fatal("Error starting formic", zap.Error(err))
		}
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
