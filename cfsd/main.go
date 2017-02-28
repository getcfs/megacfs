package main

import (
	"fmt"
	"net"
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
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

const (
	ADDR_FORMIC = iota
	ADDR_GROUP_GRPC
	ADDR_GROUP_REPL
	ADDR_VALUE_GRPC
	ADDR_VALUE_REPL
)

func main() {
	ringPath := "/etc/cfsd/cfs.ring"
	caPath := "/etc/cfsd/ca.pem"
	dataPath := "/var/lib/cfsd"
	var formicIP string
	var grpcGroupIP string
	var replGroupIP string
	var grpcValueIP string
	var replValueIP string
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
	} else {
		baseLogger, err = zap.NewProduction()
	}
	if err != nil {
		panic(err)
	}
	logger := baseLogger.With(zap.String("name", "cfsd"))

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
						nodeAddr = node.Address(ADDR_FORMIC)
						i := strings.LastIndex(nodeAddr, ":")
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
	if formicIP == "" {
		logger.Fatal("No local IP match within ring.")
	}
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
				fmt.Println("Shutting down due to signal")
				close(shutdownChan)
				waitGroup.Done()
				return
			case <-shutdownChan:
				waitGroup.Done()
				return
			}
		}
	}()

	fmt.Println("Done launching components")
	waitGroup.Wait()
}
