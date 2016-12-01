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
	pb "github.com/getcfs/megacfs/formic/proto"
	"github.com/getcfs/megacfs/ftls"
	"github.com/getcfs/megacfs/oort/api"
	"github.com/getcfs/megacfs/oort/api/server"
	"github.com/gholt/ring"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/spaolacci/murmur3"
	"github.com/uber-go/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	ADDR_FORMIC = iota
	ADDR_GROUP_GRPC
	ADDR_GROUP_REPL
	ADDR_VALUE_GRPC
	ADDR_VALUE_REPL
)

func main() {
	var ipAddr string
	ringPath := "/etc/cfsd/cfs.ring"
	caPath := "/etc/cfsd/ca.pem"
	var certPath string
	var keyPath string
	dataPath := "/var/lib/cfsd"

	baseLogger := zap.New(zap.NewJSONEncoder())
	baseLogger.SetLevel(zap.InfoLevel)
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
					i := strings.LastIndex(nodeAddr, ":")
					if i < 0 {
						continue
					}
					nodeIP := net.ParseIP(nodeAddr[:i])
					if ipNet.IP.Equal(nodeIP) {
						oneRing.SetLocalNode(node.ID())
						ipAddr = nodeIP.String()
						break FIND_LOCAL_NODE
					}
				}
			}
		}
	}
	certPath = "/etc/cfsd/" + ipAddr + ".pem"
	keyPath = "/etc/cfsd/" + ipAddr + "-key.pem"

	waitGroup := &sync.WaitGroup{}
	shutdownChan := make(chan struct{})

	groupStore, groupStoreRestartChan, err := server.NewGroupStore(&server.GroupStoreConfig{
		GRPCAddressIndex: ADDR_GROUP_GRPC,
		ReplAddressIndex: ADDR_GROUP_REPL,
		CertFile:         certPath,
		KeyFile:          keyPath,
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
		CertFile:         certPath,
		KeyFile:          keyPath,
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

	creds, err := credentials.NewServerTLSFromFile(certPath, keyPath)
	if err != nil {
		logger.Fatal("Couldn't load cert from file", zap.Error(err))
	}
	s := grpc.NewServer(
		grpc.Creds(creds),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)

	oortLogger := baseLogger.With(zap.String("name", "cfsd.formic.oort"))
	vstore := api.NewReplValueStore(&api.ValueStoreConfig{
		Logger:       oortLogger,
		AddressIndex: ADDR_VALUE_GRPC,
		StoreFTLSConfig: &ftls.Config{
			MutualTLS: true,
			CertFile:  certPath,
			KeyFile:   keyPath,
			CAFile:    caPath,
		},
		RingCachePath: ringPath,
		RingClientID:  ipAddr,
	})

	gstore := api.NewReplGroupStore(&api.GroupStoreConfig{
		Logger:       oortLogger,
		AddressIndex: ADDR_GROUP_GRPC,
		StoreFTLSConfig: &ftls.Config{
			MutualTLS: true,
			CertFile:  certPath,
			KeyFile:   keyPath,
			CAFile:    caPath,
		},
		RingCachePath: ringPath,
		RingClientID:  ipAddr,
	})

	comms, err := formic.NewStoreComms(vstore, gstore, logger)
	if err != nil {
		logger.Fatal("Error setting up comms", zap.Error(err))
	}
	deleteChan := make(chan *formic.DeleteItem, 1000)
	dirtyChan := make(chan *formic.DirtyItem, 1000)
	fs := formic.NewOortFS(comms, logger, deleteChan, dirtyChan)
	deletes := formic.NewDeletinator(deleteChan, fs, comms, baseLogger.With(zap.String("name", "formic.deletinator")))
	cleaner := formic.NewCleaninator(dirtyChan, fs, comms, baseLogger.With(zap.String("name", "formic.cleaninator")))
	go deletes.Run()
	go cleaner.Run()

	l, err := net.Listen("tcp", oneRing.LocalNode().Address(ADDR_FORMIC))
	if err != nil {
		logger.Fatal("Failed to bind formic to port", zap.Error(err))
	}
	pb.RegisterFileSystemAPIServer(s, formic.NewFileSystemAPIServer(gstore, vstore, baseLogger.With(zap.String("name", "formic.fs"))))
	formicNodeID := int(murmur3.Sum64([]byte(ipAddr)))
	pb.RegisterApiServer(s, formic.NewApiServer(fs, formicNodeID, comms, logger))
	logger.Info("Starting formic and the filesystem API", zap.Int("addr", formicNodeID))
	go s.Serve(l)

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
