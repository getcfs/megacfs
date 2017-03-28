package server

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/getcfs/megacfs/formic/formicproto"
	"github.com/getcfs/megacfs/ftls"
	"github.com/getcfs/megacfs/oort"
	"github.com/gholt/ring"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Formic struct {
	lock                  sync.RWMutex
	waitGroup             *sync.WaitGroup
	shutdownChan          chan struct{}
	started               bool
	grpcServer            *grpc.Server
	nodeID                int
	grpcAddressIndex      int
	grpcCertFile          string
	grpcKeyFile           string
	caFile                string
	groupGRPCAddressIndex int
	valueGRPCAddressIndex int
	ring                  ring.Ring
	ringPath              string
	logger                *zap.Logger
	skipAuth              bool
	authURL               string
	authUser              string
	authPassword          string
	grpcWrapper           *grpcWrapper
}

type FormicConfig struct {
	NodeID                int
	GRPCAddressIndex      int
	GroupGRPCAddressIndex int
	ValueGRPCAddressIndex int
	GRPCCertFile          string
	GRPCKeyFile           string
	CAFile                string
	Scale                 float64
	Ring                  ring.Ring
	RingPath              string
	AuthURL               string
	AuthUser              string
	AuthPassword          string
	SkipAuth              bool
	Logger                *zap.Logger
}

func resolveFormicConfig(c *FormicConfig) *FormicConfig {
	cfg := &FormicConfig{}
	if c != nil {
		*cfg = *c
	}
	if cfg.Logger == nil {
		var err error
		cfg.Logger, err = zap.NewProduction()
		if err != nil {
			panic(err)
		}
	}
	return cfg
}

func NewFormic(cfg *FormicConfig) (*Formic, error) {
	cfg = resolveFormicConfig(cfg)
	f := &Formic{
		waitGroup:             &sync.WaitGroup{},
		nodeID:                cfg.NodeID,
		grpcAddressIndex:      cfg.GRPCAddressIndex,
		grpcCertFile:          cfg.GRPCCertFile,
		grpcKeyFile:           cfg.GRPCKeyFile,
		caFile:                cfg.CAFile,
		groupGRPCAddressIndex: cfg.GroupGRPCAddressIndex,
		valueGRPCAddressIndex: cfg.ValueGRPCAddressIndex,
		ring:         cfg.Ring,
		ringPath:     cfg.RingPath,
		logger:       cfg.Logger,
		skipAuth:     cfg.SkipAuth,
		authURL:      cfg.AuthURL,
		authUser:     cfg.AuthUser,
		authPassword: cfg.AuthPassword,
	}
	return f, nil
}

func (f *Formic) Startup(ctx context.Context) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.started {
		return nil
	}
	f.started = true
	f.shutdownChan = make(chan struct{})
	f.waitGroup.Add(1)
	go func() {
		mMadeUp := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "Formic",
			Name:      "MadeUp",
			Help:      "Just a made up metric.",
		})
		prometheus.Register(mMadeUp)
		for {
			select {
			case <-f.shutdownChan:
				f.waitGroup.Done()
				return
			case <-time.After(time.Minute):
				mMadeUp.Add(1)
			}
		}
	}()

	ln := f.ring.LocalNode()
	if ln == nil {
		close(f.shutdownChan)
		return errors.New("no local node set")
	}
	grpcAddr := ln.Address(f.grpcAddressIndex)
	if grpcAddr == "" {
		close(f.shutdownChan)
		return fmt.Errorf("no local node address index %d", f.grpcAddressIndex)
	}
	grpcHostPort, err := ring.CanonicalHostPort(grpcAddr, 12300)
	if err != nil {
		close(f.shutdownChan)
		return err
	}
	// TODO: Eventually get rid of the + 1
	if grpcHostPort[len(grpcHostPort)-1] != '1' {
		grpcHostPort = grpcHostPort[:len(grpcHostPort)-1] + "1"
	}

	groupStore := oort.NewReplGroupStore(&oort.GroupStoreConfig{
		AddressIndex:    f.groupGRPCAddressIndex,
		StoreFTLSConfig: ftls.DefaultClientFTLSConf(f.grpcCertFile, f.grpcKeyFile, f.caFile),
		RingClientID:    grpcHostPort,
		RingCachePath:   f.ringPath,
		Logger:          f.logger,
	})
	valueStore := oort.NewReplValueStore(&oort.ValueStoreConfig{
		AddressIndex:    f.valueGRPCAddressIndex,
		StoreFTLSConfig: ftls.DefaultClientFTLSConf(f.grpcCertFile, f.grpcKeyFile, f.caFile),
		RingClientID:    grpcHostPort,
		RingCachePath:   f.ringPath,
		Logger:          f.logger,
	})
	comms, err := newStoreComms(valueStore, groupStore, f.logger)
	if err != nil {
		close(f.shutdownChan)
		return err
	}
	// TODO: Make sure there are ways to shut this stuff down gracefully.
	deleteChan := make(chan *deleteItem, 1000)
	dirtyChan := make(chan *dirtyItem, 1000)
	blocksize := int64(1024 * 64) // Default Block Size (64K)
	if f.nodeID == 0 {
		f.nodeID = int(murmur3.Sum32([]byte(grpcHostPort)))
		if f.nodeID == 0 {
			f.nodeID = 1
		}
	}
	fs := newOortFS(comms, f.logger, deleteChan, dirtyChan, blocksize, f.nodeID)
	f.grpcWrapper = newGRPCWrapper(fs, comms, f.skipAuth, f.authURL, f.authUser, f.authPassword)
	deletes := newDeletinator(deleteChan, fs, comms, f.logger)
	cleaner := newCleaninator(dirtyChan, fs, comms, f.logger)
	go deletes.Run()
	go cleaner.Run()

	f.logger.Debug("Listen on", zap.String("grpcHostPort", grpcHostPort))
	lis, err := net.Listen("tcp", grpcHostPort)
	if err != nil {
		close(f.shutdownChan)
		return err
	}
	ftlsCfg := ftls.DefaultServerFTLSConf(f.grpcCertFile, f.grpcKeyFile, f.caFile)
	ftlsCfg.MutualTLS = false // TODO: Currently no way to allow full cert validation
	ftlsCfg.InsecureSkipVerify = true
	tlsCfg, err := ftls.NewServerTLSConfig(ftlsCfg)
	if err != nil {
		close(f.shutdownChan)
		return err
	}
	f.grpcServer = grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsCfg)),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
	formicproto.RegisterFormicServer(f.grpcServer, f.grpcWrapper)
	grpc_prometheus.Register(f.grpcServer)
	f.waitGroup.Add(1)
	go func() {
		err := f.grpcServer.Serve(lis)
		if err != nil {
			f.logger.Debug("grpcServer.Serve error", zap.Error(err))
		}
		lis.Close()
		f.waitGroup.Done()
	}()
	f.waitGroup.Add(1)
	go func() {
		<-f.shutdownChan
		f.grpcServer.Stop()
		lis.Close()
		f.waitGroup.Done()
	}()
	return nil
}

func (f *Formic) Shutdown(ctx context.Context) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if !f.started {
		return nil
	}
	close(f.shutdownChan)
	f.waitGroup.Wait()
	return nil
}
