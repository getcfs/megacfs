// Package server provides a server implementation of a single node of
// a formic cluster.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/getcfs/megacfs/formic"
	"github.com/getcfs/megacfs/formic/formicproto"
	"github.com/getcfs/megacfs/ftls"
	"github.com/getcfs/megacfs/oort"
	"github.com/gholt/ring"
	"github.com/gholt/store"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

type Formic struct {
	sync.RWMutex
	waitGroup             *sync.WaitGroup
	shutdownChan          chan struct{}
	started               bool
	comms                 *formic.StoreComms
	fs                    formic.FileService
	grpcServer            *grpc.Server
	validIPs              map[string]map[string]time.Time
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

func NewFormicConfig() *FormicConfig {
	// TODO: We can fix this so NodeID: 0 is notset instead of -1
	return &FormicConfig{NodeID: -1}
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
		validIPs:              make(map[string]map[string]time.Time),
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
	f.Lock()
	defer f.Unlock()
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
	f.comms, err = formic.NewStoreComms(valueStore, groupStore, f.logger)
	if err != nil {
		close(f.shutdownChan)
		return err
	}
	// TODO: Make sure there are ways to shut this stuff down gracefully.
	deleteChan := make(chan *formic.DeleteItem, 1000)
	dirtyChan := make(chan *formic.DirtyItem, 1000)
	blocksize := int64(1024 * 64) // Default Block Size (64K)
	if f.nodeID == -1 {
		f.nodeID = int(murmur3.Sum32([]byte(grpcHostPort)))
	}
	f.fs = formic.NewOortFS(f.comms, f.logger, deleteChan, dirtyChan, blocksize, f.nodeID)
	deletes := formic.NewDeletinator(deleteChan, f.fs, f.comms, f.logger)
	cleaner := formic.NewCleaninator(dirtyChan, f.fs, f.comms, f.logger)
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
	formicproto.RegisterFormicServer(f.grpcServer, f)
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
	f.Lock()
	defer f.Unlock()
	if !f.started {
		return nil
	}
	close(f.shutdownChan)
	f.waitGroup.Wait()
	return nil
}

func (f *Formic) Check(stream formicproto.Formic_CheckServer) error {
	// NOTE: Each of these streams is synchronized req1, resp1, req2, resp2.
	// But it doesn't have to be that way, it was just simpler to code. Each
	// client/server pair will have a stream for each request/response type, so
	// there's a pretty good amount of concurrency going on there already.
	// Perhaps later we can experiment with intrastream concurrency and see if
	// the complexity is worth it.
	//
	// The main reason for using streams over unary grpc requests was
	// benchmarked speed gains. I suspect it is because unary requests actually
	// set up and tear down streams for each request, but that's just a guess.
	// We stopped looking into it once we noticed the speed gains from
	// switching to streaming.
	var resp formicproto.CheckResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewCheck(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) CreateFS(stream formicproto.Formic_CreateFSServer) error {
	var resp formicproto.CreateFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := f.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewCreateFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Create(stream formicproto.Formic_CreateServer) error {
	var resp formicproto.CreateResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewCreate(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) DeleteFS(stream formicproto.Formic_DeleteFSServer) error {
	var resp formicproto.DeleteFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := f.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewDeleteFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) GetAttr(stream formicproto.Formic_GetAttrServer) error {
	var resp formicproto.GetAttrResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewGetAttr(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Getxattr(stream formicproto.Formic_GetxattrServer) error {
	var resp formicproto.GetxattrResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewGetxattr(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) GrantAddrFS(stream formicproto.Formic_GrantAddrFSServer) error {
	var resp formicproto.GrantAddrFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := f.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewGrantAddrFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) InitFs(stream formicproto.Formic_InitFsServer) error {
	var resp formicproto.InitFsResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewInitFs(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) ListFS(stream formicproto.Formic_ListFSServer) error {
	var resp formicproto.ListFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := f.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewListFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Listxattr(stream formicproto.Formic_ListxattrServer) error {
	var resp formicproto.ListxattrResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewListxattr(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Lookup(stream formicproto.Formic_LookupServer) error {
	var resp formicproto.LookupResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewLookup(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) MkDir(stream formicproto.Formic_MkDirServer) error {
	var resp formicproto.MkDirResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewMkDir(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) ReadDirAll(stream formicproto.Formic_ReadDirAllServer) error {
	var resp formicproto.ReadDirAllResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewReadDirAll(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Readlink(stream formicproto.Formic_ReadlinkServer) error {
	var resp formicproto.ReadlinkResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewReadlink(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Read(stream formicproto.Formic_ReadServer) error {
	var resp formicproto.ReadResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewRead(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Remove(stream formicproto.Formic_RemoveServer) error {
	var resp formicproto.RemoveResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewRemove(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Removexattr(stream formicproto.Formic_RemovexattrServer) error {
	var resp formicproto.RemovexattrResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewRemovexattr(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Rename(stream formicproto.Formic_RenameServer) error {
	var resp formicproto.RenameResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewRename(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) RevokeAddrFS(stream formicproto.Formic_RevokeAddrFSServer) error {
	var resp formicproto.RevokeAddrFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := f.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewRevokeAddrFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) SetAttr(stream formicproto.Formic_SetAttrServer) error {
	var resp formicproto.SetAttrResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewSetAttr(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Setxattr(stream formicproto.Formic_SetxattrServer) error {
	var resp formicproto.SetxattrResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewSetxattr(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) ShowFS(stream formicproto.Formic_ShowFSServer) error {
	var resp formicproto.ShowFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := f.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewShowFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Statfs(stream formicproto.Formic_StatfsServer) error {
	var resp formicproto.StatfsResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewStatfs(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Symlink(stream formicproto.Formic_SymlinkServer) error {
	var resp formicproto.SymlinkResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewSymlink(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) UpdateFS(stream formicproto.Formic_UpdateFSServer) error {
	var resp formicproto.UpdateFSResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		acctID, err := f.validateToken(req.Token)
		if err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewUpdateFS(stream.Context(), req, &resp, acctID); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) Write(stream formicproto.Formic_WriteServer) error {
	var resp formicproto.WriteResponse
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		resp.Reset()
		if err = f.validateIP(stream.Context()); err != nil {
			resp.Err = err.Error()
		} else if err = f.fs.NewWrite(stream.Context(), req, &resp); err != nil {
			resp.Err = err.Error()
		}
		resp.Rpcid = req.Rpcid
		if err := stream.Send(&resp); err != nil {
			return err
		}
	}
}

func (f *Formic) validateIP(ctx context.Context) error {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return errors.New("couldn't get client ip")
	}
	ip, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return err
	}
	fsidUUID, err := formic.GetFsId(ctx)
	fsid := fsidUUID.String()
	if err != nil {
		return err
	}
	ips, ok := f.validIPs[fsid]
	if !ok {
		ips = make(map[string]time.Time)
		f.validIPs[fsid] = ips
	}
	cacheTime, ok := ips[ip]
	if ok && cacheTime.After(time.Now()) {
		return nil
	}
	_, err = f.comms.ReadGroupItem(ctx, []byte(fmt.Sprintf("/fs/%s/addr", fsid)), []byte(ip))
	if store.IsNotFound(err) {
		f.logger.Debug("Unauthorized IP", zap.String("unauthorized_ip", ip))
		return formic.ErrUnauthorized
	}
	if err != nil {
		return err
	}
	f.validIPs[fsid][ip] = time.Now().Add(time.Second * time.Duration(180.0+180.0*rand.NormFloat64()*0.1))
	return nil
}

type validateTokenResponse struct {
	Token struct {
		Project struct {
			ID string `json:"id"`
		} `json:"project"`
	} `json:"token"`
}

// validateToken ensure the token is valid and returns the Account ID or an
// error.
func (f *Formic) validateToken(token string) (string, error) {
	if f.skipAuth {
		return "11", nil
	}
	serverAuthToken, err := serverAuth(f.authURL, f.authUser, f.authPassword)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest("GET", f.authURL+"/v3/auth/tokens", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Auth-Token", serverAuthToken)
	req.Header.Set("X-Subject-Token", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("token validation gave status %d", resp.StatusCode)
	}
	var validateResp validateTokenResponse
	r, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if err = json.Unmarshal(r, &validateResp); err != nil {
		return "", err
	}
	return validateResp.Token.Project.ID, nil
}

// serverAuth return the X-Auth-Token to use or an error.
func serverAuth(url string, user string, password string) (string, error) {
	body := fmt.Sprintf(`{"auth":{"identity":{"methods":["password"],"password":{"user":{"domain":{"id":"default"},"name":"%s","password":"%s"}}}}}`, user, password)
	rbody := strings.NewReader(body)
	req, err := http.NewRequest("POST", url+"/v3/auth/tokens", rbody)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		return "", fmt.Errorf("server auth token request gave status %d", resp.StatusCode)
	}
	return resp.Header.Get("X-Subject-Token"), nil
}
