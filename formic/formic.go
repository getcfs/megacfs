package formic

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"

	pb "github.com/getcfs/megacfs/formic/formicproto"
	"github.com/getcfs/megacfs/ftls"
	"github.com/getcfs/megacfs/oort"
	"github.com/gholt/ring"
	"github.com/gogo/protobuf/proto"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

var ErrZeroValue = errors.New("Got 0 length message")

var gerf = grpc.Errorf // To avoid a `go vet` quirk
var ErrGRPCNotFound = gerf(codes.NotFound, "Not Found")
var ErrNotEmpty = gerf(codes.FailedPrecondition, "Not Empty")

func GetID(fsid []byte, inode, block uint64) []byte {
	// TODO: Figure out what arrangement we want to use for the hash
	h := murmur3.New128()
	h.Write(fsid)
	binary.Write(h, binary.BigEndian, inode)
	binary.Write(h, binary.BigEndian, block)
	s1, s2 := h.Sum128()
	b := bytes.NewBuffer([]byte(""))
	binary.Write(b, binary.BigEndian, s1)
	binary.Write(b, binary.BigEndian, s2)
	return b.Bytes()
}

func GetSystemID(fsid []byte, dir string) []byte {
	h := murmur3.New128()
	h.Write([]byte("/system/"))
	h.Write(fsid)
	h.Write([]byte(dir))
	s1, s2 := h.Sum128()
	b := bytes.NewBuffer([]byte(""))
	binary.Write(b, binary.BigEndian, s1)
	binary.Write(b, binary.BigEndian, s2)
	return b.Bytes()
}

func GetDeletedID(fsid []byte) []byte {
	return GetSystemID(fsid, "deleted")
}

func GetDirtyID(fsid []byte) []byte {
	return GetSystemID(fsid, "dirty")
}

func Marshal(pb proto.Message) ([]byte, error) {
	return proto.Marshal(pb)
}

func Unmarshal(buf []byte, pb proto.Message) error {
	if len(buf) == 0 {
		return ErrZeroValue
	}
	return proto.Unmarshal(buf, pb)
}

// Todo: return a server struct that we can gracefully restart and shutdown
func NewFormicServer(cfg *Config, logger *zap.Logger) error {

	creds, err := credentials.NewServerTLSFromFile(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		logger.Fatal("Couldn't load cert from file", zap.Error(err))
	}
	s := grpc.NewServer(
		grpc.Creds(creds),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)

	cfg = ResolveConfig(cfg)
	if cfg.SkipAuth {
		logger.Warn("Running with no AUTH!!!!!!")
	}

	oortLogger := logger.With(zap.String("name", "cfsd.formic.oort"))
	vstore := oort.NewReplValueStore(&oort.ValueStoreConfig{
		Logger:          oortLogger,
		AddressIndex:    cfg.ValueAddressIndex,
		StoreFTLSConfig: ftls.DefaultClientFTLSConf(cfg.CertFile, cfg.KeyFile, cfg.CAFile),
		RingCachePath:   cfg.RingPath,
		RingClientID:    cfg.IpAddr,
	})

	gstore := oort.NewReplGroupStore(&oort.GroupStoreConfig{
		Logger:          oortLogger,
		AddressIndex:    cfg.GroupAddressIndex,
		StoreFTLSConfig: ftls.DefaultClientFTLSConf(cfg.CertFile, cfg.KeyFile, cfg.CAFile),
		RingCachePath:   cfg.RingPath,
		RingClientID:    cfg.IpAddr,
	})

	//
	comms, err := NewStoreComms(vstore, gstore, logger)
	if err != nil {
		logger.Fatal("Error setting up comms", zap.Error(err))
	}
	deleteChan := make(chan *DeleteItem, 1000)
	dirtyChan := make(chan *DirtyItem, 1000)
	blocksize := int64(1024 * 64) // Default Block Size (64K)
	// TODO: Get a better way to get the Node ID
	formicNodeID := cfg.NodeID
	if formicNodeID == -1 {
		formicNodeID = int(murmur3.Sum32([]byte(cfg.IpAddr)))
	}
	fs := NewOortFS(comms, logger, deleteChan, dirtyChan, blocksize, formicNodeID, cfg.SkipAuth, cfg.AuthUrl, cfg.AuthUser, cfg.AuthPassword)
	deletes := NewDeletinator(deleteChan, fs, comms, logger.With(zap.String("name", "formic.deletinator")))
	cleaner := NewCleaninator(dirtyChan, fs, comms, logger.With(zap.String("name", "formic.cleaninator")))
	go deletes.Run()
	go cleaner.Run()

	hostPort, err := ring.CanonicalHostPort(cfg.Ring.LocalNode().Address(cfg.FormicAddressIndex), 12300)
	if err != nil {
		logger.Fatal("Failed to parse formic host:port", zap.String("host:port", cfg.Ring.LocalNode().Address(cfg.FormicAddressIndex)), zap.Error(err))
	}
	l, err := net.Listen("tcp", hostPort)
	if err != nil {
		logger.Fatal("Failed to bind formic to port", zap.String("hostPort", hostPort), zap.Error(err))
	}
	pb.RegisterFileSystemAPIServer(s, NewFileSystemAPIServer(cfg, gstore, vstore, logger.With(zap.String("name", "formic.fs"))))
	nodeID, apiServer := NewApiServer(fs, formicNodeID, comms, logger, blocksize)
	pb.RegisterApiServer(s, apiServer)
	logger.Debug("Starting formic and the filesystem API", zap.Uint64("node", nodeID))
	go s.Serve(l)
	return nil
}
