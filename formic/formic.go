package formic

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"

	"github.com/gogo/protobuf/proto"
	"github.com/spaolacci/murmur3"
	"github.com/uber-go/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"

	pb "github.com/getcfs/megacfs/formic/proto"
	"github.com/getcfs/megacfs/ftls"
	"github.com/getcfs/megacfs/oort/api"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
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
func NewFormicServer(cfg *Config, logger zap.Logger) error {

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
	oortLogger := logger.With(zap.String("name", "cfsd.formic.oort"))
	vstore := api.NewReplValueStore(&api.ValueStoreConfig{
		Logger:       oortLogger,
		AddressIndex: cfg.ValueAddressIndex,
		StoreFTLSConfig: &ftls.Config{
			MutualTLS: true,
			CertFile:  cfg.CertFile,
			KeyFile:   cfg.KeyFile,
			CAFile:    cfg.CAFile,
		},
		RingCachePath: cfg.RingPath,
		RingClientID:  cfg.IpAddr,
	})

	gstore := api.NewReplGroupStore(&api.GroupStoreConfig{
		Logger:       oortLogger,
		AddressIndex: cfg.GroupAddressIndex,
		StoreFTLSConfig: &ftls.Config{
			MutualTLS: true,
			CertFile:  cfg.CertFile,
			KeyFile:   cfg.KeyFile,
			CAFile:    cfg.CAFile,
		},
		RingCachePath: cfg.RingPath,
		RingClientID:  cfg.IpAddr,
	})

	//
	comms, err := NewStoreComms(vstore, gstore, logger)
	if err != nil {
		logger.Fatal("Error setting up comms", zap.Error(err))
	}
	deleteChan := make(chan *DeleteItem, 1000)
	dirtyChan := make(chan *DirtyItem, 1000)
	fs := NewOortFS(comms, logger, deleteChan, dirtyChan)
	deletes := NewDeletinator(deleteChan, fs, comms, logger.With(zap.String("name", "formic.deletinator")))
	cleaner := NewCleaninator(dirtyChan, fs, comms, logger.With(zap.String("name", "formic.cleaninator")))
	go deletes.Run()
	go cleaner.Run()

	l, err := net.Listen("tcp", cfg.Ring.LocalNode().Address(cfg.FormicAddressIndex))
	if err != nil {
		logger.Fatal("Failed to bind formic to port", zap.Error(err))
	}
	pb.RegisterFileSystemAPIServer(s, NewFileSystemAPIServer(cfg, gstore, vstore, logger.With(zap.String("name", "formic.fs"))))
	// TODO: Get a better way to get the Node ID
	formicNodeID := int(murmur3.Sum64([]byte(cfg.IpAddr)))
	pb.RegisterApiServer(s, NewApiServer(fs, formicNodeID, comms, logger))
	logger.Info("Starting formic and the filesystem API", zap.Int("addr", formicNodeID))
	go s.Serve(l)
	return nil
}
