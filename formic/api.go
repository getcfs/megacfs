package formic

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/getcfs/megacfs/flother"
	"github.com/gholt/store"
	uuid "github.com/satori/go.uuid"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// ErrUnauthorized ...
var ErrUnauthorized = errors.New("Unknown or unauthorized filesystem")

type apiServer struct {
	sync.RWMutex
	fs         FileService
	fl         *flother.Flother
	blocksize  int64
	updateChan chan *UpdateItem
	comms      *StoreComms
	validIPs   map[string]map[string]time.Time
	log        *zap.Logger
}

// NewApiServer ...
func NewApiServer(fs FileService, nodeID int, comms *StoreComms, logger *zap.Logger, blocksize int64) (uint64, *apiServer) {
	s := new(apiServer)
	s.fs = fs
	s.comms = comms
	s.validIPs = make(map[string]map[string]time.Time)
	s.fl = flother.NewFlother(time.Time{}, uint64(nodeID))
	s.blocksize = blocksize
	s.updateChan = make(chan *UpdateItem, 1000)
	s.log = logger
	updates := newUpdatinator(s.updateChan, fs)
	go updates.Run()
	return s.fl.GetNodeID(), s
}

// GenerateBlockID ...
func GenerateBlockID(inodeID []byte, block uint64) []byte {
	h := murmur3.New128()
	h.Write(inodeID)
	binary.Write(h, binary.BigEndian, block)
	s1, s2 := h.Sum128()
	b := bytes.NewBuffer([]byte(""))
	binary.Write(b, binary.BigEndian, s1)
	binary.Write(b, binary.BigEndian, s2)
	return b.Bytes()
}

// GetFsId ...
func GetFsId(ctx context.Context) (uuid.UUID, error) {
	md, ok := metadata.FromContext(ctx)
	if !ok {
		return uuid.UUID{}, errors.New("No metadata sent")
	}
	fsid, ok := md["fsid"]
	if !ok {
		return uuid.UUID{}, errors.New("Filesystem ID not sent")
	}

	u, err := uuid.FromString(fsid[0])
	if err != nil {
		return uuid.UUID{}, err
	}
	return u, nil
}

func (s *apiServer) validateIP(ctx context.Context) error {

	if s.comms == nil {
		// TODO: Fix abstraction so that we don't have to do this for tests
		// Assume that it is a unit test
		return nil
	}
	p, ok := peer.FromContext(ctx)
	if !ok {
		return errors.New("Couldn't get client IP")
	}
	ip, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return err
	}
	fsidUUID, err := GetFsId(ctx)
	fsid := fsidUUID.String()
	if err != nil {
		return err
	}
	// First check the cache
	ips, ok := s.validIPs[fsid]
	if !ok {
		ips = make(map[string]time.Time)
		s.validIPs[fsid] = ips
	}
	cacheTime, ok := ips[ip]
	if ok && cacheTime.After(time.Now()) {
		return nil
	}
	_, err = s.comms.ReadGroupItem(ctx, []byte(fmt.Sprintf("/fs/%s/addr", fsid)), []byte(ip))
	if store.IsNotFound(err) {
		s.log.Info("Unauthorized IP", zap.String("unauthorized_ip", ip))
		// No access
		return ErrUnauthorized
	}
	if err != nil {
		return err
	}
	// Cache the valid ip
	// ttl is a float64 number of seconds
	ttl := 180.0
	s.validIPs[fsid][ip] = time.Now().Add(time.Second * time.Duration(ttl+ttl*rand.NormFloat64()*0.1))
	return nil
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
