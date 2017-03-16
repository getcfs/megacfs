package formic

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/getcfs/megacfs/flother"
	pb "github.com/getcfs/megacfs/formic/formicproto"
	"github.com/gholt/brimtime"
	"github.com/gholt/store"
	uuid "github.com/satori/go.uuid"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
	"golang.org/x/net/context"
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
func NewApiServer(fs FileService, nodeID int, comms *StoreComms, logger *zap.Logger) (uint64, *apiServer) {
	s := new(apiServer)
	s.fs = fs
	s.comms = comms
	s.validIPs = make(map[string]map[string]time.Time)
	s.fl = flother.NewFlother(time.Time{}, uint64(nodeID))
	s.blocksize = int64(1024 * 64) // Default Block Size (64K)
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

func (s *apiServer) GetAttr(ctx context.Context, r *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	attr, err := s.fs.GetAttr(ctx, GetID(fsid.Bytes(), r.Inode, 0))
	return &pb.GetAttrResponse{Attr: attr}, err
}

func (s *apiServer) SetAttr(ctx context.Context, r *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	attr, err := s.fs.SetAttr(ctx, GetID(fsid.Bytes(), r.Attr.Inode, 0), r.Attr, r.Valid)
	return &pb.SetAttrResponse{Attr: attr}, err
}

func (s *apiServer) Create(ctx context.Context, r *pb.CreateRequest) (*pb.CreateResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	ts := time.Now().Unix()
	inode := s.fl.GetID()
	attr := &pb.Attr{
		Inode:  inode,
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   r.Attr.Mode,
		Uid:    r.Attr.Uid,
		Gid:    r.Attr.Gid,
	}
	rname, rattr, err := s.fs.Create(ctx, GetID(fsid.Bytes(), r.Parent, 0), GetID(fsid.Bytes(), inode, 0), inode, r.Name, attr, false)
	if err != nil {
		return nil, err
	}
	return &pb.CreateResponse{Name: rname, Attr: rattr}, err
}

func (s *apiServer) MkDir(ctx context.Context, r *pb.MkDirRequest) (*pb.MkDirResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	ts := time.Now().Unix()
	inode := s.fl.GetID()
	attr := &pb.Attr{
		Inode:  inode,
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   uint32(os.ModeDir) | r.Attr.Mode,
		Uid:    r.Attr.Uid,
		Gid:    r.Attr.Gid,
	}
	rname, rattr, err := s.fs.Create(ctx, GetID(fsid.Bytes(), r.Parent, 0), GetID(fsid.Bytes(), inode, 0), inode, r.Name, attr, true)
	return &pb.MkDirResponse{Name: rname, Attr: rattr}, err
}

func (s *apiServer) Read(ctx context.Context, r *pb.ReadRequest) (*pb.ReadResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	s.log.Debug("READ", zap.Uint64("inode", r.Inode), zap.Int64("offset", r.Offset), zap.Int64("size", r.Size))
	block := uint64(r.Offset / s.blocksize)
	data := make([]byte, r.Size)
	firstOffset := int64(0)
	if r.Offset%s.blocksize != 0 {
		// Handle non-aligned offset
		firstOffset = r.Offset - int64(block)*s.blocksize
	}
	cur := int64(0)
	for cur < r.Size {
		id := GetID(fsid.Bytes(), r.Inode, block+1) // block 0 is for inode data
		chunk, err := s.fs.GetChunk(ctx, id)
		if err != nil {
			s.log.Debug("Failed to read block: ", zap.Error(err))
			// NOTE: This returns basically 0's to the client.for this block in this case
			//       It is totally valid for a fs to request an invalid block
			// TODO: Do we need to differentiate between real errors and bad requests?
			return &pb.ReadResponse{}, nil
		}
		if len(chunk) == 0 {
			break
		}
		count := copy(data[cur:], chunk[firstOffset:])
		firstOffset = 0
		block++
		cur += int64(count)
		if int64(len(chunk)) < s.blocksize {
			break
		}
	}
	f := &pb.ReadResponse{Inode: r.Inode, Payload: data}
	return f, nil
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (s *apiServer) Write(ctx context.Context, r *pb.WriteRequest) (*pb.WriteResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	s.log.Debug("WRITE", zap.Uint64("inode", r.Inode), zap.Int64("offset", r.Offset), zap.Int("size", len(r.Payload)))
	block := uint64(r.Offset / s.blocksize)
	firstOffset := int64(0)
	if r.Offset%s.blocksize != 0 {
		// Handle non-aligned offset
		firstOffset = r.Offset - int64(block)*s.blocksize
	}
	cur := int64(0)
	for cur < int64(len(r.Payload)) {
		sendSize := min(s.blocksize, int64(len(r.Payload))-cur)
		if sendSize+firstOffset > s.blocksize {
			sendSize = s.blocksize - firstOffset
		}
		payload := r.Payload[cur : cur+sendSize]
		id := GetID(fsid.Bytes(), r.Inode, block+1) // 0 block is for inode data
		if firstOffset > 0 || sendSize < s.blocksize {
			// need to get the block and update
			chunk := make([]byte, firstOffset+int64(len(payload)))
			data, err := s.fs.GetChunk(ctx, id)
			if firstOffset > 0 && err != nil {
				// TODO: How do we differentiate a block that hasn't been created yet, and a block that is truely missing?
				s.log.Debug("Couldn't read block for write", zap.Binary("block_id", id))
			} else {
				if len(data) > len(chunk) {
					chunk = data
				} else {
					copy(chunk, data)
				}
			}
			copy(chunk[firstOffset:], payload)
			payload = chunk
			firstOffset = 0
		}
		err := s.fs.WriteChunk(ctx, id, payload)
		// TODO: Need better error handling for failing with multiple chunks
		if err != nil {
			return &pb.WriteResponse{Status: 1}, err
		}
		err = s.fs.Update(
			ctx,
			GetID(fsid.Bytes(), r.Inode, 0),
			block,
			uint64(s.blocksize),
			uint64(r.Offset+int64(len(r.Payload))),
			time.Now().Unix(),
		)
		if err != nil {
			return &pb.WriteResponse{Status: 1}, err
		}
		// TODO: Should we queue on error instead?
		//s.updateChan <- &UpdateItem{
		//	id:        GetID(fsid.Bytes(), r.Inode, 0),
		//	block:     block,
		//	blocksize: uint64(s.blocksize),
		//	//size:      uint64(len(payload)),
		//	size:	uint64(r.Offset + int64(len(r.Payload))),
		//	mtime:     time.Now().Unix(),
		//}
		cur += sendSize
		block++
	}
	return &pb.WriteResponse{Status: 0}, nil
}

func (s *apiServer) Lookup(ctx context.Context, r *pb.LookupRequest) (*pb.LookupResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	name, attr, err := s.fs.Lookup(ctx, GetID(fsid.Bytes(), r.Parent, 0), r.Name)
	return &pb.LookupResponse{Name: name, Attr: attr}, err
}

func (s *apiServer) ReadDirAll(ctx context.Context, n *pb.ReadDirAllRequest) (*pb.ReadDirAllResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	return s.fs.ReadDirAll(ctx, GetID(fsid.Bytes(), n.Inode, 0))
}

func (s *apiServer) Remove(ctx context.Context, r *pb.RemoveRequest) (*pb.RemoveResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	status, err := s.fs.Remove(ctx, GetID(fsid.Bytes(), r.Parent, 0), r.Name)
	return &pb.RemoveResponse{Status: status}, err
}

func (s *apiServer) Symlink(ctx context.Context, r *pb.SymlinkRequest) (*pb.SymlinkResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	ts := time.Now().Unix()
	inode := s.fl.GetID()
	attr := &pb.Attr{
		Inode:  inode,
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   uint32(os.ModeSymlink | 0755),
		Size:   uint64(len(r.Target)),
		Uid:    r.Uid,
		Gid:    r.Gid,
	}
	return s.fs.Symlink(ctx, GetID(fsid.Bytes(), r.Parent, 0), GetID(fsid.Bytes(), inode, 0), r.Name, r.Target, attr, inode)
}

func (s *apiServer) Readlink(ctx context.Context, r *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	return s.fs.Readlink(ctx, GetID(fsid.Bytes(), r.Inode, 0))
}

func (s *apiServer) Getxattr(ctx context.Context, r *pb.GetxattrRequest) (*pb.GetxattrResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	return s.fs.Getxattr(ctx, GetID(fsid.Bytes(), r.Inode, 0), r.Name)
}

func (s *apiServer) Setxattr(ctx context.Context, r *pb.SetxattrRequest) (*pb.SetxattrResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	return s.fs.Setxattr(ctx, GetID(fsid.Bytes(), r.Inode, 0), r.Name, r.Value)
}

func (s *apiServer) Listxattr(ctx context.Context, r *pb.ListxattrRequest) (*pb.ListxattrResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	return s.fs.Listxattr(ctx, GetID(fsid.Bytes(), r.Inode, 0))
}

func (s *apiServer) Removexattr(ctx context.Context, r *pb.RemovexattrRequest) (*pb.RemovexattrResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	return s.fs.Removexattr(ctx, GetID(fsid.Bytes(), r.Inode, 0), r.Name)
}

func (s *apiServer) Rename(ctx context.Context, r *pb.RenameRequest) (*pb.RenameResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	return s.fs.Rename(ctx, GetID(fsid.Bytes(), r.OldParent, 0), GetID(fsid.Bytes(), r.NewParent, 0), r.OldName, r.NewName)
}

func (s *apiServer) Statfs(ctx context.Context, r *pb.StatfsRequest) (*pb.StatfsResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	resp := &pb.StatfsResponse{
		Blocks:  281474976710656, // 1 exabyte (asuming 4K block size)
		Bfree:   281474976710656,
		Bavail:  281474976710656,
		Files:   1000000000000, // 1 trillion inodes
		Ffree:   1000000000000,
		Bsize:   4096, // it looked like ext4 used 4KB blocks
		Namelen: 256,
		Frsize:  4096, // this should probably match Bsize so we don't allow fragmented blocks
	}
	return resp, nil
}

func (s *apiServer) Check(ctx context.Context, r *pb.CheckRequest) (*pb.CheckResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	s.log.Debug("Check Initiated", zap.Uint64("Parent", r.Inode), zap.String("Name", r.Name))
	// Try looking up the dirent
	dirent, err := s.fs.GetDirent(ctx, GetID(fsid.Bytes(), r.Inode, 0), r.Name)
	if err != nil {
		s.log.Debug("Check failed to find the Dirent", zap.Error(err))
		return &pb.CheckResponse{Response: "Error: Could not find dirent."}, nil
	}
	// Read the inode block
	_, err = s.fs.GetInode(ctx, dirent.Id)
	if err != nil {
		s.log.Debug("Check failed to find the Inode", zap.Error(err))
		// Note: Unfortunately if we lose the inode block, there is no way to recover because we do not know the inode of the file itself
		// Delete the entry
		err = s.fs.DeleteListing(ctx, GetID(fsid.Bytes(), r.Inode, 0), r.Name, brimtime.TimeToUnixMicro(time.Now()))
		if err != nil {
			s.log.Debug("Check error removing listing", zap.Error(err))
			return &pb.CheckResponse{Response: fmt.Sprintf("Error: Inode not found but could not delete the listing: %v", err)}, nil
		} else {
			s.log.Debug("Check removed dir entry", zap.Uint64("Parent", r.Inode), zap.String("Name", r.Name))
			return &pb.CheckResponse{Response: "Inode could not be found, and dir entry removed."}, nil
		}
	}

	// If we get here then everything is fine
	return &pb.CheckResponse{Response: "No issues found."}, nil
}

func (s *apiServer) InitFs(ctx context.Context, r *pb.InitFsRequest) (*pb.InitFsResponse, error) {
	err := s.validateIP(ctx)
	if err != nil {
		return nil, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.InitFsResponse{}, s.fs.InitFs(ctx, fsid.Bytes())
}
