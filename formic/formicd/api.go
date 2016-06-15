package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/getcfs/megacfs/formic"
	"github.com/getcfs/megacfs/formic/flother"
	pb "github.com/getcfs/megacfs/formic/proto"
	"github.com/gholt/store"
	"github.com/satori/go.uuid"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
)

var ErrUnauthorized = errors.New("Unknown or unauthorized filesystem")

type apiServer struct {
	sync.RWMutex
	fs         FileService
	fl         *flother.Flother
	blocksize  int64
	updateChan chan *UpdateItem
	comms      *StoreComms
	validIPs   map[string]map[string]bool
}

func NewApiServer(fs FileService, nodeId int, comms *StoreComms) *apiServer {
	s := new(apiServer)
	s.fs = fs
	s.comms = comms
	s.validIPs = make(map[string]map[string]bool)
	log.Println("NodeID: ", nodeId)
	s.fl = flother.NewFlother(time.Time{}, uint64(nodeId))
	s.blocksize = int64(1024 * 64) // Default Block Size (64K)
	s.updateChan = make(chan *UpdateItem, 1000)
	updates := newUpdatinator(s.updateChan, fs)
	go updates.run()
	return s
}

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
		ips = make(map[string]bool)
		s.validIPs[fsid] = ips
	}
	valid, ok := ips[ip]
	if ok && valid {
		return nil
	}
	_, err = s.comms.ReadGroupItem(ctx, []byte(fmt.Sprintf("/fs/%s/addr", fsid)), []byte(ip))
	if store.IsNotFound(err) {
		log.Println("Invalid IP: ", ip)
		// No access
		return ErrUnauthorized
	}
	if err != nil {
		return err
	}
	// Cache the valid ip
	s.validIPs[fsid][ip] = true
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
	attr, err := s.fs.GetAttr(ctx, formic.GetID(fsid.Bytes(), r.Inode, 0))
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
	attr, err := s.fs.SetAttr(ctx, formic.GetID(fsid.Bytes(), r.Attr.Inode, 0), r.Attr, r.Valid)
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
	rname, rattr, err := s.fs.Create(ctx, formic.GetID(fsid.Bytes(), r.Parent, 0), formic.GetID(fsid.Bytes(), inode, 0), inode, r.Name, attr, false)
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
	rname, rattr, err := s.fs.Create(ctx, formic.GetID(fsid.Bytes(), r.Parent, 0), formic.GetID(fsid.Bytes(), inode, 0), inode, r.Name, attr, true)
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
	log.Printf("READ: Inode: %d Offset: %d Size: %d", r.Inode, r.Offset, r.Size)
	block := uint64(r.Offset / s.blocksize)
	data := make([]byte, r.Size)
	firstOffset := int64(0)
	if r.Offset%s.blocksize != 0 {
		// Handle non-aligned offset
		firstOffset = r.Offset - int64(block)*s.blocksize
	}
	cur := int64(0)
	for cur < r.Size {
		id := formic.GetID(fsid.Bytes(), r.Inode, block+1) // block 0 is for inode data
		chunk, err := s.fs.GetChunk(ctx, id)
		if err != nil {
			log.Print("Err: Failed to read block: ", err)
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
		block += 1
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
	} else {
		return b
	}
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
	log.Printf("WRITE: Inode %d Offset: %d Size: %d", r.Inode, r.Offset, len(r.Payload))
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
		id := formic.GetID(fsid.Bytes(), r.Inode, block+1) // 0 block is for inode data
		if firstOffset > 0 || sendSize < s.blocksize {
			// need to get the block and update
			chunk := make([]byte, firstOffset+int64(len(payload)))
			data, err := s.fs.GetChunk(ctx, id)
			if firstOffset > 0 && err != nil {
				// TODO: How do we differentiate a block that hasn't been created yet, and a block that is truely missing?
				log.Printf("WARN: couldn't get block id %d", id)
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
			formic.GetID(fsid.Bytes(), r.Inode, 0),
			block,
			uint64(s.blocksize),
			uint64(r.Offset + int64(len(r.Payload))),
			time.Now().Unix(),
		)
		//s.updateChan <- &UpdateItem{
		//	id:        formic.GetID(fsid.Bytes(), r.Inode, 0),
		//	block:     block,
		//	blocksize: uint64(s.blocksize),
		//	//size:      uint64(len(payload)),
		//	size:	uint64(r.Offset + int64(len(r.Payload))),
		//	mtime:     time.Now().Unix(),
		//}
		cur += sendSize
		block += 1
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
	name, attr, err := s.fs.Lookup(ctx, formic.GetID(fsid.Bytes(), r.Parent, 0), r.Name)
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
	return s.fs.ReadDirAll(ctx, formic.GetID(fsid.Bytes(), n.Inode, 0))
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
	status, err := s.fs.Remove(ctx, formic.GetID(fsid.Bytes(), r.Parent, 0), r.Name)
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
	return s.fs.Symlink(ctx, formic.GetID(fsid.Bytes(), r.Parent, 0), formic.GetID(fsid.Bytes(), inode, 0), r.Name, r.Target, attr, inode)
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
	return s.fs.Readlink(ctx, formic.GetID(fsid.Bytes(), r.Inode, 0))
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
	return s.fs.Getxattr(ctx, formic.GetID(fsid.Bytes(), r.Inode, 0), r.Name)
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
	return s.fs.Setxattr(ctx, formic.GetID(fsid.Bytes(), r.Inode, 0), r.Name, r.Value)
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
	return s.fs.Listxattr(ctx, formic.GetID(fsid.Bytes(), r.Inode, 0))
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
	return s.fs.Removexattr(ctx, formic.GetID(fsid.Bytes(), r.Inode, 0), r.Name)
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
	return s.fs.Rename(ctx, formic.GetID(fsid.Bytes(), r.OldParent, 0), formic.GetID(fsid.Bytes(), r.NewParent, 0), r.OldName, r.NewName)
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
