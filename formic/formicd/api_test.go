package main

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"golang.org/x/net/context"

	"github.com/getcfs/megacfs/formic"
	pb "github.com/getcfs/megacfs/formic/proto"
	"github.com/gogo/protobuf/proto"
	"github.com/satori/go.uuid"
)

// Minimal FileService for testing
type TestFS struct {
	writes [][]byte
	reads  [][]byte
}

func NewTestFS() *TestFS {
	return &TestFS{
		writes: make([][]byte, 0),
		reads:  make([][]byte, 0),
	}
}

func (fs *TestFS) InitFs(ctx context.Context, fsid []byte) error {
	return nil
}

func (fs *TestFS) GetDirent(ctx context.Context, parent []byte, name string) (*pb.DirEntry, error) {
	return nil, nil
}

func (fs *TestFS) GetInode(ctx context.Context, id []byte) (*pb.InodeEntry, error) {
	return nil, nil
}

func (fs *TestFS) GetChunk(ctx context.Context, id []byte) ([]byte, error) {
	if len(fs.reads) > 0 {
		chunk := fs.reads[0]
		fs.reads = fs.reads[1:]
		return chunk, nil
	} else {
		return []byte(""), nil
	}
}

func (fs *TestFS) WriteChunk(ctx context.Context, id, data []byte) error {
	fs.writes = append(fs.writes, data)
	return nil
}

func (fs *TestFS) DeleteChunk(ctx context.Context, id []byte, tsm int64) error {
	return nil
}

func (fs *TestFS) DeleteListing(ctx context.Context, parent []byte, name string, tsm int64) error {
	return nil
}

func (fs *TestFS) clearwrites() {
	fs.writes = make([][]byte, 0)
}

func (fs *TestFS) addread(d []byte) {
	fs.reads = append(fs.reads, d)
}

func (ds *TestFS) GetAttr(ctx context.Context, id []byte) (*pb.Attr, error) {
	return &pb.Attr{}, nil
}

func (ds *TestFS) SetAttr(ctx context.Context, id []byte, attr *pb.Attr, valid uint32) (*pb.Attr, error) {
	return &pb.Attr{}, nil
}

func (ds *TestFS) Create(ctx context.Context, parent, id []byte, inode uint64, name string, attr *pb.Attr, isdir bool) (string, *pb.Attr, error) {
	return name, attr, nil
}

func (ds *TestFS) Lookup(ctx context.Context, parent []byte, name string) (string, *pb.Attr, error) {
	return "", &pb.Attr{}, nil
}

func (ds *TestFS) ReadDirAll(ctx context.Context, id []byte) (*pb.ReadDirAllResponse, error) {
	return &pb.ReadDirAllResponse{}, nil
}

func (ds *TestFS) Remove(ctx context.Context, parent []byte, name string) (int32, error) {
	return 1, nil
}

func (ds *TestFS) Update(ctx context.Context, id []byte, block, blocksize, size uint64, mtime int64) error {
	return nil
}

func (ds *TestFS) Symlink(ctx context.Context, parent, id []byte, name string, target string, attr *pb.Attr, inode uint64) (*pb.SymlinkResponse, error) {
	return &pb.SymlinkResponse{}, nil
}

func (ds *TestFS) Readlink(ctx context.Context, id []byte) (*pb.ReadlinkResponse, error) {
	return &pb.ReadlinkResponse{}, nil
}

func (ds *TestFS) Getxattr(ctx context.Context, id []byte, name string) (*pb.GetxattrResponse, error) {
	return &pb.GetxattrResponse{}, nil
}

func (ds *TestFS) Setxattr(ctx context.Context, id []byte, name string, value []byte) (*pb.SetxattrResponse, error) {
	return &pb.SetxattrResponse{}, nil
}

func (ds *TestFS) Listxattr(ctx context.Context, id []byte) (*pb.ListxattrResponse, error) {
	return &pb.ListxattrResponse{}, nil
}

func (ds *TestFS) Removexattr(ctx context.Context, id []byte, name string) (*pb.RemovexattrResponse, error) {
	return &pb.RemovexattrResponse{}, nil
}

func (ds *TestFS) Rename(ctx context.Context, oldParent, newParent []byte, oldName, newName string) (*pb.RenameResponse, error) {
	return &pb.RenameResponse{}, nil
}

type fakePeerAddr struct {
}

func (f fakePeerAddr) Network() string {
	return "localhost"
}

func (f fakePeerAddr) String() string {
	return "127.0.0.1:1234"
}

func getContext() context.Context {
	fsid := uuid.NewV4()
	c, _ := context.WithTimeout(context.Background(), 5*time.Second)
	c = metadata.NewContext(
		c,
		metadata.Pairs("fsid", fsid.String()),
	)
	p := &peer.Peer{
		Addr: fakePeerAddr{},
	}
	c = peer.NewContext(c, p)
	return c
}

func TestGetID(t *testing.T) {
	id1 := formic.GetID([]byte("1"), uint64(1), uint64(1))
	id2 := formic.GetID([]byte("1"), uint64(1), uint64(1))
	if !bytes.Equal(id1, id2) {
		t.Errorf("Generated IDs not equal")
	}
	id3 := formic.GetID([]byte("1"), uint64(1), uint64(2))
	if bytes.Equal(id1, id3) {
		t.Errorf("Generated IDs were equal")
	}
}

func TestCreate(t *testing.T) {
	api := NewApiServer(NewTestFS(), 1, nil)
	_, err := api.Create(getContext(), &pb.CreateRequest{Parent: 1, Name: "Test", Attr: &pb.Attr{Gid: 1001, Uid: 1001}})
	if err != nil {
		t.Error("Create Failed: ", err)
	}

}

func TestWrite_Basic(t *testing.T) {
	fs := NewTestFS()
	api := NewApiServer(fs, 1, nil)
	api.blocksize = 10
	chunk := pb.WriteRequest{
		Inode:   0,
		Offset:  0,
		Payload: []byte("1234567890"),
	}
	r, err := api.Write(getContext(), &chunk)
	if err != nil {
		t.Error("Write Failed: ", err)
	}
	if r.Status != 0 {
		t.Error("Write status expected: 0, received: ", r.Status)
	}
	if !bytes.Equal(chunk.Payload, fs.writes[0]) {
		fmt.Println(fs.writes)
		t.Errorf("Expected write: '%s' recieved: '%s'", chunk.Payload, fs.writes[0])
	}
	chunk.Payload = []byte("1")
	fs.clearwrites()
	r, err = api.Write(getContext(), &chunk)
	if err != nil {
		t.Error("Write Failed: ", err)
	}
	if r.Status != 0 {
		t.Error("Write status expected: 0, received: ", r.Status)
	}
	if !bytes.Equal(chunk.Payload, fs.writes[0]) {
		fmt.Println(fs.writes)
		t.Errorf("Expected write: '%s' recieved: '%s'", chunk.Payload, fs.writes[0])
	}

}

func TestWrite_Chunk(t *testing.T) {
	fs := NewTestFS()
	api := NewApiServer(fs, 1, nil)
	api.blocksize = 5
	chunk := pb.WriteRequest{
		Inode:   0,
		Offset:  0,
		Payload: []byte("1234567890"),
	}
	r, err := api.Write(getContext(), &chunk)
	if err != nil {
		t.Error("Write Failed: ", err)
	}
	if r.Status != 0 {
		t.Error("Write status expected: 0, received: ", r.Status)
	}
	if !bytes.Equal(chunk.Payload[:5], fs.writes[0]) {
		fmt.Println(fs.writes)
		t.Errorf("Expected write: '%s' recieved: '%s'", chunk.Payload[:5], fs.writes[0])
	}
	if !bytes.Equal(chunk.Payload[5:], fs.writes[1]) {
		fmt.Println(fs.writes)
		t.Errorf("Expected write: '%s' recieved: '%s'", chunk.Payload[5:], fs.writes[1])
	}
}

func TestWrite_Offset(t *testing.T) {
	fs := NewTestFS()
	api := NewApiServer(fs, 1, nil)
	api.blocksize = 10
	chunk := pb.WriteRequest{
		Offset:  5,
		Payload: []byte("12345"),
		Inode:   0,
	}
	r, err := api.Write(getContext(), &chunk)
	if err != nil {
		t.Error("Write Failed: ", err)
	}
	if r.Status != 0 {
		t.Error("Write status expected: 0, received: ", r.Status)
	}
	if !bytes.Equal(chunk.Payload, fs.writes[0][5:]) {
		fmt.Println(fs.writes)
		t.Errorf("Expected write: '%s' recieved: '%s'", chunk.Payload, fs.writes[0][5:])
	}
	fs.clearwrites()
	chunk = pb.WriteRequest{
		Offset:  2,
		Payload: []byte("12345"),
		Inode:   0,
	}
	r, err = api.Write(getContext(), &chunk)
	if err != nil {
		t.Error("Write Failed: ", err)
	}
	if r.Status != 0 {
		t.Error("Write status expected: 0, received: ", r.Status)
	}
	if !bytes.Equal(chunk.Payload, fs.writes[0][2:7]) {
		fmt.Println(fs.writes)
		t.Errorf("Expected write: '%s' recieved: '%s'", chunk.Payload, fs.writes[0][5:])
	}

}

func TestWrite_MultiOffset(t *testing.T) {
	fs := NewTestFS()
	api := NewApiServer(fs, 1, nil)
	api.blocksize = 20
	chunk := pb.WriteRequest{
		Offset:  5,
		Payload: []byte("12345"),
		Inode:   0,
	}
	r, err := api.Write(getContext(), &chunk)
	if err != nil {
		t.Error("Write Failed: ", err)
	}
	if r.Status != 0 {
		t.Error("Write status expected: 0, received: ", r.Status)
	}
	if !bytes.Equal(chunk.Payload, fs.writes[0][5:]) {
		fmt.Println(fs.writes)
		t.Errorf("Expected write: '%s' recieved: '%s'", chunk.Payload, fs.writes[0][5:])
	}
}

func TestRead_Basic(t *testing.T) {
	fs := NewTestFS()
	api := NewApiServer(fs, 1, nil)
	api.blocksize = 10
	write := []byte("0123456789")
	fs.addread(write)
	data, err := api.Read(getContext(), &pb.ReadRequest{Inode: 0, Offset: 0, Size: 10})
	if err != nil {
		t.Error("Read Failed: ", err)
	}
	if !bytes.Equal(data.Payload, write) {
		t.Errorf("Expected read: '%s' received: '%s'", write, data)
	}
}

func TestRead_Offset(t *testing.T) {
	fs := NewTestFS()
	api := NewApiServer(fs, 1, nil)
	api.blocksize = 10
	write := []byte("0123456789")
	fs.addread(write)
	data, err := api.Read(getContext(), &pb.ReadRequest{Inode: 0, Offset: 5, Size: 5})
	if err != nil {
		t.Error("Read Failed: ", err)
	}
	if !bytes.Equal(data.Payload, write[5:]) {
		t.Errorf("Expected read: '%s' received: '%s'", write[5:], data)
	}
}

func TestRead_Chunk(t *testing.T) {
	fs := NewTestFS()
	api := NewApiServer(fs, 1, nil)
	api.blocksize = 10
	write1 := []byte("0123456789")
	write2 := []byte("9876543210")
	fs.addread(write1)
	fs.addread(write2)
	data, err := api.Read(getContext(), &pb.ReadRequest{Inode: 0, Offset: 0, Size: 20})
	if err != nil {
		t.Error("Read Failed: ", err)
	}
	if !bytes.Equal(data.Payload[:10], write1) {
		t.Errorf("Expected read: '%s' received: '%s'", write1, data.Payload[:10])
	}
	if !bytes.Equal(data.Payload[10:], write2) {
		t.Errorf("Expected read: '%s' received: '%s'", write2, data.Payload[10:])
	}
}

func TestProtoWriteSize(t *testing.T) {
	data := make([]byte, 64*1024)
	block := &pb.FileBlock{
		Data:     data,
		Checksum: uint32(8),
	}
	b, _ := proto.Marshal(block)
	fmt.Printf("Storing 64K and checksum in protobufs takes %d bytes.", len(b))
}
