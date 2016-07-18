package main

import (
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"log"
	"os"
	"sort"
	"time"

	"github.com/getcfs/fuse"

	"github.com/getcfs/megacfs/formic"
	pb "github.com/getcfs/megacfs/formic/proto"
	"github.com/gholt/brimtime"
	"github.com/gholt/store"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
)

const (
	InodeEntryVersion = 1
	DirEntryVersion   = 1
	FileBlockVersion  = 1
	MaxRetries        = 10
)

type FileService interface {
	InitFs(ctx context.Context, fsid []byte) error
	GetAttr(ctx context.Context, id []byte) (*pb.Attr, error)
	SetAttr(ctx context.Context, id []byte, attr *pb.Attr, valid uint32) (*pb.Attr, error)
	Create(ctx context.Context, parent, id []byte, inode uint64, name string, attr *pb.Attr, isdir bool) (string, *pb.Attr, error)
	Update(ctx context.Context, id []byte, block, size, blocksize uint64, mtime int64) error
	Lookup(ctx context.Context, parent []byte, name string) (string, *pb.Attr, error)
	ReadDirAll(ctx context.Context, id []byte) (*pb.ReadDirAllResponse, error)
	Remove(ctx context.Context, parent []byte, name string) (int32, error)
	Symlink(ctx context.Context, parent, id []byte, name string, target string, attr *pb.Attr, inode uint64) (*pb.SymlinkResponse, error)
	Readlink(ctx context.Context, id []byte) (*pb.ReadlinkResponse, error)
	Getxattr(ctx context.Context, id []byte, name string) (*pb.GetxattrResponse, error)
	Setxattr(ctx context.Context, id []byte, name string, value []byte) (*pb.SetxattrResponse, error)
	Listxattr(ctx context.Context, id []byte) (*pb.ListxattrResponse, error)
	Removexattr(ctx context.Context, id []byte, name string) (*pb.RemovexattrResponse, error)
	Rename(ctx context.Context, oldParent, newParent []byte, oldName, newName string) (*pb.RenameResponse, error)
	GetChunk(ctx context.Context, id []byte) ([]byte, error)
	WriteChunk(ctx context.Context, id, data []byte) error
	DeleteChunk(ctx context.Context, id []byte, tsm int64) error
	DeleteListing(ctx context.Context, parent []byte, name string, tsm int64) error
	GetInode(ctx context.Context, id []byte) (*pb.InodeEntry, error)
	GetDirent(ctx context.Context, parent []byte, name string) (*pb.DirEntry, error)
}

var ErrStoreHasNewerValue = errors.New("Error store already has newer value")
var ErrNotFound = errors.New("Not found")

type StoreComms struct {
	vstore store.ValueStore
	gstore store.GroupStore
}

func NewStoreComms(vstore store.ValueStore, gstore store.GroupStore) (*StoreComms, error) {
	return &StoreComms{
		vstore: vstore,
		gstore: gstore,
	}, nil
}

// Helper methods to get data from value and group store
func (o *StoreComms) ReadValue(ctx context.Context, id []byte) ([]byte, error) {
	// TODO: You might want to make this whole area pass in reusable []byte to
	// lessen gc pressure.
	keyA, keyB := murmur3.Sum128(id)
	_, v, err := o.vstore.Read(ctx, keyA, keyB, nil)
	return v, err
}

func (o *StoreComms) WriteValue(ctx context.Context, id, data []byte) error {
	keyA, keyB := murmur3.Sum128(id)
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	oldTimestampMicro, err := o.vstore.Write(ctx, keyA, keyB, timestampMicro, data)
	retries := 0
	for (oldTimestampMicro >= timestampMicro) && (retries < MaxRetries) {
		retries++
		timestampMicro = brimtime.TimeToUnixMicro(time.Now())
		oldTimestampMicro, err = o.vstore.Write(ctx, keyA, keyB, timestampMicro, data)
	}
	if err != nil {
		return err
	}
	if oldTimestampMicro >= timestampMicro {
		return ErrStoreHasNewerValue
	}
	return nil
}

func (o *StoreComms) DeleteValue(ctx context.Context, id []byte) error {
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	return o.DeleteValueTS(ctx, id, timestampMicro)
}

func (o *StoreComms) DeleteValueTS(ctx context.Context, id []byte, tsm int64) error {
	keyA, keyB := murmur3.Sum128(id)
	oldTimestampMicro, err := o.vstore.Delete(ctx, keyA, keyB, tsm)
	if oldTimestampMicro >= tsm {
		return ErrStoreHasNewerValue
	}
	return err
}

func (o *StoreComms) WriteGroup(ctx context.Context, key, childKey, value []byte) error {
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	return o.WriteGroupTS(ctx, key, childKey, value, timestampMicro)
}

func (o *StoreComms) WriteGroupTS(ctx context.Context, key, childKey, value []byte, tsm int64) error {
	keyA, keyB := murmur3.Sum128(key)
	childKeyA, childKeyB := murmur3.Sum128(childKey)
	oldTimestampMicro, err := o.gstore.Write(ctx, keyA, keyB, childKeyA, childKeyB, tsm, value)
	if err != nil {
		return nil
	}
	if oldTimestampMicro >= tsm {
		return ErrStoreHasNewerValue
	}
	return nil
}

func (o *StoreComms) ReadGroupItem(ctx context.Context, key, childKey []byte) ([]byte, error) {
	childKeyA, childKeyB := murmur3.Sum128(childKey)
	return o.ReadGroupItemByKey(ctx, key, childKeyA, childKeyB)
}

func (o *StoreComms) ReadGroupItemByKey(ctx context.Context, key []byte, childKeyA, childKeyB uint64) ([]byte, error) {
	keyA, keyB := murmur3.Sum128(key)
	_, v, err := o.gstore.Read(ctx, keyA, keyB, childKeyA, childKeyB, nil)
	return v, err
}

func (o *StoreComms) DeleteGroupItem(ctx context.Context, key, childKey []byte) error {
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	return o.DeleteGroupItemTS(ctx, key, childKey, timestampMicro)
}

func (o *StoreComms) DeleteGroupItemTS(ctx context.Context, key, childKey []byte, tsm int64) error {
	keyA, keyB := murmur3.Sum128(key)
	childKeyA, childKeyB := murmur3.Sum128(childKey)
	oldTimestampMicro, err := o.gstore.Delete(ctx, keyA, keyB, childKeyA, childKeyB, tsm)
	if err != nil {
		return err
	}
	if oldTimestampMicro >= tsm {
		return ErrStoreHasNewerValue
	}
	return nil
}

func (o *StoreComms) LookupGroup(ctx context.Context, key []byte) ([]store.LookupGroupItem, error) {
	keyA, keyB := murmur3.Sum128(key)
	items, err := o.gstore.LookupGroup(ctx, keyA, keyB)
	if err != nil {
		return nil, err
	}
	return items, nil
}

func (o *StoreComms) ReadGroup(ctx context.Context, key []byte) ([]store.ReadGroupItem, error) {
	keyA, keyB := murmur3.Sum128(key)
	items, err := o.gstore.ReadGroup(ctx, keyA, keyB)
	if err != nil {
		return nil, err
	}
	return items, nil
}

type OortFS struct {
	hasher     func() hash.Hash32
	comms      *StoreComms
	deleteChan chan *DeleteItem
}

func NewOortFS(comms *StoreComms) *OortFS {
	o := &OortFS{
		hasher: crc32.NewIEEE,
		comms:  comms,
	}
	// TODO: How big should the chan be, or should we have another in memory queue that feeds the chan?
	o.deleteChan = make(chan *DeleteItem, 1000)
	deletes := newDeletinator(o.deleteChan, o, comms)
	go deletes.run()
	return o
}

func (o *OortFS) InitFs(ctx context.Context, fsid []byte) error {
	id := formic.GetID(fsid, 1, 0)
	n, _ := o.GetChunk(ctx, id)
	if len(n) == 0 {
		log.Println("Creating new root at ", id)
		// Need to create the root node
		r := &pb.InodeEntry{
			Version: InodeEntryVersion,
			Inode:   1,
			IsDir:   true,
			FsId:    fsid,
		}
		ts := time.Now().Unix()
		r.Attr = &pb.Attr{
			Inode:  1,
			Atime:  ts,
			Mtime:  ts,
			Ctime:  ts,
			Crtime: ts,
			Mode:   uint32(os.ModeDir | 0775),
			Uid:    1001, // TODO: need to config default user/group id
			Gid:    1001,
		}
		b, err := formic.Marshal(r)
		if err != nil {
			return err
		}
		err = o.WriteChunk(ctx, id, b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *OortFS) GetAttr(ctx context.Context, id []byte) (*pb.Attr, error) {
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return &pb.Attr{}, err
	}
	n := &pb.InodeEntry{}
	err = formic.Unmarshal(b, n)
	if err != nil {
		return &pb.Attr{}, err
	}
	return n.Attr, nil
}

func (o *OortFS) SetAttr(ctx context.Context, id []byte, attr *pb.Attr, v uint32) (*pb.Attr, error) {
	valid := fuse.SetattrValid(v)
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return &pb.Attr{}, err
	}
	n := &pb.InodeEntry{}
	err = formic.Unmarshal(b, n)
	if err != nil {
		return &pb.Attr{}, err
	}
	if valid.Mode() {
		n.Attr.Mode = attr.Mode
	}
	if valid.Size() {
		if n.Attr.Size == 0 {
			n.Blocks = 0
		}
		n.Attr.Size = attr.Size
	}
	if valid.Mtime() {
		n.Attr.Mtime = attr.Mtime
	}
	if valid.Atime() {
		n.Attr.Atime = attr.Atime
	}
	if valid.Uid() {
		n.Attr.Uid = attr.Uid
	}
	if valid.Gid() {
		n.Attr.Gid = attr.Gid
	}
	b, err = formic.Marshal(n)
	if err != nil {
		return &pb.Attr{}, err
	}
	err = o.WriteChunk(ctx, id, b)
	if err != nil {
		return &pb.Attr{}, err
	}

	return n.Attr, nil
}

func (o *OortFS) Create(ctx context.Context, parent, id []byte, inode uint64, name string, attr *pb.Attr, isdir bool) (string, *pb.Attr, error) {
	// Check to see if the name already exists
	b, err := o.comms.ReadGroupItem(ctx, parent, []byte(name))
	if err != nil && !store.IsNotFound(err) {
		// TODO: Needs beter error handling
		return "", &pb.Attr{}, err
	}
	if len(b) > 0 {
		p := &pb.DirEntry{}
		err = formic.Unmarshal(b, p)
		if err != nil {
			return "", &pb.Attr{}, err
		}
	}
	var direntType fuse.DirentType
	if isdir {
		direntType = fuse.DT_Dir
	} else {
		direntType = fuse.DT_File
	}
	// Add the name to the group
	d := &pb.DirEntry{
		Version: DirEntryVersion,
		Name:    name,
		Id:      id,
		Type:    uint32(direntType),
	}
	b, err = formic.Marshal(d)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	err = o.comms.WriteGroup(ctx, parent, []byte(name), b)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	// Add the inode entry
	n := &pb.InodeEntry{
		Version: InodeEntryVersion,
		Inode:   inode,
		IsDir:   isdir,
		Attr:    attr,
		Blocks:  0,
	}
	b, err = formic.Marshal(n)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	err = o.WriteChunk(ctx, id, b)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	return name, attr, nil
}

func (o *OortFS) Lookup(ctx context.Context, parent []byte, name string) (string, *pb.Attr, error) {
	// Get the id
	b, err := o.comms.ReadGroupItem(ctx, parent, []byte(name))
	if store.IsNotFound(err) {
		return "", &pb.Attr{}, formic.ErrNotFound
	} else if err != nil {
		return "", &pb.Attr{}, err
	}
	d := &pb.DirEntry{}
	err = formic.Unmarshal(b, d)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	// Get the Inode entry
	b, err = o.GetChunk(ctx, d.Id)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	n := &pb.InodeEntry{}
	err = formic.Unmarshal(b, n)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	return d.Name, n.Attr, nil
}

// Needed to be able to sort the dirents
type ByDirent []*pb.DirEnt

func (d ByDirent) Len() int {
	return len(d)
}

func (d ByDirent) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d ByDirent) Less(i, j int) bool {
	return d[i].Name < d[j].Name
}

func (o *OortFS) ReadDirAll(ctx context.Context, id []byte) (*pb.ReadDirAllResponse, error) {
	// Get the keys from the group
	items, err := o.comms.ReadGroup(ctx, id)
	if err != nil {
		// TODO: Needs beter error handling
		log.Println("Error looking up group: ", err)
		return &pb.ReadDirAllResponse{}, err
	}
	// Iterate over each item, getting the ID then the Inode Entry
	e := &pb.ReadDirAllResponse{}
	dirent := &pb.DirEntry{}
	for _, item := range items {
		err = formic.Unmarshal(item.Value, dirent)
		if err != nil {
			return &pb.ReadDirAllResponse{}, err
		}
		e.DirEntries = append(e.DirEntries, &pb.DirEnt{Name: dirent.Name, Type: dirent.Type})
	}
	sort.Sort(ByDirent(e.DirEntries))
	return e, nil
}

func (o *OortFS) Remove(ctx context.Context, parent []byte, name string) (int32, error) {
	// Get the ID from the group list
	b, err := o.comms.ReadGroupItem(ctx, parent, []byte(name))
	if store.IsNotFound(err) {
		return 1, nil
	} else if err != nil {
		return 1, err
	}
	d := &pb.DirEntry{}
	err = formic.Unmarshal(b, d)
	if err != nil {
		return 1, err
	}
	fsid, err := GetFsId(ctx)
	if err != nil {
		return 1, err
	}
	if fuse.DirentType(d.Type) == fuse.DT_Dir {
		inode, err := o.GetInode(ctx, d.Id)
		if err != nil {
			return 1, err
		}
		items, err := o.comms.ReadGroup(ctx, formic.GetID(fsid.Bytes(), uint64(inode.Inode), 0))
		if err != nil {
			return 1, err
		}
		// return error if directory is not empty
		if len(items) > 0 {
			return 1, formic.ErrNotEmpty
		}
	}
	// TODO: More error handling needed
	// TODO: Handle possible race conditions where user writes and deletes the same file over and over
	t := &pb.Tombstone{}
	tsm := brimtime.TimeToUnixMicro(time.Now())
	t.Dtime = tsm
	t.Qtime = tsm
	t.FsId = fsid.Bytes()
	inode, err := o.GetInode(ctx, d.Id)
	if store.IsNotFound(err) {
		// file wasn't found. attempt to remove the group store entry
		err = o.comms.DeleteGroupItem(ctx, parent, []byte(name))
		if err != nil {
			return 1, err
		}
		return 0, nil
	} else if err != nil {
		return 1, err
	}
	t.Blocks = inode.Blocks
	t.Inode = inode.Inode
	// Write the Tombstone to the delete listing for the fsid
	err = o.comms.WriteGroupTS(ctx, formic.GetDeletedID(fsid.Bytes()), []byte(fmt.Sprintf("%d", t.Inode)), b, tsm)
	if err != nil {
		return 1, err
	}
	o.deleteChan <- &DeleteItem{
		ts: t,
	}
	// Delete the original from the listing
	err = o.comms.DeleteGroupItemTS(ctx, parent, []byte(name), tsm)
	if err != nil {
		return 1, err
	} else {
		return 0, nil
	}
}

func (o *OortFS) Update(ctx context.Context, id []byte, block, blocksize, size uint64, mtime int64) error {
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &pb.InodeEntry{}
	err = formic.Unmarshal(b, n)
	if err != nil {
		return err
	}
	if block >= n.Blocks {
		n.Blocks = block + 1
		n.BlockSize = blocksize
	}
	if size > n.Attr.Size {
		n.Attr.Size = size
	}
	if mtime > n.Attr.Mtime {
		n.Attr.Mtime = mtime
	}
	b, err = formic.Marshal(n)
	if err != nil {
		return err
	}
	err = o.WriteChunk(ctx, id, b)
	if err != nil {
		return err
	}
	return nil
}

func (o *OortFS) Symlink(ctx context.Context, parent, id []byte, name string, target string, attr *pb.Attr, inode uint64) (*pb.SymlinkResponse, error) {
	// Check to see if the name exists
	val, err := o.comms.ReadGroupItem(ctx, parent, []byte(name))
	if err != nil && !store.IsNotFound(err) {
		// TODO: Needs beter error handling
		return &pb.SymlinkResponse{}, err
	}
	if len(val) > 1 { // Exists already
		return &pb.SymlinkResponse{}, nil
	}
	n := &pb.InodeEntry{
		Version: InodeEntryVersion,
		Inode:   inode,
		IsDir:   false,
		IsLink:  true,
		Target:  target,
		Attr:    attr,
	}
	b, err := formic.Marshal(n)
	if err != nil {
		return &pb.SymlinkResponse{}, err
	}
	err = o.WriteChunk(ctx, id, b)
	if err != nil {
		return &pb.SymlinkResponse{}, err
	}
	// Add the name to the group
	d := &pb.DirEntry{
		Version: DirEntryVersion,
		Name:    name,
		Id:      id,
		Type:    uint32(fuse.DT_File),
	}
	b, err = formic.Marshal(d)
	if err != nil {
		return &pb.SymlinkResponse{}, err
	}
	err = o.comms.WriteGroup(ctx, parent, []byte(name), b)
	if err != nil {
		return &pb.SymlinkResponse{}, err
	}
	return &pb.SymlinkResponse{Name: name, Attr: attr}, nil
}

func (o *OortFS) Readlink(ctx context.Context, id []byte) (*pb.ReadlinkResponse, error) {
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return &pb.ReadlinkResponse{}, err
	}
	n := &pb.InodeEntry{}
	err = formic.Unmarshal(b, n)
	if err != nil {
		return &pb.ReadlinkResponse{}, err
	}
	return &pb.ReadlinkResponse{Target: n.Target}, nil
}

func (o *OortFS) Getxattr(ctx context.Context, id []byte, name string) (*pb.GetxattrResponse, error) {
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return &pb.GetxattrResponse{}, err
	}
	n := &pb.InodeEntry{}
	err = formic.Unmarshal(b, n)
	if err != nil {
		return &pb.GetxattrResponse{}, err
	}
	if xattr, ok := n.Xattr[name]; ok {
		return &pb.GetxattrResponse{Xattr: xattr}, nil
	}
	return &pb.GetxattrResponse{}, nil
}

func (o *OortFS) Setxattr(ctx context.Context, id []byte, name string, value []byte) (*pb.SetxattrResponse, error) {
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return &pb.SetxattrResponse{}, err
	}
	n := &pb.InodeEntry{}
	err = formic.Unmarshal(b, n)
	if err != nil {
		return &pb.SetxattrResponse{}, err
	}
	if n.Xattr == nil {
		n.Xattr = make(map[string][]byte)
	}
	n.Xattr[name] = value
	b, err = formic.Marshal(n)
	if err != nil {
		return &pb.SetxattrResponse{}, err
	}
	err = o.WriteChunk(ctx, id, b)
	if err != nil {
		return &pb.SetxattrResponse{}, err
	}
	return &pb.SetxattrResponse{}, nil
}

func (o *OortFS) Listxattr(ctx context.Context, id []byte) (*pb.ListxattrResponse, error) {
	resp := &pb.ListxattrResponse{}
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return &pb.ListxattrResponse{}, err
	}
	n := &pb.InodeEntry{}
	err = formic.Unmarshal(b, n)
	if err != nil {
		return &pb.ListxattrResponse{}, err
	}
	names := ""
	for name := range n.Xattr {
		names += name
		names += "\x00"
	}
	resp.Xattr = []byte(names)
	return resp, nil
}

func (o *OortFS) Removexattr(ctx context.Context, id []byte, name string) (*pb.RemovexattrResponse, error) {
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return &pb.RemovexattrResponse{}, err
	}
	n := &pb.InodeEntry{}
	err = formic.Unmarshal(b, n)
	if err != nil {
		return &pb.RemovexattrResponse{}, err
	}
	delete(n.Xattr, name)
	b, err = formic.Marshal(n)
	if err != nil {
		return &pb.RemovexattrResponse{}, err
	}
	err = o.WriteChunk(ctx, id, b)
	if err != nil {
		return &pb.RemovexattrResponse{}, err
	}
	return &pb.RemovexattrResponse{}, nil
}

func (o *OortFS) Rename(ctx context.Context, oldParent, newParent []byte, oldName, newName string) (*pb.RenameResponse, error) {
	// Get the ID from the group list
	b, err := o.comms.ReadGroupItem(ctx, oldParent, []byte(oldName))
	if store.IsNotFound(err) {
		return &pb.RenameResponse{}, nil
	}
	if err != nil {
		return &pb.RenameResponse{}, err
	}
	d := &pb.DirEntry{}
	err = formic.Unmarshal(b, d)
	if err != nil {
		return &pb.RenameResponse{}, err
	}
	// TODO: Handle orphaned data from overwrites
	// Create new entry
	d.Name = newName
	b, err = formic.Marshal(d)
	err = o.comms.WriteGroup(ctx, newParent, []byte(newName), b)
	if err != nil {
		return &pb.RenameResponse{}, err
	}
	// Delete old entry
	err = o.comms.DeleteGroupItem(ctx, oldParent, []byte(oldName))
	if err != nil {
		// TODO: Handle errors
		// If we fail here then we will have two entries
		return &pb.RenameResponse{}, err
	}
	return &pb.RenameResponse{}, nil
}

func (o *OortFS) GetChunk(ctx context.Context, id []byte) ([]byte, error) {
	b, err := o.comms.ReadValue(ctx, id)
	if store.IsNotFound(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	fb := &pb.FileBlock{}
	err = formic.Unmarshal(b, fb)
	if err != nil {
		return nil, err
	}
	// TODO: Validate checksum and handle errors
	return fb.Data, nil
}

func (o *OortFS) WriteChunk(ctx context.Context, id, data []byte) error {
	crc := o.hasher()
	crc.Write(data)
	fb := &pb.FileBlock{
		Version:  FileBlockVersion,
		Data:     data,
		Checksum: crc.Sum32(),
	}
	b, err := formic.Marshal(fb)
	if err != nil {
		return err
	}
	return o.comms.WriteValue(ctx, id, b)
}

func (o *OortFS) DeleteChunk(ctx context.Context, id []byte, tsm int64) error {
	return o.comms.DeleteValueTS(ctx, id, tsm)
}

func (o *OortFS) DeleteListing(ctx context.Context, parent []byte, name string, tsm int64) error {
	return o.comms.DeleteGroupItemTS(ctx, parent, []byte(name), tsm)
}

func (o *OortFS) GetInode(ctx context.Context, id []byte) (*pb.InodeEntry, error) {
	// Get the Inode entry
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return nil, err
	}
	n := &pb.InodeEntry{}
	err = formic.Unmarshal(b, n)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (o *OortFS) GetDirent(ctx context.Context, parent []byte, name string) (*pb.DirEntry, error) {
	// Get the Dir Entry
	b, err := o.comms.ReadGroupItem(ctx, parent, []byte(name))
	if store.IsNotFound(err) {
		return &pb.DirEntry{}, nil
	} else if err != nil {
		return &pb.DirEntry{}, err
	}
	d := &pb.DirEntry{}
	err = formic.Unmarshal(b, d)
	if err != nil {
		return &pb.DirEntry{}, err
	}
	return d, nil
}
