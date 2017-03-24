package formic

import (
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"os"
	"sort"
	"time"

	"bazil.org/fuse"
	"github.com/getcfs/megacfs/flother"
	pb "github.com/getcfs/megacfs/formic/formicproto"
	"github.com/getcfs/megacfs/formic/newproto"
	"github.com/gholt/brimtime"
	"github.com/gholt/store"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

const (
	// InodeEntryVersion ...
	InodeEntryVersion = 1
	// DirEntryVersion ...
	DirEntryVersion = 1
	// FileBlockVersion ...
	FileBlockVersion = 1
	// MaxRetries ...
	MaxRetries = 10
)

// FileService ...
type FileService interface {
	NewCheck(context.Context, *newproto.CheckRequest, *newproto.CheckResponse) error
	NewCreate(context.Context, *newproto.CreateRequest, *newproto.CreateResponse) error
	NewGetAttr(context.Context, *newproto.GetAttrRequest, *newproto.GetAttrResponse) error
	NewGetxattr(context.Context, *newproto.GetxattrRequest, *newproto.GetxattrResponse) error
	NewInitFs(context.Context, *newproto.InitFsRequest, *newproto.InitFsResponse) error
	NewListxattr(context.Context, *newproto.ListxattrRequest, *newproto.ListxattrResponse) error
	NewLookup(context.Context, *newproto.LookupRequest, *newproto.LookupResponse) error
	NewMkDir(context.Context, *newproto.MkDirRequest, *newproto.MkDirResponse) error
	NewReadDirAll(context.Context, *newproto.ReadDirAllRequest, *newproto.ReadDirAllResponse) error
	NewReadlink(context.Context, *newproto.ReadlinkRequest, *newproto.ReadlinkResponse) error
	NewRead(context.Context, *newproto.ReadRequest, *newproto.ReadResponse) error
	NewRemove(context.Context, *newproto.RemoveRequest, *newproto.RemoveResponse) error
	NewRemovexattr(context.Context, *newproto.RemovexattrRequest, *newproto.RemovexattrResponse) error
	NewRename(context.Context, *newproto.RenameRequest, *newproto.RenameResponse) error
	NewSetAttr(context.Context, *newproto.SetAttrRequest, *newproto.SetAttrResponse) error
	NewSetxattr(context.Context, *newproto.SetxattrRequest, *newproto.SetxattrResponse) error
	NewStatfs(context.Context, *newproto.StatfsRequest, *newproto.StatfsResponse) error
	NewSymlink(context.Context, *newproto.SymlinkRequest, *newproto.SymlinkResponse) error
	NewWrite(context.Context, *newproto.WriteRequest, *newproto.WriteResponse) error

	InitFs(ctx context.Context, fsid []byte) error
	Create(ctx context.Context, parent, id []byte, inode uint64, name string, attr *pb.Attr, isdir bool) (string, *pb.Attr, error)
	Update(ctx context.Context, id []byte, block, size, blocksize uint64, mtime int64) error
	Lookup(ctx context.Context, parent []byte, name string) (string, *pb.Attr, error)
	Remove(ctx context.Context, parent []byte, name string) (int32, error)
	GetChunk(ctx context.Context, id []byte) ([]byte, error)
	WriteChunk(ctx context.Context, id, data []byte) error
	DeleteChunk(ctx context.Context, id []byte, tsm int64) error
	DeleteListing(ctx context.Context, parent []byte, name string, tsm int64) error
	GetInode(ctx context.Context, id []byte) (*pb.InodeEntry, error)
	GetDirent(ctx context.Context, parent []byte, name string) (*pb.DirEntry, error)
}

// ErrStoreHasNewerValue ...
var ErrStoreHasNewerValue = errors.New("Error store already has newer value")

// ErrFileNotFound ...
var ErrFileNotFound = errors.New("Not found")

// StoreComms ...
type StoreComms struct {
	vstore store.ValueStore
	gstore store.GroupStore
	log    *zap.Logger
}

// NewStoreComms ...
func NewStoreComms(vstore store.ValueStore, gstore store.GroupStore, logger *zap.Logger) (*StoreComms, error) {
	return &StoreComms{
		vstore: vstore,
		gstore: gstore,
		log:    logger,
	}, nil
}

// ReadValue ... Helper methods to get data from value and group store
func (o *StoreComms) ReadValue(ctx context.Context, id []byte) ([]byte, error) {
	// TODO: You might want to make this whole area pass in reusable []byte to
	// lessen gc pressure.
	keyA, keyB := murmur3.Sum128(id)
	_, v, err := o.vstore.Read(ctx, keyA, keyB, nil)
	return v, err
}

// WriteValue ...
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

// DeleteValue ...
func (o *StoreComms) DeleteValue(ctx context.Context, id []byte) error {
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	return o.DeleteValueTS(ctx, id, timestampMicro)
}

// DeleteValueTS ...
func (o *StoreComms) DeleteValueTS(ctx context.Context, id []byte, tsm int64) error {
	keyA, keyB := murmur3.Sum128(id)
	oldTimestampMicro, err := o.vstore.Delete(ctx, keyA, keyB, tsm)
	if oldTimestampMicro >= tsm {
		return ErrStoreHasNewerValue
	}
	return err
}

// WriteGroup ...
func (o *StoreComms) WriteGroup(ctx context.Context, key, childKey, value []byte) error {
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	return o.WriteGroupTS(ctx, key, childKey, value, timestampMicro)
}

// WriteGroupTS ...
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

// ReadGroupItem ...
func (o *StoreComms) ReadGroupItem(ctx context.Context, key, childKey []byte) ([]byte, error) {
	childKeyA, childKeyB := murmur3.Sum128(childKey)
	return o.ReadGroupItemByKey(ctx, key, childKeyA, childKeyB)
}

// ReadGroupItemByKey ...
func (o *StoreComms) ReadGroupItemByKey(ctx context.Context, key []byte, childKeyA, childKeyB uint64) ([]byte, error) {
	keyA, keyB := murmur3.Sum128(key)
	_, v, err := o.gstore.Read(ctx, keyA, keyB, childKeyA, childKeyB, nil)
	return v, err
}

// DeleteGroupItem ...
func (o *StoreComms) DeleteGroupItem(ctx context.Context, key, childKey []byte) error {
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	return o.DeleteGroupItemTS(ctx, key, childKey, timestampMicro)
}

// DeleteGroupItemTS ...
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

// LookupGroup ...
func (o *StoreComms) LookupGroup(ctx context.Context, key []byte) ([]store.LookupGroupItem, error) {
	keyA, keyB := murmur3.Sum128(key)
	items, err := o.gstore.LookupGroup(ctx, keyA, keyB)
	if err != nil {
		return nil, err
	}
	return items, nil
}

// ReadGroup ...
func (o *StoreComms) ReadGroup(ctx context.Context, key []byte) ([]store.ReadGroupItem, error) {
	keyA, keyB := murmur3.Sum128(key)
	items, err := o.gstore.ReadGroup(ctx, keyA, keyB)
	if err != nil {
		return nil, err
	}
	return items, nil
}

// OortFS ...
type OortFS struct {
	hasher     func() hash.Hash32
	comms      *StoreComms
	deleteChan chan *DeleteItem
	dirtyChan  chan *DirtyItem
	log        *zap.Logger
	blocksize  int64
	fl         *flother.Flother
}

// NewOortFS ...
func NewOortFS(comms *StoreComms, logger *zap.Logger, deleteChan chan *DeleteItem, dirtyChan chan *DirtyItem, blocksize int64, nodeID int) *OortFS {
	o := &OortFS{
		hasher:     crc32.NewIEEE,
		comms:      comms,
		log:        logger,
		deleteChan: deleteChan,
		dirtyChan:  dirtyChan,
		blocksize:  blocksize,
		fl:         flother.NewFlother(time.Time{}, uint64(nodeID)),
	}
	return o
}

// InitFs ...
func (o *OortFS) InitFs(ctx context.Context, fsid []byte) error {
	id := GetID(fsid, 1, 0)
	n, _ := o.GetChunk(ctx, id)
	if len(n) == 0 {
		//o.log.Debug("Creating new root", zap.Base64("root", id))
		// // Need to create the root node
		// r := &pb.InodeEntry{
		//	 Version: InodeEntryVersion,
		//   Inode:   1,
		//	 IsDir:   true,
		//	 FsId:    fsid,
		// }
		// ts := time.Now().Unix()
		// r.Attr = &pb.Attr{
		//	 Inode:  1,
		//	 Atime:  ts,
		//	 Mtime:  ts,
		//	 Ctime:  ts,
		//	 Crtime: ts,
		// 	 Mode:   uint32(os.ModeDir | 0775),
		//	 Uid:    1001, // TODO: need to config default user/group id
		//	 Gid:    1001,
		// }
		// b, err := Marshal(r)
		// if err != nil {
		// 	 return err
		// }
		// err = o.WriteChunk(ctx, id, b)
		// if err != nil {
		//	 return err
		// }
		return errors.New("Root Entry does not Exist")
	}
	return nil
}

func (o *OortFS) NewCheck(ctx context.Context, req *newproto.CheckRequest, resp *newproto.CheckResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	fsidb := fsid.Bytes()
	// Try looking up the dirent
	dirent, err := o.GetDirent(ctx, GetID(fsidb, req.Inode, 0), req.Name)
	if err != nil {
		return fmt.Errorf("Check failed to find the Dirent: %s", err)
	}
	// Read the inode block
	_, err = o.GetInode(ctx, dirent.Id)
	if err != nil {
		// Note: Unfortunately if we lose the inode block, there is no way to
		// recover because we do not know the inode of the file itself.
		// Delete the entry.
		err = o.DeleteListing(ctx, GetID(fsidb, req.Inode, 0), req.Name, brimtime.TimeToUnixMicro(time.Now()))
		if err != nil {
			return fmt.Errorf("Error: Inode not found but could not delete the listing: %s", err)
		}
		resp.Response = "Inode could not be found, and dir entry removed."
	}
	// If we get here then everything is fine
	resp.Response = "No issues found."
	return nil
}

func (o *OortFS) NewCreate(ctx context.Context, req *newproto.CreateRequest, resp *newproto.CreateResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	fsidb := fsid.Bytes()
	ts := time.Now().Unix()
	inode := o.fl.GetID()
	attr := &pb.Attr{
		Inode:  inode,
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   req.Attr.Mode,
		Uid:    req.Attr.Uid,
		Gid:    req.Attr.Gid,
	}
	var rattr *pb.Attr
	resp.Name, rattr, err = o.Create(ctx, GetID(fsidb, req.Parent, 0), GetID(fsidb, inode, 0), inode, req.Name, attr, false)
	if err != nil {
		return err
	}
	// TODO: Set everything explicitly for now since the structs are different
	// until the newproto becomes theproto.
	resp.Attr = &newproto.Attr{
		Inode:  rattr.Inode,
		Atime:  rattr.Atime,
		Mtime:  rattr.Mtime,
		Ctime:  rattr.Ctime,
		Crtime: rattr.Crtime,
		Mode:   rattr.Mode,
		Valid:  rattr.Valid,
		Size:   rattr.Size,
		Uid:    rattr.Uid,
		Gid:    rattr.Gid,
	}
	return nil
}

func (o *OortFS) NewGetAttr(ctx context.Context, req *newproto.GetAttrRequest, resp *newproto.GetAttrResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	b, err := o.GetChunk(ctx, GetID(fsid.Bytes(), req.Inode, 0))
	if err != nil {
		return err
	}
	n := &pb.InodeEntry{}
	err = Unmarshal(b, n)
	if err != nil {
		return err
	}
	// TODO: Set everything explicitly for now since the structs are different
	// until the newproto becomes theproto.
	resp.Attr = &newproto.Attr{
		Inode:  n.Attr.Inode,
		Atime:  n.Attr.Atime,
		Mtime:  n.Attr.Mtime,
		Ctime:  n.Attr.Ctime,
		Crtime: n.Attr.Crtime,
		Mode:   n.Attr.Mode,
		Valid:  n.Attr.Valid,
		Size:   n.Attr.Size,
		Uid:    n.Attr.Uid,
		Gid:    n.Attr.Gid,
	}
	return nil
}

func (o *OortFS) NewGetxattr(ctx context.Context, req *newproto.GetxattrRequest, resp *newproto.GetxattrResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	id := GetID(fsid.Bytes(), req.Inode, 0)
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &pb.InodeEntry{}
	err = Unmarshal(b, n)
	if err != nil {
		return err
	}
	resp.Xattr = n.Xattr[req.Name]
	return nil
}

func (o *OortFS) NewInitFs(ctx context.Context, req *newproto.InitFsRequest, resp *newproto.InitFsResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	return o.InitFs(ctx, fsid.Bytes())
}

func (o *OortFS) NewListxattr(ctx context.Context, req *newproto.ListxattrRequest, resp *newproto.ListxattrResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	id := GetID(fsid.Bytes(), req.Inode, 0)
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &pb.InodeEntry{}
	err = Unmarshal(b, n)
	if err != nil {
		return err
	}
	names := ""
	for name := range n.Xattr {
		names += name
		names += "\x00"
	}
	names += "cfs.fsid\x00"
	resp.Xattr = []byte(names)
	return nil
}

func (o *OortFS) NewLookup(ctx context.Context, req *newproto.LookupRequest, resp *newproto.LookupResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	var rattr *pb.Attr
	resp.Name, rattr, err = o.Lookup(ctx, GetID(fsid.Bytes(), req.Parent, 0), req.Name)
	if err != nil {
		return err
	}
	// TODO: Set everything explicitly for now since the structs are different
	// until the newproto becomes theproto.
	resp.Attr = &newproto.Attr{
		Inode:  rattr.Inode,
		Atime:  rattr.Atime,
		Mtime:  rattr.Mtime,
		Ctime:  rattr.Ctime,
		Crtime: rattr.Crtime,
		Mode:   rattr.Mode,
		Valid:  rattr.Valid,
		Size:   rattr.Size,
		Uid:    rattr.Uid,
		Gid:    rattr.Gid,
	}
	return nil
}

func (o *OortFS) NewReadDirAll(ctx context.Context, req *newproto.ReadDirAllRequest, resp *newproto.ReadDirAllResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	id := GetID(fsid.Bytes(), req.Inode, 0)
	// Get the keys from the group
	items, err := o.comms.ReadGroup(ctx, id)
	if err != nil {
		return err
	}
	// Iterate over each item, getting the ID then the Inode Entry
	de := &pb.DirEntry{}
	for _, item := range items {
		err = Unmarshal(item.Value, de)
		if err != nil {
			return err
		}
		resp.Direntries = append(resp.Direntries, &newproto.DirEnt{Name: de.Name, Type: de.Type})
	}
	sort.Sort(ByDirent(resp.Direntries))
	return nil
}

func (o *OortFS) NewReadlink(ctx context.Context, req *newproto.ReadlinkRequest, resp *newproto.ReadlinkResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	id := GetID(fsid.Bytes(), req.Inode, 0)
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &pb.InodeEntry{}
	err = Unmarshal(b, n)
	if err != nil {
		return err
	}
	resp.Target = n.Target
	return nil
}

func (o *OortFS) NewMkDir(ctx context.Context, req *newproto.MkDirRequest, resp *newproto.MkDirResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	fsidb := fsid.Bytes()
	ts := time.Now().Unix()
	inode := o.fl.GetID()
	attr := &pb.Attr{
		Inode:  inode,
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   uint32(os.ModeDir) | req.Attr.Mode,
		Uid:    req.Attr.Uid,
		Gid:    req.Attr.Gid,
	}
	var rattr *pb.Attr
	resp.Name, rattr, err = o.Create(ctx, GetID(fsidb, req.Parent, 0), GetID(fsidb, inode, 0), inode, req.Name, attr, true)
	if err != nil {
		return err
	}
	// TODO: Set everything explicitly for now since the structs are different
	// until the newproto becomes theproto.
	resp.Attr = &newproto.Attr{
		Inode:  rattr.Inode,
		Atime:  rattr.Atime,
		Mtime:  rattr.Mtime,
		Ctime:  rattr.Ctime,
		Crtime: rattr.Crtime,
		Mode:   rattr.Mode,
		Valid:  rattr.Valid,
		Size:   rattr.Size,
		Uid:    rattr.Uid,
		Gid:    rattr.Gid,
	}
	return nil
}

func (o *OortFS) NewRead(ctx context.Context, req *newproto.ReadRequest, resp *newproto.ReadResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	block := uint64(req.Offset / o.blocksize)
	resp.Payload = make([]byte, 0, req.Size)
	firstOffset := int64(0)
	if req.Offset%o.blocksize != 0 {
		// Handle non-aligned offset
		firstOffset = req.Offset - int64(block)*o.blocksize
	}
	fsidb := fsid.Bytes()
	cur := int64(0)
	for cur < req.Size {
		id := GetID(fsidb, req.Inode, block+1) // block 0 is for inode data
		chunk, err := o.GetChunk(ctx, id)
		if err != nil {
			// NOTE: This returns basically 0's to the client.for this block in
			// this case. It is totally valid for a fs to request an invalid
			// block
			// TODO: Do we need to differentiate between real errors and bad
			// requests?
			return nil
		}
		if len(chunk) == 0 {
			break
		}
		resp.Payload = append(resp.Payload, chunk[firstOffset:]...)
		firstOffset = 0
		block++
		cur += int64(len(chunk[firstOffset:]))
		if int64(len(chunk)) < o.blocksize {
			break
		}
	}
	return nil
}

func (o *OortFS) NewRemove(ctx context.Context, req *newproto.RemoveRequest, resp *newproto.RemoveResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	// TODO: No need for this status thing; can refactor to just return errors.
	status, err := o.Remove(ctx, GetID(fsid.Bytes(), req.Parent, 0), req.Name)
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("Remove failed with status %d", status)
	}
	return nil
}

func (o *OortFS) NewRemovexattr(ctx context.Context, req *newproto.RemovexattrRequest, resp *newproto.RemovexattrResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	id := GetID(fsid.Bytes(), req.Inode, 0)
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &pb.InodeEntry{}
	err = Unmarshal(b, n)
	if err != nil {
		return err
	}
	delete(n.Xattr, req.Name)
	b, err = Marshal(n)
	if err != nil {
		return nil
	}
	err = o.WriteChunk(ctx, id, b)
	if err != nil {
		return err
	}
	return nil
}

func (o *OortFS) NewRename(ctx context.Context, req *newproto.RenameRequest, resp *newproto.RenameResponse) error {
	// TODO: Note that renames are not atomic!
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	fsidb := fsid.Bytes()
	oldParent := GetID(fsidb, req.OldParent, 0)
	newParent := GetID(fsidb, req.NewParent, 0)
	// Get the ID from the group list
	b, err := o.comms.ReadGroupItem(ctx, oldParent, []byte(req.OldName))
	if store.IsNotFound(err) {
		return nil // TODO: Is this correct or should it error?
	}
	if err != nil {
		return err
	}
	d := &pb.DirEntry{}
	err = Unmarshal(b, d)
	if err != nil {
		return err
	}
	// Be sure that old data is deleted
	// TODO: It would be better to create the tombstone for the delete, and only queue the delete if the are sure the new write happens
	_, err = o.Remove(ctx, newParent, req.NewName)
	if err != nil {
		return err
	}
	// Create new entry
	d.Name = req.NewName
	b, err = Marshal(d)
	err = o.comms.WriteGroup(ctx, newParent, []byte(req.NewName), b)
	if err != nil {
		return err
	}
	// Delete old entry
	err = o.comms.DeleteGroupItem(ctx, oldParent, []byte(req.OldName))
	if err != nil {
		// TODO: Handle errors
		// If we fail here then we will have two entries
		return err
	}
	return nil
}

func (o *OortFS) NewSetAttr(ctx context.Context, req *newproto.SetAttrRequest, resp *newproto.SetAttrResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	fsidb := fsid.Bytes()
	id := GetID(fsidb, req.Attr.Inode, 0)
	attr := req.Attr
	valid := fuse.SetattrValid(req.Valid)
	n, err := o.GetInode(ctx, id)
	if err != nil {
		return err
	}
	if valid.Mode() {
		n.Attr.Mode = attr.Mode
	}
	if valid.Size() {
		if attr.Size < n.Attr.Size {
			// We need to mark this file as dirty to clean up unused blocks
			tsm := brimtime.TimeToUnixMicro(time.Now())
			d := &pb.Dirty{
				Dtime:  tsm,
				Qtime:  tsm,
				FsId:   fsidb,
				Blocks: n.Blocks,
				Inode:  n.Inode,
			}
			b, err := Marshal(d)
			if err != nil {
				return err
			}
			err = o.comms.WriteGroupTS(ctx, GetDirtyID(fsidb), []byte(fmt.Sprintf("%d", d.Inode)), b, tsm)
			if err != nil {
				return err
			}
			o.dirtyChan <- &DirtyItem{dirty: d}
		}
		// TODO: creiht's pretty sure this should be if attr.Size == 0 or that
		// this block should be after the n.Attr.Size = attr.Size line. Leaving
		// it as it was for now; will come back to it later once I've read more
		// of the code.
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
	b, err := Marshal(n)
	if err != nil {
		return err
	}
	err = o.WriteChunk(ctx, id, b)
	if err != nil {
		return err
	}
	// TODO: Set everything explicitly for now since the structs are different
	// until the newproto becomes theproto.
	resp.Attr = &newproto.Attr{
		Inode:  n.Attr.Inode,
		Atime:  n.Attr.Atime,
		Mtime:  n.Attr.Mtime,
		Ctime:  n.Attr.Ctime,
		Crtime: n.Attr.Crtime,
		Mode:   n.Attr.Mode,
		Valid:  n.Attr.Valid,
		Size:   n.Attr.Size,
		Uid:    n.Attr.Uid,
		Gid:    n.Attr.Gid,
	}
	return nil
}

func (o *OortFS) NewSetxattr(ctx context.Context, req *newproto.SetxattrRequest, resp *newproto.SetxattrResponse) error {
	// NOTE: Setting xattrs is NOT concurrency safe!
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	fsidb := fsid.Bytes()
	id := GetID(fsidb, req.Inode, 0)
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &pb.InodeEntry{}
	err = Unmarshal(b, n)
	if err != nil {
		return err
	}
	if n.Xattr == nil {
		n.Xattr = make(map[string][]byte)
	}
	n.Xattr[req.Name] = req.Value
	b, err = Marshal(n)
	if err != nil {
		return err
	}
	err = o.WriteChunk(ctx, id, b)
	if err != nil {
		return err
	}
	return nil
}

func (o *OortFS) NewStatfs(ctx context.Context, req *newproto.StatfsRequest, resp *newproto.StatfsResponse) error {
	resp.Blocks = 281474976710656 // 1 exabyte (asuming 4K block size)
	resp.Bfree = 281474976710656
	resp.Bavail = 281474976710656
	resp.Files = 1000000000000 // 1 trillion inodes
	resp.Ffree = 1000000000000
	resp.Bsize = 4096 // it looked like ext4 used 4KB blocks
	resp.Namelen = 256
	resp.Frsize = 4096 // this should probably match Bsize so we don't allow fragmented blocks
	return nil
}

func (o *OortFS) NewSymlink(ctx context.Context, req *newproto.SymlinkRequest, resp *newproto.SymlinkResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	fsidb := fsid.Bytes()
	parent := GetID(fsidb, req.Parent, 0)
	inode := o.fl.GetID()
	id := GetID(fsidb, inode, 0)
	ts := time.Now().Unix()
	attr := &pb.Attr{
		Inode:  inode,
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   uint32(os.ModeSymlink | 0755),
		Size:   uint64(len(req.Target)),
		Uid:    req.Uid,
		Gid:    req.Gid,
	}
	// Check to see if the name exists
	val, err := o.comms.ReadGroupItem(ctx, parent, []byte(req.Name))
	if err != nil && !store.IsNotFound(err) {
		// TODO: Needs beter error handling
		return err
	}
	if len(val) > 1 { // Exists already
		return nil
	}
	n := &pb.InodeEntry{
		Version: InodeEntryVersion,
		Inode:   inode,
		IsDir:   false,
		IsLink:  true,
		Target:  req.Target,
		Attr:    attr,
	}
	b, err := Marshal(n)
	if err != nil {
		return err
	}
	err = o.WriteChunk(ctx, id, b)
	if err != nil {
		return err
	}
	// Add the name to the group
	d := &pb.DirEntry{
		Version: DirEntryVersion,
		Name:    req.Name,
		Id:      id,
		Type:    uint32(fuse.DT_File),
	}
	b, err = Marshal(d)
	if err != nil {
		return err
	}
	err = o.comms.WriteGroup(ctx, parent, []byte(req.Name), b)
	if err != nil {
		return err
	}
	// TODO: Set everything explicitly for now since the structs are different
	// until the newproto becomes theproto.
	resp.Attr = &newproto.Attr{
		Inode:  attr.Inode,
		Atime:  attr.Atime,
		Mtime:  attr.Mtime,
		Ctime:  attr.Ctime,
		Crtime: attr.Crtime,
		Mode:   attr.Mode,
		Valid:  attr.Valid,
		Size:   attr.Size,
		Uid:    attr.Uid,
		Gid:    attr.Gid,
	}
	return nil
}

func (o *OortFS) NewWrite(ctx context.Context, req *newproto.WriteRequest, resp *newproto.WriteResponse) error {
	fsid, err := GetFsId(ctx)
	if err != nil {
		return err
	}
	block := uint64(req.Offset / o.blocksize)
	firstOffset := int64(0)
	if req.Offset%o.blocksize != 0 {
		// Handle non-aligned offset
		firstOffset = req.Offset - int64(block)*o.blocksize
	}
	fsidb := fsid.Bytes()
	cur := int64(0)
	for cur < int64(len(req.Payload)) {
		sendSize := min(o.blocksize, int64(len(req.Payload))-cur)
		if sendSize+firstOffset > o.blocksize {
			sendSize = o.blocksize - firstOffset
		}
		payload := req.Payload[cur : cur+sendSize]
		id := GetID(fsidb, req.Inode, block+1) // 0 block is for inode data
		if firstOffset > 0 || sendSize < o.blocksize {
			// need to get the block and update
			chunk := make([]byte, firstOffset+int64(len(payload)))
			data, err := o.GetChunk(ctx, id)
			if firstOffset > 0 && err != nil {
				// TODO: How do we differentiate a block that hasn't been created yet, and a block that is truely missing?
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
		err := o.WriteChunk(ctx, id, payload)
		// TODO: Need better error handling for failing with multiple chunks
		if err != nil {
			return err
		}
		err = o.Update(
			ctx,
			GetID(fsidb, req.Inode, 0),
			block,
			uint64(o.blocksize),
			uint64(req.Offset+int64(len(req.Payload))),
			time.Now().Unix(),
		)
		if err != nil {
			return err
		}
		// TODO: Should we queue on error instead?
		//o.updateChan <- &UpdateItem{
		//	id:        GetID(fsidb, req.Inode, 0),
		//	block:     block,
		//	blocksize: uint64(o.blocksize),
		//	//size:      uint64(len(payload)),
		//	size:	uint64(req.Offset + int64(len(req.Payload))),
		//	mtime:     time.Now().Unix(),
		//}
		cur += sendSize
		block++
	}
	return nil
}

// Create ...
func (o *OortFS) Create(ctx context.Context, parent, id []byte, inode uint64, name string, attr *pb.Attr, isdir bool) (string, *pb.Attr, error) {
	// Check to see if the name already exists
	b, err := o.comms.ReadGroupItem(ctx, parent, []byte(name))
	if err != nil && !store.IsNotFound(err) {
		// TODO: Needs beter error handling
		return "", &pb.Attr{}, err
	}
	if len(b) > 0 {
		p := &pb.DirEntry{}
		err = Unmarshal(b, p)
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
	b, err = Marshal(d)
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
	b, err = Marshal(n)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	err = o.WriteChunk(ctx, id, b)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	return name, attr, nil
}

// Lookup ...
func (o *OortFS) Lookup(ctx context.Context, parent []byte, name string) (string, *pb.Attr, error) {
	// Get the id
	b, err := o.comms.ReadGroupItem(ctx, parent, []byte(name))
	if store.IsNotFound(err) {
		return "", &pb.Attr{}, ErrGRPCNotFound
	} else if err != nil {
		return "", &pb.Attr{}, err
	}
	d := &pb.DirEntry{}
	err = Unmarshal(b, d)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	// Get the Inode entry
	b, err = o.GetChunk(ctx, d.Id)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	n := &pb.InodeEntry{}
	err = Unmarshal(b, n)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	return d.Name, n.Attr, nil
}

// Needed to be able to sort the dirents

// ByDirent ...
type ByDirent []*newproto.DirEnt

func (d ByDirent) Len() int {
	return len(d)
}

func (d ByDirent) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d ByDirent) Less(i, j int) bool {
	return d[i].Name < d[j].Name
}

// Remove ...
func (o *OortFS) Remove(ctx context.Context, parent []byte, name string) (int32, error) {
	// Get the ID from the group list
	b, err := o.comms.ReadGroupItem(ctx, parent, []byte(name))
	if store.IsNotFound(err) {
		return 1, nil
	} else if err != nil {
		return 1, err
	}
	d := &pb.DirEntry{}
	err = Unmarshal(b, d)
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
		items, err := o.comms.ReadGroup(ctx, GetID(fsid.Bytes(), uint64(inode.Inode), 0))
		if err != nil {
			return 1, err
		}
		// return error if directory is not empty
		if len(items) > 0 {
			return 1, ErrNotEmpty
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
	b, err = Marshal(t)
	if err != nil {
		return 1, err
	}
	err = o.comms.WriteGroupTS(ctx, GetDeletedID(fsid.Bytes()), []byte(fmt.Sprintf("%d", t.Inode)), b, tsm)
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
	}
	return 0, nil
}

// Update ...
func (o *OortFS) Update(ctx context.Context, id []byte, block, blocksize, size uint64, mtime int64) error {
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &pb.InodeEntry{}
	err = Unmarshal(b, n)
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
	b, err = Marshal(n)
	if err != nil {
		return err
	}
	err = o.WriteChunk(ctx, id, b)
	if err != nil {
		return err
	}
	return nil
}

// GetChunk ...
func (o *OortFS) GetChunk(ctx context.Context, id []byte) ([]byte, error) {
	b, err := o.comms.ReadValue(ctx, id)
	if store.IsNotFound(err) {
		return nil, ErrFileNotFound
	}
	if err != nil {
		return nil, err
	}
	fb := &pb.FileBlock{}
	err = Unmarshal(b, fb)
	if err != nil {
		return nil, err
	}
	// TODO: Validate checksum and handle errors
	return fb.Data, nil
}

// WriteChunk ...
func (o *OortFS) WriteChunk(ctx context.Context, id, data []byte) error {
	crc := o.hasher()
	crc.Write(data)
	fb := &pb.FileBlock{
		Version:  FileBlockVersion,
		Data:     data,
		Checksum: crc.Sum32(),
	}
	b, err := Marshal(fb)
	if err != nil {
		return err
	}
	return o.comms.WriteValue(ctx, id, b)
}

// DeleteChunk ...
func (o *OortFS) DeleteChunk(ctx context.Context, id []byte, tsm int64) error {
	return o.comms.DeleteValueTS(ctx, id, tsm)
}

// DeleteListing ...
func (o *OortFS) DeleteListing(ctx context.Context, parent []byte, name string, tsm int64) error {
	return o.comms.DeleteGroupItemTS(ctx, parent, []byte(name), tsm)
}

// GetInode ...
func (o *OortFS) GetInode(ctx context.Context, id []byte) (*pb.InodeEntry, error) {
	// Get the Inode entry
	b, err := o.GetChunk(ctx, id)
	if err != nil {
		return nil, err
	}
	n := &pb.InodeEntry{}
	err = Unmarshal(b, n)
	if err != nil {
		return nil, err
	}
	return n, nil
}

// GetDirent ...
func (o *OortFS) GetDirent(ctx context.Context, parent []byte, name string) (*pb.DirEntry, error) {
	// Get the Dir Entry
	b, err := o.comms.ReadGroupItem(ctx, parent, []byte(name))
	if store.IsNotFound(err) {
		return &pb.DirEntry{}, nil
	} else if err != nil {
		return &pb.DirEntry{}, err
	}
	d := &pb.DirEntry{}
	err = Unmarshal(b, d)
	if err != nil {
		return &pb.DirEntry{}, err
	}
	return d, nil
}
