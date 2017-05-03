package server

import (
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"os"
	"sort"
	"strings"
	"time"

	"bazil.org/fuse"
	"github.com/getcfs/megacfs/flother"
	"github.com/getcfs/megacfs/formic/formicproto"
	"github.com/gholt/brimtime"
	"github.com/gholt/store"
	"github.com/satori/go.uuid"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc/peer"
)

const (
	inodeEntryVersion = 1
	dirEntryVersion   = 1
	fileBlockVersion  = 1
	maxRetries        = 10
)

var errStoreHasNewerValue = errors.New("Error store already has newer value")

type storeComms struct {
	vstore store.ValueStore
	gstore store.GroupStore
	log    *zap.Logger
}

func newStoreComms(vstore store.ValueStore, gstore store.GroupStore, logger *zap.Logger) (*storeComms, error) {
	return &storeComms{
		vstore: vstore,
		gstore: gstore,
		log:    logger,
	}, nil
}

// ReadValue ... Helper methods to get data from value and group store
func (o *storeComms) ReadValue(ctx context.Context, id []byte) ([]byte, error) {
	// TODO: You might want to make this whole area pass in reusable []byte to
	// lessen gc pressure.
	keyA, keyB := murmur3.Sum128(id)
	_, v, err := o.vstore.Read(ctx, keyA, keyB, nil)
	return v, err
}

// WriteValue ...
func (o *storeComms) WriteValue(ctx context.Context, id, data []byte) error {
	keyA, keyB := murmur3.Sum128(id)
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	oldTimestampMicro, err := o.vstore.Write(ctx, keyA, keyB, timestampMicro, data)
	// TODO: Should we be doing this? Seems like if "someone else" wrote a
	// newer value that we should consider that "we" lost and move on? I mean,
	// we don't do this retrying for deletes, or group writes and deletes, so
	// why here?
	retries := 0
	for (oldTimestampMicro >= timestampMicro) && (retries < maxRetries) {
		retries++
		timestampMicro = brimtime.TimeToUnixMicro(time.Now())
		oldTimestampMicro, err = o.vstore.Write(ctx, keyA, keyB, timestampMicro, data)
	}
	if err != nil {
		return err
	}
	if oldTimestampMicro >= timestampMicro {
		return errStoreHasNewerValue
	}
	return nil
}

// DeleteValue ...
func (o *storeComms) DeleteValue(ctx context.Context, id []byte) error {
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	return o.DeleteValueTS(ctx, id, timestampMicro)
}

// DeleteValueTS ...
func (o *storeComms) DeleteValueTS(ctx context.Context, id []byte, tsm int64) error {
	keyA, keyB := murmur3.Sum128(id)
	oldTimestampMicro, err := o.vstore.Delete(ctx, keyA, keyB, tsm)
	// TODO: Should this be: if err == nil && oldTimestampMicro >= tsm {
	if oldTimestampMicro >= tsm {
		return errStoreHasNewerValue
	}
	return err
}

// WriteGroup ...
func (o *storeComms) WriteGroup(ctx context.Context, key, childKey, value []byte) error {
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	return o.WriteGroupTS(ctx, key, childKey, value, timestampMicro)
}

// WriteGroupTS ...
func (o *storeComms) WriteGroupTS(ctx context.Context, key, childKey, value []byte, tsm int64) error {
	keyA, keyB := murmur3.Sum128(key)
	childKeyA, childKeyB := murmur3.Sum128(childKey)
	oldTimestampMicro, err := o.gstore.Write(ctx, keyA, keyB, childKeyA, childKeyB, tsm, value)
	if err != nil {
		// TODO: This seems weird; why not return the err?
		return nil
	}
	if oldTimestampMicro >= tsm {
		return errStoreHasNewerValue
	}
	return nil
}

// ReadGroupItem ...
func (o *storeComms) ReadGroupItem(ctx context.Context, key, childKey []byte) ([]byte, error) {
	childKeyA, childKeyB := murmur3.Sum128(childKey)
	return o.ReadGroupItemByKey(ctx, key, childKeyA, childKeyB)
}

// ReadGroupItemByKey ...
func (o *storeComms) ReadGroupItemByKey(ctx context.Context, key []byte, childKeyA, childKeyB uint64) ([]byte, error) {
	keyA, keyB := murmur3.Sum128(key)
	_, v, err := o.gstore.Read(ctx, keyA, keyB, childKeyA, childKeyB, nil)
	return v, err
}

// DeleteGroupItem ...
func (o *storeComms) DeleteGroupItem(ctx context.Context, key, childKey []byte) error {
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	return o.DeleteGroupItemTS(ctx, key, childKey, timestampMicro)
}

// DeleteGroupItemTS ...
func (o *storeComms) DeleteGroupItemTS(ctx context.Context, key, childKey []byte, tsm int64) error {
	keyA, keyB := murmur3.Sum128(key)
	childKeyA, childKeyB := murmur3.Sum128(childKey)
	oldTimestampMicro, err := o.gstore.Delete(ctx, keyA, keyB, childKeyA, childKeyB, tsm)
	if err != nil {
		return err
	}
	if oldTimestampMicro >= tsm {
		return errStoreHasNewerValue
	}
	return nil
}

// LookupGroup ...
func (o *storeComms) LookupGroup(ctx context.Context, key []byte) ([]store.LookupGroupItem, error) {
	keyA, keyB := murmur3.Sum128(key)
	items, err := o.gstore.LookupGroup(ctx, keyA, keyB)
	if err != nil {
		return nil, err
	}
	return items, nil
}

// ReadGroup ...
func (o *storeComms) ReadGroup(ctx context.Context, key []byte) ([]store.ReadGroupItem, error) {
	keyA, keyB := murmur3.Sum128(key)
	items, err := o.gstore.ReadGroup(ctx, keyA, keyB)
	if err != nil {
		return nil, err
	}
	return items, nil
}

type oortFS struct {
	hasher     func() hash.Hash32
	comms      *storeComms
	deleteChan chan *deleteItem
	dirtyChan  chan *dirtyItem
	log        *zap.Logger
	blocksize  int64
	fl         *flother.Flother
}

func newOortFS(comms *storeComms, logger *zap.Logger, deleteChan chan *deleteItem, dirtyChan chan *dirtyItem, blocksize int64, nodeID int) *oortFS {
	o := &oortFS{
		hasher:     crc32.NewIEEE,
		comms:      comms,
		log:        logger,
		deleteChan: deleteChan,
		dirtyChan:  dirtyChan,
		blocksize:  blocksize,
		fl:         flother.NewFlother(time.Time{}, uint64(nodeID), flother.DEFAULT_TIME_BITS, flother.DEFAULT_NODE_BITS),
	}
	return o
}

func (o *oortFS) Check(ctx context.Context, req *formicproto.CheckRequest, resp *formicproto.CheckResponse, fsid string) error {
	b, err := o.comms.ReadGroupItem(ctx, getID(fsid, req.ParentINode, 0), []byte(req.ChildName))
	if store.IsNotFound(err) {
		resp.Response = "not found"
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to load directory entry: %s", err)
	}
	dirEnt := &formicproto.DirEntry{}
	if err = unmarshal(b, dirEnt); err != nil {
		// TODO: We should remove this dirEnt here shouldn't we?
		return fmt.Errorf("corrupted directory entry: %s", err)
	}
	// Read the inode block
	_, err = o.getINode(ctx, dirEnt.ID)
	if err != nil {
		// Note: Unfortunately if we lose the inode block, there is no way to
		// recover because we do not know the inode of the file itself.
		// Delete the entry.
		err = o.comms.DeleteGroupItemTS(ctx, getID(fsid, req.ParentINode, 0), []byte(req.ChildName), brimtime.TimeToUnixMicro(time.Now()))
		if err != nil {
			return fmt.Errorf("Error: INode not found but could not delete the listing: %s", err)
		}
		resp.Response = "INode could not be found, and dir entry removed."
	}
	// If we get here then everything is fine
	resp.Response = "No issues found."
	return nil
}

func (o *oortFS) CreateFS(ctx context.Context, req *formicproto.CreateFSRequest, resp *formicproto.CreateFSResponse, aid string) error {
	// TODO: We need to record file system creation requests; not through the
	// standard logs, those are for true errors, but through some other
	// mechanism.
	fsid := uuid.NewV4().String()
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	// Make the /account/<aid>/<fsid> entry.
	byts, err := marshal(&formicproto.MetaAccount2Filesystem{FSID: fsid})
	if err != nil {
		return err
	}
	keyA, keyB := murmur3.Sum128([]byte(fmt.Sprintf("/account/%s", aid)))
	fsidKeyA, fsidKeyB := murmur3.Sum128([]byte(fsid))
	_, err = o.comms.gstore.Write(ctx, keyA, keyB, fsidKeyA, fsidKeyB, timestampMicro, byts)
	if err != nil {
		return err
	}
	// Make the /filesystem/<fsid> entry.
	byts, err = marshal(&formicproto.MetaFilesystemEntry{AID: aid, FSID: fsid, Name: req.Name})
	if err != nil {
		return err
	}
	keyA, keyB = murmur3.Sum128([]byte("/filesystem"))
	_, err = o.comms.gstore.Write(ctx, keyA, keyB, fsidKeyA, fsidKeyB, timestampMicro, byts)
	if err != nil {
		return err
	}
	// Make root inode entry.
	rootInodeKeyA, rootInodeKeyB := murmur3.Sum128(getID(fsid, 1, 0))
	_, value, err := o.comms.vstore.Read(ctx, rootInodeKeyA, rootInodeKeyB, nil)
	if !store.IsNotFound(err) {
		if len(value) != 0 {
			return err
		}
		return err
	}
	nr := &formicproto.INodeEntry{
		Version: inodeEntryVersion,
		INode:   1,
		IsDir:   true,
		FSID:    []byte(fsid),
	}
	ts := time.Now().Unix()
	nr.Attr = &formicproto.Attr{
		INode: 1,
		// TODO: Should we just get rid of ATime since we won't ever support it?
		ATime:  ts,
		MTime:  ts,
		CTime:  ts,
		CrTime: ts,
		Mode:   uint32(os.ModeDir | 0775),
		UID:    1001, // TODO: need to config default user/group id
		GID:    1001,
	}
	data, err := marshal(nr)
	if err != nil {
		return err
	}
	crc := crc32.NewIEEE()
	crc.Write(data)
	fb := &formicproto.FileBlock{
		Version:  fileBlockVersion,
		Data:     data,
		Checksum: crc.Sum32(),
	}
	blkdata, err := marshal(fb)
	if err != nil {
		return err
	}
	_, err = o.comms.vstore.Write(ctx, rootInodeKeyA, rootInodeKeyB, timestampMicro, blkdata)
	if err != nil {
		return err
	}
	resp.FSID = fsid
	return nil
}

func (o *oortFS) Create(ctx context.Context, req *formicproto.CreateRequest, resp *formicproto.CreateResponse, fsid string) error {
	ts := time.Now().Unix()
	inode := o.fl.NewID()
	attr := &formicproto.Attr{
		INode:  inode,
		ATime:  ts,
		MTime:  ts,
		CTime:  ts,
		CrTime: ts,
		Mode:   req.ChildAttr.Mode,
		UID:    req.ChildAttr.UID,
		GID:    req.ChildAttr.GID,
	}
	var err error
	resp.ChildAttr, err = o.create(ctx, getID(fsid, req.ParentINode, 0), getID(fsid, inode, 0), inode, req.ChildName, attr, false)
	if err != nil {
		return err
	}
	return nil
}

func (o *oortFS) DeleteFS(ctx context.Context, req *formicproto.DeleteFSRequest, resp *formicproto.DeleteFSResponse, aid string) error {
	// TODO: We need to record file system deletion requests; not through the
	// standard logs, those are for true errors, but through some other
	// mechanism.
	var err error
	var value []byte
	var metaFileSystemEntry formicproto.MetaFilesystemEntry
	// Validate aid owns this file system.
	pKeyA, pKeyB := murmur3.Sum128([]byte("/filesystem"))
	cKeyA, cKeyB := murmur3.Sum128([]byte(req.FSID))
	_, value, err = o.comms.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if err != nil {
		return err
	}
	err = unmarshal(value, &metaFileSystemEntry)
	if err != nil {
		return err
	}
	if metaFileSystemEntry.AID != aid {
		return errors.New("permission denied")
	}
	// Test if file system is empty. TODO: There is a race here that we need to solve.
	keyA, keyB := murmur3.Sum128(getID(req.FSID, 1, 0))
	items, err := o.comms.gstore.ReadGroup(context.Background(), keyA, keyB)
	if len(items) != 0 {
		return errors.New("file system not empty")
	}
	// Remove the root inode entry from the value store.
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	_, err = o.comms.vstore.Delete(context.Background(), keyA, keyB, timestampMicro)
	if err != nil {
		if !store.IsNotFound(err) {
			return err
		}
	}
	// NOTE: Once we've removed the root inode entry, any future errors should
	//       just be logged and the client can consider the filesytem deleted.
	//
	// Remove all filesystem addresses.
	pKey := fmt.Sprintf("/filesystem/%s/address", req.FSID)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	metaFilesystem2AddressEncs, err := o.comms.gstore.ReadGroup(context.Background(), pKeyA, pKeyB)
	if !store.IsNotFound(err) {
		for _, metaFilesystem2AddressEnc := range metaFilesystem2AddressEncs {
			var metaFilesystem2Address formicproto.MetaFilesystem2Address
			err = unmarshal(metaFilesystem2AddressEnc.Value, &metaFilesystem2Address)
			if err != nil {
				o.log.Error("removing MetaFilesystem2Address", zap.Error(err), zap.String("fsid", req.FSID))
				continue
			}
			err = o.deleteEntry(pKey, metaFilesystem2Address.Address)
			if err != nil {
				o.log.Error("removing MetaFilesystem2Address", zap.Error(err), zap.String("fsid", req.FSID), zap.String("address", metaFilesystem2Address.Address))
				continue
			}
		}
	}
	// Remove filesystem entry.
	err = o.deleteEntry("/filesystem", req.FSID)
	if err != nil {
		o.log.Error("removing MetaFilesystemEntry", zap.Error(err), zap.String("fsid", req.FSID))
	}
	// Remove account2filesystem reference.
	err = o.deleteEntry(fmt.Sprintf("/account/%s", aid), req.FSID)
	if err != nil {
		o.log.Error("removing MetaAccount2Filesystem", zap.Error(err), zap.String("aid", aid), zap.String("fsid", req.FSID))
	}
	return nil
}

func (o *oortFS) GetAttr(ctx context.Context, req *formicproto.GetAttrRequest, resp *formicproto.GetAttrResponse, fsid string) error {
	b, err := o.getChunk(ctx, getID(fsid, req.INode, 0))
	if err != nil {
		return err
	}
	n := &formicproto.INodeEntry{}
	err = unmarshal(b, n)
	if err != nil {
		return err
	}
	resp.Attr = n.Attr
	return nil
}

func (o *oortFS) GetXAttr(ctx context.Context, req *formicproto.GetXAttrRequest, resp *formicproto.GetXAttrResponse, fsid string) error {
	id := getID(fsid, req.INode, 0)
	b, err := o.getChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &formicproto.INodeEntry{}
	err = unmarshal(b, n)
	if err != nil {
		return err
	}
	resp.XAttr = n.XAttr[req.Name]
	return nil
}

func (o *oortFS) GrantAddressFS(ctx context.Context, req *formicproto.GrantAddressFSRequest, resp *formicproto.GrantAddressFSResponse, aid string) error {
	// TODO: We need to record file system grant requests; not through the
	// standard logs, those are for true errors, but through some other
	// mechanism.
	//
	// Ensure filesystem exists and the account matches.
	pKeyA, pKeyB := murmur3.Sum128([]byte(fmt.Sprintf("/filesystem")))
	cKeyA, cKeyB := murmur3.Sum128([]byte(req.FSID))
	_, value, err := o.comms.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if err != nil {
		return err
	}
	var metaFilesystemEntry formicproto.MetaFilesystemEntry
	err = unmarshal(value, &metaFilesystemEntry)
	if err != nil {
		return err
	}
	if metaFilesystemEntry.AID != aid {
		return errors.New("permission denied")
	}
	grantAddress := req.Address
	if grantAddress == "" {
		if pr, ok := peer.FromContext(ctx); ok {
			grantAddress = strings.Split(pr.Addr.String(), ":")[0]
		}
	}
	byts, err := marshal(&formicproto.MetaFilesystem2Address{Address: grantAddress})
	if err != nil {
		return err
	}
	pKeyA, pKeyB = murmur3.Sum128([]byte(fmt.Sprintf("/filesystem/%s/address", req.FSID)))
	cKeyA, cKeyB = murmur3.Sum128([]byte(grantAddress))
	_, err = o.comms.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, brimtime.TimeToUnixMicro(time.Now()), byts)
	return err
}

func (o *oortFS) InitFS(ctx context.Context, req *formicproto.InitFSRequest, resp *formicproto.InitFSResponse, fsid string) error {
	id := getID(fsid, 1, 0)
	n, _ := o.getChunk(ctx, id)
	if len(n) == 0 {
		return errors.New("root entry does not exist")
	}
	return nil
}

func (o *oortFS) ListFS(ctx context.Context, req *formicproto.ListFSRequest, resp *formicproto.ListFSResponse, aid string) error {
	var value []byte
	keyA, keyB := murmur3.Sum128([]byte(fmt.Sprintf("/account/%s", aid)))
	metaAccount2FilesystemEncs, err := o.comms.gstore.ReadGroup(ctx, keyA, keyB)
	if err != nil {
		return err
	}
	resp.List = make([]*formicproto.FSIDName, 0, len(metaAccount2FilesystemEncs))
	var lastErr error
	for _, metaAccount2FilesystemEnc := range metaAccount2FilesystemEncs {
		var metaAccount2Filesystem formicproto.MetaAccount2Filesystem
		err = unmarshal(metaAccount2FilesystemEnc.Value, &metaAccount2Filesystem)
		if err != nil {
			lastErr = err
			continue
		}
		pKeyA, pKeyB := murmur3.Sum128([]byte("/filesystem"))
		cKeyA, cKeyB := murmur3.Sum128([]byte(metaAccount2Filesystem.FSID))
		_, value, err = o.comms.gstore.Read(ctx, pKeyA, pKeyB, cKeyA, cKeyB, nil)
		if err != nil {
			lastErr = err
			continue
		}
		var metaFilesystemEntry formicproto.MetaFilesystemEntry
		err = unmarshal(value, &metaFilesystemEntry)
		if err != nil {
			lastErr = err
			continue
		}
		resp.List = append(resp.List, &formicproto.FSIDName{
			FSID: metaFilesystemEntry.FSID,
			Name: metaFilesystemEntry.Name,
		})
	}
	return lastErr
}

func (o *oortFS) ListXAttr(ctx context.Context, req *formicproto.ListXAttrRequest, resp *formicproto.ListXAttrResponse, fsid string) error {
	id := getID(fsid, req.INode, 0)
	b, err := o.getChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &formicproto.INodeEntry{}
	err = unmarshal(b, n)
	if err != nil {
		return err
	}
	names := ""
	for name := range n.XAttr {
		names += name
		names += "\x00"
	}
	names += "cfs.fsid\x00"
	resp.XAttr = []byte(names)
	return nil
}

func (o *oortFS) Lookup(ctx context.Context, req *formicproto.LookupRequest, resp *formicproto.LookupResponse, fsid string) error {
	var err error
	parent := getID(fsid, req.Parent, 0)
	b, err := o.comms.ReadGroupItem(ctx, parent, []byte(req.Name))
	if err != nil {
		return err
	}
	d := &formicproto.DirEntry{}
	err = unmarshal(b, d)
	if err != nil {
		return err
	}
	b, err = o.getChunk(ctx, d.ID)
	if err != nil {
		return err
	}
	n := &formicproto.INodeEntry{}
	err = unmarshal(b, n)
	if err != nil {
		return err
	}
	resp.Attr = n.Attr
	return nil
}

func (o *oortFS) ReadDirAll(ctx context.Context, req *formicproto.ReadDirAllRequest, resp *formicproto.ReadDirAllResponse, fsid string) error {
	id := getID(fsid, req.INode, 0)
	// Get the keys from the group
	items, err := o.comms.ReadGroup(ctx, id)
	if err != nil {
		return err
	}
	// Iterate over each item, getting the ID then the INode Entry
	// TODO: I think the above comment is out of date.
	de := &formicproto.DirEntry{}
	for _, item := range items {
		err = unmarshal(item.Value, de)
		if err != nil {
			return err
		}
		resp.Ents = append(resp.Ents, &formicproto.ReadDirAllEnt{Name: de.Name, Type: de.Type})
	}
	sort.Sort(byReadDirAllEnt(resp.Ents))
	return nil
}

func (o *oortFS) ReadLink(ctx context.Context, req *formicproto.ReadLinkRequest, resp *formicproto.ReadLinkResponse, fsid string) error {
	id := getID(fsid, req.INode, 0)
	b, err := o.getChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &formicproto.INodeEntry{}
	err = unmarshal(b, n)
	if err != nil {
		return err
	}
	resp.Target = n.Target
	return nil
}

func (o *oortFS) MkDir(ctx context.Context, req *formicproto.MkDirRequest, resp *formicproto.MkDirResponse, fsid string) error {
	ts := time.Now().Unix()
	inode := o.fl.NewID()
	attr := &formicproto.Attr{
		INode:  inode,
		ATime:  ts,
		MTime:  ts,
		CTime:  ts,
		CrTime: ts,
		Mode:   uint32(os.ModeDir) | req.Attr.Mode,
		UID:    req.Attr.UID,
		GID:    req.Attr.GID,
	}
	var err error
	resp.Attr, err = o.create(ctx, getID(fsid, req.Parent, 0), getID(fsid, inode, 0), inode, req.Name, attr, true)
	if err != nil {
		return err
	}
	return nil
}

func (o *oortFS) Read(ctx context.Context, req *formicproto.ReadRequest, resp *formicproto.ReadResponse, fsid string) error {
	block := uint64(req.Offset / o.blocksize)
	resp.Payload = make([]byte, 0, req.Size)
	firstOffset := int64(0)
	if req.Offset%o.blocksize != 0 {
		// Handle non-aligned offset
		firstOffset = req.Offset - int64(block)*o.blocksize
	}
	cur := int64(0)
	for cur < req.Size {
		id := getID(fsid, req.INode, block+1) // block 0 is for inode data
		chunk, err := o.getChunk(ctx, id)
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

func (o *oortFS) Remove(ctx context.Context, req *formicproto.RemoveRequest, resp *formicproto.RemoveResponse, fsid string) error {
	// TODO: No need for this status thing; can refactor to just return errors.
	status, err := o.remove(ctx, fsid, getID(fsid, req.Parent, 0), req.Name)
	if err != nil {
		return err
	}
	if status != 0 {
		return fmt.Errorf("Remove failed with status %d", status)
	}
	return nil
}

func (o *oortFS) RemoveXAttr(ctx context.Context, req *formicproto.RemoveXAttrRequest, resp *formicproto.RemoveXAttrResponse, fsid string) error {
	id := getID(fsid, req.INode, 0)
	b, err := o.getChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &formicproto.INodeEntry{}
	err = unmarshal(b, n)
	if err != nil {
		return err
	}
	delete(n.XAttr, req.Name)
	b, err = marshal(n)
	if err != nil {
		return nil
	}
	err = o.writeChunk(ctx, id, b)
	if err != nil {
		return err
	}
	return nil
}

func (o *oortFS) Rename(ctx context.Context, req *formicproto.RenameRequest, resp *formicproto.RenameResponse, fsid string) error {
	// TODO: Note that renames are not atomic!
	oldParent := getID(fsid, req.OldParent, 0)
	newParent := getID(fsid, req.NewParent, 0)
	// Get the ID from the group list
	b, err := o.comms.ReadGroupItem(ctx, oldParent, []byte(req.OldName))
	if store.IsNotFound(err) {
		return nil // TODO: Is this correct or should it error?
	}
	if err != nil {
		return err
	}
	d := &formicproto.DirEntry{}
	err = unmarshal(b, d)
	if err != nil {
		return err
	}
	// Be sure that old data is deleted
	// TODO: It would be better to create the tombstone for the delete, and only queue the delete if the are sure the new write happens
	_, err = o.remove(ctx, fsid, newParent, req.NewName)
	if err != nil {
		return err
	}
	// Create new entry
	d.Name = req.NewName
	b, err = marshal(d)
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

func (o *oortFS) RevokeAddressFS(ctx context.Context, req *formicproto.RevokeAddressFSRequest, resp *formicproto.RevokeAddressFSResponse, aid string) error {
	// TODO: We need to record file system revoke requests; not through the
	// standard logs, those are for true errors, but through some other
	// mechanism.
	//
	// Ensure filesystem exists and the account matches.
	pKeyA, pKeyB := murmur3.Sum128([]byte(fmt.Sprintf("/filesystem")))
	cKeyA, cKeyB := murmur3.Sum128([]byte(req.FSID))
	_, value, err := o.comms.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if err != nil {
		return err
	}
	var metaFilesystemEntry formicproto.MetaFilesystemEntry
	err = unmarshal(value, &metaFilesystemEntry)
	if err != nil {
		return err
	}
	if metaFilesystemEntry.AID != aid {
		return errors.New("permission denied")
	}
	revokeAddress := req.Address
	if revokeAddress == "" {
		if pr, ok := peer.FromContext(ctx); ok {
			revokeAddress = strings.Split(pr.Addr.String(), ":")[0]
		}
	}
	pKeyA, pKeyB = murmur3.Sum128([]byte(fmt.Sprintf("/filesystem/%s/address", req.FSID)))
	cKeyA, cKeyB = murmur3.Sum128([]byte(revokeAddress))
	_, err = o.comms.gstore.Delete(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, brimtime.TimeToUnixMicro(time.Now()))
	return err
}

func (o *oortFS) SetAttr(ctx context.Context, req *formicproto.SetAttrRequest, resp *formicproto.SetAttrResponse, fsid string) error {
	id := getID(fsid, req.Attr.INode, 0)
	attr := req.Attr
	valid := fuse.SetattrValid(req.Valid)
	n, err := o.getINode(ctx, id)
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
			d := &formicproto.Dirty{
				DTime:  tsm,
				QTime:  tsm,
				FSID:   fsid,
				Blocks: n.Blocks,
				INode:  n.INode,
			}
			b, err := marshal(d)
			if err != nil {
				return err
			}
			err = o.comms.WriteGroupTS(ctx, getDirtyID(fsid), []byte(fmt.Sprintf("%d", d.INode)), b, tsm)
			if err != nil {
				return err
			}
			o.dirtyChan <- &dirtyItem{dirty: d}
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
		n.Attr.MTime = attr.MTime
	}
	if valid.Atime() {
		n.Attr.ATime = attr.ATime
	}
	if valid.Uid() {
		n.Attr.UID = attr.UID
	}
	if valid.Gid() {
		n.Attr.GID = attr.GID
	}
	b, err := marshal(n)
	if err != nil {
		return err
	}
	err = o.writeChunk(ctx, id, b)
	if err != nil {
		return err
	}
	resp.Attr = n.Attr
	return nil
}

func (o *oortFS) SetXAttr(ctx context.Context, req *formicproto.SetXAttrRequest, resp *formicproto.SetXAttrResponse, fsid string) error {
	// NOTE: Setting xattrs is NOT concurrency safe!
	id := getID(fsid, req.INode, 0)
	b, err := o.getChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &formicproto.INodeEntry{}
	err = unmarshal(b, n)
	if err != nil {
		return err
	}
	if n.XAttr == nil {
		n.XAttr = make(map[string][]byte)
	}
	n.XAttr[req.Name] = req.Value
	b, err = marshal(n)
	if err != nil {
		return err
	}
	err = o.writeChunk(ctx, id, b)
	if err != nil {
		return err
	}
	return nil
}

func (o *oortFS) ShowFS(ctx context.Context, req *formicproto.ShowFSRequest, resp *formicproto.ShowFSResponse, aid string) error {
	// Ensure filesystem exists and the account matches.
	pKeyA, pKeyB := murmur3.Sum128([]byte("/filesystem"))
	cKeyA, cKeyB := murmur3.Sum128([]byte(req.FSID))
	_, value, err := o.comms.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if err != nil {
		return err
	}
	var metaFilesystemEntry formicproto.MetaFilesystemEntry
	err = unmarshal(value, &metaFilesystemEntry)
	if err != nil {
		return err
	}
	if metaFilesystemEntry.AID != aid {
		return errors.New("permission denied")
	}
	resp.Name = metaFilesystemEntry.Name
	// Retrieve address information.
	pKeyA, pKeyB = murmur3.Sum128([]byte(fmt.Sprintf("/filesystem/%s/address", req.FSID)))
	metaFilesystem2AddressEncs, lastErr := o.comms.gstore.ReadGroup(context.Background(), pKeyA, pKeyB)
	if !store.IsNotFound(err) {
		resp.Addresses = make([]string, 0, len(metaFilesystem2AddressEncs))
		for _, metaFilesystem2AddressEnc := range metaFilesystem2AddressEncs {
			var metaFilesystem2Address formicproto.MetaFilesystem2Address
			err = unmarshal(metaFilesystem2AddressEnc.Value, &metaFilesystem2Address)
			if err != nil {
				lastErr = err
				continue
			}
			resp.Addresses = append(resp.Addresses, metaFilesystem2Address.Address)
		}
	}
	return lastErr
}

func (o *oortFS) StatFS(ctx context.Context, req *formicproto.StatFSRequest, resp *formicproto.StatFSResponse, fsid string) error {
	resp.Blocks = 281474976710656 // 1 exabyte (asuming 4K block size)
	resp.BFree = 281474976710656
	resp.BAvail = 281474976710656
	resp.Files = 1000000000000 // 1 trillion inodes
	resp.FFree = 1000000000000
	resp.BSize = 4096 // it looked like ext4 used 4KB blocks
	resp.NameLen = 256
	resp.FrSize = 4096 // this should probably match Bsize so we don't allow fragmented blocks
	return nil
}

func (o *oortFS) SymLink(ctx context.Context, req *formicproto.SymLinkRequest, resp *formicproto.SymLinkResponse, fsid string) error {
	parent := getID(fsid, req.Parent, 0)
	inode := o.fl.NewID()
	id := getID(fsid, inode, 0)
	ts := time.Now().Unix()
	attr := &formicproto.Attr{
		INode:  inode,
		ATime:  ts,
		MTime:  ts,
		CTime:  ts,
		CrTime: ts,
		Mode:   uint32(os.ModeSymlink | 0755),
		Size:   uint64(len(req.Target)),
		UID:    req.UID,
		GID:    req.GID,
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
	n := &formicproto.INodeEntry{
		Version: inodeEntryVersion,
		INode:   inode,
		IsDir:   false,
		IsLink:  true,
		Target:  req.Target,
		Attr:    attr,
	}
	b, err := marshal(n)
	if err != nil {
		return err
	}
	err = o.writeChunk(ctx, id, b)
	if err != nil {
		return err
	}
	// Add the name to the group
	d := &formicproto.DirEntry{
		Version: dirEntryVersion,
		Name:    req.Name,
		ID:      id,
		Type:    uint32(fuse.DT_File),
	}
	b, err = marshal(d)
	if err != nil {
		return err
	}
	err = o.comms.WriteGroup(ctx, parent, []byte(req.Name), b)
	if err != nil {
		return err
	}
	resp.Attr = attr
	return nil
}

func (o *oortFS) UpdateFS(ctx context.Context, req *formicproto.UpdateFSRequest, resp *formicproto.UpdateFSResponse, aid string) error {
	// TODO: We need to record file system revoke requests; not through the
	// standard logs, those are for true errors, but through some other
	// mechanism.
	if req.NewName == "" {
		return errors.New("file system name cannot be empty")
	}
	// Ensure filesystem exists and the account matches.
	pKeyA, pKeyB := murmur3.Sum128([]byte("/filesystem"))
	cKeyA, cKeyB := murmur3.Sum128([]byte(req.FSID))
	_, value, err := o.comms.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if err != nil {
		return err
	}
	var metaFilesystemEntry formicproto.MetaFilesystemEntry
	err = unmarshal(value, &metaFilesystemEntry)
	if err != nil {
		return err
	}
	if metaFilesystemEntry.AID != aid {
		return errors.New("permission denied")
	}
	metaFilesystemEntry.Name = req.NewName
	byts, err := marshal(&metaFilesystemEntry)
	if err != nil {
		return err
	}
	_, err = o.comms.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, brimtime.TimeToUnixMicro(time.Now()), byts)
	return err
}

func (o *oortFS) Write(ctx context.Context, req *formicproto.WriteRequest, resp *formicproto.WriteResponse, fsid string) error {
	block := uint64(req.Offset / o.blocksize)
	firstOffset := int64(0)
	if req.Offset%o.blocksize != 0 {
		// Handle non-aligned offset
		firstOffset = req.Offset - int64(block)*o.blocksize
	}
	cur := int64(0)
	for cur < int64(len(req.Payload)) {
		sendSize := int64(len(req.Payload)) - cur
		if o.blocksize < sendSize {
			sendSize = o.blocksize
		}
		if sendSize+firstOffset > o.blocksize {
			sendSize = o.blocksize - firstOffset
		}
		payload := req.Payload[cur : cur+sendSize]
		id := getID(fsid, req.INode, block+1) // 0 block is for inode data
		if firstOffset > 0 || sendSize < o.blocksize {
			// need to get the block and update
			chunk := make([]byte, firstOffset+int64(len(payload)))
			data, err := o.getChunk(ctx, id)
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
		err := o.writeChunk(ctx, id, payload)
		// TODO: Need better error handling for failing with multiple chunks
		if err != nil {
			return err
		}
		err = o.update(
			ctx,
			getID(fsid, req.INode, 0),
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
		//	id:        getID(fsid, req.INode, 0),
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

func (o *oortFS) create(ctx context.Context, parent, id []byte, inode uint64, name string, attr *formicproto.Attr, isdir bool) (*formicproto.Attr, error) {
	// Check to see if the name already exists, TODO: Are races handled?
	b, err := o.comms.ReadGroupItem(ctx, parent, []byte(name))
	if err != nil && !store.IsNotFound(err) {
		// TODO: Needs better error handling
		return &formicproto.Attr{}, err
	}
	if len(b) > 0 {
		p := &formicproto.DirEntry{}
		err = unmarshal(b, p)
		if err != nil {
			return &formicproto.Attr{}, err
		}
		return &formicproto.Attr{}, errors.New("already exists")
	}
	var dirEntType fuse.DirentType
	if isdir {
		dirEntType = fuse.DT_Dir
	} else {
		dirEntType = fuse.DT_File
	}
	// Add the name to the group
	d := &formicproto.DirEntry{
		Version: dirEntryVersion,
		Name:    name,
		ID:      id,
		Type:    uint32(dirEntType),
	}
	b, err = marshal(d)
	if err != nil {
		return &formicproto.Attr{}, err
	}
	err = o.comms.WriteGroup(ctx, parent, []byte(name), b)
	if err != nil {
		return &formicproto.Attr{}, err
	}
	// Add the inode entry
	n := &formicproto.INodeEntry{
		Version: inodeEntryVersion,
		INode:   inode,
		IsDir:   isdir,
		Attr:    attr,
		Blocks:  0,
	}
	b, err = marshal(n)
	if err != nil {
		return &formicproto.Attr{}, err
	}
	err = o.writeChunk(ctx, id, b)
	if err != nil {
		return &formicproto.Attr{}, err
	}
	return attr, nil
}

// Needed to be able to sort the dirEnts

type byReadDirAllEnt []*formicproto.ReadDirAllEnt

func (d byReadDirAllEnt) Len() int {
	return len(d)
}

func (d byReadDirAllEnt) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d byReadDirAllEnt) Less(i, j int) bool {
	return d[i].Name < d[j].Name
}

func (o *oortFS) remove(ctx context.Context, fsid string, parent []byte, name string) (int32, error) {
	// Get the ID from the group list
	b, err := o.comms.ReadGroupItem(ctx, parent, []byte(name))
	if store.IsNotFound(err) {
		return 1, nil
	} else if err != nil {
		return 1, err
	}
	d := &formicproto.DirEntry{}
	err = unmarshal(b, d)
	if err != nil {
		return 1, err
	}
	if fuse.DirentType(d.Type) == fuse.DT_Dir {
		inode, err := o.getINode(ctx, d.ID)
		if err != nil {
			return 1, err
		}
		items, err := o.comms.ReadGroup(ctx, getID(fsid, uint64(inode.INode), 0))
		if err != nil {
			return 1, err
		}
		// return error if directory is not empty
		if len(items) > 0 {
			return 1, errors.New("not empty")
		}
	}
	// TODO: More error handling needed
	// TODO: Handle possible race conditions where user writes and deletes the same file over and over
	t := &formicproto.Tombstone{}
	tsm := brimtime.TimeToUnixMicro(time.Now())
	t.DTime = tsm
	t.QTime = tsm
	t.FSID = fsid
	inode, err := o.getINode(ctx, d.ID)
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
	t.INode = inode.INode
	// Write the Tombstone to the delete listing for the fsid
	b, err = marshal(t)
	if err != nil {
		return 1, err
	}
	err = o.comms.WriteGroupTS(ctx, getDeletedID(fsid), []byte(fmt.Sprintf("%d", t.INode)), b, tsm)
	if err != nil {
		return 1, err
	}
	o.deleteChan <- &deleteItem{
		ts: t,
	}
	// Delete the original from the listing
	err = o.comms.DeleteGroupItemTS(ctx, parent, []byte(name), tsm)
	if err != nil {
		return 1, err
	}
	return 0, nil
}

func (o *oortFS) update(ctx context.Context, id []byte, block, blocksize, size uint64, mtime int64) error {
	b, err := o.getChunk(ctx, id)
	if err != nil {
		return err
	}
	n := &formicproto.INodeEntry{}
	err = unmarshal(b, n)
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
	if mtime > n.Attr.MTime {
		n.Attr.MTime = mtime
	}
	b, err = marshal(n)
	if err != nil {
		return err
	}
	err = o.writeChunk(ctx, id, b)
	if err != nil {
		return err
	}
	return nil
}

// getChunk ...
func (o *oortFS) getChunk(ctx context.Context, id []byte) ([]byte, error) {
	b, err := o.comms.ReadValue(ctx, id)
	if store.IsNotFound(err) {
		return nil, errors.New("not found")
	}
	if err != nil {
		return nil, err
	}
	fb := &formicproto.FileBlock{}
	err = unmarshal(b, fb)
	if err != nil {
		return nil, err
	}
	// TODO: Validate checksum and handle errors
	return fb.Data, nil
}

// writeChunk ...
func (o *oortFS) writeChunk(ctx context.Context, id, data []byte) error {
	crc := o.hasher()
	crc.Write(data)
	fb := &formicproto.FileBlock{
		Version:  fileBlockVersion,
		Data:     data,
		Checksum: crc.Sum32(),
	}
	b, err := marshal(fb)
	if err != nil {
		return err
	}
	return o.comms.WriteValue(ctx, id, b)
}

// deleteChunk ...
func (o *oortFS) deleteChunk(ctx context.Context, id []byte, tsm int64) error {
	return o.comms.DeleteValueTS(ctx, id, tsm)
}

// getINode ...
func (o *oortFS) getINode(ctx context.Context, id []byte) (*formicproto.INodeEntry, error) {
	// Get the INode entry
	b, err := o.getChunk(ctx, id)
	if err != nil {
		return nil, err
	}
	n := &formicproto.INodeEntry{}
	err = unmarshal(b, n)
	if err != nil {
		return nil, err
	}
	return n, nil
}

// deleteEntry Deletes and entry in the group store and doesn't care if
// its not Found
func (o *oortFS) deleteEntry(pKey string, cKey string) error {
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(cKey))
	_, err := o.comms.gstore.Delete(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro)
	if store.IsNotFound(err) || err == nil {
		return nil
	}
	return err
}
