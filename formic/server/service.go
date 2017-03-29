package server

import (
	"encoding/json"
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
		fl:         flother.NewFlother(time.Time{}, uint64(nodeID)),
	}
	return o
}

func (o *oortFS) Check(ctx context.Context, req *formicproto.CheckRequest, resp *formicproto.CheckResponse, fsid string) error {
	b, err := o.comms.ReadGroupItem(ctx, getID(fsid, req.INode, 0), []byte(req.Name))
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
		err = o.comms.DeleteGroupItemTS(ctx, getID(fsid, req.INode, 0), []byte(req.Name), brimtime.TimeToUnixMicro(time.Now()))
		if err != nil {
			return fmt.Errorf("Error: INode not found but could not delete the listing: %s", err)
		}
		resp.Response = "INode could not be found, and dir entry removed."
	}
	// If we get here then everything is fine
	resp.Response = "No issues found."
	return nil
}

func (o *oortFS) CreateFS(ctx context.Context, req *formicproto.CreateFSRequest, resp *formicproto.CreateFSResponse, acctID string) error {
	var err error
	var fsRef fileSysRef
	var fsRefByte []byte
	var fsSysAttr fileSysAttr
	var fsSysAttrByte []byte
	fsid := uuid.NewV4().String()
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	// Write file system reference entries.
	// write /fs 								FSID						FileSysRef
	pKey := "/fs"
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(fsid))
	fsRef.AcctID = acctID
	fsRef.FSID = fsid
	fsRefByte, err = json.Marshal(fsRef)
	if err != nil {
		return err
	}
	_, err = o.comms.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro, fsRefByte)
	if err != nil {
		return err
	}
	// write /acct/acctID				FSID						FileSysRef
	pKey = fmt.Sprintf("/acct/%s", acctID)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	_, err = o.comms.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro, fsRefByte)
	if err != nil {
		return err
	}
	// Write file system attributes
	// write /fs/FSID						name						FileSysAttr
	pKey = fmt.Sprintf("/fs/%s", fsid)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB = murmur3.Sum128([]byte("name"))
	fsSysAttr.Attr = "name"
	fsSysAttr.Value = req.FSName
	fsSysAttr.FSID = fsid
	fsSysAttrByte, err = json.Marshal(fsSysAttr)
	if err != nil {
		return err
	}
	_, err = o.comms.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro, fsSysAttrByte)
	if err != nil {
		return err
	}
	uuID, err := uuid.FromString(fsid)
	id := getID(uuID.String(), 1, 0)
	vKeyA, vKeyB := murmur3.Sum128(id)
	// Check if root entry already exits
	_, value, err := o.comms.vstore.Read(ctx, vKeyA, vKeyB, nil)
	if !store.IsNotFound(err) {
		if len(value) != 0 {
			return err
		}
		return err
	}
	// Create the Root entry data
	// Prepare the root node
	nr := &formicproto.INodeEntry{
		Version: inodeEntryVersion,
		INode:   1,
		IsDir:   true,
		FSID:    uuID.Bytes(),
	}
	ts := time.Now().Unix()
	nr.Attr = &formicproto.Attr{
		INode:  1,
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
	// Use data to Create The First Block
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
	// Write root entry
	_, err = o.comms.vstore.Write(context.Background(), vKeyA, vKeyB, timestampMicro, blkdata)
	if err != nil {
		return err
	}
	// Return File System UUID
	// Log Operation
	resp.Data = fsid
	return nil
}

func (o *oortFS) Create(ctx context.Context, req *formicproto.CreateRequest, resp *formicproto.CreateResponse, fsid string) error {
	ts := time.Now().Unix()
	inode := o.fl.GetID()
	attr := &formicproto.Attr{
		INode:  inode,
		ATime:  ts,
		MTime:  ts,
		CTime:  ts,
		CrTime: ts,
		Mode:   req.Attr.Mode,
		UID:    req.Attr.UID,
		GID:    req.Attr.GID,
	}
	var err error
	resp.Name, resp.Attr, err = o.create(ctx, getID(fsid, req.Parent, 0), getID(fsid, inode, 0), inode, req.Name, attr, false)
	if err != nil {
		return err
	}
	return nil
}

func (o *oortFS) DeleteFS(ctx context.Context, req *formicproto.DeleteFSRequest, resp *formicproto.DeleteFSResponse, acctID string) error {
	var err error
	var value []byte
	var fsRef fileSysRef
	var addrData addrRef
	rowcount := 0
	// Validate acctID owns this file system
	pKey := fmt.Sprintf("/fs")
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(req.FSID))
	_, value, err = o.comms.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if store.IsNotFound(err) {
		return err
	}
	if err != nil {
		return err
	}
	err = json.Unmarshal(value, &fsRef)
	if err != nil {
		return err
	}
	if fsRef.AcctID != acctID {
		return errors.New("permission denied")
	}
	uuID, err := uuid.FromString(req.FSID)
	id := getID(uuID.String(), 1, 0)
	// Test if file system is empty.
	keyA, keyB := murmur3.Sum128(id)
	items, err := o.comms.gstore.ReadGroup(context.Background(), keyA, keyB)
	if len(items) != 0 {
		return errors.New("file system not empty")
	}
	// Remove the root file system entry from the value store
	keyA, keyB = murmur3.Sum128(id)
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	_, err = o.comms.vstore.Delete(context.Background(), keyA, keyB, timestampMicro)
	if err != nil {
		if !store.IsNotFound(err) {
			return err
		}
	}
	// Delete this record set
	// IP Addresses																				(x records)
	//    /fs/FSID/addr			  addr						AddrRef
	// File System Attributes															(1 record)
	//    /fs/FSID						name						FileSysAttr
	// File System Account Reference											(1 record)
	//    /acct/acctID				FSID						FileSysRef
	// File System Reference 															(1 record)
	//    /fs 								FSID						FileSysRef
	// Read list of granted ip addresses
	// group-lookup printf("/fs/%s/addr", FSID)
	pKey = fmt.Sprintf("/fs/%s/addr", req.FSID)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	items, err = o.comms.gstore.ReadGroup(context.Background(), pKeyA, pKeyB)
	if !store.IsNotFound(err) {
		// No addr granted
		for _, v := range items {
			err = json.Unmarshal(v.Value, &addrData)
			if err != nil {
				return err
			}
			err = o.deleteEntry(pKey, addrData.Addr)
			if err != nil {
				return err
			}
			rowcount++
		}
	}
	if err != nil {
		return err
	}
	// Delete File System Attributes
	//    /fs/FSID						name						FileSysAttr
	pKey = fmt.Sprintf("/fs/%s", req.FSID)
	err = o.deleteEntry(pKey, "name")
	if err != nil {
		return err
	}
	rowcount++
	// Delete File System Account Reference
	//    /acct/acctID				FSID						FileSysRef
	pKey = fmt.Sprintf("/acct/%s", acctID)
	err = o.deleteEntry(pKey, req.FSID)
	if err != nil {
		return err
	}
	rowcount++
	// File System Reference
	//    /fs 								FSID						FileSysRef
	err = o.deleteEntry("/fs", req.FSID)
	if err != nil {
		return err
	}
	rowcount++
	// Prep things to return
	resp.Data = req.FSID
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

func (o *oortFS) GrantAddrFS(ctx context.Context, req *formicproto.GrantAddrFSRequest, resp *formicproto.GrantAddrFSResponse, acctID string) error {
	var err error
	var fsRef fileSysRef
	var value []byte
	var addrData addrRef
	var addrByte []byte
	srcAddr := ""
	srcAddrIP := ""
	// Get incoming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}
	// Read FileSysRef entry to determine if it exists and Account matches
	pKey := fmt.Sprintf("/fs")
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(req.FSID))
	_, value, err = o.comms.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if store.IsNotFound(err) {
		return err
	}
	if err != nil {
		return err
	}
	err = json.Unmarshal(value, &fsRef)
	if err != nil {
		return err
	}
	if fsRef.AcctID != acctID {
		return errors.New("permission denied")
	}
	// GRANT an file system entry for the addr
	// 		write /fs/FSID/addr			addr						AddrRef
	if req.Addr == "" {
		srcAddrIP = strings.Split(srcAddr, ":")[0]
	} else {
		srcAddrIP = req.Addr
	}
	pKey = fmt.Sprintf("/fs/%s/addr", req.FSID)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB = murmur3.Sum128([]byte(srcAddrIP))
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	addrData.Addr = srcAddrIP
	addrData.FSID = req.FSID
	addrByte, err = json.Marshal(addrData)
	if err != nil {
		return err
	}
	_, err = o.comms.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro, addrByte)
	if err != nil {
		return err
	}
	resp.Data = srcAddrIP
	return nil
}

func (o *oortFS) InitFS(ctx context.Context, req *formicproto.InitFSRequest, resp *formicproto.InitFSResponse, fsid string) error {
	id := getID(fsid, 1, 0)
	n, _ := o.getChunk(ctx, id)
	if len(n) == 0 {
		return errors.New("root entry does not exist")
	}
	return nil
}

func (o *oortFS) ListFS(ctx context.Context, req *formicproto.ListFSRequest, resp *formicproto.ListFSResponse, acctID string) error {
	var value []byte
	// Read Group /acct/acctID				_						FileSysRef
	pKey := fmt.Sprintf("/acct/%s", acctID)
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	list, err := o.comms.gstore.ReadGroup(context.Background(), pKeyA, pKeyB)
	if err != nil {
		return err
	}
	fsList := make([]fileSysMeta, len(list))
	for k, v := range list {
		var fsRef fileSysRef
		var addrData addrRef
		var fsAttrData fileSysAttr
		var aList []string
		err = json.Unmarshal(v.Value, &fsRef)
		if err != nil {
			return err
		}
		fsList[k].AcctID = acctID
		fsList[k].ID = fsRef.FSID
		// Get File System Name
		pKey = fmt.Sprintf("/fs/%s", fsList[k].ID)
		pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
		cKeyA, cKeyB := murmur3.Sum128([]byte("name"))
		_, value, err = o.comms.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
		if store.IsNotFound(err) {
			return err
		}
		if err != nil {
			return err
		}
		err = json.Unmarshal(value, &fsAttrData)
		if err != nil {
			return err
		}
		fsList[k].Name = fsAttrData.Value
		// Get List of addrs
		pKey = fmt.Sprintf("/fs/%s/addr", fsList[k].ID)
		pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
		items, err := o.comms.gstore.ReadGroup(context.Background(), pKeyA, pKeyB)
		if !store.IsNotFound(err) {
			// No addr granted
			aList = make([]string, len(items))
			for sk, sv := range items {
				err = json.Unmarshal(sv.Value, &addrData)
				if err != nil {
					return err
				}
				aList[sk] = addrData.Addr
			}
		}
		if err != nil {
			return err
		}
		fsList[k].Addr = aList
	}
	// Return a File System List
	fsListJSON, jerr := json.Marshal(&fsList)
	if jerr != nil {
		return jerr
	}
	resp.Data = string(fsListJSON)
	return nil
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
	resp.Name = d.Name
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
	inode := o.fl.GetID()
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
	resp.Name, resp.Attr, err = o.create(ctx, getID(fsid, req.Parent, 0), getID(fsid, inode, 0), inode, req.Name, attr, true)
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

func (o *oortFS) RevokeAddrFS(ctx context.Context, req *formicproto.RevokeAddrFSRequest, resp *formicproto.RevokeAddrFSResponse, acctID string) error {
	var err error
	var value []byte
	var fsRef fileSysRef
	srcAddr := ""
	srcAddrIP := ""
	// Get incoming ip
	pr, ok := peer.FromContext(ctx)
	if ok {
		srcAddr = pr.Addr.String()
	}
	// Validate acctID owns this file system
	// Read FileSysRef entry to determine if it exists
	pKey := fmt.Sprintf("/fs")
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(req.FSID))
	_, value, err = o.comms.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if store.IsNotFound(err) {
		return err
	}
	if err != nil {
		return err
	}
	err = json.Unmarshal(value, &fsRef)
	if err != nil {
		return err
	}
	if fsRef.AcctID != acctID {
		return errors.New("permission denied")
	}
	// REVOKE an file system entry for the addr
	// 		delete /fs/FSID/addr			addr						AddrRef
	if req.Addr == "" {
		srcAddrIP = strings.Split(srcAddr, ":")[0]
	} else {
		srcAddrIP = req.Addr
	}
	pKey = fmt.Sprintf("/fs/%s/addr", req.FSID)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB = murmur3.Sum128([]byte(srcAddrIP))
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	_, err = o.comms.gstore.Delete(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro)
	if store.IsNotFound(err) {
		return err
	}
	if err != nil {
		return err
	}
	resp.Data = srcAddrIP
	return nil
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

func (o *oortFS) ShowFS(ctx context.Context, req *formicproto.ShowFSRequest, resp *formicproto.ShowFSResponse, acctID string) error {
	var err error
	var fs fileSysMeta
	var value []byte
	var fsRef fileSysRef
	var addrData addrRef
	var fsAttrData fileSysAttr
	var aList []string
	fs.ID = req.FSID
	// Read FileSysRef entry to determine if it exists
	pKey := fmt.Sprintf("/fs")
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(fs.ID))
	_, value, err = o.comms.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if store.IsNotFound(err) {
		return err
	}
	if err != nil {
		return err
	}
	err = json.Unmarshal(value, &fsRef)
	if err != nil {
		return err
	}
	if fsRef.AcctID != acctID {
		return errors.New("permission denied")
	}
	fs.AcctID = fsRef.AcctID
	// Read the file system attributes
	// group-lookup /fs			FSID
	//		Iterate over all the atributes
	pKey = fmt.Sprintf("/fs/%s", fs.ID)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB = murmur3.Sum128([]byte("name"))
	_, value, err = o.comms.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if store.IsNotFound(err) {
		return err
	}
	if err != nil {
		return err
	}
	err = json.Unmarshal(value, &fsAttrData)
	if err != nil {
		return err
	}
	fs.Name = fsAttrData.Value
	// Read list of granted ip addresses
	// group-lookup printf("/fs/%s/addr", FSID)
	pKey = fmt.Sprintf("/fs/%s/addr", fs.ID)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	items, err := o.comms.gstore.ReadGroup(context.Background(), pKeyA, pKeyB)
	if !store.IsNotFound(err) {
		// No addr granted
		aList = make([]string, len(items))
		for k, v := range items {
			err = json.Unmarshal(v.Value, &addrData)
			if err != nil {
				return err
			}
			aList[k] = addrData.Addr
		}
	}
	if err != nil {
		return err
	}
	fs.Addr = aList
	// Return File System
	fsJSON, jerr := json.Marshal(&fs)
	if jerr != nil {
		return jerr
	}
	resp.Data = string(fsJSON)
	return nil
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
	inode := o.fl.GetID()
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

func (o *oortFS) UpdateFS(ctx context.Context, req *formicproto.UpdateFSRequest, resp *formicproto.UpdateFSResponse, acctID string) error {
	var err error
	var value []byte
	var fsRef fileSysRef
	var fsSysAttr fileSysAttr
	var fsSysAttrByte []byte
	if req.FileSys.Name == "" {
		return errors.New("file system name cannot be empty")
	}
	// validate that acctID owns this file system
	// Read FileSysRef entry to determine if it exists
	pKey := fmt.Sprintf("/fs")
	pKeyA, pKeyB := murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB := murmur3.Sum128([]byte(req.FSID))
	_, value, err = o.comms.gstore.Read(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, nil)
	if store.IsNotFound(err) {
		return err
	}
	if err != nil {
		return err
	}
	err = json.Unmarshal(value, &fsRef)
	if err != nil {
		return err
	}
	if fsRef.AcctID != acctID {
		return errors.New("permission denied")
	}
	// Write file system attributes
	// write /fs/FSID						name						FileSysAttr
	pKey = fmt.Sprintf("/fs/%s", req.FSID)
	pKeyA, pKeyB = murmur3.Sum128([]byte(pKey))
	cKeyA, cKeyB = murmur3.Sum128([]byte("name"))
	fsSysAttr.Attr = "name"
	fsSysAttr.Value = req.FileSys.Name
	fsSysAttr.FSID = req.FSID
	fsSysAttrByte, err = json.Marshal(fsSysAttr)
	if err != nil {
		return err
	}
	timestampMicro := brimtime.TimeToUnixMicro(time.Now())
	_, err = o.comms.gstore.Write(context.Background(), pKeyA, pKeyB, cKeyA, cKeyB, timestampMicro, fsSysAttrByte)
	if err != nil {
		return err
	}
	// return message
	resp.Data = req.FSID
	return nil
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

func (o *oortFS) create(ctx context.Context, parent, id []byte, inode uint64, name string, attr *formicproto.Attr, isdir bool) (string, *formicproto.Attr, error) {
	// Check to see if the name already exists
	b, err := o.comms.ReadGroupItem(ctx, parent, []byte(name))
	if err != nil && !store.IsNotFound(err) {
		// TODO: Needs beter error handling
		return "", &formicproto.Attr{}, err
	}
	if len(b) > 0 {
		p := &formicproto.DirEntry{}
		err = unmarshal(b, p)
		if err != nil {
			return "", &formicproto.Attr{}, err
		}
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
		return "", &formicproto.Attr{}, err
	}
	err = o.comms.WriteGroup(ctx, parent, []byte(name), b)
	if err != nil {
		return "", &formicproto.Attr{}, err
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
		return "", &formicproto.Attr{}, err
	}
	err = o.writeChunk(ctx, id, b)
	if err != nil {
		return "", &formicproto.Attr{}, err
	}
	return name, attr, nil
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
