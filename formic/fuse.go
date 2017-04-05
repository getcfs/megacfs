package formic

import (
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fuseutil"
	"github.com/getcfs/megacfs/formic/formicproto"
	"github.com/getcfs/megacfs/ftls"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
)

const (
	attrValidTime  = 5 * time.Second
	entryValidTime = 5 * time.Second
)

type FuseFormicConfig struct {
	Logger     *zap.Logger
	Mountpoint string
	Address    string
	FSID       string
	AllowOther bool
	ReadOnly   bool
}

// FuseFormic is a client that glues a mountpoint to a Formic server via the
// Fuse kernel interface.
type FuseFormic struct {
	logger     *zap.Logger
	mountpoint string
	address    string
	fsid       string
	allowOther bool
	readOnly   bool
	handles    *fileHandles
	conn       *fuse.Conn
	wg         sync.WaitGroup
	formic     *Formic
}

func NewFuseFormic(cfg *FuseFormicConfig) *FuseFormic {
	FuseFormic := &FuseFormic{
		logger:     cfg.Logger,
		mountpoint: cfg.Mountpoint,
		address:    cfg.Address,
		fsid:       cfg.FSID,
		allowOther: cfg.AllowOther,
		readOnly:   cfg.ReadOnly,
		handles:    newFileHandles(),
		formic:     NewFormic(cfg.FSID, cfg.Address, 1, &ftls.Config{InsecureSkipVerify: true}),
	}
	return FuseFormic
}

// Serve will run forever, or until an error occurs, continuously handling Fuse
// requests and Formic responses.
func (f *FuseFormic) Serve() error {
	mountOptions := []fuse.MountOption{
		fuse.FSName("cfs"),
		fuse.Subtype("cfs"),
		fuse.DefaultPermissions(),
		fuse.MaxReadahead(128 * 1024),
		fuse.AsyncRead(),
		//fuse.WritebackCache(),
		fuse.AutoInvalData(),
	}
	if f.allowOther {
		mountOptions = append(mountOptions, fuse.AllowOther())
	}
	if f.readOnly {
		mountOptions = append(mountOptions, fuse.ReadOnly())
	}
	fusermountPath()
	var err error
	f.conn, err = fuse.Mount(f.mountpoint, mountOptions...)
	if err != nil {
		return err
	}
	defer f.conn.Close()
	// TODO: Should this really be a client side thing?
	if err := f.formic.InitFS(f.newGetContext()); err != nil {
		return err
	}
	var req fuse.Request
	for {
		req, err = f.conn.ReadRequest()
		if err != nil {
			break
		}
		f.wg.Add(1)
		go func(r fuse.Request) {
			f.handle(r)
			f.wg.Done()
		}(req)
	}
	<-f.conn.Ready
	if err == nil {
		err = f.conn.MountError
	}
	f.wg.Wait()
	if err == io.EOF {
		err = nil
	}
	return err
}

func (f *FuseFormic) handle(r fuse.Request) {
	switch tr := r.(type) {
	default:
		f.logger.Debug("Unhandled request", zap.Any("request", tr))
		tr.RespondError(fuse.ENOSYS)

	case *fuse.GetattrRequest:
		f.handleGetattr(tr)

	case *fuse.LookupRequest:
		f.handleLookup(tr)

	case *fuse.MkdirRequest:
		f.handleMkdir(tr)

	case *fuse.OpenRequest:
		f.handleOpen(tr)

	case *fuse.ReadRequest:
		f.handleRead(tr)

	case *fuse.WriteRequest:
		f.handleWrite(tr)

	case *fuse.CreateRequest:
		f.handleCreate(tr)

	case *fuse.SetattrRequest:
		f.handleSetattr(tr)

	case *fuse.ReleaseRequest:
		f.handleRelease(tr)

	case *fuse.FlushRequest:
		f.handleFlush(tr)

	case *fuse.InterruptRequest:
		f.handleInterrupt(tr)

	case *fuse.ForgetRequest:
		f.handleForget(tr)

	case *fuse.RemoveRequest:
		f.handleRemove(tr)

	case *fuse.AccessRequest:
		f.handleAccess(tr)

	case *fuse.SymlinkRequest:
		f.handleSymlink(tr)

	case *fuse.ReadlinkRequest:
		f.handleReadlink(tr)

	case *fuse.GetxattrRequest:
		f.handleGetXAttr(tr)

	case *fuse.ListxattrRequest:
		f.handleListXAttr(tr)

	case *fuse.SetxattrRequest:
		f.handleSetXAttr(tr)

	case *fuse.RemovexattrRequest:
		f.handleRemoveXAttr(tr)

	case *fuse.RenameRequest:
		f.handleRename(tr)

	case *fuse.StatfsRequest:
		f.handleStatfs(tr)

		/*
			case *fuse.InitRequest:
				f.handleInit(tr)

			case *fuse.MknodRequest:
				f.handleMknod(tr)


			case *fuse.LinkRequest:
				f.handleLink(tr)

			case *fuse.DestroyRequest:
				f.handleDestroy(tr)

			case *fuse.FsyncRequest:
				f.handleFsync(tr)
		*/
	}
}

type fileHandle struct {
	inode     fuse.NodeID
	readCache []byte
}

type fileHandles struct {
	cur     fuse.HandleID
	handles map[fuse.HandleID]*fileHandle
	sync.RWMutex
}

func newFileHandles() *fileHandles {
	return &fileHandles{
		cur:     0,
		handles: make(map[fuse.HandleID]*fileHandle),
	}
}

func (f *fileHandles) newFileHandle(inode fuse.NodeID) fuse.HandleID {
	f.Lock()
	defer f.Unlock()
	// TODO: not likely that you would use all uint64 handles, but should be better than this
	f.handles[f.cur] = &fileHandle{inode: inode}
	f.cur++
	return f.cur - 1
}

func (f *fileHandles) removeFileHandle(h fuse.HandleID) {
	f.Lock()
	defer f.Unlock()
	// TODO: Need to add error handling
	delete(f.handles, h)
}

func (f *fileHandles) cacheRead(h fuse.HandleID, data []byte) {
	f.Lock()
	defer f.Unlock()
	// TODO: Need to add error handling
	f.handles[h].readCache = data
}

func (f *fileHandles) getReadCache(h fuse.HandleID) []byte {
	f.RLock()
	defer f.RUnlock()
	// TODO: Need to add error handling
	return f.handles[h].readCache
}

func copyNewAttr(dst *fuse.Attr, src *formicproto.Attr) {
	dst.Inode = src.INode
	dst.Mode = os.FileMode(src.Mode)
	dst.Size = src.Size
	dst.Mtime = time.Unix(src.MTime, 0)
	dst.Atime = time.Unix(src.ATime, 0)
	dst.Ctime = time.Unix(src.CTime, 0)
	dst.Crtime = time.Unix(src.CrTime, 0)
	dst.Uid = src.UID
	dst.Gid = src.GID
	dst.Blocks = dst.Size / 512 // set Blocks so df works without the --apparent-size flag
}

func copyAttr(dst *fuse.Attr, src *formicproto.Attr) {
	dst.Inode = src.INode
	dst.Mode = os.FileMode(src.Mode)
	dst.Size = src.Size
	dst.Mtime = time.Unix(src.MTime, 0)
	dst.Atime = time.Unix(src.ATime, 0)
	dst.Ctime = time.Unix(src.CTime, 0)
	dst.Crtime = time.Unix(src.CrTime, 0)
	dst.Uid = src.UID
	dst.Gid = src.GID
	dst.Blocks = dst.Size / 512 // set Blocks so df works without the --apparent-size flag
}

// Get a context that includes fsid
func (f *FuseFormic) getContext() context.Context {
	// TODO: Make timeout configurable
	c, _ := context.WithTimeout(context.Background(), 10*time.Second)
	c = metadata.NewContext(
		c,
		metadata.Pairs("fsid", f.fsid),
	)
	return c
}

func (f *FuseFormic) newGetContext() context.Context {
	// TODO: Make timeout configurable
	c, _ := context.WithTimeout(context.Background(), 10*time.Second)
	return c
}

func (f *FuseFormic) handleGetattr(r *fuse.GetattrRequest) {
	f.logger.Debug("Inside handleGetattr", zap.Any("request", r))
	attr, err := f.formic.GetAttr(f.newGetContext(), uint64(r.Node))
	if err != nil {
		f.logger.Debug("Getattr failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.GetattrResponse{}
	copyNewAttr(&resp.Attr, attr)
	// TODO: should we make these configurable?
	resp.Attr.Valid = attrValidTime
	f.logger.Debug("handleGetattr returning", zap.Any("response", resp))
	r.Respond(resp)
}

func (f *FuseFormic) handleLookup(req *fuse.LookupRequest) {
	f.logger.Debug("Inside handleLookup", zap.Any("request", req))
	attr, err := f.formic.Lookup(f.newGetContext(), uint64(req.Node), req.Name)
	if err != nil {
		// TODO: Rework this and error codes like this to be more like oort.
		if strings.Contains(err.Error(), "not found by remote store") {
			f.logger.Debug("ENOENT Lookup", zap.String("name", req.Name))
			req.RespondError(fuse.ENOENT)
			return
		}
		f.logger.Debug("Lookup failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.LookupResponse{}
	resp.Node = fuse.NodeID(attr.INode)
	copyNewAttr(&resp.Attr, attr)
	// TODO: should we make these configurable?
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime
	f.logger.Debug("handleLookup returning", zap.Any("response", resp))
	req.Respond(resp)
}

func (f *FuseFormic) handleMkdir(req *fuse.MkdirRequest) {
	f.logger.Debug("Inside handleMkdir", zap.Any("request", req))
	attr, err := f.formic.MkDir(f.newGetContext(), uint64(req.Node), req.Name, &formicproto.Attr{UID: req.Uid, GID: req.Gid, Mode: uint32(req.Mode)})
	if err != nil {
		// TODO: Rework this and error codes like this to be more like oort.
		if err.Error() != "already exists" {
			f.logger.Debug("EEXIST Mkdir", zap.String("name", req.Name))
			req.RespondError(fuse.EEXIST)
			return
		}
		f.logger.Debug("Mkdir failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.MkdirResponse{}
	resp.Node = fuse.NodeID(attr.INode)
	copyNewAttr(&resp.Attr, attr)
	// TODO: should we make these configurable?
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime
	f.logger.Debug("handleMkdir returning", zap.Any("response", resp))
	req.Respond(resp)
}

func (f *FuseFormic) handleOpen(r *fuse.OpenRequest) {
	f.logger.Debug("Inside handleOpen", zap.Any("request", r))
	resp := &fuse.OpenResponse{}
	// For now use the inode as the file handle
	resp.Handle = f.handles.newFileHandle(r.Node)
	resp.Flags |= fuse.OpenKeepCache
	f.logger.Debug("handleOpen returning", zap.Any("response", resp))
	r.Respond(resp)
}

func (f *FuseFormic) handleRead(req *fuse.ReadRequest) {
	f.logger.Debug("Inside handleRead", zap.Any("request", req))
	if req.Dir {
		resp := &fuse.ReadResponse{Data: make([]byte, 0, req.Size)}
		// handle directory listing
		data := f.handles.getReadCache(req.Handle)
		if data == nil {
			readDirAllEnts, err := f.formic.ReadDirAll(f.newGetContext(), uint64(req.Node))
			if err != nil {
				f.logger.Debug("ReadDirAll failed", zap.Error(err))
				req.RespondError(fuse.EIO)
				return
			}
			data = fuse.AppendDirent(data, fuse.Dirent{
				Name:  ".",
				Inode: uint64(req.Node),
				Type:  fuse.DT_Dir,
			})
			data = fuse.AppendDirent(data, fuse.Dirent{
				Name:  "..",
				Inode: 1, // TODO: not sure what value this should be, but this seems to work fine.
				Type:  fuse.DT_Dir,
			})
			for _, e := range readDirAllEnts {
				if e == nil || e.Name == "" {
					continue
				}
				data = fuse.AppendDirent(data, fuse.Dirent{
					Name:  e.Name,
					Inode: 1, // TODO: seems to work fine with any non-zero inode.  why?
					Type:  fuse.DirentType(e.Type),
				})
			}
			f.handles.cacheRead(req.Handle, data)
		}
		fuseutil.HandleRead(req, resp, data)
		req.Respond(resp)
		return
	}
	payload, err := f.formic.Read(f.newGetContext(), uint64(req.Node), int64(req.Offset), int64(req.Size))
	if err != nil {
		f.logger.Debug("Read failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	req.Respond(&fuse.ReadResponse{Data: payload})
}

func (f *FuseFormic) handleWrite(r *fuse.WriteRequest) {
	// TODO: Implement write - Currently this is stupid simple and doesn't handle all the possibilities
	f.logger.Debug("Inside handleWrite", zap.Any("request", r))
	f.logger.Debug("Writing bytes at offset", zap.Int("bytes", len(r.Data)), zap.Int64("offset", r.Offset))
	err := f.formic.Write(f.newGetContext(), uint64(r.Node), int64(r.Offset), r.Data)
	if err != nil {
		f.logger.Debug("Write failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	r.Respond(&fuse.WriteResponse{Size: len(r.Data)})
}

func (f *FuseFormic) handleCreate(req *fuse.CreateRequest) {
	f.logger.Debug("Inside handleCreate", zap.Any("request", req))
	resultingChildAttr, err := f.formic.Create(f.newGetContext(), uint64(req.Node), req.Name, &formicproto.Attr{UID: req.Uid, GID: req.Gid, Mode: uint32(req.Mode)})
	if err != nil {
		f.logger.Debug("Create failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.CreateResponse{}
	resp.Node = fuse.NodeID(resultingChildAttr.INode)
	copyNewAttr(&resp.Attr, resultingChildAttr)
	// TODO: should we make these configurable?
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime
	copyNewAttr(&resp.LookupResponse.Attr, resultingChildAttr)
	resp.LookupResponse.Attr.Valid = attrValidTime
	resp.LookupResponse.EntryValid = entryValidTime
	f.logger.Debug("handleCreate returning", zap.Any("response", resp))
	req.Respond(resp)
}

func (f *FuseFormic) handleSetattr(r *fuse.SetattrRequest) {
	f.logger.Debug("Inside handleSetattr", zap.Any("request", r))
	resp := &fuse.SetattrResponse{}
	resp.Attr.Inode = uint64(r.Node)
	a := &formicproto.Attr{
		INode: uint64(r.Node),
	}
	if r.Valid.Size() {
		a.Size = r.Size
	}
	if r.Valid.Mode() {
		a.Mode = uint32(r.Mode)
	}
	if r.Valid.Atime() {
		a.ATime = r.Atime.Unix()
	}
	if r.Valid.AtimeNow() {
		a.ATime = time.Now().Unix()
	}
	if r.Valid.Mtime() {
		a.MTime = r.Mtime.Unix()
	}
	if r.Valid.Uid() {
		a.UID = r.Uid
	}
	if r.Valid.Gid() {
		a.GID = r.Gid
	}
	attr, err := f.formic.SetAttr(f.newGetContext(), a, uint32(r.Valid))
	if err != nil {
		f.logger.Debug("Setattr failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	copyNewAttr(&resp.Attr, attr)
	resp.Attr.Valid = attrValidTime
	f.logger.Debug("handleSetattr returning", zap.Any("response", resp))
	r.Respond(resp)
}

func (f *FuseFormic) handleFlush(r *fuse.FlushRequest) {
	f.logger.Debug("Inside handleFlush")
	r.Respond()
}

func (f *FuseFormic) handleRelease(r *fuse.ReleaseRequest) {
	f.logger.Debug("Inside handleRelease")
	f.handles.removeFileHandle(r.Handle)
	r.Respond()
}

func (f *FuseFormic) handleInterrupt(r *fuse.InterruptRequest) {
	f.logger.Debug("Inside handleInterrupt")
	// TODO: Just passing on this for now.  Need to figure out what really needs to be done here
	r.Respond()
}

func (f *FuseFormic) handleForget(r *fuse.ForgetRequest) {
	f.logger.Debug("Inside handleForget")
	// TODO: Just passing on this for now.  Need to figure out what really needs to be done here
	r.Respond()
}

func (f *FuseFormic) handleRemove(req *fuse.RemoveRequest) {
	// TODO: Handle dir deletions correctly <- I don't know what this means, but it sounds scary.
	f.logger.Debug("Inside handleRemove", zap.Any("request", req))
	err := f.formic.Remove(f.newGetContext(), uint64(req.Node), req.Name)
	if err != nil {
		if err.Error() == "not empty" {
			f.logger.Debug("Remove failed", zap.Error(err))
			req.RespondError(fuse.Errno(syscall.ENOTEMPTY))
			return
		}
		f.logger.Debug("Remove failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	f.logger.Debug("handleRemove returning")
	req.Respond()
}

func (f *FuseFormic) handleAccess(r *fuse.AccessRequest) {
	f.logger.Debug("Inside handleAccess")
	// TODO: Add real access support, for now allows everything
	r.Respond()
}

// TODO: Implement the following functions (and make sure to comment out the case)
// Note: All handle functions should call r.Respond or r.Respond error before returning

func (f *FuseFormic) handleMknod(r *fuse.MknodRequest) {
	f.logger.Debug("Inside handleMknod")
	// NOTE: We probably will not need this since we implement Create
	r.RespondError(fuse.EIO)
}

/*
func (f *FuseFormic) handleInit(r *fuse.InitRequest) {
	f.logger.Debug("Inside handleInit")
	r.RespondError(fuse.ENOSYS)
}
*/

func (f *FuseFormic) handleStatfs(req *fuse.StatfsRequest) {
	f.logger.Debug("Inside handleStatfs", zap.Any("request", req))
	resp := &fuse.StatfsResponse{}
	var err error
	resp.Blocks, resp.Bfree, resp.Bavail, resp.Files, resp.Ffree, resp.Bsize, resp.Namelen, resp.Frsize, err = f.formic.StatFS(f.newGetContext())
	if err != nil {
		f.logger.Debug("Statfs failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	req.Respond(resp)
}

func (f *FuseFormic) handleSymlink(req *fuse.SymlinkRequest) {
	f.logger.Debug("Inside handleSymlink", zap.Any("request", req))
	attr, err := f.formic.SymLink(f.newGetContext(), uint64(req.Node), req.NewName, req.Target, req.Uid, req.Gid)
	if err != nil {
		f.logger.Debug("Symlink failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.SymlinkResponse{}
	resp.Node = fuse.NodeID(attr.INode)
	copyNewAttr(&resp.Attr, attr)
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime
	f.logger.Debug("handleSymlink returning", zap.Any("response", resp))
	req.Respond(resp)
}

func (f *FuseFormic) handleReadlink(req *fuse.ReadlinkRequest) {
	f.logger.Debug("Inside handleReadlink", zap.Any("request", req))
	target, err := f.formic.ReadLink(f.newGetContext(), uint64(req.Node))
	if err != nil {
		f.logger.Debug("Readlink failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	f.logger.Debug("handleReadlink returning", zap.Any("response", target))
	req.Respond(target)
}

func (f *FuseFormic) handleLink(r *fuse.LinkRequest) {
	f.logger.Debug("Inside handleLink")
	r.RespondError(fuse.ENOSYS)
}

func (f *FuseFormic) handleGetXAttr(req *fuse.GetxattrRequest) {
	// TODO: Possiblity short circuit excessive xattr calls we don't really
	// support.
	f.logger.Debug("Inside handleGetXAttr", zap.Any("request", req))
	if req.Name == "cfs.fsid" {
		resp := &fuse.GetxattrResponse{Xattr: []byte(f.fsid)}
		f.logger.Debug("handleGetXAttr returning", zap.Any("response", resp))
		req.Respond(resp)
		return
	}
	// TODO: Best I can tell, xattr size and position were never implemented.
	// The whole xattr is always returned.
	xAttr, err := f.formic.GetXAttr(f.newGetContext(), uint64(req.Node), req.Name, req.Size, req.Position)
	if err != nil {
		f.logger.Debug("GetXAttr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.GetxattrResponse{Xattr: xAttr}
	f.logger.Debug("handleGetXAttr returning", zap.Any("response", resp))
	req.Respond(resp)
}

func (f *FuseFormic) handleListXAttr(req *fuse.ListxattrRequest) {
	f.logger.Debug("Inside handleListXAttr", zap.Any("request", req))
	// TODO: Best I can tell, xattr size and position were never implemented.
	// The whole list of xattrs are always returned.
	xAttr, err := f.formic.ListXAttr(f.newGetContext(), uint64(req.Node), req.Size, req.Position)
	if err != nil {
		f.logger.Debug("ListXAttr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.ListxattrResponse{Xattr: xAttr}
	f.logger.Debug("handleListXAttr returning", zap.Any("response", resp))
	req.Respond(resp)
}

func (f *FuseFormic) handleSetXAttr(req *fuse.SetxattrRequest) {
	// TODO: Possiblity disallow some xattr sets, like security.* system.* if
	// that makes sense.
	f.logger.Debug("Inside handleSetXAttr", zap.Any("request", req))
	if strings.HasPrefix(req.Name, "cfs.") {
		// Silently discard sets to "our" namespace.
		req.Respond()
		return
	}
	// TODO: Best I can tell, xattr position and flags were never implemented.
	// The whole xattr is always set and flags are ignored.
	err := f.formic.SetXAttr(f.newGetContext(), uint64(req.Node), req.Name, req.Xattr, req.Position, req.Flags)
	if err != nil {
		f.logger.Debug("SetXAttr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	f.logger.Debug("handleSetXAttr returning")
	req.Respond()
}

func (f *FuseFormic) handleRemoveXAttr(req *fuse.RemovexattrRequest) {
	// TODO: Possiblity disallow some xattr removes, like security.* system.*
	// if that makes sense.
	f.logger.Debug("Inside handleRemoveXAttr", zap.Any("request", req))
	// TODO: All these checks need to be server side.
	if strings.HasPrefix(req.Name, "cfs.") {
		// Silently discard removes to "our" namespace.
		req.Respond()
		return
	}
	err := f.formic.RemoveXAttr(f.newGetContext(), uint64(req.Node), req.Name)
	if err != nil {
		f.logger.Debug("RemoveXAttr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	f.logger.Debug("handleRemoveXAttr returning")
	req.Respond()
}

func (f *FuseFormic) handleDestroy(r *fuse.DestroyRequest) {
	f.logger.Debug("Inside handleDestroy")
	r.RespondError(fuse.ENOSYS)
}

func (f *FuseFormic) handleRename(req *fuse.RenameRequest) {
	f.logger.Debug("Inside handleRename", zap.Any("request", req))
	err := f.formic.Rename(f.newGetContext(), uint64(req.Node), uint64(req.NewDir), req.OldName, req.NewName)
	if err != nil {
		f.logger.Debug("Rename failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	f.logger.Debug("handleRename returning")
	req.Respond()
}

func (f *FuseFormic) handleFsync(r *fuse.FsyncRequest) {
	f.logger.Debug("Inside handleFsync")
	r.RespondError(fuse.ENOSYS)
}

// the mount command needs the path to fusermount
func fusermountPath() {
	// Grab the current path
	currentPath := os.Getenv("PATH")
	if len(currentPath) == 0 {
		// fusermount location for suse  /usr/bin
		// fusermount location on debian based distros /bin
		os.Setenv("PATH", "/usr/bin:/bin")
	}
}
