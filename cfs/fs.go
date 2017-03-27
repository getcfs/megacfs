package main

import (
	"errors"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fuseutil"
	"github.com/getcfs/megacfs/formic/formicproto"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
)

const (
	attrValidTime  = 5 * time.Second
	entryValidTime = 5 * time.Second
)

type fs struct {
	conn    *fuse.Conn
	rpc     *rpc
	handles *fileHandles
	fsid    string
}

func newfs(c *fuse.Conn, r *rpc, fsid string) *fs {
	fs := &fs{
		conn:    c,
		rpc:     r,
		handles: newFileHandles(),
		fsid:    fsid,
	}
	return fs
}

// Handle fuse request
func (f *fs) handle(r fuse.Request) {
	switch tr := r.(type) {
	default:
		logger.Debug("Unhandled request", zap.Any("request", tr))
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
		f.handleGetxattr(tr)

	case *fuse.ListxattrRequest:
		f.handleListxattr(tr)

	case *fuse.SetxattrRequest:
		f.handleSetxattr(tr)

	case *fuse.RemovexattrRequest:
		f.handleRemovexattr(tr)

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
	dst.Inode = src.Inode
	dst.Mode = os.FileMode(src.Mode)
	dst.Size = src.Size
	dst.Mtime = time.Unix(src.Mtime, 0)
	dst.Atime = time.Unix(src.Atime, 0)
	dst.Ctime = time.Unix(src.Ctime, 0)
	dst.Crtime = time.Unix(src.Crtime, 0)
	dst.Uid = src.Uid
	dst.Gid = src.Gid
	dst.Blocks = dst.Size / 512 // set Blocks so df works without the --apparent-size flag
}

func copyAttr(dst *fuse.Attr, src *formicproto.Attr) {
	dst.Inode = src.Inode
	dst.Mode = os.FileMode(src.Mode)
	dst.Size = src.Size
	dst.Mtime = time.Unix(src.Mtime, 0)
	dst.Atime = time.Unix(src.Atime, 0)
	dst.Ctime = time.Unix(src.Ctime, 0)
	dst.Crtime = time.Unix(src.Crtime, 0)
	dst.Uid = src.Uid
	dst.Gid = src.Gid
	dst.Blocks = dst.Size / 512 // set Blocks so df works without the --apparent-size flag
}

// Get a context that includes fsid
func (f *fs) getContext() context.Context {
	// TODO: Make timeout configurable
	c, _ := context.WithTimeout(context.Background(), 10*time.Second)
	c = metadata.NewContext(
		c,
		metadata.Pairs("fsid", f.fsid),
	)
	return c
}

func (f *fs) InitFs() error {
	// TODO: Should this really be a client side thing?
	logger.Debug("Inside InitFs")
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.InitFs(f.getContext())
	if err != nil {
		logger.Debug("InitFs failed", zap.Error(err))
		return err
	}
	if err = stream.Send(&formicproto.InitFsRequest{Rpcid: 1}); err != nil {
		logger.Debug("InitFs failed", zap.Error(err))
		return err
	}
	initFsResp, err := stream.Recv()
	if err != nil {
		logger.Debug("InitFs failed", zap.Error(err))
		return err
	}
	if initFsResp.Err != "" {
		logger.Debug("InitFs failed", zap.String("Err", initFsResp.Err))
		return errors.New(initFsResp.Err)
	}
	return nil
}

func (f *fs) handleGetattr(r *fuse.GetattrRequest) {
	logger.Debug("Inside handleGetattr", zap.Any("request", r))
	resp := &fuse.GetattrResponse{}
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.GetAttr(f.getContext())
	if err != nil {
		logger.Debug("Getattr failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	if err = stream.Send(&formicproto.GetAttrRequest{Rpcid: 1, Inode: uint64(r.Node)}); err != nil {
		logger.Debug("Getattr failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	getAttrResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Getattr failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	if getAttrResp.Err != "" {
		logger.Debug("Getattr failed", zap.String("Err", getAttrResp.Err))
		r.RespondError(fuse.EIO)
		return
	}
	copyNewAttr(&resp.Attr, getAttrResp.Attr)
	// TODO: should we make these configurable?
	resp.Attr.Valid = attrValidTime
	logger.Debug("handleGetattr returning", zap.Any("response", resp))
	r.Respond(resp)
}

func (f *fs) handleLookup(req *fuse.LookupRequest) {
	logger.Debug("Inside handleLookup", zap.Any("request", req))
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Lookup(f.getContext())
	if err != nil {
		logger.Debug("Lookup failed [1]", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if err = stream.Send(&formicproto.LookupRequest{Rpcid: 1, Name: req.Name, Parent: uint64(req.Node)}); err != nil {
		logger.Debug("Lookup failed [2]", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	protoResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Lookup failed [3]", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if protoResp.Err != "" {
		// TODO: Rework this and error codes like this to be more like oort.
		if protoResp.Err == "not found by remote store" {
			logger.Debug("ENOENT Lookup", zap.String("name", req.Name))
			req.RespondError(fuse.ENOENT)
			return
		}
		logger.Debug("Lookup failed [4]", zap.String("Err", protoResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.LookupResponse{}
	resp.Node = fuse.NodeID(protoResp.Attr.Inode)
	copyNewAttr(&resp.Attr, protoResp.Attr)
	// TODO: should we make these configurable?
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime
	logger.Debug("handleLookup returning", zap.Any("response", resp))
	req.Respond(resp)
}

func (f *fs) handleMkdir(req *fuse.MkdirRequest) {
	logger.Debug("Inside handleMkdir", zap.Any("request", req))
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.MkDir(f.getContext())
	if err != nil {
		logger.Debug("Mkdir failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if err = stream.Send(&formicproto.MkDirRequest{Rpcid: 1, Name: req.Name, Parent: uint64(req.Node), Attr: &formicproto.Attr{Uid: req.Uid, Gid: req.Gid, Mode: uint32(req.Mode)}}); err != nil {
		logger.Debug("Mkdir failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	protoResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Mkdir failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if protoResp.Err != "" {
		logger.Debug("Mkdir failed", zap.String("Err", protoResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	// If the name is empty, then the dir already exists
	if protoResp.Name != req.Name {
		logger.Debug("EEXIST Mkdir", zap.String("name", req.Name))
		req.RespondError(fuse.EEXIST)
		return
	}
	resp := &fuse.MkdirResponse{}
	resp.Node = fuse.NodeID(protoResp.Attr.Inode)
	copyNewAttr(&resp.Attr, protoResp.Attr)
	// TODO: should we make these configurable?
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime
	logger.Debug("handleMkdir returning", zap.Any("response", resp))
	req.Respond(resp)
}

func (f *fs) handleOpen(r *fuse.OpenRequest) {
	logger.Debug("Inside handleOpen", zap.Any("request", r))
	resp := &fuse.OpenResponse{}
	// For now use the inode as the file handle
	resp.Handle = f.handles.newFileHandle(r.Node)
	resp.Flags |= fuse.OpenKeepCache
	logger.Debug("handleOpen returning", zap.Any("response", resp))
	r.Respond(resp)
}

func (f *fs) handleRead(req *fuse.ReadRequest) {
	logger.Debug("Inside handleRead", zap.Any("request", req))
	if req.Dir {
		resp := &fuse.ReadResponse{Data: make([]byte, 0, req.Size)}
		// handle directory listing
		data := f.handles.getReadCache(req.Handle)
		if data == nil {
			// TODO: Placeholder code to get things working; needs to be
			// replaced to be more like oort's client code.
			stream, err := f.rpc.newClient.ReadDirAll(f.getContext())
			if err != nil {
				logger.Debug("ReadDirAll failed", zap.Error(err))
				req.RespondError(fuse.EIO)
				return
			}
			if err = stream.Send(&formicproto.ReadDirAllRequest{Rpcid: 1, Inode: uint64(req.Node)}); err != nil {
				logger.Debug("ReadDirAll failed", zap.Error(err))
				req.RespondError(fuse.EIO)
				return
			}
			protoResp, err := stream.Recv()
			if err != nil {
				logger.Debug("ReadDirAll failed", zap.Error(err))
				req.RespondError(fuse.EIO)
				return
			}
			if protoResp.Err != "" {
				logger.Debug("ReadDirAll failed", zap.String("Err", protoResp.Err), zap.Any("protoResp", protoResp))
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
			for _, de := range protoResp.Direntries {
				if de == nil || de.Name == "" {
					continue
				}
				data = fuse.AppendDirent(data, fuse.Dirent{
					Name:  de.Name,
					Inode: 1, // TODO: seems to work fine with any non-zero inode.  why?
					Type:  fuse.DirentType(de.Type),
				})
			}
			f.handles.cacheRead(req.Handle, data)
		}
		fuseutil.HandleRead(req, resp, data)
		req.Respond(resp)
		return
	}
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Read(f.getContext())
	if err != nil {
		logger.Debug("Read failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if err = stream.Send(&formicproto.ReadRequest{Rpcid: 1, Inode: uint64(req.Node), Offset: int64(req.Offset), Size: int64(req.Size)}); err != nil {
		logger.Debug("Read failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	readResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Read failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if readResp.Err != "" {
		logger.Debug("Read failed", zap.String("Err", readResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	req.Respond(&fuse.ReadResponse{Data: readResp.Payload})
}

func (f *fs) handleWrite(r *fuse.WriteRequest) {
	// TODO: Implement write
	// Currently this is stupid simple and doesn't handle all the possibilities
	logger.Debug("Inside handleWrite", zap.Any("request", r))
	logger.Debug("Writing bytes at offset", zap.Int("bytes", len(r.Data)), zap.Int64("offset", r.Offset))
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Write(f.getContext())
	if err != nil {
		logger.Debug("Write failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	if err = stream.Send(&formicproto.WriteRequest{Rpcid: 1, Inode: uint64(r.Node), Offset: r.Offset, Payload: r.Data}); err != nil {
		logger.Debug("Write failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	writeResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Write failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	if writeResp.Err != "" {
		logger.Debug("Write failed", zap.String("Err", writeResp.Err))
		r.RespondError(fuse.EIO)
		return
	}
	r.Respond(&fuse.WriteResponse{Size: len(r.Data)})
}

func (f *fs) handleCreate(req *fuse.CreateRequest) {
	logger.Debug("Inside handleCreate", zap.Any("request", req))

	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Create(f.getContext())
	if err != nil {
		logger.Debug("Create failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if err = stream.Send(&formicproto.CreateRequest{Rpcid: 1, Parent: uint64(req.Node), Name: req.Name, Attr: &formicproto.Attr{Uid: req.Uid, Gid: req.Gid, Mode: uint32(req.Mode)}}); err != nil {
		logger.Debug("Create failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	createResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Create failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if createResp.Err != "" {
		logger.Debug("Create failed", zap.String("Err", createResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.CreateResponse{}
	resp.Node = fuse.NodeID(createResp.Attr.Inode)
	copyNewAttr(&resp.Attr, createResp.Attr)
	// TODO: should we make these configurable?
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime
	copyNewAttr(&resp.LookupResponse.Attr, createResp.Attr)
	resp.LookupResponse.Attr.Valid = attrValidTime
	resp.LookupResponse.EntryValid = entryValidTime
	logger.Debug("handleCreate returning", zap.Any("response", resp))
	req.Respond(resp)
}

func (f *fs) handleSetattr(r *fuse.SetattrRequest) {
	logger.Debug("Inside handleSetattr", zap.Any("request", r))
	resp := &fuse.SetattrResponse{}
	resp.Attr.Inode = uint64(r.Node)
	a := &formicproto.Attr{
		Inode: uint64(r.Node),
	}
	if r.Valid.Size() {
		a.Size = r.Size
	}
	if r.Valid.Mode() {
		a.Mode = uint32(r.Mode)
	}
	if r.Valid.Atime() {
		a.Atime = r.Atime.Unix()
	}
	if r.Valid.AtimeNow() {
		a.Atime = time.Now().Unix()
	}
	if r.Valid.Mtime() {
		a.Mtime = r.Mtime.Unix()
	}
	if r.Valid.Uid() {
		a.Uid = r.Uid
	}
	if r.Valid.Gid() {
		a.Gid = r.Gid
	}
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.SetAttr(f.getContext())
	if err != nil {
		logger.Debug("Setattr failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	if err = stream.Send(&formicproto.SetAttrRequest{Rpcid: 1, Attr: a, Valid: uint32(r.Valid)}); err != nil {
		logger.Debug("Setattr failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	setAttrResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Setattr failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	if setAttrResp.Err != "" {
		logger.Debug("Setattr failed", zap.String("Err", setAttrResp.Err))
		r.RespondError(fuse.EIO)
		return
	}
	copyNewAttr(&resp.Attr, setAttrResp.Attr)
	resp.Attr.Valid = attrValidTime
	logger.Debug("handleSetattr returning", zap.Any("response", resp))
	r.Respond(resp)
}

func (f *fs) handleFlush(r *fuse.FlushRequest) {
	logger.Debug("Inside handleFlush")
	r.Respond()
}

func (f *fs) handleRelease(r *fuse.ReleaseRequest) {
	logger.Debug("Inside handleRelease")
	f.handles.removeFileHandle(r.Handle)
	r.Respond()
}

func (f *fs) handleInterrupt(r *fuse.InterruptRequest) {
	logger.Debug("Inside handleInterrupt")
	// TODO: Just passing on this for now.  Need to figure out what really needs to be done here
	r.Respond()
}

func (f *fs) handleForget(r *fuse.ForgetRequest) {
	logger.Debug("Inside handleForget")
	// TODO: Just passing on this for now.  Need to figure out what really needs to be done here
	r.Respond()
}

func (f *fs) handleRemove(req *fuse.RemoveRequest) {
	// TODO: Handle dir deletions correctly
	logger.Debug("Inside handleRemove", zap.Any("request", req))
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Remove(f.getContext())
	if err != nil {
		logger.Debug("Remove failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if err = stream.Send(&formicproto.RemoveRequest{Rpcid: 1, Parent: uint64(req.Node), Name: req.Name}); err != nil {
		logger.Debug("Remove failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	removeResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Remove failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if removeResp.Err == "not empty" {
		logger.Debug("Remove failed", zap.String("Err", removeResp.Err))
		req.RespondError(fuse.Errno(syscall.ENOTEMPTY))
		return
	}
	if removeResp.Err != "" {
		logger.Debug("Remove failed", zap.String("Err", removeResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	logger.Debug("handleRemove returning")
	req.Respond()
}

func (f *fs) handleAccess(r *fuse.AccessRequest) {
	logger.Debug("Inside handleAccess")
	// TODO: Add real access support, for now allows everything
	r.Respond()
}

// TODO: Implement the following functions (and make sure to comment out the case)
// Note: All handle functions should call r.Respond or r.Respond error before returning

func (f *fs) handleMknod(r *fuse.MknodRequest) {
	logger.Debug("Inside handleMknod")
	// NOTE: We probably will not need this since we implement Create
	r.RespondError(fuse.EIO)
}

/*
func (f *fs) handleInit(r *fuse.InitRequest) {
	logger.Debug("Inside handleInit")
	r.RespondError(fuse.ENOSYS)
}
*/

func (f *fs) handleStatfs(req *fuse.StatfsRequest) {
	logger.Debug("Inside handleStatfs", zap.Any("request", req))
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Statfs(f.getContext())
	if err != nil {
		logger.Debug("Statfs failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if err = stream.Send(&formicproto.StatfsRequest{Rpcid: 1}); err != nil {
		logger.Debug("Statfs failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	statfsResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Statfs failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if statfsResp.Err != "" {
		logger.Debug("Statfs failed", zap.String("Err", statfsResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.StatfsResponse{
		Blocks:  statfsResp.Blocks,
		Bfree:   statfsResp.Bfree,
		Bavail:  statfsResp.Bavail,
		Files:   statfsResp.Files,
		Ffree:   statfsResp.Ffree,
		Bsize:   statfsResp.Bsize,
		Namelen: statfsResp.Namelen,
		Frsize:  statfsResp.Frsize,
	}
	req.Respond(resp)
}

func (f *fs) handleSymlink(req *fuse.SymlinkRequest) {
	logger.Debug("Inside handleSymlink", zap.Any("request", req))
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Symlink(f.getContext())
	if err != nil {
		logger.Debug("Symlink failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if err = stream.Send(&formicproto.SymlinkRequest{Rpcid: 1, Parent: uint64(req.Node), Name: req.NewName, Target: req.Target, Uid: req.Uid, Gid: req.Gid}); err != nil {
		logger.Debug("Symlink failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	symlinkResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Symlink failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if symlinkResp.Err != "" {
		logger.Debug("Symlink failed", zap.String("Err", symlinkResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.SymlinkResponse{}
	resp.Node = fuse.NodeID(symlinkResp.Attr.Inode)
	copyNewAttr(&resp.Attr, symlinkResp.Attr)
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime
	logger.Debug("handleSymlink returning", zap.Any("response", resp))
	req.Respond(resp)
}

func (f *fs) handleReadlink(req *fuse.ReadlinkRequest) {
	logger.Debug("Inside handleReadlink", zap.Any("request", req))
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Readlink(f.getContext())
	if err != nil {
		logger.Debug("Readlink failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if err = stream.Send(&formicproto.ReadlinkRequest{Rpcid: 1, Inode: uint64(req.Node)}); err != nil {
		logger.Debug("Readlink failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	readlinkResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Readlink failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if readlinkResp.Err != "" {
		logger.Debug("Readlink failed", zap.String("Err", readlinkResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	logger.Debug("handleReadlink returning", zap.Any("response", readlinkResp.Target))
	req.Respond(readlinkResp.Target)
}

func (f *fs) handleLink(r *fuse.LinkRequest) {
	logger.Debug("Inside handleLink")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleGetxattr(req *fuse.GetxattrRequest) {
	// TODO: Possiblity short circuit excessive xattr calls we don't really
	// support.
	logger.Debug("Inside handleGetxattr", zap.Any("request", req))
	if req.Name == "cfs.fsid" {
		resp := &fuse.GetxattrResponse{Xattr: []byte(f.fsid)}
		logger.Debug("handleGetxattr returning", zap.Any("response", resp))
		req.Respond(resp)
		return
	}
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Getxattr(f.getContext())
	if err != nil {
		logger.Debug("Getxattr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	// TODO: Best I can tell, xattr size and position were never implemented.
	// The whole xattr is always returned.
	if err = stream.Send(&formicproto.GetxattrRequest{Rpcid: 1, Inode: uint64(req.Node), Name: req.Name, Size: req.Size, Position: req.Position}); err != nil {
		logger.Debug("Getxattr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	getxattrResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Getxattr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if getxattrResp.Err != "" {
		logger.Debug("Getxattr failed", zap.String("Err", getxattrResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.GetxattrResponse{Xattr: getxattrResp.Xattr}
	logger.Debug("handleGetxattr returning", zap.Any("response", resp))
	req.Respond(resp)
}

func (f *fs) handleListxattr(req *fuse.ListxattrRequest) {
	logger.Debug("Inside handleListxattr", zap.Any("request", req))
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Listxattr(f.getContext())
	if err != nil {
		logger.Debug("Listxattr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	// TODO: Best I can tell, xattr size and position were never implemented.
	// The whole list of xattrs are always returned.
	if err = stream.Send(&formicproto.ListxattrRequest{Rpcid: 1, Inode: uint64(req.Node), Size: req.Size, Position: req.Position}); err != nil {
		logger.Debug("Listxattr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	listxattrResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Listxattr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if listxattrResp.Err != "" {
		logger.Debug("Listxattr failed", zap.String("Err", listxattrResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	resp := &fuse.ListxattrResponse{Xattr: listxattrResp.Xattr}
	logger.Debug("handleListxattr returning", zap.Any("response", resp))
	req.Respond(resp)
}

func (f *fs) handleSetxattr(req *fuse.SetxattrRequest) {
	// TODO: Possiblity disallow some xattr sets, like security.* system.* if
	// that makes sense.
	logger.Debug("Inside handleSetxattr", zap.Any("request", req))
	if strings.HasPrefix(req.Name, "cfs.") {
		// Silently discard sets to "our" namespace.
		req.Respond()
		return
	}
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Setxattr(f.getContext())
	if err != nil {
		logger.Debug("Setxattr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	// TODO: Best I can tell, xattr position and flags were never implemented.
	// The whole xattr is always set and flags are ignored.
	if err = stream.Send(&formicproto.SetxattrRequest{Rpcid: 1, Inode: uint64(req.Node), Name: req.Name, Value: req.Xattr, Position: req.Position, Flags: req.Flags}); err != nil {
		logger.Debug("Setxattr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	setxattrResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Setxattr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if setxattrResp.Err != "" {
		logger.Debug("Setxattr failed", zap.String("Err", setxattrResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	logger.Debug("handleSetxattr returning")
	req.Respond()
}

func (f *fs) handleRemovexattr(req *fuse.RemovexattrRequest) {
	// TODO: Possiblity disallow some xattr removes, like security.* system.*
	// if that makes sense.
	logger.Debug("Inside handleRemovexattr", zap.Any("request", req))
	// TODO: All these checks need to be server side.
	if strings.HasPrefix(req.Name, "cfs.") {
		// Silently discard removes to "our" namespace.
		req.Respond()
		return
	}
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Removexattr(f.getContext())
	if err != nil {
		logger.Debug("Removexattr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	// TODO: Best I can tell, xattr size and position were never implemented.
	// The whole list of xattrs are always returned.
	if err = stream.Send(&formicproto.RemovexattrRequest{Rpcid: 1, Inode: uint64(req.Node), Name: req.Name}); err != nil {
		logger.Debug("Removexattr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	removexattrResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Removexattr failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if removexattrResp.Err != "" {
		logger.Debug("Removexattr failed", zap.String("Err", removexattrResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	logger.Debug("handleRemovexattr returning")
	req.Respond()
}

func (f *fs) handleDestroy(r *fuse.DestroyRequest) {
	logger.Debug("Inside handleDestroy")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleRename(req *fuse.RenameRequest) {
	logger.Debug("Inside handleRename", zap.Any("request", req))
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Rename(f.getContext())
	if err != nil {
		logger.Debug("Rename failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	// TODO: Best I can tell, xattr size and position were never implemented.
	// The whole list of xattrs are always returned.
	if err = stream.Send(&formicproto.RenameRequest{Rpcid: 1, OldParent: uint64(req.Node), NewParent: uint64(req.NewDir), OldName: req.OldName, NewName: req.NewName}); err != nil {
		logger.Debug("Rename failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	renameResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Rename failed", zap.Error(err))
		req.RespondError(fuse.EIO)
		return
	}
	if renameResp.Err != "" {
		logger.Debug("Rename failed", zap.String("Err", renameResp.Err))
		req.RespondError(fuse.EIO)
		return
	}
	logger.Debug("handleRename returning")
	req.Respond()
}

func (f *fs) handleFsync(r *fuse.FsyncRequest) {
	logger.Debug("Inside handleFsync")
	r.RespondError(fuse.ENOSYS)
}
