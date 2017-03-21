package main

import (
	"os"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"golang.org/x/net/context"

	"bazil.org/fuse"
	"bazil.org/fuse/fuseutil"
	pb "github.com/getcfs/megacfs/formic/formicproto"
	"github.com/getcfs/megacfs/formic/newproto"
	"go.uber.org/zap"
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

func copyNewAttr(dst *fuse.Attr, src *newproto.Attr) {
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

func copyAttr(dst *fuse.Attr, src *pb.Attr) {
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
	logger.Debug("Inside InitFs")
	_, err := f.rpc.api().InitFs(f.getContext(), &pb.InitFsRequest{})
	return err
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
	if err = stream.Send(&newproto.GetAttrRequest{Rpcid: 1, Inode: uint64(r.Node)}); err != nil {
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

func (f *fs) handleLookup(r *fuse.LookupRequest) {
	logger.Debug("Inside handleLookup", zap.String("name", r.Name), zap.Any("request", r))
	resp := &fuse.LookupResponse{}

	l, err := f.rpc.api().Lookup(f.getContext(), &pb.LookupRequest{Name: r.Name, Parent: uint64(r.Node)})

	if grpc.Code(err) == codes.NotFound {
		logger.Debug("ENOENT Lookup", zap.String("name", r.Name))
		r.RespondError(fuse.ENOENT)
		return
	}
	if err != nil {
		logger.Debug("Lookup failed", zap.String("name", r.Name), zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	resp.Node = fuse.NodeID(l.Attr.Inode)
	copyAttr(&resp.Attr, l.Attr)
	// TODO: should we make these configureable?
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime

	logger.Debug("handleLookup returning", zap.Any("response", resp))
	r.Respond(resp)
}

func (f *fs) handleMkdir(r *fuse.MkdirRequest) {
	logger.Debug("Inside handleMkdir", zap.Any("request", r))
	resp := &fuse.MkdirResponse{}

	m, err := f.rpc.api().MkDir(f.getContext(), &pb.MkDirRequest{Name: r.Name, Parent: uint64(r.Node), Attr: &pb.Attr{Uid: r.Uid, Gid: r.Gid, Mode: uint32(r.Mode)}})
	if err != nil {
		logger.Debug("Mkdir failed", zap.String("name", r.Name), zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	// If the name is empty, then the dir already exists
	if m.Name != r.Name {
		logger.Debug("EEXIST Mkdir", zap.String("name", r.Name))
		r.RespondError(fuse.EEXIST)
		return
	}
	resp.Node = fuse.NodeID(m.Attr.Inode)
	copyAttr(&resp.Attr, m.Attr)
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime

	logger.Debug("handleMkdir returning", zap.Any("response", resp))
	r.Respond(resp)
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

func (f *fs) handleRead(r *fuse.ReadRequest) {
	logger.Debug("Inside handleRead", zap.Any("request", r))
	if r.Dir {
		resp := &fuse.ReadResponse{Data: make([]byte, 0, r.Size)}
		// handle directory listing
		data := f.handles.getReadCache(r.Handle)
		if data == nil {
			d, err := f.rpc.api().ReadDirAll(f.getContext(), &pb.ReadDirAllRequest{Inode: uint64(r.Node)})
			if err != nil {
				logger.Debug("Read on dir failed", zap.Error(err))
				r.RespondError(fuse.EIO)
				return
			}
			data = fuse.AppendDirent(data, fuse.Dirent{
				Name:  ".",
				Inode: uint64(r.Node),
				Type:  fuse.DT_Dir,
			})
			data = fuse.AppendDirent(data, fuse.Dirent{
				Name:  "..",
				Inode: 1, // TODO: not sure what value this should be, but this seems to work fine.
				Type:  fuse.DT_Dir,
			})
			for _, de := range d.DirEntries {
				if de == nil || de.Name == "" {
					continue
				}
				data = fuse.AppendDirent(data, fuse.Dirent{
					Name:  de.Name,
					Inode: 1, // TODO: seems to work fine with any non-zero inode.  why?
					Type:  fuse.DirentType(de.Type),
				})
			}
			f.handles.cacheRead(r.Handle, data)
		}
		fuseutil.HandleRead(r, resp, data)
		r.Respond(resp)
		return
	}
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := f.rpc.newClient.Read(f.getContext())
	if err != nil {
		logger.Debug("Read failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	if err = stream.Send(&newproto.ReadRequest{Rpcid: 1, Inode: uint64(r.Node), Offset: int64(r.Offset), Size: int64(r.Size)}); err != nil {
		logger.Debug("Read failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	readResp, err := stream.Recv()
	if err != nil {
		logger.Debug("Read failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	if readResp.Err != "" {
		logger.Debug("Read failed", zap.String("Err", readResp.Err))
		r.RespondError(fuse.EIO)
		return
	}
	r.Respond(&fuse.ReadResponse{Data: readResp.Payload})
}

func (f *fs) handleWrite(r *fuse.WriteRequest) {
	logger.Debug("Inside handleWrite", zap.Any("request", r))
	logger.Debug("Writing bytes at offset", zap.Int("bytes", len(r.Data)), zap.Int64("offset", r.Offset))
	// TODO: Implement write
	// Currently this is stupid simple and doesn't handle all the possibilities
	resp := &fuse.WriteResponse{}
	w, err := f.rpc.api().Write(f.getContext(), &pb.WriteRequest{Inode: uint64(r.Node), Offset: r.Offset, Payload: r.Data})
	if err != nil {
		logger.Debug("Write to file failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	if w.Status != 0 {
		logger.Debug("Write status non zero", zap.Int32("status", w.Status))
	}
	resp.Size = len(r.Data)
	r.Respond(resp)
}

func (f *fs) handleCreate(r *fuse.CreateRequest) {
	logger.Debug("Inside handleCreate", zap.Any("request", r))
	resp := &fuse.CreateResponse{}
	c, err := f.rpc.api().Create(f.getContext(), &pb.CreateRequest{Parent: uint64(r.Node), Name: r.Name, Attr: &pb.Attr{Uid: r.Uid, Gid: r.Gid, Mode: uint32(r.Mode)}})
	if err != nil {
		logger.Debug("Failed to create file", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	resp.Node = fuse.NodeID(c.Attr.Inode)
	copyAttr(&resp.Attr, c.Attr)
	resp.EntryValid = entryValidTime
	resp.Attr.Valid = attrValidTime
	copyAttr(&resp.LookupResponse.Attr, c.Attr)
	resp.LookupResponse.EntryValid = entryValidTime
	resp.LookupResponse.Attr.Valid = attrValidTime
	r.Respond(resp)
}

func (f *fs) handleSetattr(r *fuse.SetattrRequest) {
	logger.Debug("Inside handleSetattr", zap.Any("request", r))
	resp := &fuse.SetattrResponse{}
	resp.Attr.Inode = uint64(r.Node)
	a := &newproto.Attr{
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
	if err = stream.Send(&newproto.SetAttrRequest{Rpcid: 1, Attr: a, Valid: uint32(r.Valid)}); err != nil {
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

func (f *fs) handleRemove(r *fuse.RemoveRequest) {
	// TODO: Handle dir deletions correctly
	logger.Debug("Inside handleRemove", zap.Any("request", r))
	_, err := f.rpc.api().Remove(f.getContext(), &pb.RemoveRequest{Parent: uint64(r.Node), Name: r.Name})
	if err != nil {
		logger.Debug("Failed to delete file", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	r.Respond()
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

func (f *fs) handleStatfs(r *fuse.StatfsRequest) {
	logger.Debug("Inside handleStatfs", zap.Any("request", r))
	resp, err := f.rpc.api().Statfs(f.getContext(), &pb.StatfsRequest{})
	if err != nil {
		logger.Debug("Failed to Statfs", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	fuseResp := &fuse.StatfsResponse{
		Blocks:  resp.Blocks,
		Bfree:   resp.Bfree,
		Bavail:  resp.Bavail,
		Files:   resp.Files,
		Ffree:   resp.Ffree,
		Bsize:   resp.Bsize,
		Namelen: resp.Namelen,
		Frsize:  resp.Frsize,
	}
	r.Respond(fuseResp)
}

func (f *fs) handleSymlink(r *fuse.SymlinkRequest) {
	logger.Debug("Inside handleSymlink", zap.Any("request", r))
	resp := &fuse.SymlinkResponse{}
	symlink, err := f.rpc.api().Symlink(f.getContext(), &pb.SymlinkRequest{Parent: uint64(r.Node), Name: r.NewName, Target: r.Target, Uid: r.Uid, Gid: r.Gid})
	if err != nil {
		logger.Debug("Symlink failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	resp.Node = fuse.NodeID(symlink.Attr.Inode)
	copyAttr(&resp.Attr, symlink.Attr)
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime
	logger.Debug("handleSymlink returning", zap.Any("response", resp))
	r.Respond(resp)
}

func (f *fs) handleReadlink(r *fuse.ReadlinkRequest) {
	logger.Debug("Inside handleReadlink", zap.Any("request", r))
	resp, err := f.rpc.api().Readlink(f.getContext(), &pb.ReadlinkRequest{Inode: uint64(r.Node)})
	if err != nil {
		logger.Debug("Readlink failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	logger.Debug("handleReadlink returning", zap.Any("reponse", resp))
	r.Respond(resp.Target)
}

func (f *fs) handleLink(r *fuse.LinkRequest) {
	logger.Debug("Inside handleLink")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleGetxattr(r *fuse.GetxattrRequest) {
	logger.Debug("Inside handleGetxattr", zap.Any("request", r))
	if r.Name == "security.capability" {
		// Ignore this for now
		// TODO: Figure out if we want to allow this or not
		r.RespondError(fuse.ENOSYS)
		return
	}
	if r.Name == "system.posix_acl_access" || r.Name == "system.posix_acl_default" {
		r.RespondError(fuse.ENOSYS)
		return
	}
	if r.Name == "cfs.fsid" {
		r.Respond(&fuse.GetxattrResponse{Xattr: []byte(f.fsid)})
		return
	}
	req := &pb.GetxattrRequest{
		Inode:    uint64(r.Node),
		Name:     r.Name,
		Size:     r.Size,
		Position: r.Position,
	}
	resp, err := f.rpc.api().Getxattr(f.getContext(), req)
	if err != nil {
		logger.Debug("Getxattr failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	fuseResp := &fuse.GetxattrResponse{Xattr: resp.Xattr}
	logger.Debug("handleGetxattr returning", zap.Any("response", fuseResp))
	r.Respond(fuseResp)
}

func (f *fs) handleListxattr(r *fuse.ListxattrRequest) {
	logger.Debug("Inside handleListxattr", zap.Any("request", r))
	req := &pb.ListxattrRequest{
		Inode:    uint64(r.Node),
		Size:     r.Size,
		Position: r.Position,
	}
	resp, err := f.rpc.api().Listxattr(f.getContext(), req)
	if err != nil {
		logger.Debug("Listxattr failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	fuseResp := &fuse.ListxattrResponse{Xattr: resp.Xattr}
	logger.Debug("handleListxattr returning", zap.Any("response", fuseResp))
	r.Respond(fuseResp)
}

func (f *fs) handleSetxattr(r *fuse.SetxattrRequest) {
	logger.Debug("Inside handleSetxattr", zap.Any("request", r))
	if r.Name == "system.posix_acl_access" || r.Name == "system.posix_acl_default" {
		r.RespondError(fuse.ENOENT)
		return
	}
	if strings.HasPrefix(r.Name, "cfs.") {
		r.RespondError(fuse.ENOSYS)
		return
	}
	req := &pb.SetxattrRequest{
		Inode:    uint64(r.Node),
		Name:     r.Name,
		Value:    r.Xattr,
		Position: r.Position,
		Flags:    r.Flags,
	}
	_, err := f.rpc.api().Setxattr(f.getContext(), req)
	if err != nil {
		logger.Debug("Setxattr failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	r.Respond()
}

func (f *fs) handleRemovexattr(r *fuse.RemovexattrRequest) {
	logger.Debug("Inside handleRemovexattr", zap.Any("request", r))
	if strings.HasPrefix(r.Name, "cfs.") {
		r.RespondError(fuse.ENOSYS)
		return
	}
	req := &pb.RemovexattrRequest{
		Inode: uint64(r.Node),
		Name:  r.Name,
	}
	_, err := f.rpc.api().Removexattr(f.getContext(), req)
	if err != nil {
		logger.Debug("Removexattr failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	r.Respond()
}

func (f *fs) handleDestroy(r *fuse.DestroyRequest) {
	logger.Debug("Inside handleDestroy")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleRename(r *fuse.RenameRequest) {
	logger.Debug("Inside handleRename", zap.Any("request", r))
	_, err := f.rpc.api().Rename(f.getContext(), &pb.RenameRequest{OldParent: uint64(r.Node), NewParent: uint64(r.NewDir), OldName: r.OldName, NewName: r.NewName})
	if err != nil {
		logger.Debug("Rename failed", zap.Error(err))
		r.RespondError(fuse.EIO)
		return
	}
	r.Respond()
}

func (f *fs) handleFsync(r *fuse.FsyncRequest) {
	logger.Debug("Inside handleFsync")
	r.RespondError(fuse.ENOSYS)
}
