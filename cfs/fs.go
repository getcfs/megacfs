package main

import (
	"log"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"golang.org/x/net/context"

	"github.com/getcfs/fuse"
	"github.com/getcfs/fuse/fuseutil"
	pb "github.com/getcfs/megacfs/formic/proto"
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
	switch r := r.(type) {
	default:
		log.Printf("Unhandled request: %s", r)
		r.RespondError(fuse.ENOSYS)

	case *fuse.GetattrRequest:
		f.handleGetattr(r)

	case *fuse.LookupRequest:
		f.handleLookup(r)

	case *fuse.MkdirRequest:
		f.handleMkdir(r)

	case *fuse.OpenRequest:
		f.handleOpen(r)

	case *fuse.ReadRequest:
		f.handleRead(r)

	case *fuse.WriteRequest:
		f.handleWrite(r)

	case *fuse.CreateRequest:
		f.handleCreate(r)

	case *fuse.SetattrRequest:
		f.handleSetattr(r)

	case *fuse.ReleaseRequest:
		f.handleRelease(r)

	case *fuse.FlushRequest:
		f.handleFlush(r)

	case *fuse.InterruptRequest:
		f.handleInterrupt(r)

	case *fuse.ForgetRequest:
		f.handleForget(r)

	case *fuse.RemoveRequest:
		f.handleRemove(r)

	case *fuse.AccessRequest:
		f.handleAccess(r)

	case *fuse.SymlinkRequest:
		f.handleSymlink(r)

	case *fuse.ReadlinkRequest:
		f.handleReadlink(r)

	case *fuse.GetxattrRequest:
		f.handleGetxattr(r)

	case *fuse.ListxattrRequest:
		f.handleListxattr(r)

	case *fuse.SetxattrRequest:
		f.handleSetxattr(r)

	case *fuse.RemovexattrRequest:
		f.handleRemovexattr(r)

	case *fuse.RenameRequest:
		f.handleRename(r)

	case *fuse.StatfsRequest:
		f.handleStatfs(r)

		/*
			case *fuse.InitRequest:
				f.handleInit(r)

			case *fuse.MknodRequest:
				f.handleMknod(r)


			case *fuse.LinkRequest:
				f.handleLink(r)

			case *fuse.DestroyRequest:
				f.handleDestroy(r)

			case *fuse.FsyncRequest:
				f.handleFsync(r)
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
	log.Println("Inside InitFs")
	_, err := f.rpc.api().InitFs(f.getContext(), &pb.InitFsRequest{})
	return err
}

func (f *fs) handleGetattr(r *fuse.GetattrRequest) {
	log.Println("Inside handleGetattr")
	log.Println(r)
	resp := &fuse.GetattrResponse{}

	a, err := f.rpc.api().GetAttr(f.getContext(), &pb.GetAttrRequest{Inode: uint64(r.Node)})
	if err != nil {
		log.Printf("GetAttr fail: %s", err)
		r.RespondError(fuse.EIO)
		return
	}
	copyAttr(&resp.Attr, a.Attr)
	// TODO: should we make these configurable?
	resp.Attr.Valid = attrValidTime

	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleLookup(r *fuse.LookupRequest) {
	log.Println("Inside handleLookup")
	log.Printf("Running Lookup for %s", r.Name)
	log.Println(r)
	resp := &fuse.LookupResponse{}

	l, err := f.rpc.api().Lookup(f.getContext(), &pb.LookupRequest{Name: r.Name, Parent: uint64(r.Node)})

	if grpc.Code(err) == codes.NotFound {
		log.Printf("ENOENT Lookup(%s)", r.Name)
		r.RespondError(fuse.ENOENT)
		return
	}
	if err != nil {
		log.Printf("Lookup failed(%s): %s", r.Name, err)
		r.RespondError(fuse.EIO)
		return
	}
	resp.Node = fuse.NodeID(l.Attr.Inode)
	copyAttr(&resp.Attr, l.Attr)
	// TODO: should we make these configureable?
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime

	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleMkdir(r *fuse.MkdirRequest) {
	log.Println("Inside handleMkdir")
	log.Println(r)
	resp := &fuse.MkdirResponse{}

	m, err := f.rpc.api().MkDir(f.getContext(), &pb.MkDirRequest{Name: r.Name, Parent: uint64(r.Node), Attr: &pb.Attr{Uid: r.Uid, Gid: r.Gid, Mode: uint32(r.Mode)}})
	if err != nil {
		log.Printf("Mkdir failed(%s): %s", r.Name, err)
		r.RespondError(fuse.EIO)
		return
	}
	// If the name is empty, then the dir already exists
	if m.Name != r.Name {
		log.Printf("EEXIST Mkdir(%s)", r.Name)
		r.RespondError(fuse.EEXIST)
		return
	}
	resp.Node = fuse.NodeID(m.Attr.Inode)
	copyAttr(&resp.Attr, m.Attr)
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime

	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleOpen(r *fuse.OpenRequest) {
	log.Println("Inside handleOpen")
	log.Println(r)
	resp := &fuse.OpenResponse{}
	// For now use the inode as the file handle
	resp.Handle = f.handles.newFileHandle(r.Node)
	resp.Flags |= fuse.OpenKeepCache
	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleRead(r *fuse.ReadRequest) {
	log.Println("Inside handleRead")
	log.Println(r)
	resp := &fuse.ReadResponse{Data: make([]byte, r.Size)}
	if r.Dir {
		// handle directory listing
		data := f.handles.getReadCache(r.Handle)
		if data == nil {
			d, err := f.rpc.api().ReadDirAll(f.getContext(), &pb.ReadDirAllRequest{Inode: uint64(r.Node)})
			if err != nil {
				log.Printf("Read on dir failed: %s", err)
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

	// handle file read
	data, err := f.rpc.api().Read(f.getContext(), &pb.ReadRequest{
		Inode:  uint64(r.Node),
		Offset: int64(r.Offset),
		Size:   int64(r.Size),
	})
	if err != nil {
		log.Printf("Read on file failed: %s", err)
		r.RespondError(fuse.EIO)
		return
	}
	copy(resp.Data, data.Payload)
	r.Respond(resp)
}

func (f *fs) handleWrite(r *fuse.WriteRequest) {
	log.Println("Inside handleWrite")
	log.Printf("Writing %d bytes at offset %d", len(r.Data), r.Offset)
	log.Println(r)
	// TODO: Implement write
	// Currently this is stupid simple and doesn't handle all the possibilities
	resp := &fuse.WriteResponse{}
	w, err := f.rpc.api().Write(f.getContext(), &pb.WriteRequest{Inode: uint64(r.Node), Offset: r.Offset, Payload: r.Data})
	if err != nil {
		log.Printf("Write to file failed: %s", err)
		r.RespondError(fuse.EIO)
		return
	}
	if w.Status != 0 {
		log.Printf("Write status non zero(%d)\n", w.Status)
	}
	resp.Size = len(r.Data)
	r.Respond(resp)
}

func (f *fs) handleCreate(r *fuse.CreateRequest) {
	log.Println("Inside handleCreate")
	log.Println(r)
	resp := &fuse.CreateResponse{}
	c, err := f.rpc.api().Create(f.getContext(), &pb.CreateRequest{Parent: uint64(r.Node), Name: r.Name, Attr: &pb.Attr{Uid: r.Uid, Gid: r.Gid, Mode: uint32(r.Mode)}})
	if err != nil {
		log.Printf("Failed to create file: %s", err)
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
	log.Println("Inside handleSetattr")
	log.Println(r)
	resp := &fuse.SetattrResponse{}
	resp.Attr.Inode = uint64(r.Node)
	a := &pb.Attr{
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
	setAttrResp, err := f.rpc.api().SetAttr(f.getContext(), &pb.SetAttrRequest{Attr: a, Valid: uint32(r.Valid)})
	if err != nil {
		log.Printf("Setattr failed: %s", err)
		r.RespondError(fuse.EIO)
		return
	}
	copyAttr(&resp.Attr, setAttrResp.Attr)
	resp.Attr.Valid = attrValidTime
	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleFlush(r *fuse.FlushRequest) {
	log.Println("Inside handleFlush")
	r.Respond()
}

func (f *fs) handleRelease(r *fuse.ReleaseRequest) {
	log.Println("Inside handleRelease")
	f.handles.removeFileHandle(r.Handle)
	r.Respond()
}

func (f *fs) handleInterrupt(r *fuse.InterruptRequest) {
	log.Println("Inside handleInterrupt")
	// TODO: Just passing on this for now.  Need to figure out what really needs to be done here
	r.Respond()
}

func (f *fs) handleForget(r *fuse.ForgetRequest) {
	log.Println("Inside handleForget")
	// TODO: Just passing on this for now.  Need to figure out what really needs to be done here
	r.Respond()
}

func (f *fs) handleRemove(r *fuse.RemoveRequest) {
	// TODO: Handle dir deletions correctly
	log.Println("Inside handleRemove")
	log.Println(r)
	_, err := f.rpc.api().Remove(f.getContext(), &pb.RemoveRequest{Parent: uint64(r.Node), Name: r.Name})
	if err != nil {
		log.Printf("Failed to delete file: %s", err)
		r.RespondError(fuse.EIO)
		return
	}
	r.Respond()
}

func (f *fs) handleAccess(r *fuse.AccessRequest) {
	log.Println("Inside handleAccess")
	// TODO: Add real access support, for now allows everything
	r.Respond()
}

// TODO: Implement the following functions (and make sure to comment out the case)
// Note: All handle functions should call r.Respond or r.Respond error before returning

func (f *fs) handleMknod(r *fuse.MknodRequest) {
	log.Println("Inside handleMknod")
	// NOTE: We probably will not need this since we implement Create
	r.RespondError(fuse.EIO)
}

/*
func (f *fs) handleInit(r *fuse.InitRequest) {
	log.Println("Inside handleInit")
	r.RespondError(fuse.ENOSYS)
}
*/

func (f *fs) handleStatfs(r *fuse.StatfsRequest) {
	log.Println("Inside handleStatfs")
	log.Println(r)
	resp, err := f.rpc.api().Statfs(f.getContext(), &pb.StatfsRequest{})
	if err != nil {
		log.Printf("Failed to Statfs : %s", err)
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
	log.Println("Inside handleSymlink")
	log.Println(r)
	resp := &fuse.SymlinkResponse{}
	symlink, err := f.rpc.api().Symlink(f.getContext(), &pb.SymlinkRequest{Parent: uint64(r.Node), Name: r.NewName, Target: r.Target, Uid: r.Uid, Gid: r.Gid})
	if err != nil {
		log.Printf("Symlink failed: %s", err)
		r.RespondError(fuse.EIO)
		return
	}
	resp.Node = fuse.NodeID(symlink.Attr.Inode)
	copyAttr(&resp.Attr, symlink.Attr)
	resp.Attr.Valid = attrValidTime
	resp.EntryValid = entryValidTime
	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleReadlink(r *fuse.ReadlinkRequest) {
	log.Println("Inside handleReadlink")
	log.Println(r)
	resp, err := f.rpc.api().Readlink(f.getContext(), &pb.ReadlinkRequest{Inode: uint64(r.Node)})
	if err != nil {
		log.Printf("Readlink failed: %s", err)
		r.RespondError(fuse.EIO)
		return
	}
	log.Println(resp)
	r.Respond(resp.Target)
}

func (f *fs) handleLink(r *fuse.LinkRequest) {
	log.Println("Inside handleLink")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleGetxattr(r *fuse.GetxattrRequest) {
	log.Println("Inside handleGetxattr")
	log.Println(r)
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
	req := &pb.GetxattrRequest{
		Inode:    uint64(r.Node),
		Name:     r.Name,
		Size:     r.Size,
		Position: r.Position,
	}
	resp, err := f.rpc.api().Getxattr(f.getContext(), req)
	if err != nil {
		log.Printf("Getxattr failed: %s", err)
		r.RespondError(fuse.EIO)
		return
	}
	fuseResp := &fuse.GetxattrResponse{Xattr: resp.Xattr}
	log.Println(fuseResp)
	r.Respond(fuseResp)
}

func (f *fs) handleListxattr(r *fuse.ListxattrRequest) {
	log.Println("Inside handleListxattr")
	log.Println(r)
	req := &pb.ListxattrRequest{
		Inode:    uint64(r.Node),
		Size:     r.Size,
		Position: r.Position,
	}
	resp, err := f.rpc.api().Listxattr(f.getContext(), req)
	if err != nil {
		log.Printf("Listxattr failed: %s", err)
		r.RespondError(fuse.EIO)
		return
	}
	fuseResp := &fuse.ListxattrResponse{Xattr: resp.Xattr}
	log.Println(fuseResp)
	r.Respond(fuseResp)
}

func (f *fs) handleSetxattr(r *fuse.SetxattrRequest) {
	log.Println("Inside handleSetxattr")
	log.Println(r)
	if r.Name == "system.posix_acl_access" || r.Name == "system.posix_acl_default" {
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
		log.Printf("Setxattr failed: %s", err)
		r.RespondError(fuse.EIO)
		return
	}
	r.Respond()
}

func (f *fs) handleRemovexattr(r *fuse.RemovexattrRequest) {
	log.Println("Inside handleRemovexattr")
	log.Println(r)
	req := &pb.RemovexattrRequest{
		Inode: uint64(r.Node),
		Name:  r.Name,
	}
	_, err := f.rpc.api().Removexattr(f.getContext(), req)
	if err != nil {
		log.Printf("Removexattr failed: %s", err)
		r.RespondError(fuse.EIO)
		return
	}
	r.Respond()
}

func (f *fs) handleDestroy(r *fuse.DestroyRequest) {
	log.Println("Inside handleDestroy")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleRename(r *fuse.RenameRequest) {
	log.Println("Inside handleRename")
	log.Println(r)
	_, err := f.rpc.api().Rename(f.getContext(), &pb.RenameRequest{OldParent: uint64(r.Node), NewParent: uint64(r.NewDir), OldName: r.OldName, NewName: r.NewName})
	if err != nil {
		log.Printf("Rename failed: %s", err)
		r.RespondError(fuse.EIO)
		return
	}
	r.Respond()
}

func (f *fs) handleFsync(r *fuse.FsyncRequest) {
	log.Println("Inside handleFsync")
	r.RespondError(fuse.ENOSYS)
}
