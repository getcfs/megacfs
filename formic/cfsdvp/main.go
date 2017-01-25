package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/docker/go-plugins-helpers/sdk"
)

const (
	// DefaultDockerRootDirectory is the default directory where volumes will be created.
	DefaultDockerRootDirectory = "/var/lib/docker-volumes"
	manifest                   = `{"Implements": ["VolumeDriver"]}`
	createPath                 = "/VolumeDriver.Create"
	remotePath                 = "/VolumeDriver.Remove"
	hostVirtualPath            = "/VolumeDriver.Path"
	mountPath                  = "/VolumeDriver.Mount"
	unmountPath                = "/VolumeDriver.Unmount"
)

// Request is the structure that docker's requests are deserialized to.
type Request struct {
	Name    string
	Options map[string]string `json:"Opts,omitempty"`
}

// Response is the strucutre that the plugin's responses are serialized to.
type Response struct {
	Mountpoint string
	Err        string
}

// Driver represent the interface a driver must fulfill.
type Driver interface {
	Create(Request) Response
	Remove(Request) Response
	Path(Request) Response
	Mount(Request) Response
	Unmount(Request) Response
}

// Handler forwards requests and responses between the docker daemon and the plugin.
type Handler struct {
	driver Driver
	sdk.Handler
}

type actionHandler func(Request) Response

// NewHandler initializes the request handler with a driver implementation.
func NewHandler(driver Driver) *Handler {
	h := &Handler{driver, sdk.NewHandler(manifest)}
	h.initMux()
	return h
}

func (h *Handler) initMux() {
	h.handle(createPath, func(req Request) Response {
		return h.driver.Create(req)
	})

	h.handle(remotePath, func(req Request) Response {
		return h.driver.Remove(req)
	})

	h.handle(hostVirtualPath, func(req Request) Response {
		return h.driver.Path(req)
	})

	h.handle(mountPath, func(req Request) Response {
		return h.driver.Mount(req)
	})

	h.handle(unmountPath, func(req Request) Response {
		return h.driver.Unmount(req)
	})
}

func (h *Handler) handle(name string, actionCall actionHandler) {
	h.HandleFunc(name, func(w http.ResponseWriter, r *http.Request) {
		var req Request
		if err := sdk.DecodeRequest(w, r, &req); err != nil {
			return
		}

		res := actionCall(req)

		sdk.EncodeResponse(w, res, res.Err)
	})
}

const (
	SocketPath = "/run/docker/plugins/cfs.sock"
	MountDir   = "/var/lib/docker/cfs"
)

type CFSDriver struct {
	mutex  *sync.Mutex
	mounts map[string]int
}

func newCFSDriver() CFSDriver {
	return CFSDriver{
		mutex:  &sync.Mutex{},
		mounts: map[string]int{},
	}
}

func (d CFSDriver) Create(r Request) Response {
	fs := strings.Replace(r.Name, "/", ":", -1)
	log.Printf("Create volume %s\n", fs)
	return Response{}
}

func (d CFSDriver) Remove(r Request) Response {
	fs := strings.Replace(r.Name, "/", ":", -1)
	log.Printf("Remove volume %s\n", fs)
	return Response{}
}

func (d CFSDriver) Path(r Request) Response {
	fs := strings.Replace(r.Name, "/", ":", -1)
	mp := path.Join(MountDir, fs)
	log.Printf("Path for volume %s\n", fs)
	return Response{Mountpoint: mp}
}

func (d CFSDriver) Mount(r Request) Response {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	fs := strings.Replace(r.Name, "/", ":", -1)
	mp := path.Join(MountDir, fs)
	log.Printf("Mounting volume %s\n", fs)
	if _, ok := d.mounts[fs]; ok {
		d.mounts[fs]++
		return Response{Mountpoint: mp}
	}
	d.mounts[fs] = 1
	os.MkdirAll(mp, 0755)
	cmd := exec.Command("cfs", "-host", fs, mp)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	//create a new process group so the fuse clients don't stop when the plugin is stopped
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		log.Printf("Error attempting to mount %s\n", mp)
		return Response{Err: fmt.Sprintf("Error attempting to mount %s\n", fs)}
	}
	time.Sleep(1000 * time.Millisecond) // TODO: find a better way to do this
	log.Printf("Mounted %s\n", mp)
	return Response{Mountpoint: mp}
}

func (d CFSDriver) Unmount(r Request) Response {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	fs := strings.Replace(r.Name, "/", ":", -1)
	mp := path.Join(MountDir, fs)
	log.Printf("Unmounting volume %s\n", fs)
	if count, ok := d.mounts[fs]; ok {
		d.mounts[fs]--
		if count == 1 {
			cmd := exec.Command("fusermount", "-u", mp)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				log.Printf("Error attempting to unmount %s\n", mp)
				return Response{Err: fmt.Sprintf("Error attempting to unmount %s", fs)}
			}
			delete(d.mounts, fs)
			os.Remove(mp)
			log.Printf("Unmounted %s\n", mp)
		}
		return Response{}
	}
	return Response{Err: fmt.Sprintf("%s not mounted", fs)}
}

func main() {
	d := newCFSDriver()
	h := NewHandler(d)
	h.ServeUnix(SocketPath, 0)
}
