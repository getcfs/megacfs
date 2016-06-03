package oort

import (
	"fmt"
	"log"
	"sync"

	"github.com/gholt/ring"
	"github.com/pandemicsyn/cmdctrl"
)

type OortService interface {
	Stats() []byte
	//Start is called before ListenAndServe to startup any needed stuff
	Start()
	//Stop is called before StopListenAndServe
	Stop()
	//The method we'll invoke when we receive a new ring
	UpdateRing(ring.Ring)
	// ListenAndServe is assumed to bind to an address and just handle/pass off requests, Start is called BEFORE this to make sure
	// any need backend services/chan's are up and running before we start accepting requests.
	ListenAndServe()
	// StopListenAndServe is assumed to only stop the OortService's network listener. It shouldn't return as soon as
	// the service is no longer listening on the interface
	StopListenAndServe()
	// Wait() should block until all active requests are serviced (or return immediately if not implemented).
	Wait()
}

const (
	DefaultBaseDir = "/var/lib"
)

type Server struct {
	sync.RWMutex
	serviceName string
	cwd         string
	ringFile    string
	ring        ring.Ring
	localID     uint64
	backend     OortService //the backend service
	// TODO: should probably share ch with backend so a stop on one stops both.
	ch                chan bool //os signal chan,
	ShutdownComplete  chan bool
	waitGroup         *sync.WaitGroup
	cmdCtrlLock       sync.RWMutex
	CmdCtrlConfig     cmdctrl.ConfigOpts
	cmdCtrlLoopActive bool
	stopped           bool
	binaryUpgrade     *cmdctrl.GithubUpdater
}

// New returns a instance of oort.Server for a given serviceName, workingDir, and buildVersion.
// if workingDir is empty the default dir of "/var/lib/<servicename>" is used.
// if buildVersion is empty the default version of "DEV" will be passed on to the self-upgrade service.
func New(serviceName, workingDir string, binaryUpdater *cmdctrl.GithubUpdater) (*Server, error) {
	if workingDir == "" {
		workingDir = fmt.Sprintf("%s/%s", DefaultBaseDir, serviceName)
	}
	o := &Server{
		serviceName:      serviceName,
		cwd:              workingDir,
		ch:               make(chan bool),
		ShutdownComplete: make(chan bool),
		waitGroup:        &sync.WaitGroup{},
		stopped:          false,
	}
	err := o.ObtainConfig()
	if err != nil {
		return o, err
	}
	o.binaryUpgrade = binaryUpdater
	return o, err
}

//SetBackend sets the current backend
func (o *Server) SetBackend(backend OortService) {
	o.Lock()
	o.backend = backend
	o.Unlock()
}

func (o *Server) SetRing(r ring.Ring, ringFile string) {
	o.Lock()
	o.ring = r
	o.ringFile = ringFile
	o.ring.SetLocalNode(o.localID)
	o.backend.UpdateRing(o.ring)
	log.Println("Ring version is now:", o.ring.Version())
	o.Unlock()
}

// Ring returns an instance of the current Ring
func (o *Server) Ring() ring.Ring {
	o.RLock()
	defer o.RUnlock()
	return o.ring
}

// GetLocalID returns the current local id
func (o *Server) GetLocalID() uint64 {
	o.RLock()
	defer o.RUnlock()
	return o.localID
}

// GetListenAddr returns the current localnode.address2 instance
func (o *Server) GetListenAddr() string {
	o.RLock()
	defer o.RUnlock()
	return o.ring.LocalNode().Address(2)
}

func (o *Server) CmdCtrlLoopActive() bool {
	o.RLock()
	defer o.RUnlock()
	return o.cmdCtrlLoopActive
}

func (o *Server) runCmdCtrlLoop() {
	if !o.cmdCtrlLoopActive {
		go func(o *Server) {
			firstAttempt := true
			for {
				o.cmdCtrlLoopActive = true
				cc := cmdctrl.NewCCServer(o, &o.CmdCtrlConfig)
				err := cc.Serve()
				if err != nil && firstAttempt {
					//since this is our first attempt to bind/serve and we blew up
					//we're probably missing something import and wont be able to
					//recover.
					log.Fatalln("Error on first attempt to launch CmdCtrl Serve")
				} else if err != nil && !firstAttempt {
					log.Println("CmdCtrl Serve encountered error:", err)
				} else {
					log.Println("CmdCtrl Serve exited without error, quiting")
					break
				}
				firstAttempt = false
			}
		}(o)
	}
}

func (o *Server) Serve() {
	defer o.waitGroup.Done()
	o.waitGroup.Add(1)
	if o.CmdCtrlConfig.Enabled {
		o.CmdCtrlConfig.ListenAddress = o.ring.Node(o.localID).Address(0)
		o.runCmdCtrlLoop()
	} else {
		log.Println("Command and Control functionality disabled via config")
	}
	go o.backend.ListenAndServe()
}
