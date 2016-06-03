package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"net"

	"github.com/BurntSushi/toml"
	log "github.com/Sirupsen/logrus"
	pb "github.com/getcfs/megacfs/syndicate/api/proto"
	"github.com/getcfs/megacfs/syndicate/syndicate"
	"github.com/getcfs/megacfs/syndicate/utils/sysmetrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	printVersionInfo  = flag.Bool("version", false, "print version/build info")
	enabledCollectors = flag.String("collectors", sysmetrics.FilterAvailableCollectors(sysmetrics.DefaultCollectors), "Comma-separated list of collectors to use.")
)

var syndVersion string
var ringVersion string
var goVersion string
var buildDate string

/*
func newRingDistServer() *ringslave {
	s := new(ringslave)
	return s
}
*/

type RingSyndicate struct {
	sync.RWMutex
	active bool
	name   string
	config syndicate.Config
	server *syndicate.Server
	gs     *grpc.Server
}

type RingSyndicates struct {
	sync.RWMutex
	Syndics          []*RingSyndicate
	ch               chan bool //os signal chan,
	ShutdownComplete chan bool
	waitGroup        *sync.WaitGroup
	stopped          bool
}

type ClusterConfigs struct {
	ValueSyndicate *syndicate.Config
	GroupSyndicate *syndicate.Config
}

func (rs *RingSyndicates) Stop() {
	log.Println("Exiting...")
	close(rs.ch)
	for i, _ := range rs.Syndics {
		rs.Syndics[i].gs.Stop()
	}
	rs.waitGroup.Wait()
	close(rs.ShutdownComplete)
}

func (rs *RingSyndicates) launchSyndicates(k int) {
	rs.Syndics[k].Lock()
	defer rs.waitGroup.Done()
	rs.waitGroup.Add(1)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", rs.Syndics[k].config.Port))
	if err != nil {
		log.Fatalln(err)
		return
	}
	var opts []grpc.ServerOption
	creds, err := credentials.NewServerTLSFromFile(rs.Syndics[k].config.CertFile, rs.Syndics[k].config.KeyFile)
	if err != nil {
		log.Fatalln("Error load cert or key:", err)
	}
	opts = []grpc.ServerOption{grpc.Creds(creds)}
	rs.Syndics[k].gs = grpc.NewServer(opts...)

	if rs.Syndics[k].config.Master {
		pb.RegisterSyndicateServer(rs.Syndics[k].gs, rs.Syndics[k].server)
		log.Println("Master starting up on", rs.Syndics[k].config.Port)
		rs.Syndics[k].gs.Serve(l)
	} else {
		//pb.RegisterRingDistServer(s, newRingDistServer())
		//log.Printf("Starting ring slave up on %d...\n", cfg.Port)
		//s.Serve(l)
		log.Fatalln("Syndicate slaves not implemented yet")
	}
	rs.Syndics[k].Unlock()
}

func main() {
	var err error
	configFile := "/etc/syndicate/syndicate.toml"
	if os.Getenv("SYNDICATE_CONFIG") != "" {
		configFile = os.Getenv("SYNDICATE_CONFIG")
	}
	flag.Parse()
	if *printVersionInfo {
		fmt.Println("syndicate-client:", syndVersion)
		fmt.Println("ring version:", ringVersion)
		fmt.Println("build date:", buildDate)
		fmt.Println("go version:", goVersion)
		return
	}
	rs := &RingSyndicates{
		ch:               make(chan bool),
		ShutdownComplete: make(chan bool),
		waitGroup:        &sync.WaitGroup{},
		stopped:          false,
	}

	var tc map[string]syndicate.Config
	if _, err := toml.DecodeFile(configFile, &tc); err != nil {
		log.Fatalln(err)
	}
	for k, v := range tc {
		log.Println("Found config for", k)
		log.Println("Config:", v)
		syndic := &RingSyndicate{
			active: false,
			name:   k,
			config: v,
		}
		syndic.server, err = syndicate.NewServer(&syndic.config, k)
		if err != nil {
			log.Fatalln(err)
		}
		rs.Syndics = append(rs.Syndics, syndic)
	}
	rs.Lock()
	defer rs.Unlock()
	for k, _ := range rs.Syndics {
		go rs.launchSyndicates(k)
	}
	//now that syndics are up and running launch global metrics endpoint
	//setup node_collector for system level metrics first
	collectors, err := sysmetrics.LoadCollectors(*enabledCollectors)
	if err != nil {
		log.Fatalf("Couldn't load collectors: %s", err)
	}
	nodeCollector := sysmetrics.New(collectors)
	prometheus.MustRegister(nodeCollector)
	http.Handle("/metrics", prometheus.Handler())
	go http.ListenAndServe(":9100", nil)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-ch:
			rs.Stop()
			<-rs.ShutdownComplete
			return
		case <-rs.ShutdownComplete:
			return
		}
	}
}
