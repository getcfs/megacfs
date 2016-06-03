package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pandemicsyn/cmdctrl"
	"github.com/getcfs/megacfs/oort/oort"
	"github.com/getcfs/megacfs/oort/oortstore"
	"github.com/getcfs/megacfs/syndicate/utils/sysmetrics"
)

const (
	//ServiceName is the internal name of this oort instance
	ServiceName = "value"
	//BinaryName is the name of this services executable (used for binary upgrades)
	BinaryName = "oort-valued"
	//BinaryPath is the absolute path and name of this executable (used for binary upgrades)
	BinaryPath = "/usr/local/bin/oort-valued"
	//GithubRepo is the github repo where production releases are (used for binary upgrades)
	GithubRepo = "cfs-binary-release"
	//GithubProject is the github project where proudction release are (used for binary upgrades)
	GithubProject = "getcfs"
)

var (
	printVersionInfo = flag.Bool("version", false, "print version/build info")
	cwd              = flag.String("cwd", "/var/lib/oort-value", "the working directory use")
)

var oortVersion string
var ringVersion string
var valuestoreVersion string
var cmdctrlVersion string
var goVersion string
var buildDate string

func main() {
	flag.Parse()
	if *printVersionInfo {
		fmt.Println("oort version:", oortVersion)
		fmt.Println("ring version:", ringVersion)
		fmt.Println("cmdctrl version:", cmdctrlVersion)
		fmt.Println("valuestore version:", valuestoreVersion)
		fmt.Println("build date:", buildDate)
		fmt.Println("go version:", goVersion)
		return
	}

	updater := cmdctrl.NewGithubUpdater(
		GithubRepo,
		GithubProject,
		BinaryName,
		BinaryPath,
		fmt.Sprintf("%s/%s.canary", *cwd, ServiceName),
		oortVersion,
	)
	o, err := oort.New(ServiceName, *cwd, updater)
	if err != nil {
		log.Fatalln("Unable to obtain config:", err)
	}
	log.Println("Using valuestore backend")
	backend, err := oortstore.NewValueStore(o)
	if err != nil {
		log.Fatalln("Unable to initialize ValueStore:", err)
	}
	sysmetrics.StartupMetrics(backend.Config.MetricsAddr, backend.Config.MetricsCollectors)
	o.SetBackend(backend)
	o.Serve()

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-ch:
			o.Exit()
			<-o.ShutdownComplete
			return
		case <-o.ShutdownComplete:
			return
		}
	}
}
