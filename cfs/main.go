package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"sync"

	"bazil.org/fuse"
	pb "github.com/getcfs/megacfs/formic/proto"
	"github.com/gholt/cpcp"
	"github.com/gholt/dudu"
	"github.com/gholt/findfind"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// FORMIC PORT
const (
	PORT = "12300"
)

type server struct {
	fs *fs
	wg sync.WaitGroup
}

func newserver(fs *fs) *server {
	s := &server{
		fs: fs,
	}
	return s
}

func (s *server) serve() error {
	defer s.wg.Wait()

	for {
		req, err := s.fs.conn.ReadRequest()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.fs.handle(req)
		}()
	}
	return nil
}

func debuglog(msg interface{}) {
	fmt.Fprintf(os.Stderr, "%v\n", msg)
}

type rpc struct {
	apiClients []pb.ApiClient
}

func newrpc(addr string) *rpc {
	var opts []grpc.DialOption
	creds := credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	clients := []pb.ApiClient{}

	// TODO: Rework this simplistic connection pooling
	for i := 0; i < 1; i++ { // hardcoded to 1 connection for now
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		clients = append(clients, pb.NewApiClient(conn))
	}
	r := &rpc{
		apiClients: clients,
	}

	return r
}

func (r *rpc) api() pb.ApiClient {
	return r.apiClients[rand.Intn(len(r.apiClients))]
}

// NullWriter ...
type NullWriter int

func (NullWriter) Write([]byte) (int, error) { return 0, nil }

var cfsVersion string
var buildDate string
var commitVersion string
var goVersion string

func auth(authURL string, username string, password string) string {

	var token string

	if authURL == "dev" {
		return ""
	}

	if token = os.Getenv("OS_TOKEN"); token != "" {
		return token
	}

	body := fmt.Sprintf(`{"auth":{"identity":{"methods":["password"],"password":{"user":{
		"domain":{"id":"default"},"name":"%s","password":"%s"}}}}}`, username, password)
	rbody := strings.NewReader(body)
	req, err := http.NewRequest("POST", authURL+"v3/auth/tokens", rbody)
	if err != nil {
		fmt.Printf("%v", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("%v", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		fmt.Println(resp.Status)
		os.Exit(1)
	}

	token = resp.Header.Get("X-Subject-Token")

	return token
}

func mount() error {
	f := flag.NewFlagSet("mount", flag.ContinueOnError)
	f.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("    cfs mount [-o option,...] <server addr>:<fsid> <mountpoint>")
		fmt.Println("Options:")
		fmt.Println("    -o debug          enables debug output")
		fmt.Println("    -o ro             mount the filesystem read only")
		fmt.Println("    -o allow_other    allow access to other users")
		fmt.Println("Examples:")
		fmt.Println("    cfs mount 127.0.0.1:11111111-1111-1111-1111-111111111111 /mnt/test")
		fmt.Println("    cfs mount -o debug,ro 127.0.0.1:11111111-1111-1111-1111-111111111111 /mnt/test")
		os.Exit(1)
	}
	var options string
	f.StringVar(&options, "o", "", "")
	f.Parse(flag.Args()[1:])
	if f.NArg() != 2 {
		f.Usage()
	}
	addrFsid := f.Args()[0]
	parts := strings.Split(addrFsid, ":")
	if len(parts) != 2 {
		fmt.Println("Invalid filesystem:", addrFsid)
		f.Usage()
	}
	addr := fmt.Sprintf("%s:%s", strings.ToLower(parts[0]), PORT)
	fsid := parts[1]
	mountpoint := f.Args()[1]
	// Verify mountpoint exists
	_, err := os.Stat(mountpoint)
	if os.IsNotExist(err) {
		fmt.Printf("Mount point %s does not exist\n", mountpoint)
		os.Exit(1)
	}
	// Verify addr

	// handle fuse mount options
	mountOptions := []fuse.MountOption{
		fuse.FSName("cfs"),
		fuse.Subtype("cfs"),
		fuse.DefaultPermissions(),
		fuse.MaxReadahead(128 * 1024),
		fuse.AsyncRead(),
		//fuse.WritebackCache(),
		fuse.AutoInvalData(),
	}

	// parse mount options string
	clargs := getArgs(options)

	// handle debug mount option
	_, debug := clargs["debug"]
	if !debug {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	// handle allow_other mount option
	_, allowOther := clargs["allow_other"]
	if allowOther {
		mountOptions = append(mountOptions, fuse.AllowOther())
	}

	// handle ro mount option
	_, readOnly := clargs["ro"]
	if readOnly {
		mountOptions = append(mountOptions, fuse.ReadOnly())
	}

	// perform fuse mount
	fusermountPath()
	cfs, err := fuse.Mount(mountpoint, mountOptions...)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer cfs.Close()

	// setup rpc client
	rpc := newrpc(addr)
	fs := newfs(cfs, rpc, fsid)
	err = fs.InitFs()
	if err != nil {
		fmt.Println(err)
		exec.Command("fusermount", "-uz", mountpoint).Run()
		os.Exit(1)
	}
	srv := newserver(fs)

	if err := srv.serve(); err != nil {
		fmt.Println(err)
		exec.Command("fusermount", "-uz", mountpoint).Run()
		os.Exit(1)
	}

	<-cfs.Ready
	if err := cfs.MountError; err != nil {
		fmt.Println(err)
		exec.Command("fusermount", "-uz", mountpoint).Run()
		os.Exit(1)
	}
	return nil
}

func main() {

	// try to read config file
	configured := false
	config := map[string]string{}
	user, err := user.Current()
	if err != nil {
		fmt.Printf("Unable to identify curent user: %v\n", err)
		os.Exit(1)
	}
	var configfile string
	if user.HomeDir != "" {
		configfile = user.HomeDir + "/.cfs.json"
	} else {
		configfile = ".cfs.json"
	}
	f, err := ioutil.ReadFile(configfile)
	if err == nil {
		json.Unmarshal(f, &config)
		configured = true
	}
	ip, _ := config["addr"]
	authURL, _ := config["authURL"]
	username, _ := config["username"]
	password, _ := config["password"]
	addr := fmt.Sprintf("%s:%s", ip, PORT)

	flag.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("    cfs <command> [options] <args>")
		fmt.Println("Commands:")
		fmt.Println("    configure    create configuration file")
		fmt.Println("    list         list all filesystems")
		fmt.Println("    create       create a new filesytem")
		fmt.Println("    show         show filesystem details")
		fmt.Println("    update       update filesystem name")
		fmt.Println("    delete       delete an existing filesystem")
		fmt.Println("    grant        grant access to a filesystem")
		fmt.Println("    revoke       revoke access to a filesystem")
		fmt.Println("    mount        mount an existing filesystem")
		fmt.Println("    version      show client version")
		fmt.Println("    cp           parallel cp command; cfs cp --help")
		fmt.Println("    du           parallel du command; cfs du --help")
		fmt.Println("    find         parallel find command; cfs find --help")
		fmt.Println("    help         show usage for cfs")
		fmt.Println("Examples:")
		fmt.Println("    cfs configure")
		fmt.Println("    cfs create <name>")
		fmt.Println("    cfs grant <client addr> <fsid>")
		fmt.Println("    cfs mount <server addr>:<fsid> <mountpoint>")
		os.Exit(1)
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
	}
	command := flag.Args()[0]
	switch command {
	default:
		fmt.Println("Invalid Command")
		flag.Usage()
	case "help":
		flag.Usage()
	case "version":
		fmt.Println("version:", cfsVersion)
		fmt.Println("commit:", commitVersion)
		fmt.Println("build date:", buildDate)
		fmt.Println("go version:", goVersion)
		os.Exit(0)
	case "configure":
		err := configure(configfile)
		if err != nil {
			fmt.Printf("Error writing config file: %v\n", err)
			os.Exit(1)
		}

	case "list":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		err := list(addr, authURL, username, password)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "show":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		err := show(addr, authURL, username, password)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "create":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		err := create(addr, authURL, username, password)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "delete":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		err := del(addr, authURL, username, password)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "update":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		err := update(addr, authURL, username, password)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "grant":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		err := grant(addr, authURL, username, password)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "revoke":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		err := revoke(addr, authURL, username, password)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "mount":
		err := mount()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "cp":
		if err := cpcp.CPCP(flag.Args()[1:]); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "du":
		if err := dudu.DUDU(flag.Args()[1:]); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "find":
		if err := findfind.FindFind(flag.Args()[1:]); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}

func getArgs(args string) map[string]string {
	clargs := make(map[string]string)
	optList := strings.Split(args, ",")
	for _, item := range optList {
		if strings.Contains(item, "=") {
			value := strings.Split(item, "=")
			if value[0] == "" || value[1] == "" {
				log.Printf("Invalid option %s, %s no value\n\n", value[0], value[1])
				os.Exit(1)
			} else {
				clargs[value[0]] = value[1]
			}
		} else {
			clargs[item] = ""
		}
	}
	return clargs
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

// setupWS ...
func setupWS(svr string) *grpc.ClientConn {
	var opts []grpc.DialOption
	creds := credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(svr, opts...)
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	return conn
}
