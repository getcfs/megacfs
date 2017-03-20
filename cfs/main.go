// Package main builds a 'cfs' executable that is the client application for
// CFS.
package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"sync"

	"bazil.org/fuse"
	pb "github.com/getcfs/megacfs/formic/formicproto"
	"github.com/getcfs/megacfs/formic/newproto"
	"github.com/gholt/brimtext"
	"github.com/gholt/cpcp"
	"github.com/gholt/dudu"
	"github.com/gholt/findfind"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
)

var logger *zap.Logger

type redirectGRPCLogger struct {
	sugaredLogger *zap.SugaredLogger
}

func (logger *redirectGRPCLogger) Fatal(args ...interface{}) {
	logger.sugaredLogger.Fatal(args)
}

func (logger *redirectGRPCLogger) Fatalf(format string, args ...interface{}) {
	logger.sugaredLogger.Fatalf(format, args)
}

func (logger *redirectGRPCLogger) Fatalln(args ...interface{}) {
	logger.sugaredLogger.Fatal(args)
}

func (logger *redirectGRPCLogger) Print(args ...interface{}) {
	logger.sugaredLogger.Debug(args)
}

func (logger *redirectGRPCLogger) Printf(format string, args ...interface{}) {
	logger.sugaredLogger.Debugf(format, args)
}

func (logger *redirectGRPCLogger) Println(args ...interface{}) {
	logger.sugaredLogger.Debug(args)
}

var redirectGRPCLoggerV redirectGRPCLogger

func init() {
	debug := brimtext.TrueString(os.Getenv("DEBUG"))
	for i := 1; i < len(os.Args)-1; i++ {
		if os.Args[i] == "-o" {
			v := os.Args[i+1]
			if v == "debug" || strings.HasPrefix(v, "debug,") || strings.Contains(v, ",debug,") || strings.HasSuffix(v, ",debug") {
				debug = true
				break
			}
		}
	}
	var baseLogger *zap.Logger
	var err error
	if debug {
		baseLogger, err = zap.NewDevelopmentConfig().Build()
		baseLogger.Debug("Logging in developer mode.")
	} else {
		baseLogger, err = zap.NewProduction()
	}
	if err != nil {
		panic(err)
	}
	logger = baseLogger.With(zap.String("name", "cfs"))
	redirectGRPCLoggerV.sugaredLogger = baseLogger.With(zap.String("name", "cfs")).Sugar()
	grpclog.SetLogger(&redirectGRPCLoggerV)
}

// FORMIC PORT
const (
	PORT = 12300
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

type rpc struct {
	apiClients []pb.ApiClient
	newClient  newproto.FormicClient
}

func newrpc(addr string, newAddr string) *rpc {
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

	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	conn, err := grpc.Dial(newAddr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	r.newClient = newproto.NewFormicClient(conn)

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

func mount(config map[string]string) error {
	f := flag.NewFlagSet("mount", flag.ContinueOnError)
	usageError := false
	f.Usage = func() {
		usageError = true
	}
	usageErr := errors.New(strings.TrimSpace(`
Usage:
    cfs mount [-o option,...] [serveraddr:]<fsid> <mountpoint>
Options:
    -o debug          enables debug output
    -o ro             mount the filesystem read only
    -o allow_other    allow access to other users
    -o noumount       skips the fusermount -uz retry on first error
Examples:
    cfs mount 11111111-1111-1111-1111-111111111111 /mnt/test
    cfs mount -o debug,ro 127.0.0.1:11111111-1111-1111-1111-111111111111 /mnt/test
`))
	var options string
	f.StringVar(&options, "o", "", "")
	f.Parse(flag.Args()[1:])
	if usageError {
		return usageErr
	}
	if f.NArg() != 2 {
		return usageErr
	}
	addrFsid := f.Args()[0]
	if !strings.Contains(addrFsid, ":") {
		addrFsid = config["addr"] + ":" + addrFsid
	}
	parts := strings.Split(addrFsid, ":")
	if len(parts) != 2 {
		fmt.Println("Invalid filesystem:", addrFsid)
		return usageErr
	}
	addr := fmt.Sprintf("%s:%d", strings.ToLower(parts[0]), PORT)
	// TODO: Just temp, get rid of + 1 at some point
	newAddr := fmt.Sprintf("%s:%d", strings.ToLower(parts[0]), PORT+1)
	fsid := parts[1]
	mountpoint := f.Args()[1]
	// Verify mountpoint exists
	_, err := os.Stat(mountpoint)
	if os.IsNotExist(err) {
		return fmt.Errorf("Mount point %s does not exist\n", mountpoint)
	}
	// Verify addr

	// parse mount options string
	clargs := getArgs(options)

	for iteration := 0; ; iteration++ {
		switch iteration {
		case 0:
		case 1:
			if _, noumount := clargs["noumount"]; noumount {
				return err
			}
			fmt.Println(err, ":: retrying after fusermount -uz")
			exec.Command("fusermount", "-uz", mountpoint).Run()
		default:
			return err
		}

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
		var cfs *fuse.Conn
		cfs, err = fuse.Mount(mountpoint, mountOptions...)
		if err != nil {
			continue
		}
		defer cfs.Close()

		// setup rpc client
		rpc := newrpc(addr, newAddr)
		fs := newfs(cfs, rpc, fsid)
		err = fs.InitFs()
		if err != nil {
			continue
		}
		srv := newserver(fs)

		if err = srv.serve(); err != nil {
			continue
		}

		<-cfs.Ready
		if err = cfs.MountError; err != nil {
			continue
		}
		break
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
	addr := fmt.Sprintf("%s:%d", ip, PORT)

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
		fmt.Println("    check        check a file for errors")
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
	case "check":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		err := check(addr)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "mount":
		err := mount(config)
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
				logger.Fatal("Invalid option", zap.String("left side", value[0]), zap.String("right side", value[1]))
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
		logger.Fatal("failed to dial", zap.Error(err))
	}
	return conn
}
