package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/getcfs/fuse"
	pb "github.com/getcfs/megacfs/formic/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var regions = map[string]string{
	"aio": "127.0.0.1:8445",
	"iad": "api.ea.iad.rackfs.com:8445",
}

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
	conn *grpc.ClientConn
	api  pb.ApiClient
}

func newrpc(conn *grpc.ClientConn) *rpc {
	r := &rpc{
		conn: conn,
		api:  pb.NewApiClient(conn),
	}

	return r
}

// NullWriter ...
type NullWriter int

func (NullWriter) Write([]byte) (int, error) { return 0, nil }

var cfsVersion string
var buildDate string
var commitVersion string
var goVersion string

func main() {

	// try to read config file
	configured := false
	config := map[string]string{}
	user, err := user.Current()
	if err != nil {
		log.Fatalf("Unable to identify curent user: %v", err)
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
	region, _ := config["region"]
	username, _ := config["username"]
	apikey, _ := config["apikey"]

	// get auth token for username and apikey
	token := apikey // TODO: get token from identity

	flag.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("    cfs <command> [options] <args>")
		fmt.Println("Commands:")
		fmt.Println("    configure    create configuration file")
		fmt.Println("    list         list all filesystems")
		fmt.Println("    create       create a new filesytem")
		fmt.Println("    show         show filesystem details")
		fmt.Println("    delete       delete an existing filesystem")
		fmt.Println("    grant        grant access to a filesystem")
		fmt.Println("    revoke       revoke access to a filesystem")
		fmt.Println("    mount        mount an existing filesystem")
		fmt.Println("    version      show client version")
		fmt.Println("    help         show usage for cfs")
		fmt.Println("Examples:")
		fmt.Println("    cfs configure")
		fmt.Println("    cfs create <name>")
		fmt.Println("    cfs grant <ip> <fsid>")
		fmt.Println("    cfs mount <region>:<fsid> <mountpoint>")
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
		//fmt.Sprintf(": %s\ncommit: %s\nbuild date: %s\ngo version: %s", cfsVersion, commitVersion, buildDate, goVersion)
		fmt.Println("version:", cfsVersion)
		fmt.Println("commit:", commitVersion)
		fmt.Println("build date:", buildDate)
		fmt.Println("go version:", goVersion)
		os.Exit(0)
	case "configure":
		fmt.Println("This is an interactive session to configure the cfs client.")
		fmt.Print("CFS Region: ")
		fmt.Scan(&region)
		fmt.Print("CFS Username: ")
		fmt.Scan(&username)
		fmt.Print("CFS APIKey: ")
		fmt.Scan(&apikey)
		config := map[string]string{
			"region":   strings.ToLower(region),
			"username": username,
			"apikey":   apikey,
		}
		c, err := json.MarshalIndent(config, "", "  ")
		if err != nil {
			fmt.Printf("Error writing config file: %v\n", err)
			os.Exit(1)
		}
		err = ioutil.WriteFile(configfile, c, 0600)
		if err != nil {
			fmt.Printf("Error writing config file: %v\n", err)
			os.Exit(1)
		}
		fmt.Println()
		os.Exit(0)
	case "list":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		f := flag.NewFlagSet("list", flag.ContinueOnError)
		f.Usage = func() {
			fmt.Println("Usage:")
			fmt.Println("    cfs list")
			os.Exit(1)
		}
		f.Parse(flag.Args()[1:])
		if f.NArg() != 0 {
			f.Usage()
		}
		addr, ok := regions[region]
		if !ok {
			fmt.Println("Invalid region:", region)
			os.Exit(1)
		}
		c := setupWS(addr)
		ws := pb.NewFileSystemAPIClient(c)
		res, err := ws.ListFS(context.Background(), &pb.ListFSRequest{Token: token})
		if err != nil {
			log.Fatalf("Request Error: %v", err)
			c.Close()
			os.Exit(1)
		}
		c.Close()
		var data []map[string]interface{}
		if err := json.Unmarshal([]byte(res.Data), &data); err != nil {
			log.Fatalf("Error unmarshalling response: %v", err)
			os.Exit(1)
		}
		fmt.Printf("%-36s    %s\n", "ID", "Name")
		for _, fs := range data {
			fmt.Printf("%-36s    %s\n", fs["id"], fs["name"])
		}
	case "show":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		f := flag.NewFlagSet("show", flag.ContinueOnError)
		f.Usage = func() {
			fmt.Println("Usage:")
			fmt.Println("    cfs show <fsid>")
			fmt.Println("Example:")
			fmt.Println("    cfs show 11111111-1111-1111-1111-111111111111")
			os.Exit(1)
		}
		f.Parse(flag.Args()[1:])
		if f.NArg() != 1 {
			f.Usage()
		}
		fsid := f.Args()[0]
		addr, ok := regions[region]
		if !ok {
			fmt.Println("Invalid region:", region)
			os.Exit(1)
		}
		c := setupWS(addr)
		ws := pb.NewFileSystemAPIClient(c)
		res, err := ws.ShowFS(context.Background(), &pb.ShowFSRequest{Token: token, FSid: fsid})
		if err != nil {
			log.Fatalf("Request Error: %v", err)
			c.Close()
			os.Exit(1)
		}
		c.Close()
		var data map[string]interface{}
		if err := json.Unmarshal([]byte(res.Data), &data); err != nil {
			log.Fatalf("Error unmarshalling response: %v", err)
			os.Exit(1)
		}
		fmt.Println("ID:", data["id"])
		fmt.Println("Name:", data["name"])
		//fmt.Println("Status:", data["status"])
		for _, ip := range data["addrs"].([]interface{}) {
			fmt.Println("IP:", ip)
		}
	case "create":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		f := flag.NewFlagSet("create", flag.ContinueOnError)
		f.Usage = func() {
			fmt.Println("Usage:")
			fmt.Println("    cfs create <name>")
			fmt.Println("Examples:")
			fmt.Println("    cfs create myfs")
			fmt.Println("    cfs create \"name with spaces\"")
			os.Exit(1)
		}
		f.Parse(flag.Args()[1:])
		if f.NArg() != 1 {
			f.Usage()
		}
		name := f.Args()[0]
		addr, ok := regions[region]
		if !ok {
			fmt.Println("Invalid region:", region)
			os.Exit(1)
		}
		c := setupWS(addr)
		ws := pb.NewFileSystemAPIClient(c)
		res, err := ws.CreateFS(context.Background(), &pb.CreateFSRequest{Token: token, FSName: name})
		if err != nil {
			log.Fatalf("Request Error: %v", err)
			c.Close()
			os.Exit(1)
		}
		c.Close()
		fmt.Println("ID:", res.Data)
	case "delete":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		f := flag.NewFlagSet("delete", flag.ContinueOnError)
		f.Usage = func() {
			fmt.Println("Usage:")
			fmt.Println("    cfs delete <fsid>")
			fmt.Println("Example:")
			fmt.Println("    cfs delete 11111111-1111-1111-1111-111111111111")
			os.Exit(1)
		}
		f.Parse(flag.Args()[1:])
		if f.NArg() != 1 {
			f.Usage()
		}
		fsid := f.Args()[0]
		addr, ok := regions[region]
		if !ok {
			fmt.Println("Invalid region:", region)
			os.Exit(1)
		}
		c := setupWS(addr)
		ws := pb.NewFileSystemAPIClient(c)
		res, err := ws.DeleteFS(context.Background(), &pb.DeleteFSRequest{Token: token, FSid: fsid})
		if err != nil {
			log.Fatalf("Request Error: %v", err)
			c.Close()
			os.Exit(1)
		}
		c.Close()
		fmt.Println(res.Data)
	case "grant":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		f := flag.NewFlagSet("grant", flag.ContinueOnError)
		f.Usage = func() {
			fmt.Println("Usage:")
			fmt.Println("    cfs grant <ip> <fsid>")
			fmt.Println("Example:")
			fmt.Println("    cfs grant 1.1.1.1 11111111-1111-1111-1111-111111111111")
			os.Exit(1)
		}
		f.Parse(flag.Args()[1:])
		if f.NArg() != 2 {
			f.Usage()
		}
		ip := f.Args()[0]
		fsid := f.Args()[1]
		addr, ok := regions[region]
		if !ok {
			fmt.Println("Invalid region:", region)
			os.Exit(1)
		}
		c := setupWS(addr)
		ws := pb.NewFileSystemAPIClient(c)
		_, err := ws.GrantAddrFS(context.Background(), &pb.GrantAddrFSRequest{Token: token, FSid: fsid, Addr: ip})
		if err != nil {
			log.Fatalf("Request Error: %v", err)
			c.Close()
			os.Exit(1)
		}
		c.Close()
	case "revoke":
		if !configured {
			fmt.Println("You must run \"cfs configure\" first.")
			os.Exit(1)
		}
		f := flag.NewFlagSet("revoke", flag.ContinueOnError)
		f.Usage = func() {
			fmt.Println("Usage:")
			fmt.Println("    cfs revoke <ip> <fsid>")
			fmt.Println("Example:")
			fmt.Println("    cfs revoke 1.1.1.1 11111111-1111-1111-1111-111111111111")
			os.Exit(1)
		}
		f.Parse(flag.Args()[1:])
		if f.NArg() != 2 {
			f.Usage()
		}
		ip := f.Args()[0]
		fsid := f.Args()[1]
		addr, ok := regions[region]
		if !ok {
			fmt.Println("Invalid region:", region)
			os.Exit(1)
		}
		c := setupWS(addr)
		ws := pb.NewFileSystemAPIClient(c)
		_, err := ws.RevokeAddrFS(context.Background(), &pb.RevokeAddrFSRequest{Token: token, FSid: fsid, Addr: ip})
		if err != nil {
			log.Fatalf("Request Error: %v", err)
			c.Close()
			os.Exit(1)
		}
		c.Close()
	case "mount":
		f := flag.NewFlagSet("mount", flag.ContinueOnError)
		f.Usage = func() {
			fmt.Println("Usage:")
			fmt.Println("    cfs mount [-o option,...] <region>:<fsid> <mountpoint>")
			fmt.Println("Options:")
			fmt.Println("    -o debug          enables debug output")
			fmt.Println("    -o ro             mount the filesystem read only")
			fmt.Println("    -o allow_other    allow access to other users")
			fmt.Println("Examples:")
			fmt.Println("    cfs mount iad:11111111-1111-1111-1111-111111111111 /mnt/test")
			fmt.Println("    cfs mount -o debug,ro iad:11111111-1111-1111-1111-111111111111 /mnt/test")
			os.Exit(1)
		}
		var options string
		f.StringVar(&options, "o", "", "")
		f.Parse(flag.Args()[1:])
		if f.NArg() != 2 {
			f.Usage()
		}
		region_fsid := f.Args()[0]
		parts := strings.Split(region_fsid, ":")
		if len(parts) != 2 {
			fmt.Println("Invalid filesystem:", region_fsid)
			f.Usage()
		}
		region := strings.ToLower(parts[0])
		fsid := parts[1]
		mountpoint := f.Args()[1]
		addr, ok := regions[region]
		if !ok {
			fmt.Println("Invalid region:", region)
			os.Exit(1)
		}

		// Setup grpc
		var opts []grpc.DialOption
		creds := credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})
		opts = append(opts, grpc.WithTransportCredentials(creds))
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			log.Fatalf("failed to dial: %v", err)
		}
		defer conn.Close()

		// handle fuse mount options
		mountOptions := []fuse.MountOption{
			fuse.FSName("cfs"),
			fuse.Subtype("cfs"),
			fuse.DefaultPermissions(),
			fuse.MaxReadahead(128 * 1024),
			fuse.AsyncRead(),
			//fuse.WritebackCache(),
			//fuse.AutoInvalData(),
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
			log.Fatal(err)
		}
		defer cfs.Close()

		// setup rpc client
		rpc := newrpc(conn)
		fs := newfs(cfs, rpc, fsid)
		err = fs.InitFs()
		if err != nil {
			log.Fatal(err)
		}
		srv := newserver(fs)

		if err := srv.serve(); err != nil {
			log.Fatal(err)
		}

		<-cfs.Ready
		if err := cfs.MountError; err != nil {
			log.Fatal(err)
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
		os.Setenv("PATH", "/bin;/usr/bin")
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
