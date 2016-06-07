package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"github.com/getcfs/fuse"
	pb "github.com/getcfs/megacfs/formic/proto"
	"github.com/pkg/profile"
	"github.com/satori/go.uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/urfave/cli.v2"
)

var regions = map[string]string{
	"aio": "127.0.0.1",
	"iad": "api.ea.iad.rackfs.com",
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

	if gogoprofile := os.Getenv("CFS_PROFILE"); gogoprofile == "true" {
		defer profile.Start(profile.MemProfile).Stop()
	}

	// Process command line arguments
	var gtoken string
	var fsNum string
	var serverAddr string
	var fsName string
	var addrValue string
	var fsRegion string
	var ok bool

	app := cli.NewApp()
	app.Name = "cfs"
	app.Usage = "Client used to test filesysd"
	app.Version = fmt.Sprintf(": %s\ncommit: %s\nbuild date: %s\ngo version: %s", cfsVersion, commitVersion, buildDate, goVersion)
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:        "token",
			Aliases:     []string{"T"},
			Value:       "",
			Usage:       "Access token",
			EnvVars:     []string{"OOHHC_TOKEN_KEY"},
			Destination: &gtoken,
		},
	}
	app.Commands = []*cli.Command{
		{
			Name:      "show",
			Usage:     "Show a File Systems",
			ArgsUsage: "<region>://<file system uuid>",
			Action: func(c *cli.Context) error {
				if !c.Args().Present() {
					fmt.Println("Invalid syntax for show.")
					os.Exit(1)
				}
				if gtoken == "" {
					fmt.Println("Token is required")
					os.Exit(1)
				}
				serverAddr, fsNum = parseurl(c.Args().Get(0), "8445")
				if fsNum == "" {
					fmt.Println("Missing file system id")
					os.Exit(1)
				}
				conn := setupWS(serverAddr)
				ws := pb.NewFileSystemAPIClient(conn)
				result, err := ws.ShowFS(context.Background(), &pb.ShowFSRequest{Token: gtoken, FSid: fsNum})
				if err != nil {
					log.Fatalf("Bad Request: %v", err)
					conn.Close()
					os.Exit(1)
				}
				conn.Close()
				log.Printf("SHOW Results: %s", result.Data)
				return nil
			},
		},
		{
			Name:      "create",
			Usage:     "Create a File Systems",
			ArgsUsage: "[R|region] [aio|iad]  [N|name] <file system name>",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:        "name",
					Aliases:     []string{"N"},
					Value:       "",
					Usage:       "Name of the file system",
					Destination: &fsName,
				},
				&cli.StringFlag{
					Name:        "region",
					Aliases:     []string{"R"},
					Value:       "",
					Usage:       "Target region",
					Destination: &fsRegion,
				},
			},
			Action: func(c *cli.Context) error {
				if gtoken == "" {
					fmt.Println("Token is required")
				}
				// For create serverAddr and acctnum are required
				serverAddr, ok = regions[fsRegion]
				if !ok {
					fmt.Printf("Invalid region %s", fsRegion)
					os.Exit(1)
				}
				serverAddr = fmt.Sprintf("%s:%s", serverAddr, "8445")
				if fsName == "" {
					fmt.Println("File system name is a required field.")
					os.Exit(1)
				}
				conn := setupWS(serverAddr)
				ws := pb.NewFileSystemAPIClient(conn)
				result, err := ws.CreateFS(context.Background(), &pb.CreateFSRequest{Token: gtoken, FSName: fsName})
				if err != nil {
					log.Fatalf("Bad Request: %v", err)
					conn.Close()
					os.Exit(1)
				}
				conn.Close()
				log.Printf("Create Results: %s", result.Data)
				return nil
			},
		},
		{
			Name:      "list",
			Usage:     "List File Systems for an account",
			ArgsUsage: "[R|region] [aio|iad]",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:        "region",
					Aliases:     []string{"R"},
					Value:       "",
					Usage:       "Target region",
					Destination: &fsRegion,
				},
			},
			Action: func(c *cli.Context) error {
				if gtoken == "" {
					fmt.Println("Token is required")
					os.Exit(1)
				}
				serverAddr, ok = regions[fsRegion]
				if !ok {
					fmt.Printf("Invalid region %s", fsRegion)
					os.Exit(1)
				}
				serverAddr = fmt.Sprintf("%s:%s", serverAddr, "8445")
				conn := setupWS(serverAddr)
				ws := pb.NewFileSystemAPIClient(conn)
				result, err := ws.ListFS(context.Background(), &pb.ListFSRequest{Token: gtoken})
				if err != nil {
					log.Fatalf("Bad Request: %v", err)
					conn.Close()
					os.Exit(1)
				}
				conn.Close()
				log.Printf("LIST Results: %s", result.Data)
				return nil
			},
		},
		{
			Name:      "delete",
			Usage:     "Delete a File Systems",
			ArgsUsage: "<region>://<file system uuid>",
			Action: func(c *cli.Context) error {
				if !c.Args().Present() {
					fmt.Println("Invalid syntax for delete.")
					os.Exit(1)
				}
				if gtoken == "" {
					fmt.Println("Token is required")
				}
				serverAddr, fsNum = parseurl(c.Args().Get(0), "8445")
				if fsNum == "" {
					fmt.Println("Missing file system id")
					os.Exit(1)
				}
				conn := setupWS(serverAddr)
				ws := pb.NewFileSystemAPIClient(conn)
				result, err := ws.DeleteFS(context.Background(), &pb.DeleteFSRequest{Token: gtoken, FSid: fsNum})
				if err != nil {
					log.Fatalf("Bad Request: %v", err)
					conn.Close()
					os.Exit(1)
				}
				conn.Close()
				log.Printf("Delete Results: %s", result.Data)
				return nil
			},
		},
		{
			Name:      "update",
			Usage:     "Update a File Systems",
			ArgsUsage: "<region>://<file system uuid> -o [OPTIONS]",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:        "name",
					Aliases:     []string{"N"},
					Value:       "",
					Usage:       "Name of the file system",
					Destination: &fsName,
				},
			},
			Action: func(c *cli.Context) error {
				if !c.Args().Present() {
					fmt.Println("Invalid syntax for update.")
					os.Exit(1)
				}
				if gtoken == "" {
					fmt.Println("Token is required")
					os.Exit(1)
				}
				serverAddr, fsNum = parseurl(c.Args().Get(0), "8445")
				if fsNum == "" {
					fmt.Println("Missing file system id")
					os.Exit(1)
				}
				if fsName != "" {
					fmt.Printf("Invalid File System String: %q\n", fsName)
					os.Exit(1)
				}
				fsMod := &pb.ModFS{
					Name: c.String("name"),
				}
				conn := setupWS(serverAddr)
				ws := pb.NewFileSystemAPIClient(conn)
				result, err := ws.UpdateFS(context.Background(), &pb.UpdateFSRequest{Token: gtoken, FSid: fsNum, Filesys: fsMod})
				if err != nil {
					log.Fatalf("Bad Request: %v", err)
					conn.Close()
					os.Exit(1)
				}
				conn.Close()
				log.Printf("Update Results: %s", result.Data)
				return nil
			},
		},
		{
			Name:      "grant",
			Usage:     "Grant an Addr access to a File Systems",
			ArgsUsage: "-addr <IP Address>  <region>://<file system uuid>",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:        "addr",
					Value:       "",
					Usage:       "Address to Grant",
					Destination: &addrValue,
				},
			},
			Action: func(c *cli.Context) error {
				if !c.Args().Present() {
					fmt.Println("Invalid syntax for delete.")
					os.Exit(1)
				}
				if gtoken == "" {
					fmt.Println("Token is required")
					os.Exit(1)
				}
				if addrValue == "" {
					fmt.Println("addr is required")
					os.Exit(1)
				}
				serverAddr, fsNum = parseurl(c.Args().Get(0), "8445")
				if fsNum == "" {
					fmt.Println("Missing file system id")
					os.Exit(1)
				}
				conn := setupWS(serverAddr)
				ws := pb.NewFileSystemAPIClient(conn)
				result, err := ws.GrantAddrFS(context.Background(), &pb.GrantAddrFSRequest{Token: gtoken, FSid: fsNum, Addr: addrValue})
				if err != nil {
					log.Fatalf("Bad Request: %v", err)
					conn.Close()
					os.Exit(1)
				}
				conn.Close()
				log.Printf("Result: %s\n", result.Data)
				return nil
			},
		},
		{
			Name:      "revoke",
			Usage:     "Revoke an Addr's access to a File Systems",
			ArgsUsage: "-addr <IP Address>  <region>://<file system uuid>",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:        "addr",
					Value:       "",
					Usage:       "Address to Revoke",
					Destination: &addrValue,
				},
			},
			Action: func(c *cli.Context) error {
				if !c.Args().Present() {
					fmt.Println("Invalid syntax for revoke.")
					os.Exit(1)
				}
				if gtoken == "" {
					fmt.Println("Token is required")
					os.Exit(1)
				}
				if addrValue == "" {
					fmt.Println("addr is required")
					os.Exit(1)
				}
				serverAddr, fsNum = parseurl(c.Args().Get(0), "8445")
				if fsNum == "" {
					fmt.Println("Missing file system id")
					os.Exit(1)
				}
				conn := setupWS(serverAddr)
				ws := pb.NewFileSystemAPIClient(conn)
				result, err := ws.RevokeAddrFS(context.Background(), &pb.RevokeAddrFSRequest{Token: gtoken, FSid: fsNum, Addr: addrValue})
				if err != nil {
					log.Fatalf("Bad Request: %v", err)
					conn.Close()
					os.Exit(1)
				}
				conn.Close()
				log.Printf("Result: %s\n", result.Data)
				return nil
			},
		},
		{
			Name:      "mount",
			Usage:     "mount a file system",
			ArgsUsage: "<region>://<file system uuid> <mount point> -o [OPTIONS]",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "o",
					Value: "",
					Usage: "mount options",
				},
			},
			Action: func(c *cli.Context) error {
				if !c.Args().Present() {
					fmt.Println("Invalid syntax for revoke.")
					os.Exit(1)
				}
				serverAddr, fsNum = parseurl(c.Args().Get(0), "8445")
				fsnum, err := uuid.FromString(fsNum)
				if err != nil {
					fmt.Print("File System id is not valid: ", err)
				}
				mountpoint := c.Args().Get(1)
				// check mountpoint exists
				if _, ferr := os.Stat(mountpoint); os.IsNotExist(ferr) {
					log.Printf("Mount point %s does not exist\n\n", mountpoint)
					os.Exit(1)
				}
				fusermountPath()
				// process file system options
				allowOther := false
				debugOff := true
				if c.String("o") != "" {
					clargs := getArgs(c.String("o"))
					// crapy debug log handling :)
					if debug, ok := clargs["debug"]; ok {
						debugOff = debug == "false"
					}
					_, allowOther = clargs["allow_other"]
				}
				if debugOff {
					log.SetFlags(0)
					log.SetOutput(ioutil.Discard)
				}
				// Setup grpc
				var opts []grpc.DialOption
				creds := credentials.NewTLS(&tls.Config{
					InsecureSkipVerify: true,
				})
				opts = append(opts, grpc.WithTransportCredentials(creds))
				conn, err := grpc.Dial(serverAddr, opts...)
				if err != nil {
					log.Fatalf("failed to dial: %v", err)
				}
				defer conn.Close()
				// Work with fuse
				var cfs *fuse.Conn
				// TODO: Make setting the fuse config cleaner
				if allowOther {
					cfs, err = fuse.Mount(
						mountpoint,
						fuse.FSName("cfs"),
						fuse.Subtype("cfs"),
						fuse.LocalVolume(),
						fuse.VolumeName("CFS"),
						fuse.AllowOther(),
						fuse.DefaultPermissions(),
						fuse.MaxReadahead(128*1024),
						fuse.AsyncRead(),
					)
				} else {
					cfs, err = fuse.Mount(
						mountpoint,
						fuse.FSName("cfs"),
						fuse.Subtype("cfs"),
						fuse.LocalVolume(),
						fuse.VolumeName("CFS"),
						fuse.DefaultPermissions(),
						fuse.MaxReadahead(128*1024),
						fuse.AsyncRead(),
						//fuse.WritebackCache(), // Waiting on concurrent chunk update fix
						//fuse.AutoInvalData(),  // requires https://github.com/bazil/fuse/pull/137
					)
				}
				if err != nil {
					log.Fatal(err)
				}
				defer cfs.Close()

				rpc := newrpc(conn)
				fs := newfs(cfs, rpc, fsnum.String())
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
				return nil
			},
		},
	}
	app.Run(os.Args)
}

// getArgs is passed a command line and breaks it up into commands
// the valid format is <device> <mount point> -o [Options]
func getArgs(args string) map[string]string {
	// Setup declarations
	var optList []string
	requiredOptions := []string{}
	clargs := make(map[string]string)

	// process options -o
	optList = strings.Split(args, ",")
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

	// Verify required options exist
	for _, v := range requiredOptions {
		_, ok := clargs[v]
		if !ok {
			log.Printf("%s is a required option", v)
			os.Exit(1)
		}
	}

	// load in device and mountPoint
	return clargs
}

func fusermountPath() {
	// Grab the current path
	currentPath := os.Getenv("PATH")
	if len(currentPath) == 0 {
		// using mount seem to not have a path
		// fusermount is in /bin
		os.Setenv("PATH", "/bin")
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

// parseurl ...
func parseurl(urlstr string, port string) (string, string) {

	u, err := url.Parse(urlstr)
	if err != nil {
		fmt.Printf("Url parse error: %v\n", err)
		os.Exit(1)
	}
	srv, ok := regions[u.Scheme]
	if !ok {
		fmt.Printf("Invalid region %s", u.Scheme)
		os.Exit(1)
	}
	return fmt.Sprintf("%s:%s", srv, port), u.Host
}
