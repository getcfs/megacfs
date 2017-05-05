// Package main builds a 'cfs' executable that is the client application for
// CFS.
package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"strings"
	"sync"
	"syscall"

	"github.com/getcfs/megacfs/formic"
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

// NullWriter ...
type NullWriter int

func (NullWriter) Write([]byte) (int, error) { return 0, nil }

var cfsVersion string
var buildDate string
var commitVersion string
var goVersion string

func auth(authURL string, username string, password string) string {
	var token string
	logger.Debug("auth called", zap.String("authURL", authURL), zap.String("username", username))
	if authURL == "dev" {
		logger.Debug("auth short-circuited due to 'dev' mode")
		return ""
	}
	if token = os.Getenv("OS_TOKEN"); token != "" {
		logger.Debug("auth short-circuited due to OS_TOKEN set")
		return token
	}
	body := fmt.Sprintf(`{"auth":{"identity":{"methods":["password"],"password":{"user":{
		"domain":{"id":"default"},"name":"%s","password":"%s"}}}}}`, username, password)
	rbody := strings.NewReader(body)
	if !strings.HasSuffix(authURL, "/") {
		authURL += "/"
	}
	req, err := http.NewRequest("POST", authURL+"v3/auth/tokens", rbody)
	if err != nil {
		logger.Debug("auth error from NewRequest POST", zap.Error(err))
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Debug("auth error from DefaultClient.Do POST", zap.Error(err))
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		logger.Debug("auth error from POST response status", zap.Int("status", resp.StatusCode))
		os.Exit(1)
	}
	token = resp.Header.Get("X-Subject-Token")
	if len(token) == 0 {
		logger.Debug("auth succeeded but ended up with zero-length token")
	} else {
		logger.Debug("auth succeeded")
	}
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
    -o debug               enables debug output
    -o ro                  mount the filesystem read only
    -o allow_other         allow access to other users
    -o noumount            skips the fusermount -uz retry on first error
    -o pprof=<host:port>   Go tool pprof support; listens on <host:port>
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
	addrFSID := f.Args()[0]
	if !strings.Contains(addrFSID, ":") {
		addrFSID = config["addr"] + ":" + addrFSID
	}
	parts := strings.Split(addrFSID, ":")
	if len(parts) != 2 {
		fmt.Println("Invalid filesystem:", addrFSID)
		return usageErr
	}
	addr := fmt.Sprintf("%s:%d", strings.ToLower(parts[0]), PORT)
	fsid := parts[1]
	mountpoint := f.Args()[1]
	// Verify mountpoint exists
	_, err := os.Stat(mountpoint)
	if os.IsNotExist(err) {
		return fmt.Errorf("Mount point %s does not exist\n", mountpoint)
	}
	// parse mount options string
	clargs := getArgs(options)
	if hostPort, ok := clargs["pprof"]; ok {
		if hostPort == "" {
			hostPort = ":9100"
		}
		if strings.HasPrefix(hostPort, "*:") {
			hostPort = hostPort[1:]
		}
		go http.ListenAndServe(hostPort, nil)
	}
	for iteration := 0; ; iteration++ {
		switch iteration {
		case 0:
		case 1:
			if _, noumount := clargs["noumount"]; noumount {
				return err
			}
			logger.Debug("retrying after fusermount -uz", zap.Error(err))
			exec.Command("fusermount", "-uz", mountpoint).Run()
		default:
			return err
		}
		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		waitGroup := &sync.WaitGroup{}
		shutdownChan := make(chan struct{})
		waitGroup.Add(1)
		go func() {
			for {
				select {
				case <-ch:
					logger.Debug("exit signal received, attempting to unmount")
					exec.Command("fusermount", "-u", mountpoint).Run()
					waitGroup.Done()
					return
				case <-shutdownChan:
					waitGroup.Done()
					return
				}
			}
		}()
		_, allowOther := clargs["allow_other"]
		_, readOnly := clargs["ro"]
		if err = formic.NewFuseFormic(&formic.FuseFormicConfig{
			Logger:     logger,
			Mountpoint: mountpoint,
			Address:    addr,
			FSID:       fsid,
			AllowOther: allowOther,
			ReadOnly:   readOnly,
		}).Serve(); err != nil {
			continue
		}
		close(shutdownChan)
		waitGroup.Wait()
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
