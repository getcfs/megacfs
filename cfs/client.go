package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"syscall"

	pb "github.com/getcfs/megacfs/formic/formicproto"
	"github.com/getcfs/megacfs/formic/newproto"
	"github.com/pkg/xattr"
	"golang.org/x/net/context"
	"google.golang.org/grpc/metadata"
)

// configuration ...
func configure(configfile string) error {
	stat, _ := os.Stdin.Stat()
	var authURL, username, password, addr string
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		fmt.Scan(&authURL)
		fmt.Scan(&username)
		fmt.Scan(&password)
		fmt.Scan(&addr)
	} else {
		fmt.Println("This is an interactive session to configure the cfs client.")
		fmt.Print("Auth URL: ")
		fmt.Scan(&authURL)
		fmt.Print("Username: ")
		fmt.Scan(&username)
		fmt.Print("Password: ")
		fmt.Scan(&password)
		fmt.Print("CFS Server: ")
		fmt.Scan(&addr)
	}
	config := map[string]string{
		"authURL":  authURL,
		"username": username,
		"password": password,
		"addr":     addr,
	}
	c, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(configfile, c, 0600)
	if err != nil {
		return err
	}
	return nil
}

func list(addr, authURL, username, password string) error {
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
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(authURL, username, password)
	res, err := ws.ListFS(context.Background(), &pb.ListFSRequest{Token: token})
	if err != nil {
		return fmt.Errorf("Request Error: %v", err)
	}
	var data []map[string]interface{}
	if err := json.Unmarshal([]byte(res.Data), &data); err != nil {
		return fmt.Errorf("Error unmarshalling response: %v", err)
	}
	fmt.Printf("%-36s    %s\n", "ID", "Name")
	for _, fs := range data {
		fmt.Printf("%-36s    %s\n", fs["id"], fs["name"])
	}

	return nil
}

func show(addr, authURL, username, password string) error {
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
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(authURL, username, password)
	res, err := ws.ShowFS(context.Background(), &pb.ShowFSRequest{Token: token, FSid: fsid})
	if err != nil {
		return fmt.Errorf("Request Error: %v", err)
	}
	c.Close()
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(res.Data), &data); err != nil {
		return fmt.Errorf("Error unmarshalling response: %v", err)
	}
	fmt.Println("ID:", data["id"])
	fmt.Println("Name:", data["name"])
	//fmt.Println("Status:", data["status"])
	for _, ip := range data["addrs"].([]interface{}) {
		fmt.Println("IP:", ip)
	}

	return nil
}

func create(addr, authURL, username, password string) error {
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
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(authURL, username, password)
	res, err := ws.CreateFS(context.Background(), &pb.CreateFSRequest{Token: token, FSName: name})
	if err != nil {
		return fmt.Errorf("Request Error: %v", err)
	}
	fmt.Println("ID:", res.Data)

	return nil
}

func del(addr, authURL, username, password string) error {
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
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(authURL, username, password)
	res, err := ws.DeleteFS(context.Background(), &pb.DeleteFSRequest{Token: token, FSid: fsid})
	if err != nil {
		return fmt.Errorf("Request Error: %v", err)
	}
	fmt.Println(res.Data)

	return nil
}

func update(addr, authURL, username, password string) error {
	f := flag.NewFlagSet("update", flag.ContinueOnError)
	f.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("    cfs update <name> <fsid>")
		fmt.Println("Example:")
		fmt.Println("    cfs update newname 11111111-1111-1111-1111-111111111111")
		os.Exit(1)
	}
	f.Parse(flag.Args()[1:])
	if f.NArg() != 2 {
		f.Usage()
	}
	newFS := &pb.ModFS{
		Name: f.Args()[0],
	}
	fsid := f.Args()[1]
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(authURL, username, password)
	res, err := ws.UpdateFS(context.Background(), &pb.UpdateFSRequest{Token: token, FSid: fsid, Filesys: newFS})
	if err != nil {
		return fmt.Errorf("Request Error: %v", err)
	}
	fmt.Println(res.Data)

	return nil
}

func grant(addr, authURL, username, password string) error {
	var ip, fsid string
	f := flag.NewFlagSet("grant", flag.ContinueOnError)
	f.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("    cfs grant [client ip] <fsid>")
		fmt.Println("Example:")
		fmt.Println("    cfs grant 11111111-1111-1111-1111-111111111111")
		fmt.Println("    cfs grant 1.1.1.1 11111111-1111-1111-1111-111111111111")
		os.Exit(1)
	}
	f.Parse(flag.Args()[1:])
	if f.NArg() == 1 {
		ip = ""
		fsid = f.Args()[0]
	} else if f.NArg() == 2 {
		ip = f.Args()[0]
		fsid = f.Args()[1]
	} else {

		f.Usage()
	}
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(authURL, username, password)
	res, err := ws.GrantAddrFS(context.Background(), &pb.GrantAddrFSRequest{Token: token, FSid: fsid, Addr: ip})
	if err != nil {
		return fmt.Errorf("Request Error: %v", err)
	}
	fmt.Println(res.Data)
	return nil
}

func revoke(addr, authURL, username, password string) error {
	var ip, fsid string
	f := flag.NewFlagSet("revoke", flag.ContinueOnError)
	f.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("    cfs revoke [client ip] <fsid>")
		fmt.Println("Example:")
		fmt.Println("    cfs revoke 11111111-1111-1111-1111-111111111111")
		fmt.Println("    cfs revoke 1.1.1.1 11111111-1111-1111-1111-111111111111")
		os.Exit(1)
	}
	f.Parse(flag.Args()[1:])
	if f.NArg() == 1 {
		ip = ""
		fsid = f.Args()[0]
	} else if f.NArg() == 2 {
		ip = f.Args()[0]
		fsid = f.Args()[1]
	} else {
		f.Usage()
	}
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(authURL, username, password)
	res, err := ws.RevokeAddrFS(context.Background(), &pb.RevokeAddrFSRequest{Token: token, FSid: fsid, Addr: ip})
	if err != nil {
		return fmt.Errorf("Request Error: %v", err)
	}
	fmt.Println(res.Data)
	return nil
}

func check(addr string) error {
	var filePath string
	f := flag.NewFlagSet("check", flag.ContinueOnError)
	f.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("    cfs check <path/of/file/to/check>")
		fmt.Println("Example:")
		fmt.Println("    cfs check ./badfile.txt")
		os.Exit(1)
	}
	f.Parse(flag.Args()[1:])
	if f.NArg() == 1 {
		filePath = f.Args()[0]
	} else {
		f.Usage()
	}
	filePath = path.Clean(filePath)
	fileName := path.Base(filePath)
	dirPath := path.Dir(filePath)
	// Check for the fsid
	fsidBytes, err := xattr.Get(dirPath, "cfs.fsid")
	if err == nil && len(fsidBytes) != 36 {
		err = fmt.Errorf("cfs.fsid incorrect length %d != 36", len(fsidBytes))
	}
	if err != nil {
		fmt.Println("Error determining fsid for path: ", dirPath)
		fmt.Println(err)
		os.Exit(1)
	}
	fsid := string(fsidBytes)
	// Get the inode of the parent dir
	var stat syscall.Stat_t
	err = syscall.Stat(dirPath, &stat)
	if err != nil {
		fmt.Println("Error determining inode for: ", dirPath)
		fmt.Println(err)
		os.Exit(1)
	}
	c := setupWS(addr)
	defer c.Close()
	ws := newproto.NewFormicClient(c)
	ctx := context.Background()
	ctx = metadata.NewContext(
		ctx,
		metadata.Pairs("fsid", fsid),
	)
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := ws.Check(ctx)
	if err != nil {
		fmt.Println("Check failed", err)
		os.Exit(1)
	}
	if err = stream.Send(&newproto.CheckRequest{Rpcid: 1, Inode: stat.Ino, Name: fileName}); err != nil {
		fmt.Println("Check failed", err)
		os.Exit(1)
	}
	checkResp, err := stream.Recv()
	if err != nil {
		fmt.Println("Check failed", err)
		os.Exit(1)
	}
	if checkResp.Err != "" {
		fmt.Println("Check failed", checkResp.Err)
		os.Exit(1)
	}
	fmt.Println("Check complete:", checkResp.Response)
	return nil
}
