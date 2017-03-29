package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"syscall"

	"github.com/getcfs/megacfs/formic/formicproto"
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
	ws := formicproto.NewFormicClient(c)
	token := auth(authURL, username, password)
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	ctx := context.Background()
	stream, err := ws.ListFS(ctx)
	if err != nil {
		fmt.Println("ListFS failed", err)
		os.Exit(1)
	}
	if err = stream.Send(&formicproto.ListFSRequest{RPCID: 1, Token: token}); err != nil {
		fmt.Println("ListFS failed", err)
		os.Exit(1)
	}
	listFSResp, err := stream.Recv()
	if err != nil {
		fmt.Println("ListFS failed", err)
		os.Exit(1)
	}
	if listFSResp.Err != "" {
		fmt.Println("ListFS failed", listFSResp.Err)
		os.Exit(1)
	}
	var data []map[string]interface{}
	if err := json.Unmarshal([]byte(listFSResp.Data), &data); err != nil {
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
	ws := formicproto.NewFormicClient(c)
	token := auth(authURL, username, password)
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	ctx := context.Background()
	stream, err := ws.ShowFS(ctx)
	if err != nil {
		fmt.Println("ShowFS failed", err)
		os.Exit(1)
	}
	if err = stream.Send(&formicproto.ShowFSRequest{RPCID: 1, Token: token, FSID: fsid}); err != nil {
		fmt.Println("ShowFS failed", err)
		os.Exit(1)
	}
	showFSResp, err := stream.Recv()
	if err != nil {
		fmt.Println("ShowFS failed", err)
		os.Exit(1)
	}
	if showFSResp.Err != "" {
		fmt.Println("ShowFS failed", showFSResp.Err)
		os.Exit(1)
	}
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(showFSResp.Data), &data); err != nil {
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
	ws := formicproto.NewFormicClient(c)
	token := auth(authURL, username, password)
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	ctx := context.Background()
	stream, err := ws.CreateFS(ctx)
	if err != nil {
		fmt.Println("CreateFS failed", err)
		os.Exit(1)
	}
	if err = stream.Send(&formicproto.CreateFSRequest{RPCID: 1, Token: token, FSName: name}); err != nil {
		fmt.Println("CreateFS failed", err)
		os.Exit(1)
	}
	createFSResp, err := stream.Recv()
	if err != nil {
		fmt.Println("CreateFS failed", err)
		os.Exit(1)
	}
	if createFSResp.Err != "" {
		fmt.Println("CreateFS failed", createFSResp.Err)
		os.Exit(1)
	}
	fmt.Println("ID:", createFSResp.Data)
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
	ws := formicproto.NewFormicClient(c)
	token := auth(authURL, username, password)

	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := ws.DeleteFS(context.Background())
	if err != nil {
		fmt.Println("DeleteFS failed", err)
		os.Exit(1)
	}
	if err = stream.Send(&formicproto.DeleteFSRequest{RPCID: 1, Token: token, FSID: fsid}); err != nil {
		fmt.Println("DeleteFS failed", err)
		os.Exit(1)
	}
	deleteFSResp, err := stream.Recv()
	if err != nil {
		fmt.Println("DeleteFS failed", err)
		os.Exit(1)
	}
	if deleteFSResp.Err != "" {
		fmt.Println("DeleteFS failed", deleteFSResp.Err)
		os.Exit(1)
	}
	fmt.Println(deleteFSResp.Data)
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
	newFS := &formicproto.ModFS{
		Name: f.Args()[0],
	}
	fsid := f.Args()[1]
	c := setupWS(addr)
	defer c.Close()
	ws := formicproto.NewFormicClient(c)
	token := auth(authURL, username, password)
	ctx := context.Background()
	ctx = metadata.NewContext(
		ctx,
		metadata.Pairs("fsid", fsid),
	)
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := ws.UpdateFS(ctx)
	if err != nil {
		fmt.Println("UpdateFS failed", err)
		os.Exit(1)
	}
	if err = stream.Send(&formicproto.UpdateFSRequest{RPCID: 1, Token: token, FSID: fsid, FileSys: newFS}); err != nil {
		fmt.Println("UpdateFS failed", err)
		os.Exit(1)
	}
	updateFSResp, err := stream.Recv()
	if err != nil {
		fmt.Println("UpdateFS failed", err)
		os.Exit(1)
	}
	if updateFSResp.Err != "" {
		fmt.Println("UpdateFS failed", updateFSResp.Err)
		os.Exit(1)
	}
	fmt.Println(updateFSResp.Data)
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
	ws := formicproto.NewFormicClient(c)
	token := auth(authURL, username, password)
	ctx := context.Background()
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := ws.GrantAddrFS(ctx)
	if err != nil {
		fmt.Println("GrantAddrFS failed", err)
		os.Exit(1)
	}
	if err = stream.Send(&formicproto.GrantAddrFSRequest{RPCID: 1, Token: token, FSID: fsid, Addr: ip}); err != nil {
		fmt.Println("GrantAddrFS failed", err)
		os.Exit(1)
	}
	grantAddrFSResp, err := stream.Recv()
	if err != nil {
		fmt.Println("GrantAddrFS failed", err)
		os.Exit(1)
	}
	if grantAddrFSResp.Err != "" {
		fmt.Println("GrantAddrFS failed", grantAddrFSResp.Err)
		os.Exit(1)
	}
	fmt.Println(grantAddrFSResp.Data)
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
	ws := formicproto.NewFormicClient(c)
	token := auth(authURL, username, password)
	ctx := context.Background()
	// TODO: Placeholder code to get things working; needs to be replaced to be
	// more like oort's client code.
	stream, err := ws.RevokeAddrFS(ctx)
	if err != nil {
		fmt.Println("RevokeAddrFS failed", err)
		os.Exit(1)
	}
	if err = stream.Send(&formicproto.RevokeAddrFSRequest{RPCID: 1, Token: token, FSID: fsid, Addr: ip}); err != nil {
		fmt.Println("RevokeAddrFS failed", err)
		os.Exit(1)
	}
	revokeAddrFSResp, err := stream.Recv()
	if err != nil {
		fmt.Println("RevokeAddrFS failed", err)
		os.Exit(1)
	}
	if revokeAddrFSResp.Err != "" {
		fmt.Println("RevokeAddrFS failed", revokeAddrFSResp.Err)
		os.Exit(1)
	}
	fmt.Println(revokeAddrFSResp.Data)
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
	var stat syscall.Stat_t
	err = syscall.Stat(dirPath, &stat)
	if err != nil {
		fmt.Println("Error stating:", dirPath)
		fmt.Println(err)
		os.Exit(1)
	}
	c := setupWS(addr)
	defer c.Close()
	ws := formicproto.NewFormicClient(c)
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
	if err = stream.Send(&formicproto.CheckRequest{RPCID: 1, INode: stat.Ino, Name: fileName}); err != nil {
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
