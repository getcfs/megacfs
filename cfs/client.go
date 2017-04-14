package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"syscall"

	"github.com/getcfs/megacfs/formic"
	"github.com/getcfs/megacfs/ftls"
	"github.com/pkg/xattr"
	"golang.org/x/net/context"
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
	token := auth(authURL, username, password)
	rpc := formic.NewFormic("", addr, 1, &ftls.Config{InsecureSkipVerify: true})
	fsList, err := rpc.ListFS(context.Background(), token)
	if err != nil {
		fmt.Println("ListFS failed:", err)
		os.Exit(1)
	}
	fmt.Printf("%-36s    %s\n", "ID", "Name")
	for _, fs := range fsList {
		fmt.Printf("%-36s    %s\n", fs.FSID, fs.Name)
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
	token := auth(authURL, username, password)
	rpc := formic.NewFormic("", addr, 1, &ftls.Config{InsecureSkipVerify: true})
	name, addresses, err := rpc.ShowFS(context.Background(), token, fsid)
	if err != nil {
		fmt.Println("ShowFS failed", err)
		os.Exit(1)
	}
	fmt.Println("FSID:   ", fsid)
	fmt.Println("Name:   ", name)
	for _, address := range addresses {
		fmt.Println("Address:", address)
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
	token := auth(authURL, username, password)
	rpc := formic.NewFormic("", addr, 1, &ftls.Config{InsecureSkipVerify: true})
	fsid, err := rpc.CreateFS(context.Background(), token, name)
	if err != nil {
		fmt.Println("CreateFS failed:", err)
		os.Exit(1)
	}
	fmt.Println("FSID:", fsid)
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
	token := auth(authURL, username, password)
	rpc := formic.NewFormic("", addr, 1, &ftls.Config{InsecureSkipVerify: true})
	err := rpc.DeleteFS(context.Background(), token, fsid)
	if err != nil {
		fmt.Println("DeleteFS failed:", err)
		os.Exit(1)
	}
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
	newFSName := f.Args()[0]
	fsid := f.Args()[1]
	token := auth(authURL, username, password)
	rpc := formic.NewFormic("", addr, 1, &ftls.Config{InsecureSkipVerify: true})
	err := rpc.UpdateFS(context.Background(), token, fsid, newFSName)
	if err != nil {
		fmt.Println("UpdateFS failed:", err)
		os.Exit(1)
	}
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
	token := auth(authURL, username, password)
	rpc := formic.NewFormic("", addr, 1, &ftls.Config{InsecureSkipVerify: true})
	err := rpc.GrantAddressFS(context.Background(), token, fsid, ip)
	if err != nil {
		fmt.Println("GrantAddressFS failed:", err)
		os.Exit(1)
	}
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
	token := auth(authURL, username, password)
	rpc := formic.NewFormic("", addr, 1, &ftls.Config{InsecureSkipVerify: true})
	err := rpc.RevokeAddressFS(context.Background(), token, fsid, ip)
	if err != nil {
		fmt.Println("RevokeAddressFS failed", err)
		os.Exit(1)
	}
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
	parentPath := path.Dir(filePath)
	childName := path.Base(filePath)
	fsidBytes, err := xattr.Get(parentPath, "cfs.fsid")
	if err != nil {
		fmt.Println("Error determining fsid for path: ", parentPath, err)
		os.Exit(1)
	}
	var stat syscall.Stat_t
	if err = syscall.Stat(parentPath, &stat); err != nil {
		fmt.Println("Error stating:", parentPath, err)
		os.Exit(1)
	}
	rpc := formic.NewFormic(string(fsidBytes), addr, 1, &ftls.Config{InsecureSkipVerify: true})
	resp, err := rpc.Check(context.Background(), stat.Ino, childName)
	if err != nil {
		fmt.Println("Check failed:", err)
		os.Exit(1)
	}
	fmt.Println("Check complete:", resp)
	return nil
}
