package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	pb "github.com/getcfs/megacfs/formic/proto"
	"golang.org/x/net/context"
)

var regions = map[string]string{
	"aio": "127.0.0.1:8445",
	"iad": "api.ea.iad.rackfs.com:8445",
	"dev": "api.dev.iad.rackfs.com:8445",
}

func configure(configfile string) error {
	stat, _ := os.Stdin.Stat()
	var region, username, apikey string
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		fmt.Scan(&region)
		fmt.Scan(&username)
		fmt.Scan(&apikey)
	} else {
		fmt.Println("This is an interactive session to configure the cfs client.")
		fmt.Print("CFS Region: ")
		fmt.Scan(&region)
		fmt.Print("CFS Username: ")
		fmt.Scan(&username)
		fmt.Print("CFS APIKey: ")
		fmt.Scan(&apikey)
	}
	config := map[string]string{
		"region":   strings.ToLower(region),
		"username": username,
		"apikey":   apikey,
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

func list(region, username, apikey string) error {
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
		return errors.New(fmt.Sprintf("Invalid region: %s\n", region))
	}
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(username, apikey)
	res, err := ws.ListFS(context.Background(), &pb.ListFSRequest{Token: token})
	if err != nil {
		return errors.New(fmt.Sprintf("Request Error: %v\n", err))
	}
	var data []map[string]interface{}
	if err := json.Unmarshal([]byte(res.Data), &data); err != nil {
		return errors.New(fmt.Sprintf("Error unmarshalling response: %v\n", err))
	}
	fmt.Printf("%-36s    %s\n", "ID", "Name")
	for _, fs := range data {
		fmt.Printf("%-36s    %s\n", fs["id"], fs["name"])
	}

	return nil
}

func show(region, username, apikey string) error {
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
		return errors.New(fmt.Sprintf("Invalid region: %s\n", region))
	}
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(username, apikey)
	res, err := ws.ShowFS(context.Background(), &pb.ShowFSRequest{Token: token, FSid: fsid})
	if err != nil {
		return errors.New(fmt.Sprintf("Request Error: %v\n", err))
	}
	c.Close()
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(res.Data), &data); err != nil {
		return errors.New(fmt.Sprintf("Error unmarshalling response: %v\n", err))
	}
	fmt.Println("ID:", data["id"])
	fmt.Println("Name:", data["name"])
	//fmt.Println("Status:", data["status"])
	for _, ip := range data["addrs"].([]interface{}) {
		fmt.Println("IP:", ip)
	}

	return nil
}

func create(region, username, apikey string) error {
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
		return errors.New(fmt.Sprintf("Invalid region: %s\n", region))
	}
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(username, apikey)
	res, err := ws.CreateFS(context.Background(), &pb.CreateFSRequest{Token: token, FSName: name})
	if err != nil {
		return errors.New(fmt.Sprintf("Request Error: %v\n", err))
	}
	fmt.Println("ID:", res.Data)

	return nil
}

func del(region, username, apikey string) error {
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
		return errors.New(fmt.Sprintf("Invalid region: %s\n", region))
	}
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(username, apikey)
	res, err := ws.DeleteFS(context.Background(), &pb.DeleteFSRequest{Token: token, FSid: fsid})
	if err != nil {
		return errors.New(fmt.Sprintf("Request Error: %v\n", err))
	}
	fmt.Println(res.Data)

	return nil
}

func update(region, username, apikey string) error {
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
	addr, ok := regions[region]
	if !ok {
		return errors.New(fmt.Sprintf("Invalid region: %s\n", region))
	}
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(username, apikey)
	res, err := ws.UpdateFS(context.Background(), &pb.UpdateFSRequest{Token: token, FSid: fsid, Filesys: newFS})
	if err != nil {
		return errors.New(fmt.Sprintf("Request Error: %v\n", err))
	}
	fmt.Println(res.Data)

	return nil
}

func grant(region, username, apikey string) error {
	var ip, fsid string
	f := flag.NewFlagSet("grant", flag.ContinueOnError)
	f.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("    cfs grant [ip] <fsid>")
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
	addr, ok := regions[region]
	if !ok {
		return errors.New(fmt.Sprintf("Invalid region: %s\n", region))
	}
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(username, apikey)
	_, err := ws.GrantAddrFS(context.Background(), &pb.GrantAddrFSRequest{Token: token, FSid: fsid, Addr: ip})
	if err != nil {
		return errors.New(fmt.Sprintf("Request Error: %v\n", err))
	}

	return nil
}

func revoke(region, username, apikey string) error {
	var ip, fsid string
	f := flag.NewFlagSet("revoke", flag.ContinueOnError)
	f.Usage = func() {
		fmt.Println("Usage:")
		fmt.Println("    cfs revoke [ip] <fsid>")
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
	addr, ok := regions[region]
	if !ok {
		return errors.New(fmt.Sprintf("Invalid region: %s\n", region))
	}
	c := setupWS(addr)
	defer c.Close()
	ws := pb.NewFileSystemAPIClient(c)
	token := auth(username, apikey)
	_, err := ws.RevokeAddrFS(context.Background(), &pb.RevokeAddrFSRequest{Token: token, FSid: fsid, Addr: ip})
	if err != nil {
		return errors.New(fmt.Sprintf("Request Error: %v\n", err))
	}

	return nil
}
