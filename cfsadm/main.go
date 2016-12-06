package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/cloudflare/cfssl/cli/genkey"
	"github.com/cloudflare/cfssl/config"
	"github.com/cloudflare/cfssl/csr"
	"github.com/cloudflare/cfssl/initca"
	"github.com/cloudflare/cfssl/signer"
	"github.com/cloudflare/cfssl/signer/universal"
	"github.com/gholt/ring"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		args = []string{"help"}
	}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "help":
			fmt.Println("init       Creates a new CA and initial ring builder file")
			fmt.Println("add <ip>   Adds the IP to the ring and creates a new certificate for it")
			os.Exit(1)
		case "init":
			for _, name := range []string{"/etc/cfsd/ca.pem", "/etc/cfsd/ca-key.pem", "/etc/cfsd/cfs.builder", "/etc/cfsd/cfs.ring"} {
				if _, err := os.Lstat(name); err == nil {
					panic(name + " already exists")
				}
			}
			cert, _, key, err := initca.New(&csr.CertificateRequest{
				CN:         "CA",
				KeyRequest: &csr.BasicKeyRequest{A: "rsa", S: 2048},
			})
			if err != nil {
				panic(err)
			}
			if err = ioutil.WriteFile("/etc/cfsd/ca.pem", cert, 0664); err != nil {
				panic(err)
			}
			if err = ioutil.WriteFile("/etc/cfsd/ca-key.pem", key, 0600); err != nil {
				panic(err)
			}
			if err = ring.CLI([]string{"cfsadm init", "/etc/cfsd/cfs.builder", "create"}, os.Stdout, false); err != nil {
				panic(err)
			}
		case "add":
			i++
			if i >= len(args) {
				panic("The 'add' command requires the IP address to add.")
			}
			ipAddr := args[i]
			for _, name := range []string{"/etc/cfsd/" + ipAddr + ".pem", "/etc/cfsd/" + ipAddr + "-key.pem"} {
				if _, err := os.Lstat(name); err == nil {
					panic(name + " already exists")
				}
			}
			csrBytes, key, err := (&csr.Generator{
				Validator: genkey.Validator,
			}).ProcessRequest(&csr.CertificateRequest{
				CN:         "CA",
				KeyRequest: &csr.BasicKeyRequest{A: "rsa", S: 2048},
			})
			if err != nil {
				panic(err)
			}
			root := universal.Root{
				Config: map[string]string{
					"cert-file": "/etc/cfsd/ca.pem",
					"key-file":  "/etc/cfsd/ca-key.pem",
				},
			}
			policy := &config.Signing{
				Profiles: map[string]*config.SigningProfile{},
				Default:  config.DefaultConfig(),
			}
			signerr, err := universal.NewSigner(root, policy)
			if err != nil {
				panic(err)
			}
			cert, err := signerr.Sign(signer.SignRequest{
				Request: string(csrBytes),
				Hosts:   []string{ipAddr},
			})
			if err != nil {
				panic(err)
			}
			if err = ioutil.WriteFile("/etc/cfsd/"+ipAddr+".pem", cert, 0664); err != nil {
				panic(err)
			}
			if err = ioutil.WriteFile("/etc/cfsd/"+ipAddr+"-key.pem", key, 0600); err != nil {
				panic(err)
			}
			replicaCount := 3
			if _, b, err := ring.RingOrBuilder("/etc/cfsd/cfs.builder"); err != nil || b == nil {
				panic(err)
			} else if len(b.Nodes())+1 < replicaCount {
				replicaCount = len(b.Nodes()) + 1
			}
			if err = ring.CLI([]string{"cfsadm add", "/etc/cfsd/cfs.builder", "set-replicas", fmt.Sprintf("%d", replicaCount)}, os.Stdout, false); err != nil {
				panic(err)
			}
			if err = ring.CLI([]string{"cfsadm add", "/etc/cfsd/cfs.builder", "add", "address0=" + ipAddr + ":12300", "address1=" + ipAddr + ":12310", "address2=" + ipAddr + ":12311", "address3=" + ipAddr + ":12320", "address4=" + ipAddr + ":12321"}, os.Stdout, false); err != nil {
				panic(err)
			}
			if err = ring.CLI([]string{"cfsadm add", "/etc/cfsd/cfs.builder", "ring"}, os.Stdout, false); err != nil {
				panic(err)
			}
		default:
			panic(args[i])
		}
	}
}
