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
	help := `
init        Creates a new CA, conf file, and initial ring builder file
add <ip>    Adds the IP to the ring, one for all services, and creates a new
            certificate for it
add <public_ip> <private_ip>
            Same as the single IP add, but the first IP is just for Formic (the
            public facing API service) and the other IP is for the rest.
`
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println(help)
		os.Exit(1)
	}
	switch args[0] {
	default:
		fmt.Println(help)
		os.Exit(1)
	case "init":
		if len(args) > 1 {
			fmt.Println("'init' takes no arguments")
			fmt.Println(help)
			os.Exit(1)
		}
		if err := os.Mkdir("/etc/cfsd", 0700); err != nil && !os.IsExist(err) {
			panic(err)
		}
		fp, err := os.OpenFile("/etc/cfsd/cfsd.conf", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			if !os.IsExist(err) {
				panic(err)
			}
		} else {
			fp.Write([]byte("AUTH_URL=http://localhost:5000\nAUTH_USER=admin\nAUTH_PASSWORD=admin\n"))
			fp.Close()
		}
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
		args = args[1:]
		var publicIP string
		var privateIP string
		if len(args) > 0 {
			publicIP = args[0]
			args = args[1:]
		}
		if len(args) > 0 {
			privateIP = args[0]
			args = args[1:]
		}
		if len(args) > 0 {
			fmt.Println("too many arguments; syntax: <ip> [ip]")
			fmt.Println(help)
			os.Exit(1)
		}
		if publicIP == "" {
			panic("The 'add' command requires the IP address to add.")
		}
		ips := []string{publicIP}
		if privateIP != "" {
			ips = append(ips, privateIP)
		} else {
			privateIP = publicIP
		}
		for _, ip := range ips {
			for _, name := range []string{"/etc/cfsd/" + ip + ".pem", "/etc/cfsd/" + ip + "-key.pem"} {
				if _, err := os.Lstat(name); err == nil {
					panic(name + " already exists")
				}
			}
		}
		for _, ip := range ips {
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
				Hosts:   []string{ip},
			})
			if err != nil {
				panic(err)
			}
			if err = ioutil.WriteFile("/etc/cfsd/"+ip+".pem", cert, 0664); err != nil {
				panic(err)
			}
			if err = ioutil.WriteFile("/etc/cfsd/"+ip+"-key.pem", key, 0600); err != nil {
				panic(err)
			}
		}
		replicaCount := 3
		if _, b, err := ring.RingOrBuilder("/etc/cfsd/cfs.builder"); err != nil || b == nil {
			panic(err)
		} else if len(b.Nodes())+1 < replicaCount {
			replicaCount = len(b.Nodes()) + 1
		}
		if err := ring.CLI([]string{"cfsadm add", "/etc/cfsd/cfs.builder", "set-replicas", fmt.Sprintf("%d", replicaCount)}, os.Stdout, false); err != nil {
			panic(err)
		}
		if err := ring.CLI([]string{"cfsadm add", "/etc/cfsd/cfs.builder", "add", "address0=" + publicIP + ":9100", "address1=" + publicIP + ":12300", "address2=" + privateIP + ":12310", "address3=" + privateIP + ":12311", "address4=" + privateIP + ":12320", "address5=" + privateIP + ":12321"}, os.Stdout, false); err != nil {
			panic(err)
		}
		if err := ring.CLI([]string{"cfsadm add", "/etc/cfsd/cfs.builder", "ring"}, os.Stdout, false); err != nil {
			panic(err)
		}
	}
}
