package main

import (
	"io/ioutil"
	"os"

	"github.com/cloudflare/cfssl/cli/genkey"
	"github.com/cloudflare/cfssl/config"
	"github.com/cloudflare/cfssl/csr"
	"github.com/cloudflare/cfssl/initca"
	"github.com/cloudflare/cfssl/signer"
	"github.com/cloudflare/cfssl/signer/universal"
)

func main() {
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "ca":
			cert, _, key, err := initca.New(&csr.CertificateRequest{
				CN:         "CA",
				KeyRequest: &csr.BasicKeyRequest{A: "rsa", S: 2048},
			})
			if err != nil {
				panic(err)
			}
			if err = ioutil.WriteFile("ca.pem", cert, 0664); err != nil {
				panic(err)
			}
			if err = ioutil.WriteFile("ca-key.pem", key, 0600); err != nil {
				panic(err)
			}
		case "add":
			i++
			if i >= len(os.Args) {
				panic("The 'add' command requires the IP address to add.")
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
					"cert-file": "ca.pem",
					"key-file":  "ca-key.pem",
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
				Hosts:   []string{os.Args[i]},
			})
			if err != nil {
				panic(err)
			}
			if err = ioutil.WriteFile("cert.pem", cert, 0664); err != nil {
				panic(err)
			}
			if err = ioutil.WriteFile("cert-key.pem", key, 0600); err != nil {
				panic(err)
			}
		default:
			panic(os.Args[i])
		}
	}
}
