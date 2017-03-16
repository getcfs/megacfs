// Package ftls provides simpler/default ways to use TLS, including Mutual TLS,
// with CFS.
package ftls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var DefaultMinVersion = uint16(tls.VersionTLS12)

type Config struct {
	StrictClientVerify bool
	MutualTLS          bool
	InsecureSkipVerify bool
	MinVersion         uint16
	CipherSet          []uint16
	CertFile           string
	KeyFile            string
	CAFile             string
}

// DefaultServerFTLSConf returns a ftls config with the most commonly used
// server config set.
func DefaultServerFTLSConf(CertFile, KeyFile, CAFile string) *Config {
	return &Config{
		StrictClientVerify: true,
		MutualTLS:          true,
		InsecureSkipVerify: false,
		MinVersion:         DefaultMinVersion,
		CertFile:           CertFile,
		KeyFile:            KeyFile,
		CAFile:             CAFile,
	}
}

// DefaultClientFTLSConf returns a ftls config with the most commonly used
// client config set.
func DefaultClientFTLSConf(CertFile, KeyFile, CAFile string) *Config {
	return &Config{
		MutualTLS:          true,
		InsecureSkipVerify: false,
		CertFile:           CertFile,
		KeyFile:            KeyFile,
		CAFile:             CAFile,
	}
}

// newClientTLSConfig constructs a client tls.Conf from the provided ftls
// Config.
func newClientTLSConfig(c *Config) (*tls.Config, error) {
	clientCertPool := x509.NewCertPool()
	if c.CAFile != "" {
		clientCACert, err := ioutil.ReadFile(c.CAFile)
		if err != nil {
			return &tls.Config{}, fmt.Errorf("Unable to load CA cert %s: %s", c.CAFile, err.Error())
		}
		clientCertPool.AppendCertsFromPEM(clientCACert)
	}
	if c.MutualTLS {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return &tls.Config{}, fmt.Errorf("Unable to load cert %s %s: %s", c.CertFile, c.KeyFile, err.Error())
		}
		tlsConf := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      clientCertPool,
			CipherSuites: c.CipherSet,
			MinVersion:   DefaultMinVersion,
		}
		tlsConf.BuildNameToCertificate()
		return tlsConf, nil
	}
	return &tls.Config{RootCAs: clientCertPool, InsecureSkipVerify: c.InsecureSkipVerify}, nil
}

// NewServerTLSConfig constructs a server tls.Conf from the provided ftls
// Config.
func NewServerTLSConfig(c *Config) (*tls.Config, error) {
	tlsConf := &tls.Config{}
	if c.MutualTLS {
		caCert, err := ioutil.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("Unable to load ca cert %s: %s", c.CAFile, err.Error())
		}
		clientCertPool := x509.NewCertPool()
		if ok := clientCertPool.AppendCertsFromPEM(caCert); !ok {
			return nil, fmt.Errorf("Unable to append cert %s to pool.", c.CAFile)
		}
		strictness := tls.RequireAndVerifyClientCert
		if !c.StrictClientVerify {
			strictness = tls.RequireAnyClientCert
		}
		tlsConf = &tls.Config{
			ClientAuth:               strictness,
			ClientCAs:                clientCertPool,
			CipherSuites:             c.CipherSet,
			PreferServerCipherSuites: true,
			MinVersion:               c.MinVersion,
		}
		tlsConf.BuildNameToCertificate()
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, err
	}
	tlsConf.Certificates = []tls.Certificate{cert}
	tlsConf.InsecureSkipVerify = c.InsecureSkipVerify
	return tlsConf, nil
}

// NewGRPCClientDialOpt provides a GRPC option for TLS configuration using the
// provided ftls Config.
func NewGRPCClientDialOpt(c *Config) (grpc.DialOption, error) {
	var opt grpc.DialOption
	if c.InsecureSkipVerify {
		return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})), nil
	}
	tlsConf, err := newClientTLSConfig(c)
	if err != nil {
		return opt, err
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)), nil
}

// verifyClientAddrMatch ensures the client certificate in use matches the
// client IP address.
//
// NOTE: This isn't in use now because GRPC handles it for us, but I'm leaving
// it in case we need the code later.
func verifyClientAddrMatch(c *tls.Conn) error {
	err := c.Handshake()
	if err != nil {
		return err
	}
	addr, _, err := net.SplitHostPort(c.RemoteAddr().String())
	if err != nil {
		return err
	}
	return c.ConnectionState().VerifiedChains[0][0].VerifyHostname(addr)
}
