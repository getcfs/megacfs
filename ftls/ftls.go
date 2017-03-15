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

var (
	DefaultMinVersion = uint16(tls.VersionTLS12)
	DefaultCipherSet  = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384}
)

type Ftsl struct {
	c *Config
}

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

// DefaultServerFTLSConf returns a ftls config with the most commonly used config set.
func DefaultServerFTLSConf(CertFile, KeyFile, CAFile string) *Config {
	return &Config{
		StrictClientVerify: true,
		MutualTLS:          true,
		InsecureSkipVerify: false,
		// TODO: I know syn wanted these minimums, but they weren't working last I tested.
		// MinVersion:         DefaultMinVersion,
		// CipherSet:          DefaultCipherSet,
		CertFile: CertFile,
		KeyFile:  KeyFile,
		CAFile:   CAFile,
	}
}

// DefaultClientFTLSConf returns a ftls config with the most commonly used config set.
func DefaultClientFTLSConf(CertFile, KeyFile, CAFile string) *Config {
	return &Config{
		MutualTLS:          true,
		InsecureSkipVerify: false,
		// TODO: I know syn wanted these minimums, but they weren't working last I tested.
		// MinVersion:         DefaultMinVersion,
		CertFile: CertFile,
		KeyFile:  KeyFile,
		CAFile:   CAFile,
	}
}

// NewClientTLSConfig constructs a client tls.Conf from provide ftls Config.
func NewClientTLSConfig(c *Config) (*tls.Config, error) {
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

// NewServerTLSConfig constructs a server tls.Conf from the provided ftls Config.
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
			MinVersion:               DefaultMinVersion,
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

func NewGRPCClientDialOpt(c *Config) (grpc.DialOption, error) {
	var opt grpc.DialOption
	if c.InsecureSkipVerify {
		return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})), nil
	}
	tlsConf, err := NewClientTLSConfig(c)
	if err != nil {
		return opt, err
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)), nil
}

func VerifyClientAddrMatch(c *tls.Conn) error {
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
