package cmdctrl

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"time"

	pb "github.com/pandemicsyn/cmdctrl/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type CmdCtrl interface {
	Start() (err error)
	Stop() (err error)
	Exit() (err error)
	Reload() (err error)
	Restart() (err error)
	RingUpdate(version int64, ringBytes []byte) (newversion int64)
	SelfUpgrade(version string, bindiff []byte, hash []byte) (status bool, msg string)
	Stats() (encoded []byte)
	HealthCheck() (status bool, msg string)
	SoftwareVersion() (version string)
}

type CCServer struct {
	cmdctrl CmdCtrl
	cfg     *ConfigOpts
}

type ConfigOpts struct {
	ListenAddress      string
	CAFile             string
	CertFile           string
	KeyFile            string
	UseTLS             bool
	Enabled            bool
	MutualTLS          bool
	InsecureSkipVerify bool
}

func NewCCServer(c CmdCtrl, cfg *ConfigOpts) *CCServer {
	return &CCServer{
		cmdctrl: c,
		cfg:     cfg,
	}
}

func mutualTLS(c ConfigOpts) (*tls.Config, error) {
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
		tlsConf = &tls.Config{
			ClientAuth:               strictness,
			ClientCAs:                clientCertPool,
			CipherSuites:             []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384},
			PreferServerCipherSuites: true,
			MinVersion:               uint16(tls.VersionTLS12),
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

func (cc *CCServer) Serve() error {
	l, err := net.Listen("tcp", cc.cfg.ListenAddress)
	if err != nil {
		return err
	}
	var opts []grpc.ServerOption
	if cc.cfg.UseTLS {
		if cc.cfg.MutualTLS {
			mtc, err := mutualTLS(*cc.cfg)
			if err != nil {
				return err
			}
			creds := credentials.NewTLS(mtc)
			opts = []grpc.ServerOption{grpc.Creds(creds)}
		} else {
			creds, err := credentials.NewServerTLSFromFile(cc.cfg.CertFile, cc.cfg.KeyFile)
			if err != nil {
				return err
			}
			opts = []grpc.ServerOption{grpc.Creds(creds)}
		}
	}
	s := grpc.NewServer(opts...)
	pb.RegisterCmdCtrlServer(s, cc)
	return s.Serve(l)
}

func (cc *CCServer) RingUpdate(c context.Context, r *pb.Ring) (*pb.RingUpdateResult, error) {
	res := pb.RingUpdateResult{}
	res.Newversion = cc.cmdctrl.RingUpdate(r.Version, r.Ring)
	return &res, nil
}

func (cc *CCServer) Start(c context.Context, r *pb.EmptyMsg) (*pb.StatusMsg, error) {
	err := cc.cmdctrl.Start()
	if err != nil {
		return &pb.StatusMsg{Status: false, Msg: err.Error()}, nil
	}
	return &pb.StatusMsg{Status: true, Msg: ""}, nil
}

func (cc *CCServer) Stop(c context.Context, r *pb.EmptyMsg) (*pb.StatusMsg, error) {
	err := cc.cmdctrl.Stop()
	if err != nil {
		return &pb.StatusMsg{Status: false, Msg: err.Error()}, nil
	}
	return &pb.StatusMsg{Status: true, Msg: ""}, nil
}

func (cc *CCServer) Restart(c context.Context, r *pb.EmptyMsg) (*pb.StatusMsg, error) {
	err := cc.cmdctrl.Restart()
	if err != nil {
		return &pb.StatusMsg{Status: false, Msg: err.Error()}, nil
	}
	return &pb.StatusMsg{Status: true, Msg: ""}, nil
}

func (cc *CCServer) Reload(c context.Context, r *pb.EmptyMsg) (*pb.StatusMsg, error) {
	err := cc.cmdctrl.Reload()
	if err != nil {
		return &pb.StatusMsg{Status: false, Msg: err.Error()}, nil
	}
	return &pb.StatusMsg{Status: true, Msg: ""}, nil
}

func (cc *CCServer) Exit(c context.Context, r *pb.EmptyMsg) (*pb.StatusMsg, error) {
	err := cc.cmdctrl.Exit()
	if err != nil {
		return &pb.StatusMsg{Status: false, Msg: err.Error()}, nil
	}
	return &pb.StatusMsg{Status: true, Msg: ""}, nil
}

func (cc *CCServer) Stats(c context.Context, r *pb.EmptyMsg) (*pb.StatsMsg, error) {
	return &pb.StatsMsg{Statsjson: cc.cmdctrl.Stats()}, nil
}

func (cc *CCServer) HealthCheck(c context.Context, r *pb.EmptyMsg) (*pb.HealthCheckMsg, error) {
	hm := &pb.HealthCheckMsg{Ts: time.Now().Unix()}
	hm.Status, hm.Msg = cc.cmdctrl.HealthCheck()
	return hm, nil
}

//SelfUpgrade will have the instance upgrade to the provided version via the cmdctrl.GithubUpdater
func (cc *CCServer) SelfUpgrade(c context.Context, r *pb.SelfUpgradeMsg) (*pb.StatusMsg, error) {
	sm := &pb.StatusMsg{}
	sm.Status, sm.Msg = cc.cmdctrl.SelfUpgrade(r.Version, r.Bindiff, r.Checksum)
	return sm, nil
}

//SoftwareVersion returns the currently running version
func (cc *CCServer) SoftwareVersion(c context.Context, r *pb.EmptyMsg) (*pb.SoftwareVersionMsg, error) {
	return &pb.SoftwareVersionMsg{Version: cc.cmdctrl.SoftwareVersion()}, nil
}
