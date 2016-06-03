package main

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/gholt/ring"
	cc "github.com/pandemicsyn/cmdctrl/api"
	"golang.org/x/net/context"
)

//CmdCtrlClient return's a client for interacting with a remote cmdctrl instance
type CmdCtrlClient struct {
	conn   *grpc.ClientConn
	client cc.CmdCtrlClient
}

//NewCmdCtrlClient returns a cmdctrl client for the given address
func NewCmdCtrlClient(address string) (*CmdCtrlClient, error) {
	var err error
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	s := CmdCtrlClient{}
	s.conn, err = grpc.Dial(address, opts...)
	if err != nil {
		return &CmdCtrlClient{}, fmt.Errorf("Failed to dial ring server for config: %v", err)
	}
	s.client = cc.NewCmdCtrlClient(s.conn)
	return &s, nil
}

func (s *CmdCtrlClient) upgradeNodeCmd(version string) error {
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	status, err := s.client.SelfUpgrade(ctx, &cc.SelfUpgradeMsg{Version: version})
	if err != nil {
		return err
	}
	fmt.Println("Upgraded:", status.Status, " Msg:", status.Msg)
	return nil
}

func (s *CmdCtrlClient) startNodeCmd() error {
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	status, err := s.client.Start(ctx, &cc.EmptyMsg{})
	if err != nil {
		return err
	}
	fmt.Println("Started:", status.Status, " Msg:", status.Msg)
	return nil
}

func (s *CmdCtrlClient) restartNodeCmd() error {
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	status, err := s.client.Restart(ctx, &cc.EmptyMsg{})
	if err != nil {
		return err
	}
	fmt.Println("Restarted:", status.Status, " Msg:", status.Msg)
	return nil
}

func (s *CmdCtrlClient) stopNodeCmd() error {
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	status, err := s.client.Stop(ctx, &cc.EmptyMsg{})
	if err != nil {
		return err
	}
	fmt.Println("Stopped:", status.Status, " Msg:", status.Msg)
	return nil
}

func (s *CmdCtrlClient) exitNodeCmd() error {
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	status, err := s.client.Exit(ctx, &cc.EmptyMsg{})
	if err != nil {
		return err
	}
	fmt.Println("Stopped:", status.Status, " Msg:", status.Msg)
	return nil
}

func (s *CmdCtrlClient) getSoftwareVersionCmd() error {
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	version, err := s.client.SoftwareVersion(ctx, &cc.EmptyMsg{})
	if err != nil {
		return err
	}
	fmt.Println("Version:", version.Version)
	return nil
}

func (s *CmdCtrlClient) ringUpdateNodeCmd(filename string) error {
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	r, _, err := ring.RingOrBuilder(filename)
	if err != nil {
		return err
	}
	if r == nil {
		return fmt.Errorf("Provided builder file rather than ring file")
	}
	ru := &cc.Ring{}
	ru.Version = r.Version()
	ru.Ring, err = ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	status, err := s.client.RingUpdate(ctx, ru)
	if err != nil {
		return err
	}
	if status.Newversion != ru.Version {
		return fmt.Errorf("Ring update seems to have failed. Expected: %d, but remote host reports: %d\n", ru.Version, status.Newversion)
	}
	fmt.Println("Remote version is now", status.Newversion)
	return nil
}
