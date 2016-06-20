package srvconf

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"time"

	pb "github.com/getcfs/megacfs/syndicate/api/proto"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	ErrSRVLookupFailed = errors.New("srv lookup failed")
)

// lookup returned records are sorted by priority and randomized by weight within a priority.
func lookup(service string) ([]*net.SRV, error) {
	_, addrs, err := net.LookupSRV("", "", service)
	if err != nil {
		log.Println("srv:", service)
		log.Println(err)
		return nil, ErrSRVLookupFailed
	}
	return addrs, nil
}

type SRVLoader struct {
	Record       string
	SyndicateURL string
}

func GetHardwareProfile() (*pb.HardwareProfile, error) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return &pb.HardwareProfile{}, err
	}
	d, err := disk.Partitions(true)
	if err != nil {
		return &pb.HardwareProfile{}, err
	}
	hw := &pb.HardwareProfile{
		Disks:    make([]*pb.Disk, 0),
		Cpus:     int64(runtime.NumCPU()),
		Memtotal: v.Total,
		Memfree:  v.Free,
	}
	for k := range d {
		usage, err := disk.Usage(d[k].Mountpoint)
		if err != nil {
			continue
		}
		entry := &pb.Disk{
			Path:   d[k].Mountpoint,
			Device: d[k].Device,
			Size_:  usage.Total,
			Used:   usage.Used,
		}
		hw.Disks = append(hw.Disks, entry)
	}
	return hw, nil
}

func (s *SRVLoader) getConfig() (*pb.NodeConfig, error) {
	nconfig := &pb.NodeConfig{}
	var opts []grpc.DialOption
	var creds credentials.TransportCredentials
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(s.SyndicateURL, opts...)
	if err != nil {
		return nconfig, fmt.Errorf("Failed to dial ring server for config: %s", err)
	}
	defer conn.Close()

	client := pb.NewSyndicateClient(conn)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	rr := &pb.RegisterRequest{}
	rr.Hostname, _ = os.Hostname()
	addrs, _ := net.InterfaceAddrs()
	for k, _ := range addrs {
		rr.Addrs = append(rr.Addrs, addrs[k].String())
	}
	rr.Hardware, err = GetHardwareProfile()
	if err != nil {
		return nconfig, err
	}
	rr.Tiers = []string{rr.Hostname}

	nconfig, err = client.RegisterNode(ctx, rr)
	return nconfig, err
}

func (s *SRVLoader) Load() (nodeconfig *pb.NodeConfig, err error) {
	if s.SyndicateURL == "" {
		// Specific endpoint given
		if _, _, err := net.SplitHostPort(s.Record); err == nil {
			s.SyndicateURL = s.Record
		} else {
			serviceAddrs, err := lookup(s.Record)
			if err != nil {
				return &pb.NodeConfig{}, err
			}
			s.SyndicateURL = fmt.Sprintf("%s:%d", serviceAddrs[0].Target, serviceAddrs[0].Port)
		}
	}
	return s.getConfig()
}
