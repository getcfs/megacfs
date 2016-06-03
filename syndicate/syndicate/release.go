package syndicate

import (
	"fmt"

	pb "github.com/getcfs/megacfs/syndicate/api/proto"
	"golang.org/x/net/context"
)

//GetNodeSoftwareVersion asks a managed node for its running software version
func (s *Server) GetNodeSoftwareVersion(c context.Context, n *pb.Node) (*pb.NodeSoftwareVersion, error) {
	s.RLock()
	defer s.RUnlock()
	if _, ok := s.managedNodes[n.Id]; ok {
		version, err := s.managedNodes[n.Id].GetSoftwareVersion()
		return &pb.NodeSoftwareVersion{Version: version}, err
	}
	return &pb.NodeSoftwareVersion{}, fmt.Errorf("Node %d not found or not managed node", n.Id)
}

//GetNodeSoftwareVersion asks a managed node for its running software version
func (s *Server) NodeUpgradeSoftwareVersion(c context.Context, n *pb.NodeUpgrade) (*pb.NodeUpgradeStatus, error) {
	s.RLock()
	defer s.RUnlock()
	if _, ok := s.managedNodes[n.Id]; ok {
		status, err := s.managedNodes[n.Id].UpgradeSoftwareVersion(n.Version)
		return &pb.NodeUpgradeStatus{Status: status, Msg: ""}, err
	}
	return &pb.NodeUpgradeStatus{Status: false}, fmt.Errorf("Node %d not found or not managed node", n.Id)
}
