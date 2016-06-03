package syndicate

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gholt/ring"
	pb "github.com/getcfs/megacfs/syndicate/api/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type RingSlave struct {
	sync.RWMutex
	status  bool
	last    time.Time
	addr    string
	version int64
	conn    *grpc.ClientConn
	client  pb.RingDistClient
}

func (s *Server) RegisterSlave(slave *RingSlave) error {
	log.Printf("--> Attempting to register: %+v", slave)
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithBlock())
	opts = append(opts, grpc.WithTimeout(_SYN_DIAL_TIMEOUT*time.Second))
	var err error
	slave.conn, err = grpc.Dial(slave.addr, opts...)
	if err != nil {
		return err
	}
	slave.client = pb.NewRingDistClient(slave.conn)
	log.Printf("--> Setting up slave: %s", slave.addr)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(_SYN_REGISTER_TIMEOUT)*time.Second)
	i := &pb.RingMsg{
		Version:  s.r.Version(),
		Ring:     *s.rb,
		Builder:  *s.bb,
		Deadline: 0,
		Rollback: 0,
	}
	res, err := slave.client.Setup(ctx, i)
	if err != nil {
		return err
	}
	if res.Version != s.r.Version() {
		return fmt.Errorf("Version or master on remote node %+v did not match local entries. Got %+v.", slave, res)
	}
	if !res.Ring || !res.Builder {
		log.Printf("res is: %#v\n", res)
		return fmt.Errorf("Slave failed to store ring or builder: %s", res.ErrMsg)
	}
	log.Printf("<-- Slave response: %+v", res)
	slave.version = res.Version
	slave.last = time.Now()
	slave.status = true
	log.Printf("--> Slave state is now: %+v\n", slave)
	return nil
}

//TODO: Need concurrency, we should just fire of replicates in goroutines
// and collects the results. On a failure we still need to send the rollback
// or have the slave's commit deadline trigger.
func (s *Server) replicateRing(r ring.Ring, rb, bb *[]byte) error {
	failcount := 0
	for _, slave := range s.slaves {
		ctx, _ := context.WithTimeout(context.Background(), time.Duration(_SYN_REGISTER_TIMEOUT)*time.Second)
		i := &pb.RingMsg{
			Version:  r.Version(),
			Ring:     *rb,
			Builder:  *bb,
			Deadline: time.Now().Add(60 * time.Second).Unix(),
			Rollback: s.r.Version(),
		}
		res, err := slave.client.Store(ctx, i)
		if err != nil {
			log.Println(err)
			failcount++
			continue
		}
		if res.Version != r.Version() {
			log.Printf("Version or master on remote node %+v did not match local entries. Got %+v.", slave, res)
			failcount++
			continue
		}
		if !res.Ring || !res.Builder {
			log.Printf("res is: %#v\n", res)
			log.Printf("Slave failed to store ring or builder: %s", res.ErrMsg)
			failcount++
			continue
		}
		log.Printf("<-- Slave response: %+v", res)
		slave.version = res.Version
		slave.last = time.Now()
		slave.status = true
	}
	if failcount > (len(s.slaves) / 2) {
		return fmt.Errorf("Failed to get replication majority")
	}
	return nil
}
