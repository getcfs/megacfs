package syndicate

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"golang.org/x/net/context"

	pb "github.com/getcfs/megacfs/syndicate/api/proto"
	"github.com/gholt/ring"
)

type ringslave struct {
	sync.RWMutex
	version int64
	last    time.Time
	r       ring.Ring
	b       *ring.Builder
	spath   string
}

func (s *ringslave) Store(c context.Context, r *pb.RingMsg) (*pb.StoreResult, error) {
	log.Println("Got store request:", r.Version, r.Deadline, r.Rollback)
	s.Lock()
	defer s.Unlock()

	s.last = time.Now()
	bs, rs, err := s.saveRingAndBuilderBytes(&r.Ring, &r.Builder, r.Version)
	if err != nil {
		return &pb.StoreResult{
			Version: r.Version,
			Ring:    rs,
			Builder: bs,
			ErrMsg:  fmt.Sprintf("Encountered error during save: %s", err),
		}, nil
	}
	s.version = r.Version

	_, builder, err := ring.RingOrBuilder(path.Join(s.spath, fmt.Sprintf("%d.oort.builder", r.Version)))
	if err != nil || builder == nil {
		return &pb.StoreResult{
			Version: r.Version,
			Ring:    false,
			Builder: false,
			ErrMsg:  fmt.Sprintf("Encountered error during builder load: %s", err),
		}, nil

	}
	oldbuilder := s.b
	s.b = builder

	ring, _, err := ring.RingOrBuilder(path.Join(s.spath, fmt.Sprintf("%d.oort.ring.gz", r.Version)))
	if err != nil || ring == nil || ring.Version() != r.Version {
		//restore builder
		s.b = oldbuilder
		return &pb.StoreResult{
			Version: r.Version,
			Ring:    false,
			Builder: false,
			ErrMsg:  fmt.Sprintf("Encountered error during ring load: %s", err),
		}, nil

	}
	s.r = ring
	s.version = r.Version
	return &pb.StoreResult{Version: r.Version, Ring: true, Builder: true, ErrMsg: ""}, nil
}

func (s *ringslave) Revert(c context.Context, r *pb.RingMsg) (*pb.StoreResult, error) {
	log.Println("Got revert request to revert to version:", r.Version)
	return &pb.StoreResult{}, nil
}

func (s *ringslave) Status(c context.Context, r *pb.StatusRequest) (*pb.StatusMsg, error) {
	log.Printf("Current status: %+v\n", s)
	return &pb.StatusMsg{}, nil
}

func writeBytes(filename string, b *[]byte) error {
	dir, name := path.Split(filename)
	if dir == "" {
		dir = "."
	}
	_ = os.MkdirAll(dir, 0755)
	f, err := ioutil.TempFile(dir, name+".")
	if err != nil {
		return err
	}
	tmp := f.Name()
	s, err := f.Write(*b)
	if err != nil {
		f.Close()
		return err
	}
	if s != len(*b) {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filename)
}

func (s *ringslave) saveRingAndBuilderBytes(ring, builder *[]byte, version int64) (builderstatus, ringstatus bool, err error) {
	err = writeBytes(path.Join(s.spath, fmt.Sprintf("%d.oort.builder.gz", version)), builder)
	if err != nil {
		return false, false, err
	}
	err = writeBytes(path.Join(s.spath, fmt.Sprintf("%d.oort.ring.gz", version)), ring)
	if err != nil {
		return true, false, err
	}
	return true, true, err
}

func (s *ringslave) Setup(c context.Context, r *pb.RingMsg) (*pb.StoreResult, error) {
	log.Println("Got setup request:", r.Version, r.Deadline, r.Rollback)
	s.Lock()
	defer s.Unlock()

	s.last = time.Now()
	bs, rs, err := s.saveRingAndBuilderBytes(&r.Ring, &r.Builder, r.Version)
	if err != nil {
		return &pb.StoreResult{
			Version: r.Version,
			Ring:    rs,
			Builder: bs,
			ErrMsg:  fmt.Sprintf("Encountered error during save: %s", err),
		}, nil
	}
	s.version = r.Version

	_, builder, err := ring.RingOrBuilder(path.Join(s.spath, fmt.Sprintf("%d.oort.builder.gz", r.Version)))
	if err != nil || builder == nil {
		return &pb.StoreResult{
			Version: r.Version,
			Ring:    false,
			Builder: false,
			ErrMsg:  fmt.Sprintf("Encountered error during builder load: %s", err),
		}, nil

	}
	oldbuilder := s.b
	s.b = builder

	ring, _, err := ring.RingOrBuilder(path.Join(s.spath, fmt.Sprintf("%d.oort.ring.gz", r.Version)))
	if err != nil || ring == nil || ring.Version() != r.Version {
		//restore builder
		s.b = oldbuilder
		return &pb.StoreResult{
			Version: r.Version,
			Ring:    false,
			Builder: false,
			ErrMsg:  fmt.Sprintf("Encountered error during ring load: %s", err),
		}, nil

	}
	s.r = ring
	s.version = r.Version
	return &pb.StoreResult{Version: r.Version, Ring: true, Builder: true, ErrMsg: ""}, nil
}
