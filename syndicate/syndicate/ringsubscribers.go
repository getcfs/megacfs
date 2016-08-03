package syndicate

import (
	"sync"

	pb "github.com/getcfs/megacfs/syndicate/api/proto"
)

type RingSubscribers struct {
	sync.RWMutex
	subs map[string]chan *pb.Ring
}

func (s *Server) addRingSubscriber(id string) chan *pb.Ring {
	s.ringSubs.Lock()
	defer s.ringSubs.Unlock()
	c, exists := s.ringSubs.subs[id]
	if exists {
		close(c)
		s.ctxlog.WithField("id", id).Debug("ring subscriber entry already existed, closed origin chan")
	}
	s.ringSubs.subs[id] = make(chan *pb.Ring, 1)
	s.metrics.subscriberNodes.Inc()
	return s.ringSubs.subs[id]
}

func (s *Server) removeRingSubscriber(id string) {
	s.ringSubs.Lock()
	defer s.ringSubs.Unlock()
	c, ok := s.ringSubs.subs[id]
	if !ok {
		return
	}
	close(c)
	delete(s.ringSubs.subs, id)
	s.metrics.subscriberNodes.Dec()
	return
}

//ringSubscribersNotify listens for ring changes on s.subsChangeChan,
// and distributes them out to the chan's used by connected GetRingStream
// instances
func (s *Server) ringSubscribersNotify() {
	for change := range s.subsChangeChan {
		s.ringSubs.RLock()
		ring := &pb.Ring{
			Ring:    *change.rb,
			Version: change.v,
		}
		for id, ch := range s.ringSubs.subs {
			go func(id string, ch chan *pb.Ring, ring *pb.Ring) {
				ch <- ring
			}(id, ch, ring)
		}
		s.ringSubs.RUnlock()
	}
}
