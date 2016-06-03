package syndicate

import log "github.com/Sirupsen/logrus"

type changeMsg struct {
	rb *[]byte
	v  int64
}

// NotifyNodes is called when a ring change occur's and just
// drops a change message on the changeChan for the RingChangeManager.
func (s *Server) NotifyNodes() {
	s.RLock()
	m := &changeMsg{
		rb: s.rb,
		v:  s.r.Version(),
	}
	s.RUnlock()
	s.changeChan <- m
	s.subsChangeChan <- m
}

//RingChangeManager gets ring change messages from the change chan and handles
//notifying all managed nodes.
func (s *Server) RingChangeManager() {
	for msg := range s.changeChan {
		s.RLock()
		for k := range s.managedNodes {
			updated, err := s.managedNodes[k].RingUpdate(msg.rb, msg.v)
			if !updated || err != nil {
				s.ctxlog.WithFields(log.Fields{"nodeid": k, "updated": updated, "err": err}).Warning("sent node ringupdate")
			} else {
				s.ctxlog.WithFields(log.Fields{"nodeid": k, "updated": updated, "err": err}).Debug("sent node ringupdate")
			}

		}
		s.RUnlock()
	}
}

// TODO: remove me, test func
func (s *Server) pingSweep() {
	responses := make(map[string]string, len(s.managedNodes))
	for k := range s.managedNodes {
		_, msg, err := s.managedNodes[k].Ping()
		if err != nil {
			responses[s.managedNodes[k].Address()] = err.Error()
			continue
		}
		responses[s.managedNodes[k].Address()] = msg
	}
}
