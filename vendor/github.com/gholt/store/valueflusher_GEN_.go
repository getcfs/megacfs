package store

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-go/zap"
	"golang.org/x/net/context"
)

type valueFlusherState struct {
	interval         int
	flusherThreshold int32

	startupShutdownLock sync.Mutex
	notifyChan          chan *bgNotification
}

func (store *defaultValueStore) flusherConfig(cfg *ValueStoreConfig) {
	store.flusherState.interval = 60
	store.flusherState.flusherThreshold = cfg.FlusherThreshold
}

func (store *defaultValueStore) flusherStartup() {
	store.flusherState.startupShutdownLock.Lock()
	if store.flusherState.notifyChan == nil {
		store.flusherState.notifyChan = make(chan *bgNotification, 1)
		go store.flusherLauncher(store.flusherState.notifyChan)
	}
	store.flusherState.startupShutdownLock.Unlock()
}

func (store *defaultValueStore) flusherShutdown() {
	store.flusherState.startupShutdownLock.Lock()
	if store.flusherState.notifyChan != nil {
		c := make(chan struct{}, 1)
		store.flusherState.notifyChan <- &bgNotification{
			action:   _BG_DISABLE,
			doneChan: c,
		}
		<-c
		store.flusherState.notifyChan = nil
	}
	store.flusherState.startupShutdownLock.Unlock()
}

func (store *defaultValueStore) flusherLauncher(notifyChan chan *bgNotification) {
	interval := float64(store.flusherState.interval) * float64(time.Second)
	store.randMutex.Lock()
	nextRun := time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
	store.randMutex.Unlock()
	justFlushed := false
	running := true
	for running {
		var notification *bgNotification
		sleep := nextRun.Sub(time.Now())
		if sleep > 0 {
			select {
			case notification = <-notifyChan:
			case <-time.After(sleep):
			}
		} else {
			select {
			case notification = <-notifyChan:
			default:
			}
		}
		store.randMutex.Lock()
		nextRun = time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
		store.randMutex.Unlock()
		if notification != nil {
			if notification.action == _BG_DISABLE {
				running = false
			} else {
				store.logger.Error("invalid action requested", zap.String("section", "flusher"), zap.Int("action", int(notification.action)))
			}
			notification.doneChan <- struct{}{}
			continue
		}
		m := atomic.LoadInt32(&store.modifications)
		atomic.AddInt32(&store.modifications, -m)
		if (m == 0 && !justFlushed) || (m > 0 && m < store.flusherState.flusherThreshold) {
			store.logger.Debug("flushing", zap.String("section", "flusher"), zap.Uint64("modifications", uint64(m)), zap.Int64("flusherThreshold", int64(store.flusherState.flusherThreshold)))
			store.Flush(context.Background())
			justFlushed = true
		} else {
			justFlushed = false
		}
	}
}
