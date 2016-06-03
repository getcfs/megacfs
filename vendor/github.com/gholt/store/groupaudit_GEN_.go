package store

import (
	"errors"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
)

type groupAuditState struct {
	interval     int
	ageThreshold int64

	startupShutdownLock sync.Mutex
	notifyChan          chan *bgNotification
}

func (store *defaultGroupStore) auditConfig(cfg *GroupStoreConfig) {
	store.auditState.interval = cfg.AuditInterval
	store.auditState.ageThreshold = int64(cfg.AuditAgeThreshold) * int64(time.Second)
}

func (store *defaultGroupStore) auditStartup() {
	store.auditState.startupShutdownLock.Lock()
	if store.auditState.notifyChan == nil {
		store.auditState.notifyChan = make(chan *bgNotification, 1)
		go store.auditLauncher(store.auditState.notifyChan)
	}
	store.auditState.startupShutdownLock.Unlock()
}

func (store *defaultGroupStore) auditShutdown() {
	store.auditState.startupShutdownLock.Lock()
	if store.auditState.notifyChan != nil {
		c := make(chan struct{}, 1)
		store.auditState.notifyChan <- &bgNotification{
			action:   _BG_DISABLE,
			doneChan: c,
		}
		<-c
		store.auditState.notifyChan = nil
	}
	store.auditState.startupShutdownLock.Unlock()
}

func (store *defaultGroupStore) AuditPass(ctx context.Context) error {
	store.auditState.startupShutdownLock.Lock()
	if store.auditState.notifyChan == nil {
		store.auditPass(true, make(chan *bgNotification))
	} else {
		c := make(chan struct{}, 1)
		store.auditState.notifyChan <- &bgNotification{
			action:   _BG_PASS,
			doneChan: c,
		}
		<-c
	}
	store.auditState.startupShutdownLock.Unlock()
	return nil
}

func (store *defaultGroupStore) auditLauncher(notifyChan chan *bgNotification) {
	interval := float64(store.auditState.interval) * float64(time.Second)
	store.randMutex.Lock()
	nextRun := time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
	store.randMutex.Unlock()
	var notification *bgNotification
	running := true
	for running {
		if notification == nil {
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
		}
		store.randMutex.Lock()
		nextRun = time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
		store.randMutex.Unlock()
		if notification != nil {
			var nextNotification *bgNotification
			switch notification.action {
			case _BG_PASS:
				nextNotification = store.auditPass(true, notifyChan)
			case _BG_DISABLE:
				running = false
			default:
				// Critical because there was a coding error that needs to be
				// fixed by a person.
				store.logCritical("audit: invalid action requested: %d", notification.action)
			}
			notification.doneChan <- struct{}{}
			notification = nextNotification
		} else {
			notification = store.auditPass(false, notifyChan)
		}
	}
}

// NOTE: For now, there is no difference between speed=true and speed=false;
// eventually the background audits will try to slow themselves down to finish
// in approximately the store.auditState.interval.
func (store *defaultGroupStore) auditPass(speed bool, notifyChan chan *bgNotification) *bgNotification {
	begin := time.Now()
	defer func() {
		store.logDebug("audit: took %s", time.Now().Sub(begin))
	}()
	names, err := store.readdirnames(store.pathtoc)
	if err != nil {
		store.logError("audit: %s", err)
		return nil
	}
	shuffledNames := make([]string, len(names))
	store.randMutex.Lock()
	for x, y := range store.rand.Perm(len(names)) {
		shuffledNames[x] = names[y]
	}
	store.randMutex.Unlock()
	names = shuffledNames
	for i := 0; i < len(names); i++ {
		select {
		case notification := <-notifyChan:
			return notification
		default:
		}
		if !strings.HasSuffix(names[i], ".grouptoc") {
			continue
		}
		namets := int64(0)
		if namets, err = strconv.ParseInt(names[i][:len(names[i])-len(".grouptoc")], 10, 64); err != nil {
			store.logError("audit: bad timestamp in name: %#v", names[i])
			continue
		}
		if namets == 0 {
			store.logError("audit: bad timestamp in name: %#v", names[i])
			continue
		}
		if namets == int64(atomic.LoadUint64(&store.activeTOCA)) || namets == int64(atomic.LoadUint64(&store.activeTOCB)) {
			store.logDebug("audit: skipping current %s", names[i])
			continue
		}
		if namets >= time.Now().UnixNano()-store.auditState.ageThreshold {
			store.logDebug("audit: skipping young %s", names[i])
			continue
		}
		store.logDebug("audit: checking %s", names[i])
		failedAudit := uint32(0)
		canceledAudit := uint32(0)
		dataName := names[i][:len(names[i])-3]
		fpr, err := store.openReadSeeker(path.Join(store.path, dataName))
		if err != nil {
			atomic.AddUint32(&failedAudit, 1)
			if store.isNotExist(err) {
				store.logDebug("audit: error opening %s: %s", dataName, err)
			} else {
				store.logError("audit: error opening %s: %s", dataName, err)
			}
		} else {
			corruptions, errs := groupChecksumVerify(fpr)
			closeIfCloser(fpr)
			for _, err := range errs {
				if err != io.EOF && err != io.ErrUnexpectedEOF {
					store.logError("audit: error with %s: %s", dataName, err)
				}
			}
			workers := uint64(1)
			pendingBatchChans := make([]chan []groupTOCEntry, workers)
			freeBatchChans := make([]chan []groupTOCEntry, len(pendingBatchChans))
			for i := 0; i < len(pendingBatchChans); i++ {
				pendingBatchChans[i] = make(chan []groupTOCEntry, 3)
				freeBatchChans[i] = make(chan []groupTOCEntry, cap(pendingBatchChans[i]))
				for j := 0; j < cap(freeBatchChans[i]); j++ {
					freeBatchChans[i] <- make([]groupTOCEntry, store.recoveryBatchSize)
				}
			}
			nextNotificationChan := make(chan *bgNotification, 1)
			controlChan := make(chan struct{})
			go func() {
				select {
				case n := <-notifyChan:
					if atomic.AddUint32(&canceledAudit, 1) == 0 {
						close(controlChan)
					}
					nextNotificationChan <- n
				case <-controlChan:
					nextNotificationChan <- nil
				}
			}()
			wg := &sync.WaitGroup{}
			wg.Add(len(pendingBatchChans))
			for i := 0; i < len(pendingBatchChans); i++ {
				go func(pendingBatchChan chan []groupTOCEntry, freeBatchChan chan []groupTOCEntry) {
					for {
						batch := <-pendingBatchChan
						if batch == nil {
							break
						}
						if atomic.LoadUint32(&failedAudit) == 0 {
							for j := 0; j < len(batch); j++ {
								wr := &batch[j]
								if wr.TimestampBits&_TSB_DELETION != 0 {
									continue
								}
								if groupInCorruptRange(wr.Offset, wr.Length, corruptions) {
									if atomic.AddUint32(&failedAudit, 1) == 0 {
										close(controlChan)
									}
									break
								}
							}
						}
						freeBatchChan <- batch
					}
					wg.Done()
				}(pendingBatchChans[i], freeBatchChans[i])
			}
			fpr, err = store.openReadSeeker(path.Join(store.pathtoc, names[i]))
			if err != nil {
				atomic.AddUint32(&failedAudit, 1)
				if !store.isNotExist(err) {
					store.logError("audit: error opening %s: %s", names[i], err)
				}
			} else {
				// NOTE: The block ID is unimportant in this context, so it's
				// just set 1 and ignored elsewhere.
				_, errs := groupReadTOCEntriesBatched(fpr, 1, freeBatchChans, pendingBatchChans, controlChan)
				closeIfCloser(fpr)
				if len(errs) > 0 {
					atomic.AddUint32(&failedAudit, 1)
					for _, err := range errs {
						store.logError("audit: error with %s: %s", names[i], err)
					}
				}
			}
			for i := 0; i < len(pendingBatchChans); i++ {
				pendingBatchChans[i] <- nil
			}
			wg.Wait()
			close(controlChan)
			if n := <-nextNotificationChan; n != nil {
				return n
			}
		}
		if atomic.LoadUint32(&canceledAudit) != 0 {
			store.logDebug("audit: canceled during %s", names[i])
		} else if atomic.LoadUint32(&failedAudit) == 0 {
			store.logDebug("audit: passed %s", names[i])
		} else {
			store.logError("audit: failed %s", names[i])
			nextNotificationChan := make(chan *bgNotification, 1)
			controlChan := make(chan struct{})
			controlChan2 := make(chan struct{})
			go func() {
				select {
				case n := <-notifyChan:
					close(controlChan)
					nextNotificationChan <- n
				case <-controlChan2:
					nextNotificationChan <- nil
				}
			}()
			store.compactFile(names[i], store.locBlockIDFromTimestampnano(namets), controlChan, "auditPass")
			close(controlChan2)
			if n := <-nextNotificationChan; n != nil {
				return n
			}
			go func() {
				store.logError("audit: all audit actions require store restarts at this time.")
				store.Shutdown(context.Background())
				store.restartChan <- errors.New("audit failure occurred requiring a restart")
			}()
			return &bgNotification{
				action:   _BG_DISABLE,
				doneChan: make(chan struct{}, 1),
			}
		}
	}
	return nil
}
