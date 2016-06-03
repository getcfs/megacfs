package store

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudfoundry/gosigar"
	"github.com/ricochet2200/go-disk-usage/du"
)

type groupWatcherState struct {
	interval                   int
	diskFreeDisableThreshold   uint64
	diskFreeReenableThreshold  uint64
	diskUsageDisableThreshold  float32
	diskUsageReenableThreshold float32
	diskFree                   uint64
	diskUsed                   uint64
	diskSize                   uint64
	diskFreeTOC                uint64
	diskUsedTOC                uint64
	diskSizeTOC                uint64
	memFreeDisableThreshold    uint64
	memFreeReenableThreshold   uint64
	memUsageDisableThreshold   float32
	memUsageReenableThreshold  float32
	memFree                    uint64
	memUsed                    uint64
	memSize                    uint64

	startupShutdownLock sync.Mutex
	notifyChan          chan *bgNotification
}

func (store *defaultGroupStore) watcherConfig(cfg *GroupStoreConfig) {
	store.watcherState.interval = 60
	store.watcherState.diskFreeDisableThreshold = cfg.DiskFreeDisableThreshold
	store.watcherState.diskFreeReenableThreshold = cfg.DiskFreeReenableThreshold
	store.watcherState.diskUsageDisableThreshold = cfg.DiskUsageDisableThreshold
	store.watcherState.diskUsageReenableThreshold = cfg.DiskUsageReenableThreshold
	store.watcherState.memFreeDisableThreshold = cfg.MemFreeDisableThreshold
	store.watcherState.memFreeReenableThreshold = cfg.MemFreeReenableThreshold
	store.watcherState.memUsageDisableThreshold = cfg.MemUsageDisableThreshold
	store.watcherState.memUsageReenableThreshold = cfg.MemUsageReenableThreshold
}

func (store *defaultGroupStore) watcherStartup() {
	store.watcherState.startupShutdownLock.Lock()
	if store.watcherState.notifyChan == nil {
		store.watcherState.notifyChan = make(chan *bgNotification, 1)
		go store.watcherLauncher(store.watcherState.notifyChan)
	}
	store.watcherState.startupShutdownLock.Unlock()
}

func (store *defaultGroupStore) watcherShutdown() {
	store.watcherState.startupShutdownLock.Lock()
	if store.watcherState.notifyChan != nil {
		c := make(chan struct{}, 1)
		store.watcherState.notifyChan <- &bgNotification{
			action:   _BG_DISABLE,
			doneChan: c,
		}
		<-c
		store.watcherState.notifyChan = nil
	}
	store.watcherState.startupShutdownLock.Unlock()
}

func (store *defaultGroupStore) watcherLauncher(notifyChan chan *bgNotification) {
	interval := float64(store.watcherState.interval) * float64(time.Second)
	store.randMutex.Lock()
	nextRun := time.Now().Add(time.Duration(interval + interval*store.rand.NormFloat64()*0.1))
	store.randMutex.Unlock()
	disabled := false
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
				store.logCritical("watcher: invalid action requested: %d", notification.action)
			}
			notification.doneChan <- struct{}{}
			continue
		}
		u := du.NewDiskUsage(store.path)
		utoc := u
		if store.pathtoc != store.path {
			utoc = du.NewDiskUsage(store.pathtoc)
		}
		diskFree := u.Free()
		diskUsed := u.Used()
		diskSize := u.Size()
		diskUsage := u.Usage()
		diskFreeTOC := utoc.Free()
		diskUsedTOC := utoc.Used()
		diskSizeTOC := utoc.Size()
		diskUsageTOC := utoc.Usage()
		atomic.StoreUint64(&store.watcherState.diskFree, diskFree)
		atomic.StoreUint64(&store.watcherState.diskUsed, diskUsed)
		atomic.StoreUint64(&store.watcherState.diskSize, diskSize)
		atomic.StoreUint64(&store.watcherState.diskFreeTOC, diskFreeTOC)
		atomic.StoreUint64(&store.watcherState.diskUsedTOC, diskUsedTOC)
		atomic.StoreUint64(&store.watcherState.diskSizeTOC, diskSizeTOC)
		store.logDebug("watcher: disk: %d free, %d used, %d size, %.02f%% usage; toc: %d free, %d used, %d size, %.02f%% usage", diskFree, diskUsed, diskSize, diskUsage*100, diskFreeTOC, diskUsedTOC, diskSizeTOC, diskUsageTOC*100)
		m := &sigar.Mem{}
		var memUsage float32
		if err := m.Get(); err != nil {
			m = nil
			store.logDebug("watcher: could not stat memory: %s", err)
		} else {
			memUsage = float32(m.ActualUsed) / float32(m.Total)
			atomic.StoreUint64(&store.watcherState.memFree, m.ActualFree)
			atomic.StoreUint64(&store.watcherState.memUsed, m.ActualUsed)
			atomic.StoreUint64(&store.watcherState.memSize, m.Total)
			store.logDebug("watcher: mem: %d free, %d used, %d size, %.02f%% usage", m.ActualFree, m.ActualUsed, m.Total, memUsage*100)
		}
		var wantToDisable string
		var wantToReenable string
		if store.watcherState.diskFreeDisableThreshold > 1 && (diskFree <= store.watcherState.diskFreeDisableThreshold || diskFreeTOC <= store.watcherState.diskFreeDisableThreshold) {
			wantToDisable = "watcher: passed the disk free threshold for automatic disabling"
		}
		if store.watcherState.diskUsageDisableThreshold > 0 && (diskUsage >= store.watcherState.diskUsageDisableThreshold || diskUsageTOC >= store.watcherState.diskUsageDisableThreshold) {
			wantToDisable = "watcher: passed the disk usage threshold for automatic disabling"
		}
		if store.watcherState.memFreeDisableThreshold > 1 && m != nil && m.ActualFree <= store.watcherState.memFreeDisableThreshold {
			wantToDisable = "watcher: passed the mem free threshold for automatic disabling"
		}
		if store.watcherState.memUsageDisableThreshold > 0 && m != nil && memUsage >= store.watcherState.memUsageDisableThreshold {
			wantToDisable = "watcher: passed the mem usage threshold for automatic disabling"
		}
		if store.watcherState.diskFreeReenableThreshold > 1 && diskFree >= store.watcherState.diskFreeReenableThreshold && diskFreeTOC >= store.watcherState.diskFreeReenableThreshold {
			wantToReenable = "watcher: passed the disk free threshold for automatic re-enabling"
		}
		if store.watcherState.diskUsageReenableThreshold > 0 && diskUsage <= store.watcherState.diskUsageReenableThreshold && diskUsageTOC <= store.watcherState.diskUsageReenableThreshold {
			wantToReenable = "watcher: passed the disk usage threshold for automatic re-enabling"
		}
		if store.watcherState.memFreeReenableThreshold > 1 && m != nil && m.ActualFree >= store.watcherState.memFreeReenableThreshold {
			wantToReenable = "watcher: passed the mem free threshold for automatic re-enabling"
		}
		if store.watcherState.memUsageReenableThreshold > 0 && m != nil && memUsage <= store.watcherState.memUsageReenableThreshold {
			wantToReenable = "watcher: passed the mem usage threshold for automatic re-enabling"
		}
		if wantToDisable != "" {
			if !disabled {
				store.logCritical(wantToDisable)
				store.disableWrites(false) // false indicates non-user call
				disabled = true
			}
		} else if wantToReenable != "" {
			if disabled {
				store.logCritical(wantToReenable)
				store.enableWrites(false)
				disabled = false
			}
		}
	}
}
