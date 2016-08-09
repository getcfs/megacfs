package store

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudfoundry/gosigar"
	"github.com/ricochet2200/go-disk-usage/du"
	"github.com/uber-go/zap"
)

type groupWatcherState struct {
	interval                   int
	diskFreeDisableThreshold   uint64
	diskFreeReenableThreshold  uint64
	diskUsageDisableThreshold  float64
	diskUsageReenableThreshold float64
	diskFree                   uint64
	diskUsed                   uint64
	diskSize                   uint64
	diskFreeTOC                uint64
	diskUsedTOC                uint64
	diskSizeTOC                uint64
	memFreeDisableThreshold    uint64
	memFreeReenableThreshold   uint64
	memUsageDisableThreshold   float64
	memUsageReenableThreshold  float64
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
				store.logger.Error("invalid action requested", zap.String("section", "watcher"), zap.Int("action", int(notification.action)))
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
		diskUsage := float64(u.Usage())
		diskFreeTOC := utoc.Free()
		diskUsedTOC := utoc.Used()
		diskSizeTOC := utoc.Size()
		diskUsageTOC := float64(utoc.Usage())
		atomic.StoreUint64(&store.watcherState.diskFree, diskFree)
		atomic.StoreUint64(&store.watcherState.diskUsed, diskUsed)
		atomic.StoreUint64(&store.watcherState.diskSize, diskSize)
		atomic.StoreUint64(&store.watcherState.diskFreeTOC, diskFreeTOC)
		atomic.StoreUint64(&store.watcherState.diskUsedTOC, diskUsedTOC)
		atomic.StoreUint64(&store.watcherState.diskSizeTOC, diskSizeTOC)
		store.logger.Debug("diskStat", zap.String("section", "watcher"), zap.Uint64("diskFree", diskFree), zap.Uint64("diskUsed", diskUsed), zap.Uint64("diskSize", diskSize), zap.Float64("diskUsage", float64(diskUsage)*100), zap.Uint64("diskFreeTOC", diskFreeTOC), zap.Uint64("diskUsedTOC", diskUsedTOC), zap.Uint64("diskSizeTOC", diskSizeTOC), zap.Float64("diskUsageTOC", float64(diskUsageTOC)*100))
		m := &sigar.Mem{}
		var memUsage float64
		if err := m.Get(); err != nil {
			m = nil
			store.logger.Debug("could not stat memory", zap.String("section", "watcher"), zap.Error(err))
		} else {
			memUsage = float64(m.ActualUsed) / float64(m.Total)
			atomic.StoreUint64(&store.watcherState.memFree, m.ActualFree)
			atomic.StoreUint64(&store.watcherState.memUsed, m.ActualUsed)
			atomic.StoreUint64(&store.watcherState.memSize, m.Total)
			store.logger.Debug("memStat", zap.String("section", "watcher"), zap.Uint64("actualFree", m.ActualFree), zap.Uint64("actualUsed", m.ActualUsed), zap.Uint64("total", m.Total), zap.Float64("usage", memUsage*100))
		}
		var wantToDisable string
		var wantToReenable string
		if store.watcherState.diskFreeDisableThreshold > 1 && (diskFree <= store.watcherState.diskFreeDisableThreshold || diskFreeTOC <= store.watcherState.diskFreeDisableThreshold) {
			wantToDisable = "passed the disk free threshold for automatic disabling"
		}
		if store.watcherState.diskUsageDisableThreshold > 0 && (diskUsage >= store.watcherState.diskUsageDisableThreshold || diskUsageTOC >= store.watcherState.diskUsageDisableThreshold) {
			wantToDisable = "passed the disk usage threshold for automatic disabling"
		}
		if store.watcherState.memFreeDisableThreshold > 1 && m != nil && m.ActualFree <= store.watcherState.memFreeDisableThreshold {
			wantToDisable = "passed the mem free threshold for automatic disabling"
		}
		if store.watcherState.memUsageDisableThreshold > 0 && m != nil && memUsage >= store.watcherState.memUsageDisableThreshold {
			wantToDisable = "passed the mem usage threshold for automatic disabling"
		}
		if store.watcherState.diskFreeReenableThreshold > 1 && diskFree >= store.watcherState.diskFreeReenableThreshold && diskFreeTOC >= store.watcherState.diskFreeReenableThreshold {
			wantToReenable = "passed the disk free threshold for automatic re-enabling"
		}
		if store.watcherState.diskUsageReenableThreshold > 0 && diskUsage <= store.watcherState.diskUsageReenableThreshold && diskUsageTOC <= store.watcherState.diskUsageReenableThreshold {
			wantToReenable = "passed the disk usage threshold for automatic re-enabling"
		}
		if store.watcherState.memFreeReenableThreshold > 1 && m != nil && m.ActualFree >= store.watcherState.memFreeReenableThreshold {
			wantToReenable = "passed the mem free threshold for automatic re-enabling"
		}
		if store.watcherState.memUsageReenableThreshold > 0 && m != nil && memUsage <= store.watcherState.memUsageReenableThreshold {
			wantToReenable = "passed the mem usage threshold for automatic re-enabling"
		}
		if wantToDisable != "" {
			if !disabled {
				store.logger.Error(wantToDisable, zap.String("section", "watcher"))
				store.disableWrites(false) // false indicates non-user call
				disabled = true
			}
		} else if wantToReenable != "" {
			if disabled {
				store.logger.Error(wantToReenable, zap.String("section", "watcher"))
				store.enableWrites(false)
				disabled = false
			}
		}
	}
}
