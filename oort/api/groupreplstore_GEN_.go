package api

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getcfs/megacfs/ftls"
	"github.com/getcfs/megacfs/oort/oort"
	synpb "github.com/getcfs/megacfs/syndicate/api/proto"
	"github.com/gholt/flog"
	"github.com/gholt/ring"
	"github.com/gholt/store"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type ReplGroupStore struct {
	logError                   func(string, ...interface{})
	logDebug                   func(string, ...interface{})
	logDebugOn                 bool
	addressIndex               int
	valueCap                   int
	concurrentRequestsPerStore int
	failedConnectRetryDelay    int
	ftlsConfig                 *ftls.Config
	grpcOpts                   []grpc.DialOption

	ringLock           sync.RWMutex
	ring               ring.Ring
	ringCachePath      string
	ringServer         string
	ringServerGRPCOpts []grpc.DialOption
	ringServerExitChan chan struct{}
	ringClientID       string

	storesLock sync.RWMutex
	stores     map[string]*replGroupStoreAndTicketChan
}

type replGroupStoreAndTicketChan struct {
	store      store.GroupStore
	ticketChan chan struct{}
}

func NewReplGroupStore(c *ReplGroupStoreConfig) *ReplGroupStore {
	cfg := resolveReplGroupStoreConfig(c)
	rs := &ReplGroupStore{
		logError:                   cfg.LogError,
		logDebug:                   cfg.LogDebug,
		logDebugOn:                 cfg.LogDebug != nil,
		addressIndex:               cfg.AddressIndex,
		valueCap:                   int(cfg.ValueCap),
		concurrentRequestsPerStore: cfg.ConcurrentRequestsPerStore,
		failedConnectRetryDelay:    cfg.FailedConnectRetryDelay,
		ftlsConfig:                 cfg.StoreFTLSConfig,
		grpcOpts:                   cfg.GRPCOpts,
		stores:                     make(map[string]*replGroupStoreAndTicketChan),
		ringServer:                 cfg.RingServer,
		ringServerGRPCOpts:         cfg.RingServerGRPCOpts,
		ringCachePath:              cfg.RingCachePath,
		ringClientID:               cfg.RingClientID,
	}
	if rs.logError == nil {
		rs.logError = flog.Default.ErrorPrintf
	}
	if rs.logDebug == nil {
		rs.logDebug = func(string, ...interface{}) {}
	}
	if rs.ringCachePath != "" {
		if fp, err := os.Open(rs.ringCachePath); err != nil {
			rs.logDebug("replGroupStore: error loading cached ring %q: %s", rs.ringCachePath, err)
		} else if r, err := ring.LoadRing(fp); err != nil {
			fp.Close()
			rs.logDebug("replGroupStore: error loading cached ring %q: %s", rs.ringCachePath, err)
		} else {
			fp.Close()
			rs.ring = r
		}
	}
	return rs
}

func (rs *ReplGroupStore) Ring(ctx context.Context) ring.Ring {
	var r ring.Ring
	rs.ringLock.RLock()
	r = rs.ring
	rs.ringLock.RUnlock()
	for r == nil {
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return nil
		}
		rs.ringLock.RLock()
		r = rs.ring
		rs.ringLock.RUnlock()
	}
	return r
}

func (rs *ReplGroupStore) SetRing(r ring.Ring) {
	if r == nil {
		return
	}
	rs.ringLock.Lock()
	if rs.ringCachePath != "" {
		dir, name := path.Split(rs.ringCachePath)
		_ = os.MkdirAll(dir, 0755)
		fp, err := ioutil.TempFile(dir, name)
		if err != nil {
			rs.logDebug("replGroupStore: error caching ring %q: %s", rs.ringCachePath, err)
		} else if err := r.Persist(fp); err != nil {
			fp.Close()
			os.Remove(fp.Name())
			rs.logDebug("replGroupStore: error caching ring %q: %s", rs.ringCachePath, err)
		} else {
			fp.Close()
			if err := os.Rename(fp.Name(), rs.ringCachePath); err != nil {
				os.Remove(fp.Name())
				rs.logDebug("replGroupStore: error caching ring %q: %s", rs.ringCachePath, err)
			}
		}
	}
	rs.ring = r
	var currentAddrs map[string]struct{}
	if r != nil {
		nodes := r.Nodes()
		currentAddrs = make(map[string]struct{}, len(nodes))
		for _, n := range nodes {
			currentAddrs[n.Address(rs.addressIndex)] = struct{}{}
		}
	}
	var shutdownAddrs []string
	rs.storesLock.RLock()
	for a := range rs.stores {
		if _, ok := currentAddrs[a]; !ok {
			shutdownAddrs = append(shutdownAddrs, a)
		}
	}
	rs.storesLock.RUnlock()
	if len(shutdownAddrs) > 0 {
		shutdownStores := make([]*replGroupStoreAndTicketChan, len(shutdownAddrs))
		rs.storesLock.Lock()
		for i, a := range shutdownAddrs {
			shutdownStores[i] = rs.stores[a]
			rs.stores[a] = nil
		}
		rs.storesLock.Unlock()
		for i, s := range shutdownStores {
			if err := s.store.Shutdown(context.Background()); err != nil {
				rs.logDebug("replGroupStore: error during shutdown of store %s: %s", shutdownAddrs[i], err)
			}
		}
	}
	rs.ringLock.Unlock()
}

func (rs *ReplGroupStore) storesFor(ctx context.Context, keyA uint64) ([]*replGroupStoreAndTicketChan, error) {
	r := rs.Ring(ctx)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if r == nil {
		return nil, noRingErr
	}
	ns := r.ResponsibleNodes(uint32(keyA >> (64 - r.PartitionBitCount())))
	as := make([]string, len(ns))
	for i, n := range ns {
		as[i] = n.Address(rs.addressIndex)
	}
	ss := make([]*replGroupStoreAndTicketChan, len(ns))
	var someNil bool
	rs.storesLock.RLock()
	for i := len(ss) - 1; i >= 0; i-- {
		ss[i] = rs.stores[as[i]]
		if ss[i] == nil {
			someNil = true
		}
	}
	rs.storesLock.RUnlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if someNil {
		rs.storesLock.Lock()
		select {
		case <-ctx.Done():
			rs.storesLock.Unlock()
			return nil, ctx.Err()
		default:
		}
		for i := len(ss) - 1; i >= 0; i-- {
			if ss[i] == nil {
				ss[i] = rs.stores[as[i]]
				if ss[i] == nil {
					tc := make(chan struct{}, rs.concurrentRequestsPerStore)
					for i := cap(tc); i > 0; i-- {
						tc <- struct{}{}
					}
					ss[i] = &replGroupStoreAndTicketChan{ticketChan: tc}
					ss[i].store = NewGroupStore(as[i], rs.concurrentRequestsPerStore, rs.ftlsConfig, rs.grpcOpts...)
					rs.stores[as[i]] = ss[i]
					select {
					case <-ctx.Done():
						rs.storesLock.Unlock()
						return nil, ctx.Err()
					default:
					}
				}
			}
		}
		rs.storesLock.Unlock()
	}
	return ss, nil
}

func (rs *ReplGroupStore) ringServerConnector(exitChan chan struct{}) {
	sleeperTicks := 2
	sleeperTicker := time.NewTicker(time.Second)
	sleeper := func() {
		for i := sleeperTicks; i > 0; i-- {
			select {
			case <-exitChan:
				break
			case <-sleeperTicker.C:
			}
		}
		if sleeperTicks < 60 {
			sleeperTicks *= 2
		}
	}
	for {
		select {
		case <-exitChan:
			break
		default:
		}
		ringServer := rs.ringServer
		if ringServer == "" {
			var err error

			ringServer, err = oort.GetRingServer("group")
			if err != nil {
				rs.logError("replGroupStore: error resolving ring service: %s", err)
				sleeper()
				continue
			}
		}
		conn, err := grpc.Dial(ringServer, rs.ringServerGRPCOpts...)
		if err != nil {
			rs.logError("replGroupStore: error connecting to ring service %q: %s", ringServer, err)
			sleeper()
			continue
		}
		stream, err := synpb.NewSyndicateClient(conn).GetRingStream(context.Background(), &synpb.SubscriberID{Id: rs.ringClientID})
		if err != nil {
			rs.logError("replGroupStore: error creating stream with ring service %q: %s", ringServer, err)
			sleeper()
			continue
		}
		connDoneChan := make(chan struct{})
		somethingICanTakeAnAddressOf := int32(0)
		activity := &somethingICanTakeAnAddressOf
		// This goroutine will detect when the exitChan is closed so it can
		// close the conn so that the blocking stream.Recv will get an error
		// and everything will unwind properly.
		// However, if the conn errors out on its own and exitChan isn't
		// closed, we're going to loop back around and try a new conn, but we
		// need to clear out this goroutine, which is what the connDoneChan is
		// for.
		// One last thing is that if nothing happens for fifteen minutes, we
		// can assume the conn has gone stale and close it, causing a loop
		// around to try a new conn.
		// It would be so much easier if Recv could use a timeout Context...
		go func(c *grpc.ClientConn, a *int32, cdc chan struct{}) {
			for {
				select {
				case <-exitChan:
				case <-cdc:
				case <-time.After(15 * time.Minute):
					// I'm comfortable with time.After here since it's just
					// once per fifteen minutes or new conn.
					v := atomic.LoadInt32(a)
					if v != 0 {
						atomic.AddInt32(a, -v)
						continue
					}
				}
				break
			}
			c.Close()
		}(conn, activity, connDoneChan)
		for {
			select {
			case <-exitChan:
				break
			default:
			}
			res, err := stream.Recv()
			if err != nil {
				rs.logDebug("replGroupStore: error with stream to ring service %q: %s", ringServer, err)
				break
			}
			atomic.AddInt32(activity, 1)
			if res != nil {
				if r, err := ring.LoadRing(bytes.NewBuffer(res.Ring)); err != nil {
					rs.logDebug("replGroupStore: error with ring received from stream to ring service %q: %s", ringServer, err)
				} else {
					// This will cache the ring if ringCachePath is not empty.
					rs.SetRing(r)
					// Resets the exponential sleeper since we had success.
					sleeperTicks = 2
					rs.logDebug("replGroupStore: got new ring from stream to ring service %q: %d", ringServer, res.Version)
				}
			}
		}
		close(connDoneChan)
		sleeper()
	}
}

// Startup is not required to use the ReplGroupStore; it will automatically
// connect to backend stores as needed. However, if you'd like to use the ring
// service to receive ring updates and have the ReplGroupStore automatically
// update itself accordingly, Startup will launch a connector to that service.
// Otherwise, you will need to call SetRing yourself to inform the
// ReplGroupStore of which backends to connect to.
func (rs *ReplGroupStore) Startup(ctx context.Context) error {
	rs.ringLock.Lock()
	if rs.ringServerExitChan == nil {
		rs.ringServerExitChan = make(chan struct{})
		go rs.ringServerConnector(rs.ringServerExitChan)
	}
	rs.ringLock.Unlock()
	return nil
}

// Shutdown will close all connections to backend stores and shutdown any
// running ring service connector. Note that the ReplGroupStore can still be
// used after Shutdown, it will just start reconnecting to backends again. To
// relaunch the ring service connector, you will need to call Startup.
func (rs *ReplGroupStore) Shutdown(ctx context.Context) error {
	rs.ringLock.Lock()
	if rs.ringServerExitChan != nil {
		close(rs.ringServerExitChan)
		rs.ringServerExitChan = nil
	}
	rs.storesLock.Lock()
	for addr, stc := range rs.stores {
		if err := stc.store.Shutdown(ctx); err != nil {
			rs.logDebug("replGroupStore: error during shutdown of store %s: %s", addr, err)
		}
		delete(rs.stores, addr)
		select {
		case <-ctx.Done():
			rs.storesLock.Unlock()
			return ctx.Err()
		default:
		}
	}
	rs.storesLock.Unlock()
	rs.ringLock.Unlock()
	return nil
}

func (rs *ReplGroupStore) EnableWrites(ctx context.Context) error {
	return nil
}

func (rs *ReplGroupStore) DisableWrites(ctx context.Context) error {
	return errors.New("cannot disable writes with this client at this time")
}

func (rs *ReplGroupStore) Flush(ctx context.Context) error {
	return nil
}

func (rs *ReplGroupStore) AuditPass(ctx context.Context) error {
	return errors.New("audit passes not available with this client at this time")
}

func (rs *ReplGroupStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	return noStats, nil
}

func (rs *ReplGroupStore) ValueCap(ctx context.Context) (uint32, error) {
	return uint32(rs.valueCap), nil
}

func (rs *ReplGroupStore) Lookup(ctx context.Context, keyA, keyB uint64, childKeyA, childKeyB uint64) (int64, uint32, error) {
	type rettype struct {
		timestampMicro int64
		length         uint32
		err            ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		return 0, 0, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.timestampMicro, ret.length, err = s.store.Lookup(ctx, keyA, keyB, childKeyA, childKeyB)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var length uint32
	var hadNotFoundErr bool
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.timestampMicro > timestampMicro || timestampMicro == 0 {
			timestampMicro = ret.timestampMicro
			length = ret.length
			hadNotFoundErr = ret.err != nil && store.IsNotFound(ret.err.Err())
		}
		if ret.err != nil {
			errs = append(errs, ret.err)
		}
	}
	if hadNotFoundErr {
		nferrs := make(ReplGroupStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		return timestampMicro, length, nferrs
	}
	if len(errs) < len(stores) {
		for _, err := range errs {
			rs.logDebug("replGroupStore: error during lookup: %s", err)
		}
		errs = nil
	}
	if errs == nil {
		return timestampMicro, length, nil
	}
	return timestampMicro, length, errs
}

func (rs *ReplGroupStore) Read(ctx context.Context, keyA uint64, keyB uint64, childKeyA, childKeyB uint64, value []byte) (int64, []byte, error) {
	type rettype struct {
		timestampMicro int64
		value          []byte
		err            ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		rs.logDebug("replGroupStore Read %x %x %x %x: error from storesFor: %s", keyA, keyB, childKeyA, childKeyB, err)
		return 0, nil, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.timestampMicro, ret.value, err = s.store.Read(ctx, keyA, keyB, childKeyA, childKeyB, nil)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var rvalue []byte
	var hadNotFoundErr bool
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.timestampMicro > timestampMicro || timestampMicro == 0 {
			timestampMicro = ret.timestampMicro
			rvalue = ret.value
			hadNotFoundErr = ret.err != nil && store.IsNotFound(ret.err.Err())
		}
		if ret.err != nil {
			errs = append(errs, ret.err)
		}
	}
	if value != nil && rvalue != nil {
		rvalue = append(value, rvalue...)
	}
	for _, err := range errs {
		rs.logDebug("replGroupStore Read %x %x %x %x: error during read: %s", keyA, keyB, childKeyA, childKeyB, err)
	}
	if hadNotFoundErr {
		nferrs := make(ReplGroupStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		rs.logDebug("replGroupStore Read %x %x %x %x: returning at point1: %d %d %v", keyA, keyB, childKeyA, childKeyB, timestampMicro, len(rvalue), nferrs)
		return timestampMicro, rvalue, nferrs
	}
	if len(errs) < len(stores) {
		errs = nil
	}
	if errs == nil {
		rs.logDebug("replGroupStore Read %x %x %x %x: returning at point2: %d %d", keyA, keyB, childKeyA, childKeyB, timestampMicro, len(rvalue))
		return timestampMicro, rvalue, nil
	}
	rs.logDebug("replGroupStore Read %x %x %x %x: returning at point3: %d %d %v", keyA, keyB, childKeyA, childKeyB, timestampMicro, len(rvalue), errs)
	return timestampMicro, rvalue, errs
}

func (rs *ReplGroupStore) Write(ctx context.Context, keyA uint64, keyB uint64, childKeyA, childKeyB uint64, timestampMicro int64, value []byte) (int64, error) {
	if len(value) == 0 {
		panic(fmt.Sprintf("REMOVEME ReplGroupStore asked to Write a zlv"))
	}
	if len(value) > rs.valueCap {
		return 0, fmt.Errorf("value length of %d > %d", len(value), rs.valueCap)
	}
	type rettype struct {
		oldTimestampMicro int64
		err               ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		return 0, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				if len(value) == 0 {
					panic(fmt.Sprintf("REMOVEME inside ReplGroupStore asked to Write a zlv"))
				}
				ret.oldTimestampMicro, err = s.store.Write(ctx, keyA, keyB, childKeyA, childKeyB, timestampMicro, value)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.oldTimestampMicro > oldTimestampMicro {
			oldTimestampMicro = ret.oldTimestampMicro
		}
	}
	if len(errs) < (len(stores)+1)/2 {
		for _, err := range errs {
			rs.logDebug("replGroupStore: error during write: %s", err)
		}
		errs = nil
	}
	if errs == nil {
		return oldTimestampMicro, nil
	}
	return oldTimestampMicro, errs
}

func (rs *ReplGroupStore) Delete(ctx context.Context, keyA uint64, keyB uint64, childKeyA, childKeyB uint64, timestampMicro int64) (int64, error) {
	type rettype struct {
		oldTimestampMicro int64
		err               ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		return 0, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.oldTimestampMicro, err = s.store.Delete(ctx, keyA, keyB, childKeyA, childKeyB, timestampMicro)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if ret.oldTimestampMicro > oldTimestampMicro {
			oldTimestampMicro = ret.oldTimestampMicro
		}
	}
	if len(errs) < (len(stores)+1)/2 {
		for _, err := range errs {
			rs.logDebug("replGroupStore: error during delete: %s", err)
		}
		errs = nil
	}
	if errs == nil {
		return oldTimestampMicro, nil
	}
	return oldTimestampMicro, errs
}

func (rs *ReplGroupStore) LookupGroup(ctx context.Context, parentKeyA, parentKeyB uint64) ([]store.LookupGroupItem, error) {
	type rettype struct {
		items []store.LookupGroupItem
		err   ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, parentKeyA)
	if err != nil {
		return nil, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.items, err = s.store.LookupGroup(ctx, parentKeyA, parentKeyB)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var items []store.LookupGroupItem
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if len(ret.items) > len(items) {
			items = ret.items
		}
	}
	if len(errs) == len(stores) {
		return items, errs
	} else {
		for _, err := range errs {
			rs.logDebug("replGroupStore: error during lookup group: %s", err)
		}
	}
	return items, nil
}

func (rs *ReplGroupStore) ReadGroup(ctx context.Context, parentKeyA, parentKeyB uint64) ([]store.ReadGroupItem, error) {
	type rettype struct {
		items []store.ReadGroupItem
		err   ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, parentKeyA)
	if err != nil {
		return nil, err
	}
	for _, s := range stores {
		go func(s *replGroupStoreAndTicketChan) {
			ret := &rettype{}
			var err error
			select {
			case <-s.ticketChan:
				ret.items, err = s.store.ReadGroup(ctx, parentKeyA, parentKeyB)
				s.ticketChan <- struct{}{}
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				ret.err = &replGroupStoreError{store: s.store, err: err}
			}
			ec <- ret
		}(s)
	}
	var items []store.ReadGroupItem
	var errs ReplGroupStoreErrorSlice
	for _ = range stores {
		ret := <-ec
		if ret.err != nil {
			errs = append(errs, ret.err)
		} else if len(ret.items) > len(items) {
			items = ret.items
		}
	}
	if len(errs) == len(stores) {
		return items, errs
	} else {
		for _, err := range errs {
			rs.logDebug("replGroupStore: error during read group: %s", err)
		}
	}
	return items, nil
}

type ReplGroupStoreError interface {
	error
	Store() store.GroupStore
	Err() error
}

type ReplGroupStoreErrorSlice []ReplGroupStoreError

func (es ReplGroupStoreErrorSlice) Error() string {
	if len(es) <= 0 {
		return "unknown error"
	} else if len(es) == 1 {
		return es[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(es), es[0])
}

type ReplGroupStoreErrorNotFound ReplGroupStoreErrorSlice

func (e ReplGroupStoreErrorNotFound) Error() string {
	if len(e) <= 0 {
		return "not found"
	} else if len(e) == 1 {
		return e[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(e), e[0])
}

func (e ReplGroupStoreErrorNotFound) ErrNotFound() string {
	return e.Error()
}

type replGroupStoreError struct {
	store store.GroupStore
	err   error
}

func (e *replGroupStoreError) Error() string {
	if e.err == nil {
		return "unknown error"
	}
	return e.err.Error()
}

func (e *replGroupStoreError) Store() store.GroupStore {
	return e.store
}

func (e *replGroupStoreError) Err() error {
	return e.err
}
