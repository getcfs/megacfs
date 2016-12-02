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
	"github.com/gholt/ring"
	"github.com/gholt/store"
	"github.com/uber-go/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type ReplGroupStore struct {
	logger                     zap.Logger
	addressIndex               int
	valueCap                   int
	poolSize                   int
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
	stores     map[string]store.GroupStore
}

func NewReplGroupStore(c *GroupStoreConfig) *ReplGroupStore {
	cfg := resolveGroupStoreConfig(c)
	rs := &ReplGroupStore{
		logger:                     cfg.Logger,
		addressIndex:               cfg.AddressIndex,
		valueCap:                   int(cfg.ValueCap),
		poolSize:                   cfg.PoolSize,
		concurrentRequestsPerStore: cfg.ConcurrentRequestsPerStore,
		failedConnectRetryDelay:    cfg.FailedConnectRetryDelay,
		ftlsConfig:                 cfg.StoreFTLSConfig,
		grpcOpts:                   cfg.GRPCOpts,
		stores:                     make(map[string]store.GroupStore),
		ringServer:                 cfg.RingServer,
		ringServerGRPCOpts:         cfg.RingServerGRPCOpts,
		ringCachePath:              cfg.RingCachePath,
		ringClientID:               cfg.RingClientID,
	}
	if rs.logger == nil {
		rs.logger = zap.New(zap.NewJSONEncoder())
	}
	if rs.ringCachePath != "" {
		if fp, err := os.Open(rs.ringCachePath); err != nil {
			rs.logger.Debug("error loading cached ring", zap.String("path", rs.ringCachePath), zap.Error(err))
		} else if r, err := ring.LoadRing(fp); err != nil {
			fp.Close()
			rs.logger.Debug("error loading cached ring", zap.String("path", rs.ringCachePath), zap.Error(err))
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
			rs.logger.Debug("error caching ring", zap.String("path", rs.ringCachePath), zap.Error(err))
		} else if err := r.Persist(fp); err != nil {
			fp.Close()
			os.Remove(fp.Name())
			rs.logger.Debug("error caching ring", zap.String("path", rs.ringCachePath), zap.Error(err))
		} else {
			fp.Close()
			if err := os.Rename(fp.Name(), rs.ringCachePath); err != nil {
				os.Remove(fp.Name())
				rs.logger.Debug("error caching ring", zap.String("path", rs.ringCachePath), zap.Error(err))
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
		shutdownStores := make([]store.GroupStore, len(shutdownAddrs))
		rs.storesLock.Lock()
		for i, a := range shutdownAddrs {
			shutdownStores[i] = rs.stores[a]
			rs.stores[a] = nil
		}
		rs.storesLock.Unlock()
		for i, s := range shutdownStores {
			if err := s.Shutdown(context.Background()); err != nil {
				rs.logger.Debug("error during shutdown of store", zap.String("addr", shutdownAddrs[i]), zap.Error(err))
			}
		}
	}
	rs.ringLock.Unlock()
}

func (rs *ReplGroupStore) storesFor(ctx context.Context, keyA uint64) ([]store.GroupStore, error) {
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
	as := make([]string, 0, len(ns))
	for _, n := range ns {
		a := n.Address(rs.addressIndex)
		for _, b := range as {
			if a == b {
				a = ""
				break
			}
		}
		if a != "" {
			as = append(as, a)
		}
	}
	ss := make([]store.GroupStore, len(as))
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
					ss[i] = NewPoolGroupStore(as[i], rs.poolSize, rs.concurrentRequestsPerStore, rs.ftlsConfig, rs.grpcOpts...)
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
				rs.logger.Error("error resolving ring service", zap.Error(err))
				sleeper()
				continue
			}
		}
		conn, err := grpc.Dial(ringServer, rs.ringServerGRPCOpts...)
		if err != nil {
			rs.logger.Error("error connecting to ring service", zap.String("ringServer", ringServer), zap.Error(err))
			sleeper()
			continue
		}
		stream, err := synpb.NewSyndicateClient(conn).GetRingStream(context.Background(), &synpb.SubscriberID{Id: rs.ringClientID})
		if err != nil {
			rs.logger.Error("error creating stream with ring service", zap.String("ringServer", ringServer), zap.Error(err))
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
				rs.logger.Debug("error with stream to ring service", zap.String("ringServer", ringServer), zap.Error(err))
				break
			}
			atomic.AddInt32(activity, 1)
			if res != nil {
				if r, err := ring.LoadRing(bytes.NewBuffer(res.Ring)); err != nil {
					rs.logger.Debug("error with ring received from stream to ring service", zap.String("ringServer", ringServer), zap.Error(err))
				} else {
					// This will cache the ring if ringCachePath is not empty.
					rs.SetRing(r)
					// Resets the exponential sleeper since we had success.
					sleeperTicks = 2
					rs.logger.Debug("got new ring from stream to ring service", zap.String("ringServer", ringServer), zap.Int64("version", res.Version))
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
	for addr, s := range rs.stores {
		if err := s.Shutdown(ctx); err != nil {
			rs.logger.Debug("error during shutdown of store", zap.String("addr", addr), zap.Error(err))
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
		go func(s store.GroupStore) {
			ret := &rettype{}
			var err error
			ret.timestampMicro, ret.length, err = s.Lookup(ctx, keyA, keyB, childKeyA, childKeyB)
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
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
			rs.logger.Debug("error during lookup", zap.Error(err))
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
		rs.logger.Debug("Read error from storesFor", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Error(err))
		return 0, nil, err
	}
	for _, s := range stores {
		go func(s store.GroupStore) {
			ret := &rettype{}
			var err error
			ret.timestampMicro, ret.value, err = s.Read(ctx, keyA, keyB, childKeyA, childKeyB, nil)
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
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
		rs.logger.Debug("Read error", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Error(err))
	}
	if hadNotFoundErr {
		nferrs := make(ReplGroupStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		rs.logger.Debug("Read: returning at point1", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(rvalue)), zap.Error(nferrs))
		return timestampMicro, rvalue, nferrs
	}
	if len(errs) < len(stores) {
		errs = nil
	}
	if errs == nil {
		rs.logger.Debug("Read: returning at point2", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(rvalue)))
		return timestampMicro, rvalue, nil
	}
	rs.logger.Debug("Read: returning at point3", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Int64("timestmapMicro", timestampMicro), zap.Int("len", len(rvalue)), zap.Error(errs))
	return timestampMicro, rvalue, errs
}

func (rs *ReplGroupStore) Write(ctx context.Context, keyA uint64, keyB uint64, childKeyA, childKeyB uint64, timestampMicro int64, value []byte) (int64, error) {
	rs.logger.Debug("Write", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(value)))
	if len(value) == 0 {
		rs.logger.Fatal("REMOVEME ReplGroupStore asked to Write a zlv")
	}
	if len(value) > rs.valueCap {
		rs.logger.Debug("Write return point 1", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(value)), zap.Int("valueCap", rs.valueCap))
		return 0, fmt.Errorf("value length of %d > %d", len(value), rs.valueCap)
	}
	type rettype struct {
		oldTimestampMicro int64
		err               ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		rs.logger.Debug("Write return point 2", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(value)), zap.Error(err))
		return 0, err
	}
	for _, s := range stores {
		go func(s store.GroupStore) {
			ret := &rettype{}
			var err error
			if len(value) == 0 {
				rs.logger.Fatal("REMOVEME inside ReplGroupStore asked to Write a zlv")
			}
			ret.oldTimestampMicro, err = s.Write(ctx, keyA, keyB, childKeyA, childKeyB, timestampMicro, value)
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
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
			rs.logger.Debug("error during write", zap.Error(err))
		}
		errs = nil
	}
	if errs == nil {
		rs.logger.Debug("Write return point 3", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(value)), zap.Int64("oldTimestampMicro", oldTimestampMicro))
		return oldTimestampMicro, nil
	}
	rs.logger.Debug("Write return point 4", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(value)), zap.Int64("oldTimestampMicro", oldTimestampMicro), zap.Int("lenErrs", len(errs)))
	return oldTimestampMicro, errs
}

func (rs *ReplGroupStore) Delete(ctx context.Context, keyA uint64, keyB uint64, childKeyA, childKeyB uint64, timestampMicro int64) (int64, error) {
	rs.logger.Debug("Delete", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Int64("timestampMicro", timestampMicro))
	type rettype struct {
		oldTimestampMicro int64
		err               ReplGroupStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		rs.logger.Debug("Delete return point 1", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Int64("timestampMicro", timestampMicro), zap.Error(err))
		return 0, err
	}
	for _, s := range stores {
		go func(s store.GroupStore) {
			ret := &rettype{}
			var err error
			ret.oldTimestampMicro, err = s.Delete(ctx, keyA, keyB, childKeyA, childKeyB, timestampMicro)
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
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
			rs.logger.Debug("error during delete", zap.Error(err))
		}
		errs = nil
	}
	if errs == nil {
		rs.logger.Debug("Delete return point 2", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Int64("timestampMicro", timestampMicro))
		return oldTimestampMicro, nil
	}
	rs.logger.Debug("Delete return point 3", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Uint64("childKeyA", childKeyA), zap.Uint64("childKeyB", childKeyB), zap.Int64("timestampMicro", timestampMicro), zap.Int64("oldTimestampMicro", oldTimestampMicro), zap.Int("lenErrs", len(errs)))
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
		go func(s store.GroupStore) {
			ret := &rettype{}
			var err error
			ret.items, err = s.LookupGroup(ctx, parentKeyA, parentKeyB)
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
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
			rs.logger.Debug("error during lookup group", zap.Error(err))
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
		go func(s store.GroupStore) {
			ret := &rettype{}
			var err error
			ret.items, err = s.ReadGroup(ctx, parentKeyA, parentKeyB)
			if err != nil {
				ret.err = &replGroupStoreError{store: s, err: err}
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
			rs.logger.Debug("error during read group", zap.Error(err))
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
