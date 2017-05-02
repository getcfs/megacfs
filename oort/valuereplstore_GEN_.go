package oort

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/getcfs/megacfs/ftls"
	"github.com/gholt/ring"
	"github.com/gholt/store"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type ReplValueStore struct {
	logger                     *zap.Logger
	addressIndex               int
	valueCap                   int
	poolSize                   int
	concurrentRequestsPerStore int
	failedConnectRetryDelay    int
	ftlsConfig                 *ftls.Config
	grpcOpts                   []grpc.DialOption

	ringLock      sync.RWMutex
	ring          ring.Ring
	ringCachePath string
	ringClientID  string

	storesLock sync.RWMutex
	stores     map[string]store.ValueStore
}

func NewReplValueStore(c *ValueStoreConfig) *ReplValueStore {
	cfg := resolveValueStoreConfig(c)
	rs := &ReplValueStore{
		logger:                     cfg.Logger,
		addressIndex:               cfg.AddressIndex,
		valueCap:                   int(cfg.ValueCap),
		poolSize:                   cfg.PoolSize,
		concurrentRequestsPerStore: cfg.ConcurrentRequestsPerStore,
		failedConnectRetryDelay:    cfg.FailedConnectRetryDelay,
		ftlsConfig:                 cfg.StoreFTLSConfig,
		grpcOpts:                   cfg.GRPCOpts,
		stores:                     make(map[string]store.ValueStore),
		ringCachePath:              cfg.RingCachePath,
		ringClientID:               cfg.RingClientID,
	}
	if rs.logger == nil {
		var err error
		rs.logger, err = zap.NewProduction()
		if err != nil {
			panic(err)
		}
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

func (rs *ReplValueStore) Ring(ctx context.Context) ring.Ring {
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

func (rs *ReplValueStore) SetRing(r ring.Ring) {
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
		shutdownStores := make([]store.ValueStore, len(shutdownAddrs))
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

func (rs *ReplValueStore) storesFor(ctx context.Context, keyA uint64) ([]store.ValueStore, error) {
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
	ss := make([]store.ValueStore, len(as))
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
					ss[i] = newPoolValueStore(rs.logger, as[i], rs.poolSize, rs.concurrentRequestsPerStore, rs.ftlsConfig, rs.grpcOpts...)
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

// Startup is not required to use the ReplValueStore; it will automatically
// connect to backend stores as needed.
func (rs *ReplValueStore) Startup(ctx context.Context) error {
	return nil
}

// Shutdown will close all connections to backend stores. Note that the
// ReplValueStore can still be used after Shutdown, it will just start
// reconnecting to backends again.
func (rs *ReplValueStore) Shutdown(ctx context.Context) error {
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
	return nil
}

func (rs *ReplValueStore) EnableWrites(ctx context.Context) error {
	return nil
}

func (rs *ReplValueStore) DisableWrites(ctx context.Context) error {
	return errors.New("cannot disable writes with this client at this time")
}

func (rs *ReplValueStore) Flush(ctx context.Context) error {
	return nil
}

func (rs *ReplValueStore) AuditPass(ctx context.Context) error {
	return errors.New("audit passes not available with this client at this time")
}

func (rs *ReplValueStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	return noStats, nil
}

func (rs *ReplValueStore) ValueCap(ctx context.Context) (uint32, error) {
	return uint32(rs.valueCap), nil
}

func (rs *ReplValueStore) Lookup(ctx context.Context, keyA, keyB uint64) (int64, uint32, error) {
	type rettype struct {
		timestampMicro int64
		length         uint32
		err            ReplValueStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		return 0, 0, err
	}
	for _, s := range stores {
		go func(s store.ValueStore) {
			ret := &rettype{}
			var err error
			ret.timestampMicro, ret.length, err = s.Lookup(ctx, keyA, keyB)
			if err != nil {
				ret.err = &replValueStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var length uint32
	var hadNotFoundErr bool
	var errs ReplValueStoreErrorSlice
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
		nferrs := make(ReplValueStoreErrorNotFound, len(errs))
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

func (rs *ReplValueStore) Read(ctx context.Context, keyA uint64, keyB uint64, value []byte) (int64, []byte, error) {
	type rettype struct {
		timestampMicro int64
		value          []byte
		err            ReplValueStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		rs.logger.Debug("Read error from storesFor", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Error(err))
		return 0, nil, err
	}
	for _, s := range stores {
		go func(s store.ValueStore) {
			ret := &rettype{}
			var err error
			ret.timestampMicro, ret.value, err = s.Read(ctx, keyA, keyB, nil)
			if err != nil {
				ret.err = &replValueStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var timestampMicro int64
	var rvalue []byte
	var hadNotFoundErr bool
	var errs ReplValueStoreErrorSlice
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
		rs.logger.Debug("Read error", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Error(err))
	}
	if hadNotFoundErr {
		nferrs := make(ReplValueStoreErrorNotFound, len(errs))
		for i, v := range errs {
			nferrs[i] = v
		}
		rs.logger.Debug("Read: returning at point1", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(rvalue)), zap.Error(nferrs))
		return timestampMicro, rvalue, nferrs
	}
	if len(errs) < len(stores) {
		errs = nil
	}
	if errs == nil {
		rs.logger.Debug("Read: returning at point2", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(rvalue)))
		return timestampMicro, rvalue, nil
	}
	rs.logger.Debug("Read: returning at point3", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Int64("timestmapMicro", timestampMicro), zap.Int("len", len(rvalue)), zap.Error(errs))
	return timestampMicro, rvalue, errs
}

func (rs *ReplValueStore) Write(ctx context.Context, keyA uint64, keyB uint64, timestampMicro int64, value []byte) (int64, error) {
	rs.logger.Debug("Write", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(value)))
	if len(value) == 0 {
		rs.logger.Fatal("REMOVEME ReplValueStore asked to Write a zlv")
	}
	if len(value) > rs.valueCap {
		rs.logger.Debug("Write return point 1", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(value)), zap.Int("valueCap", rs.valueCap))
		return 0, fmt.Errorf("value length of %d > %d", len(value), rs.valueCap)
	}
	type rettype struct {
		oldTimestampMicro int64
		err               ReplValueStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		rs.logger.Debug("Write return point 2", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(value)), zap.Error(err))
		return 0, err
	}
	for _, s := range stores {
		go func(s store.ValueStore) {
			ret := &rettype{}
			var err error
			if len(value) == 0 {
				rs.logger.Fatal("REMOVEME inside ReplValueStore asked to Write a zlv")
			}
			ret.oldTimestampMicro, err = s.Write(ctx, keyA, keyB, timestampMicro, value)
			if err != nil {
				ret.err = &replValueStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplValueStoreErrorSlice
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
		rs.logger.Debug("Write return point 3", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(value)), zap.Int64("oldTimestampMicro", oldTimestampMicro))
		return oldTimestampMicro, nil
	}
	rs.logger.Debug("Write return point 4", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Int64("timestampMicro", timestampMicro), zap.Int("len", len(value)), zap.Int64("oldTimestampMicro", oldTimestampMicro), zap.Int("lenErrs", len(errs)))
	return oldTimestampMicro, errs
}

func (rs *ReplValueStore) Delete(ctx context.Context, keyA uint64, keyB uint64, timestampMicro int64) (int64, error) {
	rs.logger.Debug("Delete", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Int64("timestampMicro", timestampMicro))
	type rettype struct {
		oldTimestampMicro int64
		err               ReplValueStoreError
	}
	ec := make(chan *rettype)
	stores, err := rs.storesFor(ctx, keyA)
	if err != nil {
		rs.logger.Debug("Delete return point 1", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Int64("timestampMicro", timestampMicro), zap.Error(err))
		return 0, err
	}
	for _, s := range stores {
		go func(s store.ValueStore) {
			ret := &rettype{}
			var err error
			ret.oldTimestampMicro, err = s.Delete(ctx, keyA, keyB, timestampMicro)
			if err != nil {
				ret.err = &replValueStoreError{store: s, err: err}
			}
			ec <- ret
		}(s)
	}
	var oldTimestampMicro int64
	var errs ReplValueStoreErrorSlice
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
		rs.logger.Debug("Delete return point 2", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Int64("timestampMicro", timestampMicro))
		return oldTimestampMicro, nil
	}
	rs.logger.Debug("Delete return point 3", zap.Uint64("keyA", keyA), zap.Uint64("keyB", keyB), zap.Int64("timestampMicro", timestampMicro), zap.Int64("oldTimestampMicro", oldTimestampMicro), zap.Int("lenErrs", len(errs)))
	return oldTimestampMicro, errs
}

type ReplValueStoreError interface {
	error
	Store() store.ValueStore
	Err() error
}

type ReplValueStoreErrorSlice []ReplValueStoreError

func (es ReplValueStoreErrorSlice) Error() string {
	if len(es) <= 0 {
		return "unknown error"
	} else if len(es) == 1 {
		return es[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(es), es[0])
}

type ReplValueStoreErrorNotFound ReplValueStoreErrorSlice

func (e ReplValueStoreErrorNotFound) Error() string {
	if len(e) <= 0 {
		return "not found"
	} else if len(e) == 1 {
		return e[0].Error()
	}
	return fmt.Sprintf("%d errors, first is: %s", len(e), e[0])
}

func (e ReplValueStoreErrorNotFound) ErrNotFound() string {
	return e.Error()
}

type replValueStoreError struct {
	store store.ValueStore
	err   error
}

func (e *replValueStoreError) Error() string {
	if e.err == nil {
		return "unknown error"
	}
	return e.err.Error()
}

func (e *replValueStoreError) Store() store.ValueStore {
	return e.store
}

func (e *replValueStoreError) Err() error {
	return e.err
}
