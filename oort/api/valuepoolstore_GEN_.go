package api

import (
	"fmt"
	"sync"

	"github.com/getcfs/megacfs/ftls"
	"github.com/gholt/store"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type PoolValueStore struct {
	addr          string
	size          int
	concurrency   int
	ftlsConfig    *ftls.Config
	opts          []grpc.DialOption
	lock          sync.Mutex
	storeChan     chan store.ValueStore
	writesEnabled bool
}

func NewValuePoolStore(addr string, size int, concurrency int, ftlsConfig *ftls.Config, opts ...grpc.DialOption) *PoolValueStore {
	ps := &PoolValueStore{
		addr:          addr,
		size:          size,
		concurrency:   concurrency,
		ftlsConfig:    ftlsConfig,
		opts:          opts,
		writesEnabled: true,
		storeChan:     make(chan store.ValueStore, size),
	}
	return ps
}

func (ps *PoolValueStore) Startup(ctx context.Context) error {
	ps.lock.Lock()
	if ps.storeChan != nil {
		ps.lock.Unlock()
		return nil
	}
	for i := 0; i < ps.size; i++ {
		s := NewValueStore(ps.addr, ps.concurrency, ps.ftlsConfig, ps.opts...)
		select {
		case ps.storeChan <- s:
		case <-ctx.Done():
			go func(sc chan store.ValueStore, sz int) {
				for i := 0; i < sz; i++ {
					s := <-sc
					s.Shutdown(ctx)
				}
			}(ps.storeChan, ps.size)
			ps.storeChan = nil
			ps.lock.Unlock()
			return ctx.Err()
		}
	}
	ps.lock.Unlock()
	return nil
}

func (ps *PoolValueStore) Shutdown(ctx context.Context) error {
	ps.lock.Lock()
	if ps.storeChan == nil {
		ps.lock.Unlock()
		return nil
	}
	for i := 0; i < ps.size; i++ {
		s := <-ps.storeChan
		s.Shutdown(ctx)
	}
	ps.storeChan = nil
	ps.lock.Unlock()
	return nil
}

func (ps *PoolValueStore) EnableWrites(ctx context.Context) error {
	ps.lock.Lock()
	ps.writesEnabled = true
	ps.lock.Unlock()
	return nil
}

func (ps *PoolValueStore) DisableWrites(ctx context.Context) error {
	ps.lock.Lock()
	ps.writesEnabled = false
	ps.lock.Unlock()
	return nil
}

func (ps *PoolValueStore) Flush(ctx context.Context) error {
	// TODO: NOP for now
	return nil
}

func (ps *PoolValueStore) AuditPass(ctx context.Context) error {
	// TODO: NOP for now
	return nil
}

func (ps *PoolValueStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	// TODO: NOP for now
	return nil, nil
}

func (ps *PoolValueStore) ValueCap(ctx context.Context) (uint32, error) {
	// TODO: NOP for now
	return 0xffffffff, nil
}

func (ps *PoolValueStore) Lookup(ctx context.Context, keyA uint64, keyB uint64) (int64, uint32, error) {
	select {
	case s := <-ps.storeChan:
		return s.Lookup(ctx, keyA, keyB)
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

func (ps *PoolValueStore) Read(ctx context.Context, keyA uint64, keyB uint64, value []byte) (int64, []byte, error) {
	select {
	case s := <-ps.storeChan:
		return s.Read(ctx, keyA, keyB, value)
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (ps *PoolValueStore) Write(ctx context.Context, keyA uint64, keyB uint64, timestampMicro int64, value []byte) (int64, error) {
	select {
	case s := <-ps.storeChan:
		return s.Write(ctx, keyA, keyB, timestampMicro, value)
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (ps *PoolValueStore) Delete(ctx context.Context, keyA uint64, keyB uint64, timestampMicro int64) (int64, error) {
	select {
	case s := <-ps.storeChan:
		return s.Delete(ctx, keyA, keyB, timestampMicro)
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}
