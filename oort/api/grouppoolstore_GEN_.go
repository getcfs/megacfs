package api

import (
	"fmt"
	"sync"

	"github.com/getcfs/megacfs/ftls"
	"github.com/gholt/store"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type PoolGroupStore struct {
	addr          string
	size          int
	concurrency   int
	ftlsConfig    *ftls.Config
	opts          []grpc.DialOption
	lock          sync.Mutex
	storeChan     chan store.GroupStore
	writesEnabled bool
}

func NewGroupPoolStore(addr string, size int, concurrency int, ftlsConfig *ftls.Config, opts ...grpc.DialOption) *PoolGroupStore {
	ps := &PoolGroupStore{
		addr:          addr,
		size:          size,
		concurrency:   concurrency,
		ftlsConfig:    ftlsConfig,
		opts:          opts,
		writesEnabled: true,
		storeChan:     make(chan store.GroupStore, size),
	}
	return ps
}

func (ps *PoolGroupStore) Startup(ctx context.Context) error {
	ps.lock.Lock()
	if ps.storeChan != nil {
		ps.lock.Unlock()
		return nil
	}
	for i := 0; i < ps.size; i++ {
		s := NewGroupStore(ps.addr, ps.concurrency, ps.ftlsConfig, ps.opts...)
		select {
		case ps.storeChan <- s:
		case <-ctx.Done():
			go func(sc chan store.GroupStore, sz int) {
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

func (ps *PoolGroupStore) Shutdown(ctx context.Context) error {
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

func (ps *PoolGroupStore) EnableWrites(ctx context.Context) error {
	ps.lock.Lock()
	ps.writesEnabled = true
	ps.lock.Unlock()
	return nil
}

func (ps *PoolGroupStore) DisableWrites(ctx context.Context) error {
	ps.lock.Lock()
	ps.writesEnabled = false
	ps.lock.Unlock()
	return nil
}

func (ps *PoolGroupStore) Flush(ctx context.Context) error {
	// TODO: NOP for now
	return nil
}

func (ps *PoolGroupStore) AuditPass(ctx context.Context) error {
	// TODO: NOP for now
	return nil
}

func (ps *PoolGroupStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	// TODO: NOP for now
	return nil, nil
}

func (ps *PoolGroupStore) ValueCap(ctx context.Context) (uint32, error) {
	// TODO: NOP for now
	return 0xffffffff, nil
}

func (ps *PoolGroupStore) Lookup(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64) (int64, uint32, error) {
	select {
	case s := <-ps.storeChan:
		return s.Lookup(ctx, keyA, keyB, childKeyA, childKeyB)
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

func (ps *PoolGroupStore) Read(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, value []byte) (int64, []byte, error) {
	select {
	case s := <-ps.storeChan:
		return s.Read(ctx, keyA, keyB, childKeyA, childKeyB, value)
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (ps *PoolGroupStore) Write(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampMicro int64, value []byte) (int64, error) {
	select {
	case s := <-ps.storeChan:
		return s.Write(ctx, keyA, keyB, childKeyA, childKeyB, timestampMicro, value)
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (ps *PoolGroupStore) Delete(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampMicro int64) (int64, error) {
	select {
	case s := <-ps.storeChan:
		return s.Delete(ctx, keyA, keyB, childKeyA, childKeyB, timestampMicro)
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (ps *PoolGroupStore) LookupGroup(ctx context.Context, parentKeyA, parentKeyB uint64) ([]store.LookupGroupItem, error) {
	select {
	case s := <-ps.storeChan:
		return s.LookupGroup(ctx, parentKeyA, parentKeyB)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (ps *PoolGroupStore) ReadGroup(ctx context.Context, parentKeyA, parentKeyB uint64) ([]store.ReadGroupItem, error) {
	select {
	case s := <-ps.storeChan:
		return s.ReadGroup(ctx, parentKeyA, parentKeyB)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
