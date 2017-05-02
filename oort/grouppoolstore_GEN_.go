package oort

import (
	"fmt"

	"github.com/getcfs/megacfs/ftls"
	"github.com/gholt/store"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type poolGroupStore struct {
	logger      *zap.Logger
	addr        string
	size        int
	concurrency int
	ftlsConfig  *ftls.Config
	opts        []grpc.DialOption
	stores      []store.GroupStore
	storeChan   chan store.GroupStore
}

func newPoolGroupStore(logger *zap.Logger, addr string, size int, concurrency int, ftlsConfig *ftls.Config, opts ...grpc.DialOption) *poolGroupStore {
	ps := &poolGroupStore{
		logger:      logger,
		addr:        addr,
		size:        size,
		concurrency: concurrency,
		ftlsConfig:  ftlsConfig,
		opts:        opts,
		stores:      make([]store.GroupStore, size),
		storeChan:   make(chan store.GroupStore, size),
	}
	for i := 0; i < ps.size; i++ {
		ps.stores[i] = newGroupStore(logger, ps.addr, ps.concurrency, ps.ftlsConfig, ps.opts...)
		ps.storeChan <- ps.stores[i]
	}
	return ps
}

func (ps *poolGroupStore) Startup(ctx context.Context) error {
	for i := 0; i < ps.size; i++ {
		ps.stores[i].Startup(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func (ps *poolGroupStore) Shutdown(ctx context.Context) error {
	for i := 0; i < ps.size; i++ {
		ps.stores[i].Shutdown(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

func (ps *poolGroupStore) EnableWrites(ctx context.Context) error {
	// TODO: Should actually implement this feature.
	return nil
}

func (ps *poolGroupStore) DisableWrites(ctx context.Context) error {
	return nil
}

func (ps *poolGroupStore) Flush(ctx context.Context) error {
	// TODO: NOP for now
	return nil
}

func (ps *poolGroupStore) AuditPass(ctx context.Context) error {
	// TODO: NOP for now
	return nil
}

func (ps *poolGroupStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	// TODO: NOP for now
	return nil, nil
}

func (ps *poolGroupStore) ValueCap(ctx context.Context) (uint32, error) {
	// TODO: NOP for now
	return 0xffffffff, nil
}

func (ps *poolGroupStore) Lookup(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64) (int64, uint32, error) {
	select {
	case s := <-ps.storeChan:
		a, b, c := s.Lookup(ctx, keyA, keyB, childKeyA, childKeyB)
		ps.storeChan <- s
		return a, b, c
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

func (ps *poolGroupStore) Read(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, value []byte) (int64, []byte, error) {
	select {
	case s := <-ps.storeChan:
		a, b, c := s.Read(ctx, keyA, keyB, childKeyA, childKeyB, value)
		ps.storeChan <- s
		return a, b, c
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (ps *poolGroupStore) Write(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampMicro int64, value []byte) (int64, error) {
	select {
	case s := <-ps.storeChan:
		a, b := s.Write(ctx, keyA, keyB, childKeyA, childKeyB, timestampMicro, value)
		ps.storeChan <- s
		return a, b
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (ps *poolGroupStore) Delete(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampMicro int64) (int64, error) {
	select {
	case s := <-ps.storeChan:
		a, b := s.Delete(ctx, keyA, keyB, childKeyA, childKeyB, timestampMicro)
		ps.storeChan <- s
		return a, b
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (ps *poolGroupStore) LookupGroup(ctx context.Context, parentKeyA, parentKeyB uint64) ([]store.LookupGroupItem, error) {
	select {
	case s := <-ps.storeChan:
		a, b := s.LookupGroup(ctx, parentKeyA, parentKeyB)
		ps.storeChan <- s
		return a, b
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (ps *poolGroupStore) ReadGroup(ctx context.Context, parentKeyA, parentKeyB uint64) ([]store.ReadGroupItem, error) {
	select {
	case s := <-ps.storeChan:
		a, b := s.ReadGroup(ctx, parentKeyA, parentKeyB)
		ps.storeChan <- s
		return a, b
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
