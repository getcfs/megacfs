package oort

import (
	"fmt"

	"github.com/getcfs/megacfs/ftls"
	"github.com/gholt/store"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type poolValueStore struct {
	logger      *zap.Logger
	addr        string
	size        int
	concurrency int
	ftlsConfig  *ftls.Config
	opts        []grpc.DialOption
	stores      []store.ValueStore
	storeChan   chan store.ValueStore
}

func newPoolValueStore(logger *zap.Logger, addr string, size int, concurrency int, ftlsConfig *ftls.Config, opts ...grpc.DialOption) *poolValueStore {
	ps := &poolValueStore{
		logger:      logger,
		addr:        addr,
		size:        size,
		concurrency: concurrency,
		ftlsConfig:  ftlsConfig,
		opts:        opts,
		stores:      make([]store.ValueStore, size),
		storeChan:   make(chan store.ValueStore, size),
	}
	for i := 0; i < ps.size; i++ {
		ps.stores[i] = newValueStore(logger, ps.addr, ps.concurrency, ps.ftlsConfig, ps.opts...)
		ps.storeChan <- ps.stores[i]
	}
	return ps
}

func (ps *poolValueStore) Startup(ctx context.Context) error {
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

func (ps *poolValueStore) Shutdown(ctx context.Context) error {
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

func (ps *poolValueStore) EnableWrites(ctx context.Context) error {
	// TODO: Should actually implement this feature.
	return nil
}

func (ps *poolValueStore) DisableWrites(ctx context.Context) error {
	return nil
}

func (ps *poolValueStore) Flush(ctx context.Context) error {
	// TODO: NOP for now
	return nil
}

func (ps *poolValueStore) AuditPass(ctx context.Context) error {
	// TODO: NOP for now
	return nil
}

func (ps *poolValueStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	// TODO: NOP for now
	return nil, nil
}

func (ps *poolValueStore) ValueCap(ctx context.Context) (uint32, error) {
	// TODO: NOP for now
	return 0xffffffff, nil
}

func (ps *poolValueStore) Lookup(ctx context.Context, keyA uint64, keyB uint64) (int64, uint32, error) {
	select {
	case s := <-ps.storeChan:
		a, b, c := s.Lookup(ctx, keyA, keyB)
		ps.storeChan <- s
		return a, b, c
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

func (ps *poolValueStore) Read(ctx context.Context, keyA uint64, keyB uint64, value []byte) (int64, []byte, error) {
	select {
	case s := <-ps.storeChan:
		a, b, c := s.Read(ctx, keyA, keyB, value)
		ps.storeChan <- s
		return a, b, c
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

func (ps *poolValueStore) Write(ctx context.Context, keyA uint64, keyB uint64, timestampMicro int64, value []byte) (int64, error) {
	select {
	case s := <-ps.storeChan:
		a, b := s.Write(ctx, keyA, keyB, timestampMicro, value)
		ps.storeChan <- s
		return a, b
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (ps *poolValueStore) Delete(ctx context.Context, keyA uint64, keyB uint64, timestampMicro int64) (int64, error) {
	select {
	case s := <-ps.storeChan:
		a, b := s.Delete(ctx, keyA, keyB, timestampMicro)
		ps.storeChan <- s
		return a, b
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}
