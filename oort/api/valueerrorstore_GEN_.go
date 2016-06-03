package api

import (
	"fmt"

	"golang.org/x/net/context"
)

type errorValueStore string

func (es errorValueStore) String() string {
	return string(es)
}

func (es errorValueStore) Error() string {
	return string(es)
}

func (es errorValueStore) Startup(ctx context.Context) error {
	return es
}

func (es errorValueStore) Shutdown(ctx context.Context) error {
	return es
}

func (es errorValueStore) EnableWrites(ctx context.Context) error {
	return es
}

func (es errorValueStore) DisableWrites(ctx context.Context) error {
	return es
}

func (es errorValueStore) Flush(ctx context.Context) error {
	return es
}

func (es errorValueStore) AuditPass(ctx context.Context) error {
	return es
}

func (es errorValueStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	return es, es
}

func (es errorValueStore) ValueCap(ctx context.Context) (uint32, error) {
	return 0, es
}

func (es errorValueStore) Lookup(ctx context.Context, keyA uint64, keyB uint64) (int64, uint32, error) {
	return 0, 0, es
}

func (es errorValueStore) Read(ctx context.Context, keyA uint64, keyB uint64, value []byte) (int64, []byte, error) {
	return 0, value, es
}

func (es errorValueStore) Write(ctx context.Context, keyA uint64, keyB uint64, timestampMicro int64, value []byte) (int64, error) {
	return 0, es
}

func (es errorValueStore) Delete(ctx context.Context, keyA uint64, keyB uint64, timestampMicro int64) (int64, error) {
	return 0, es
}
