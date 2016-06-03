package api

import (
	"fmt"

	"github.com/gholt/store"
	"golang.org/x/net/context"
)

type errorGroupStore string

func (es errorGroupStore) String() string {
	return string(es)
}

func (es errorGroupStore) Error() string {
	return string(es)
}

func (es errorGroupStore) Startup(ctx context.Context) error {
	return es
}

func (es errorGroupStore) Shutdown(ctx context.Context) error {
	return es
}

func (es errorGroupStore) EnableWrites(ctx context.Context) error {
	return es
}

func (es errorGroupStore) DisableWrites(ctx context.Context) error {
	return es
}

func (es errorGroupStore) Flush(ctx context.Context) error {
	return es
}

func (es errorGroupStore) AuditPass(ctx context.Context) error {
	return es
}

func (es errorGroupStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	return es, es
}

func (es errorGroupStore) ValueCap(ctx context.Context) (uint32, error) {
	return 0, es
}

func (es errorGroupStore) Lookup(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64) (int64, uint32, error) {
	return 0, 0, es
}

func (es errorGroupStore) Read(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, value []byte) (int64, []byte, error) {
	return 0, value, es
}

func (es errorGroupStore) Write(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampMicro int64, value []byte) (int64, error) {
	return 0, es
}

func (es errorGroupStore) Delete(ctx context.Context, keyA uint64, keyB uint64, childKeyA uint64, childKeyB uint64, timestampMicro int64) (int64, error) {
	return 0, es
}

func (es errorGroupStore) LookupGroup(ctx context.Context, parentKeyA, parentKeyB uint64) ([]store.LookupGroupItem, error) {
	return nil, es
}

func (es errorGroupStore) ReadGroup(ctx context.Context, parentKeyA, parentKeyB uint64) ([]store.ReadGroupItem, error) {
	return nil, es
}
