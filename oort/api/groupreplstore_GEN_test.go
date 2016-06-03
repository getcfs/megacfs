package api

import (
	"testing"

	"github.com/gholt/store"
)

func TestGroupStoreInterface(t *testing.T) {
	func(s store.GroupStore) {}(NewReplGroupStore(nil))
}
