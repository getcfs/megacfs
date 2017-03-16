package oort

import (
	"testing"

	"github.com/gholt/store"
)

func TestValueStoreInterface(t *testing.T) {
	func(s store.ValueStore) {}(NewReplValueStore(nil))
}
