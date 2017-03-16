// Package oort provides a client for accessing an Oort server cluster.
//
// Note that errors in this package may be of the types Repl*StoreError,
// Repl*StoreErrorNotFound, and Repl*StoreErrorSlice. These error types pair
// the store and specific error from that store into an overall error type
// since this package's Store implementations will behave like a single store,
// but in reality are communicating with multiple stores in a cluster.
// Generally speaking, the overall error is what is important, but the
// encapsulated errors can be very beneficial with troubleshooting.
package oort

import "errors"

// got is at https://github.com/gholt/got
//go:generate got config.got valueconfig_GEN_.go TT=VALUE T=Value t=value
//go:generate got config.got groupconfig_GEN_.go TT=GROUP T=Group t=group
//go:generate got store.got valuestore_GEN_.go TT=VALUE T=Value t=value R,Delete,Lookup,Read,Write
//go:generate got store.got groupstore_GEN_.go TT=GROUP T=Group t=group R,Delete,LookupGroup,Lookup,ReadGroup,Read,Write
//go:generate got poolstore.got valuepoolstore_GEN_.go TT=VALUE T=Value t=value
//go:generate got poolstore.got grouppoolstore_GEN_.go TT=GROUP T=Group t=group
//go:generate got replstore.got valuereplstore_GEN_.go TT=VALUE T=Value t=value
//go:generate got replstore.got groupreplstore_GEN_.go TT=GROUP T=Group t=group
//go:generate got replstore_test.got valuereplstore_GEN_test.go TT=VALUE T=Value t=value
//go:generate got replstore_test.got groupreplstore_GEN_test.go TT=GROUP T=Group t=group
//go:generate got errorstore.got valueerrorstore_GEN_.go TT=VALUE T=Value t=value
//go:generate got errorstore.got grouperrorstore_GEN_.go TT=GROUP T=Group t=group

type s struct{}

func (*s) String() string {
	return "stats not available with this client at this time"
}

var noStats = &s{}

var noRingErr = errors.New("no ring")
