// Package server provides a server implementation of a single node of
// an Oort cluster.
package server

// got is at https://github.com/gholt/got
//go:generate got store.got groupstore_GEN_.go TT=GROUP T=Group t=group
//go:generate got store.got valuestore_GEN_.go TT=VALUE T=Value t=value
