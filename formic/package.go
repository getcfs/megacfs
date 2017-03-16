// Package formic provides a server for cfs clients to use in managing and
// accessing file systems.
//
// It provides a FUSE-like GRPC service to cfs clients as well as a
// meta-service for creating and deleting file systems and maintaining
// permissions for file systems.
//
// It makes use of Oort services in the cluster to persist file system data and
// metadata.
package formic
