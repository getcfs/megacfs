// Package brimio contains I/O related Go code.
package brimio

// NullIO implements io.WriteCloser by throwing away all data.
type NullIO struct {
}

// Write discards the value but returns its length and nil error to fulfill the
// io.Closer interface.
func (nw *NullIO) Write(v []byte) (int, error) {
	return len(v), nil
}

// Close is a no-op to fulfill the io.Closer interface.
func (nw *NullIO) Close() error {
	return nil
}
