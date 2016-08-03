package brimio

import (
	"bytes"
	"hash/crc32"
	"io"
	"io/ioutil"
	"runtime"
	"testing"
)

func TestChecksummedReader(t *testing.T) {
	// Establish written data for testing against
	buf := &bytes.Buffer{}
	cw := NewChecksummedWriter(buf, 16, crc32.NewIEEE)
	if cw == nil {
		t.Fatal(cw)
	}
	n, err := cw.Write([]byte("12345678901234567890ghijklmnopqrstuvwxyz"))
	if n != 40 {
		t.Fatal(n)
	}
	if err != nil {
		t.Fatal(err)
	}
	err = cw.Close()
	if err != nil {
		t.Fatal(err)
	}
	hash := crc32.NewIEEE()
	hash.Write([]byte("1234567890123456"))
	hash2 := crc32.NewIEEE()
	hash2.Write([]byte("7890ghijklmnopqr"))
	if !bytes.Equal(buf.Bytes(), []byte("1234567890123456"+string(hash.Sum(nil))+"7890ghijklmnopqr"+string(hash2.Sum(nil))+"stuvwxyz")) {
		t.Fatalf("%#v", string(buf.Bytes()))
	}
	// Test reading all data straight through
	cr := NewChecksummedReader(bytes.NewReader(buf.Bytes()), 16, crc32.NewIEEE)
	v, err := ioutil.ReadAll(cr)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "12345678901234567890ghijklmnopqrstuvwxyz" {
		t.Fatalf("%#v", string(v))
	}
	// Test reading a small amount within an interval
	o, err := cr.Seek(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if o != 0 {
		t.Fatal(o)
	}
	v = make([]byte, 10)
	n, err = io.ReadFull(cr, v)
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatal(n)
	}
	if string(v) != "1234567890" {
		t.Fatalf("%#v", string(v))
	}
	// Test reading a small amount crossing an interval
	v = make([]byte, 8)
	n, err = io.ReadFull(cr, v)
	if err != nil {
		t.Fatal(err)
	}
	if n != 8 {
		t.Fatal(n)
	}
	if string(v) != "12345678" {
		t.Fatalf("%#v", string(v))
	}
	// Testing reading the rest and hitting EOF
	v = make([]byte, 80)
	n, err = io.ReadFull(cr, v)
	if err != io.ErrUnexpectedEOF {
		t.Fatal(err)
	}
	if n != 22 {
		t.Fatal(n)
	}
	if string(v[:n]) != "90ghijklmnopqrstuvwxyz" {
		t.Fatalf("%#v", string(v))
	}
	// Test seeking to position from start
	o, err = cr.Seek(18, 0)
	if err != nil {
		t.Fatal(err)
	}
	if o != 18 {
		t.Fatal(o)
	}
	v = make([]byte, 20)
	n, err = io.ReadFull(cr, v)
	if err != nil {
		t.Fatal(err)
	}
	if n != 20 {
		t.Fatal(n)
	}
	if string(v) != "90ghijklmnopqrstuvwx" {
		t.Fatalf("%#v", string(v))
	}
	// Test seeking to position from current
	o, err = cr.Seek(-20, 1)
	if err != nil {
		t.Fatal(err)
	}
	if o != 18 {
		t.Fatal(o)
	}
	v = make([]byte, 18)
	n, err = io.ReadFull(cr, v)
	if err != nil {
		t.Fatal(err)
	}
	if n != 18 {
		t.Fatal(n)
	}
	if string(v) != "90ghijklmnopqrstuv" {
		t.Fatalf("%#v", string(v))
	}
	// Test seeking to position from end
	o, err = cr.Seek(-20, 2)
	if err != nil {
		t.Fatal(err)
	}
	if o != 20 {
		t.Fatal(o)
	}
	v = make([]byte, 22)
	n, err = io.ReadFull(cr, v)
	if err != io.ErrUnexpectedEOF {
		t.Fatal(err)
	}
	if n != 20 {
		t.Fatalf("%#v", string(v[:n]))
		t.Fatal(n)
	}
	if string(v[:n]) != "ghijklmnopqrstuvwxyz" {
		t.Fatalf("%#v", string(v))
	}
	// Test verifying from start of interval
	o, err = cr.Seek(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if o != 0 {
		t.Fatal(o)
	}
	ok, err := cr.Verify()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal(ok)
	}
	v = make([]byte, 4)
	n, err = io.ReadFull(cr, v)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatal(n)
	}
	if string(v) != "1234" {
		t.Fatalf("%#v", string(v))
	}
	// Test verifying from middle of interval
	o, err = cr.Seek(12, 0)
	if err != nil {
		t.Fatal(err)
	}
	if o != 12 {
		t.Fatal(o)
	}
	ok, err = cr.Verify()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal(ok)
	}
	v = make([]byte, 4)
	n, err = io.ReadFull(cr, v)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatal(n)
	}
	if string(v) != "3456" {
		t.Fatalf("%#v", string(v))
	}
	// Test verifying from end of interval
	o, err = cr.Seek(15, 0)
	if err != nil {
		t.Fatal(err)
	}
	if o != 15 {
		t.Fatal(o)
	}
	ok, err = cr.Verify()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal(ok)
	}
	v = make([]byte, 4)
	n, err = io.ReadFull(cr, v)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatal(n)
	}
	if string(v) != "6789" {
		t.Fatalf("%#v", string(v))
	}
	// Test verifying shortened last interval in file
	// In such a case, the ending bytes are not covered by a checksum
	o, err = cr.Seek(35, 0)
	if err != nil {
		t.Fatal(err)
	}
	if o != 35 {
		t.Fatal(o)
	}
	ok, err = cr.Verify()
	if err != io.ErrUnexpectedEOF {
		t.Fatal(err)
	}
	if ok {
		t.Fatal(ok)
	}
	o, err = cr.Seek(35, 0)
	if err != nil {
		t.Fatal(err)
	}
	if o != 35 {
		t.Fatal(o)
	}
	v = make([]byte, 4)
	n, err = io.ReadFull(cr, v)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatal(n)
	}
	if string(v) != "vwxy" {
		t.Fatalf("%#v", string(v))
	}
}

func TestChecksummedWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	cw := NewChecksummedWriter(buf, 16, crc32.NewIEEE)
	if cw == nil {
		t.Fatal(cw)
	}
	n, err := cw.Write([]byte("12345678901234567890"))
	if n != 20 {
		t.Fatal(n)
	}
	if err != nil {
		t.Fatal(err)
	}
	hash := crc32.NewIEEE()
	hash.Write([]byte("1234567890123456"))
	if !bytes.Equal(buf.Bytes(), []byte("1234567890123456"+string(hash.Sum(nil))+"7890")) {
		t.Fatalf("%#v", string(buf.Bytes()))
	}
	n, err = cw.Write([]byte("ghijklmnopqrstuvwxyz"))
	if n != 20 {
		t.Fatal(n)
	}
	if err != nil {
		t.Fatal(err)
	}
	hash2 := crc32.NewIEEE()
	hash2.Write([]byte("7890ghijklmnopqr"))
	if !bytes.Equal(buf.Bytes(), []byte("1234567890123456"+string(hash.Sum(nil))+"7890ghijklmnopqr"+string(hash2.Sum(nil))+"stuvwxyz")) {
		t.Fatalf("%#v", string(buf.Bytes()))
	}
	err = cw.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), []byte("1234567890123456"+string(hash.Sum(nil))+"7890ghijklmnopqr"+string(hash2.Sum(nil))+"stuvwxyz")) {
		t.Fatalf("%#v", string(buf.Bytes()))
	}
}

func TestMultiCoreChecksummedWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	cw := NewMultiCoreChecksummedWriter(buf, 16, crc32.NewIEEE, runtime.GOMAXPROCS(0))
	if cw == nil {
		t.Fatal(cw)
	}
	n, err := cw.Write([]byte("12345678901234567890"))
	if n != 20 {
		t.Fatal(n)
	}
	if err != nil {
		t.Fatal(err)
	}
	hash := crc32.NewIEEE()
	hash.Write([]byte("1234567890123456"))
	n, err = cw.Write([]byte("ghijklmnopqrstuvwxyz"))
	if n != 20 {
		t.Fatal(n)
	}
	if err != nil {
		t.Fatal(err)
	}
	hash2 := crc32.NewIEEE()
	hash2.Write([]byte("7890ghijklmnopqr"))
	err = cw.Close()
	if err != nil {
		t.Fatal(err)
	}
	hash3 := crc32.NewIEEE()
	hash3.Write([]byte("stuvwxyz"))
	if !bytes.Equal(buf.Bytes(), []byte("1234567890123456"+string(hash.Sum(nil))+"7890ghijklmnopqr"+string(hash2.Sum(nil))+"stuvwxyz")) {
		t.Fatalf("%#v", string(buf.Bytes()))
	}
}

func Benchmark16x7ChecksummedWriter________________(b *testing.B) {
	cw := NewChecksummedWriter(&NullIO{}, 16, crc32.NewIEEE)
	v := []byte{1, 2, 3, 4, 5, 6, 7}
	b.SetBytes(int64(len(v)))
	for i := 0; i < b.N; i++ {
		n, err := cw.Write(v)
		if n != len(v) {
			panic(n)
		}
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark16x7MultiCoreChecksummedWriter_______(b *testing.B) {
	cw := NewMultiCoreChecksummedWriter(&NullIO{}, 16, crc32.NewIEEE, runtime.GOMAXPROCS(0))
	v := []byte{1, 2, 3, 4, 5, 6, 7}
	b.SetBytes(int64(len(v)))
	for i := 0; i < b.N; i++ {
		n, err := cw.Write(v)
		if n != len(v) {
			panic(n)
		}
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark16x60001ChecksummedWriter____________(b *testing.B) {
	cw := NewChecksummedWriter(&NullIO{}, 16, crc32.NewIEEE)
	v := make([]byte, 60001)
	NewSeededScrambled(1).Read(v)
	b.SetBytes(int64(len(v)))
	for i := 0; i < b.N; i++ {
		n, err := cw.Write(v)
		if n != len(v) {
			panic(n)
		}
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark16x60001MultiCoreChecksummedWriter___(b *testing.B) {
	cw := NewMultiCoreChecksummedWriter(&NullIO{}, 16, crc32.NewIEEE, runtime.GOMAXPROCS(0))
	v := make([]byte, 60001)
	NewSeededScrambled(1).Read(v)
	b.SetBytes(int64(len(v)))
	for i := 0; i < b.N; i++ {
		n, err := cw.Write(v)
		if n != len(v) {
			panic(n)
		}
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark65532x7ChecksummedWriter_____________(b *testing.B) {
	cw := NewChecksummedWriter(&NullIO{}, 65532, crc32.NewIEEE)
	v := []byte{1, 2, 3, 4, 5, 6, 7}
	NewSeededScrambled(1).Read(v)
	b.SetBytes(int64(len(v)))
	for i := 0; i < b.N; i++ {
		n, err := cw.Write(v)
		if n != len(v) {
			panic(n)
		}
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark65532x7MultiCoreChecksummedWriter____(b *testing.B) {
	cw := NewMultiCoreChecksummedWriter(&NullIO{}, 65532, crc32.NewIEEE, runtime.GOMAXPROCS(0))
	v := []byte{1, 2, 3, 4, 5, 6, 7}
	NewSeededScrambled(1).Read(v)
	b.SetBytes(int64(len(v)))
	for i := 0; i < b.N; i++ {
		n, err := cw.Write(v)
		if n != len(v) {
			panic(n)
		}
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark65532x60001ChecksummedWriter_________(b *testing.B) {
	cw := NewChecksummedWriter(&NullIO{}, 65532, crc32.NewIEEE)
	v := make([]byte, 60001)
	NewSeededScrambled(1).Read(v)
	b.SetBytes(int64(len(v)))
	for i := 0; i < b.N; i++ {
		n, err := cw.Write(v)
		if n != len(v) {
			panic(n)
		}
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark65532x60001MultiCoreChecksummedWriter(b *testing.B) {
	cw := NewMultiCoreChecksummedWriter(&NullIO{}, 65532, crc32.NewIEEE, runtime.GOMAXPROCS(0))
	v := make([]byte, 60001)
	NewSeededScrambled(1).Read(v)
	b.SetBytes(int64(len(v)))
	for i := 0; i < b.N; i++ {
		n, err := cw.Write(v)
		if n != len(v) {
			panic(n)
		}
		if err != nil {
			panic(err)
		}
	}
}
