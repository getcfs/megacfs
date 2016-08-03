package brimio

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"sync"
)

// ChecksummedReader reads content written by ChecksummedWriter, verifying
// checksums when requested. Implements the io.ReadSeeker and io.Closer
// interfaces.
//
// Any errors from Read or Verify should make no assumptions about any
// resulting position and should Seek before continuing to use the
// ChecksummedReader.
type ChecksummedReader interface {
	// Read implements the io.Reader interface.
	//
	// Any error should make no assumption about any resulting position and
	// should Seek before continuing to use the ChecksummedReader.
	Read(v []byte) (n int, err error)
	// Seek implements the io.Seeker interface.
	Seek(offset int64, whence int) (n int64, err error)
	// Verify verifies the checksum for the section of the content containing
	// the current read position.
	//
	// If there is an error, whether the section is checksum valid is
	// indeterminate by this routine and the caller should decide what to do
	// based on the error. For example, if the underlying i/o causes a timeout
	// error, the content may be fine and just temporarily unreachable.
	//
	// Any error should also make no assumption about any resulting position
	// and should Seek before continuing to use the ChecksummedReader.
	//
	// With no error, the bool indicates whether the content is checksum valid
	// and the position within the ChecksummedReader will not have changed.
	Verify() (bool, error)
	// Close implements the io.Closer interface.
	Close() error
}

// NewChecksummedReader returns a ChecksummedReader that delegates requests to
// an underlying io.ReadSeeker expecting checksums of the content at given
// intervals using the hashing function given.
func NewChecksummedReader(delegate io.ReadSeeker, interval int, newHash func() hash.Hash32) ChecksummedReader {
	return newChecksummedReaderImpl(delegate, interval, newHash)
}

// ChecksummedWriter writes content with additional checksums embedded in the
// underlying content.
//
// Implements the io.WriteCloser interface.
//
// Note that this generally only works for brand new writers starting at offset
// 0. Appending to existing files or starting at offsets other than 0 requires
// special care when working with ChecksummedReader later and is beyond the
// basic usage described here.
//
// Also, note that the trailing bytes may not be covered by a checksum unless
// it happens to just fall on a checksum interval.
type ChecksummedWriter interface {
	// Write implements the io.Writer interface.
	Write(v []byte) (n int, err error)
	// Close implements the io.Closer interface.
	Close() error
}

// NewChecksummedWriter returns a ChecksummedWriter that delegates requests to
// an underlying io.Writer and embeds checksums of the content at given
// intervals using the hashing function given.
func NewChecksummedWriter(delegate io.Writer, checksumInterval int, newHash func() hash.Hash32) ChecksummedWriter {
	return newChecksummedWriterImpl(delegate, checksumInterval, newHash)
}

type checksummedReaderImpl struct {
	delegate         io.ReadSeeker
	checksumInterval int
	checksumOffset   int
	newHash          func() hash.Hash32
	checksum         []byte
}

func newChecksummedReaderImpl(delegate io.ReadSeeker, interval int, newHash func() hash.Hash32) ChecksummedReader {
	return &checksummedReaderImpl{
		delegate:         delegate,
		checksumInterval: interval,
		newHash:          newHash,
		checksum:         make([]byte, 4),
	}
}

func (cri *checksummedReaderImpl) Read(v []byte) (int, error) {
	if cri.checksumOffset+len(v) > cri.checksumInterval {
		v = v[:cri.checksumInterval-cri.checksumOffset]
	}
	n, err := cri.delegate.Read(v)
	cri.checksumOffset += n
	if err == nil {
		if cri.checksumOffset == cri.checksumInterval {
			io.ReadFull(cri.delegate, cri.checksum)
			cri.checksumOffset = 0
		}
	}
	return n, err
}

func (cri *checksummedReaderImpl) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
	case 1:
		o, err := cri.delegate.Seek(0, 1)
		cri.checksumOffset = int(o % (int64(cri.checksumInterval) + 4))
		if err != nil {
			return o - (o / (int64(cri.checksumInterval) + 4) * 4), err
		}
		offset = o - (o / (int64(cri.checksumInterval) + 4) * 4) + offset
	case 2:
		o, err := cri.delegate.Seek(0, 2)
		cri.checksumOffset = int(o % (int64(cri.checksumInterval) + 4))
		if err != nil {
			return o - (o / (int64(cri.checksumInterval) + 4) * 4), err
		}
		offset = o - (o / (int64(cri.checksumInterval) + 4) * 4) + offset
	default:
		o, _ := cri.delegate.Seek(0, 1)
		return o, fmt.Errorf("invalid whence %d", whence)
	}
	o, err := cri.delegate.Seek(offset+(offset/int64(cri.checksumInterval)*4), 0)
	cri.checksumOffset = int(o % (int64(cri.checksumInterval) + 4))
	return o - (o / (int64(cri.checksumInterval) + 4) * 4), err
}

func (cri *checksummedReaderImpl) Verify() (bool, error) {
	originalOffset, err := cri.delegate.Seek(0, 1)
	if err != nil {
		return false, err
	}
	if cri.checksumOffset > 0 {
		_, err = cri.delegate.Seek(-int64(cri.checksumOffset), 1)
		if err != nil {
			return false, err
		}
	}
	block := make([]byte, cri.checksumInterval+4)
	checksum := block[cri.checksumInterval:]
	_, err = io.ReadFull(cri.delegate, block)
	if err != nil {
		return false, err
	}
	block = block[:cri.checksumInterval]
	hash := cri.newHash()
	hash.Write(block)
	verified := bytes.Equal(checksum, hash.Sum(cri.checksum[:0]))
	_, err = cri.delegate.Seek(originalOffset, 0)
	if err != nil {
		return verified, err
	}
	return verified, nil
}

func (cri *checksummedReaderImpl) Close() error {
	var err error
	if c, ok := cri.delegate.(io.Closer); ok {
		err = c.Close()
	}
	cri.delegate = errDelegate
	return err
}

type checksummedWriterImpl struct {
	delegate         io.Writer
	checksumInterval int
	checksumOffset   int
	newHash          func() hash.Hash32
	hash             hash.Hash32
	checksum         []byte
}

func newChecksummedWriterImpl(delegate io.Writer, checksumInterval int, newHash func() hash.Hash32) *checksummedWriterImpl {
	return &checksummedWriterImpl{
		delegate:         delegate,
		checksumInterval: checksumInterval,
		newHash:          newHash,
		hash:             newHash(),
		checksum:         make([]byte, 4),
	}
}

func (cwi *checksummedWriterImpl) Write(v []byte) (int, error) {
	var n int
	var n2 int
	var err error
	for cwi.checksumOffset+len(v) >= cwi.checksumInterval {
		n2, err = cwi.delegate.Write(v[:cwi.checksumInterval-cwi.checksumOffset])
		n += n2
		if err != nil {
			cwi.delegate = errDelegate
			return n, err
		}
		cwi.hash.Write(v[:cwi.checksumInterval-cwi.checksumOffset])
		v = v[cwi.checksumInterval-cwi.checksumOffset:]
		binary.BigEndian.PutUint32(cwi.checksum, cwi.hash.Sum32())
		_, err = cwi.delegate.Write(cwi.checksum)
		if err != nil {
			cwi.delegate = errDelegate
			return n, err
		}
		cwi.hash = cwi.newHash()
		cwi.checksumOffset = 0
	}
	if len(v) > 0 {
		n2, err = cwi.delegate.Write(v)
		n += n2
		if err != nil {
			cwi.delegate = errDelegate
			return n, err
		}
		cwi.hash.Write(v)
		cwi.checksumOffset += n2
	}
	return n, err
}

func (cwi *checksummedWriterImpl) Close() error {
	var err error
	if c, ok := cwi.delegate.(io.Closer); ok {
		err = c.Close()
	}
	cwi.delegate = errDelegate
	return err
}

type multiCoreChecksummedWriter struct {
	delegate         io.Writer
	checksumInterval int
	cores            int
	newHash          func() hash.Hash32
	hash             hash.Hash32
	buffer           *multiCoreChecksummedWriterBuffer
	freeChan         chan *multiCoreChecksummedWriterBuffer
	checksumChan     chan *multiCoreChecksummedWriterBuffer
	writeChan        chan *multiCoreChecksummedWriterBuffer
	doneChan         chan struct{}
	lock             sync.Mutex
	err              error
	closed           bool
}

type multiCoreChecksummedWriterBuffer struct {
	seq int64
	buf []byte
}

// NewMultiCoreChecksummedWriter returns a ChecksummedWriter that delegates
// requests to an underlying io.Writer and embeds checksums of the content at
// given intervals using the hashing function given; it will use multiple cores
// for computing the checksums.
//
// Note that this is generally only faster for large files and reasonably sized
// checksum intervals (e.g. 65532). It can be quite a bit slower on single core
// systems or when using tiny checksum intervals.
func NewMultiCoreChecksummedWriter(delegate io.Writer, checksumInterval int, newHash func() hash.Hash32, cores int) ChecksummedWriter {
	cwi := &multiCoreChecksummedWriter{
		delegate:         delegate,
		checksumInterval: checksumInterval,
		newHash:          newHash,
		cores:            cores,
		freeChan:         make(chan *multiCoreChecksummedWriterBuffer, cores+1),
		checksumChan:     make(chan *multiCoreChecksummedWriterBuffer, cores+1),
		writeChan:        make(chan *multiCoreChecksummedWriterBuffer, cores+1),
		doneChan:         make(chan struct{}),
	}
	for i := 0; i < cores; i++ {
		cwi.freeChan <- &multiCoreChecksummedWriterBuffer{0, make([]byte, 0, checksumInterval+4)}
	}
	cwi.buffer = <-cwi.freeChan
	go cwi.writer()
	for i := 0; i < cores; i++ {
		go cwi.checksummer()
	}
	return cwi
}

func (cwi *multiCoreChecksummedWriter) Write(v []byte) (int, error) {
	var n int
	for len(cwi.buffer.buf)+len(v) >= cwi.checksumInterval {
		n2 := cwi.checksumInterval - len(cwi.buffer.buf)
		cwi.buffer.buf = append(cwi.buffer.buf, v[:n2]...)
		s := cwi.buffer.seq + 1
		cwi.checksumChan <- cwi.buffer
		n += n2
		v = v[n2:]
		cwi.buffer = <-cwi.freeChan
		cwi.buffer.seq = s
	}
	if len(v) > 0 {
		cwi.buffer.buf = append(cwi.buffer.buf, v...)
		n += len(v)
	}
	cwi.lock.Lock()
	err := cwi.err
	cwi.lock.Unlock()
	return n, err
}

func (cwi *multiCoreChecksummedWriter) Close() error {
	cwi.lock.Lock()
	if cwi.closed {
		err := cwi.err
		cwi.lock.Unlock()
		return err
	}
	cwi.closed = true
	cwi.lock.Unlock()
	if len(cwi.buffer.buf) > 0 {
		cwi.checksumChan <- cwi.buffer
	}
	close(cwi.checksumChan)
	for i := 0; i < cwi.cores; i++ {
		<-cwi.doneChan
	}
	cwi.writeChan <- nil
	<-cwi.doneChan
	cwi.lock.Lock()
	err := cwi.err
	cwi.lock.Unlock()
	return err
}

func (cwi *multiCoreChecksummedWriter) checksummer() {
	for {
		b := <-cwi.checksumChan
		if b == nil {
			break
		}
		if len(b.buf) >= cwi.checksumInterval {
			h := cwi.newHash()
			h.Write(b.buf)
			b.buf = b.buf[:len(b.buf)+4]
			binary.BigEndian.PutUint32(b.buf[len(b.buf)-4:], h.Sum32())
		}
		cwi.writeChan <- b
	}
	cwi.doneChan <- struct{}{}
}

func (cwi *multiCoreChecksummedWriter) writer() {
	var seq int64
	var lastWasNil bool
	for {
		b := <-cwi.writeChan
		if b == nil {
			if lastWasNil {
				break
			}
			lastWasNil = true
			cwi.writeChan <- nil
			continue
		}
		lastWasNil = false
		if b.seq != seq {
			cwi.writeChan <- b
			continue
		}
		_, err := cwi.delegate.Write(b.buf)
		cwi.lock.Lock()
		cwi.err = err
		cwi.lock.Unlock()
		b.buf = b.buf[:0]
		cwi.freeChan <- b
		seq++
	}
	if c, ok := cwi.delegate.(io.Closer); ok {
		err := c.Close()
		cwi.lock.Lock()
		cwi.err = err
		cwi.lock.Unlock()
	}
	cwi.doneChan <- struct{}{}
}

type errDelegateStruct struct {
}

var errDelegate = &errDelegateStruct{}

func (ed *errDelegateStruct) Read(v []byte) (int, error) {
	return 0, fmt.Errorf("closed")
}

func (ed *errDelegateStruct) Write(v []byte) (int, error) {
	return 0, fmt.Errorf("closed")
}

func (ed *errDelegateStruct) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("closed")
}

func (ed *errDelegateStruct) Close() error {
	return fmt.Errorf("already closed")
}
