package store

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/gholt/ring"
)

type memBuf struct {
	buf []byte
}

type memFile struct {
	buf *memBuf
	pos int64
}

func (f *memFile) Read(p []byte) (int, error) {
	n := copy(p, f.buf.buf[f.pos:])
	if n == 0 {
		return 0, io.EOF
	}
	f.pos += int64(n)
	return n, nil
}

func (f *memFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		f.pos = offset
	case 1:
		f.pos += offset
	case 2:
		f.pos = int64(len(f.buf.buf)) + offset
	}
	return f.pos, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	pl := int64(len(p))
	if int64(len(f.buf.buf))-f.pos < pl {
		buf := make([]byte, int64(f.pos+pl))
		copy(buf, f.buf.buf)
		copy(buf[f.pos:], p)
		f.buf.buf = buf
		f.pos += pl
		return int(pl), nil
	}
	copy(f.buf.buf[f.pos:], p)
	f.pos += pl
	return int(pl), nil
}

func (f *memFile) Close() error {
	return nil
}

type msgRingPlaceholder struct {
	ring            ring.Ring
	lock            sync.Mutex
	msgToNodeIDs    []uint64
	msgToPartitions []uint32
}

func (m *msgRingPlaceholder) Ring() ring.Ring {
	return m.ring
}

func (m *msgRingPlaceholder) MaxMsgLength() uint64 {
	return 65536
}

func (m *msgRingPlaceholder) SetMsgHandler(msgType uint64, handler ring.MsgUnmarshaller) {
}

func (m *msgRingPlaceholder) MsgToNode(msg ring.Msg, nodeID uint64, timeout time.Duration) {
	m.lock.Lock()
	m.msgToNodeIDs = append(m.msgToNodeIDs, nodeID)
	m.lock.Unlock()
	msg.Free(0, 0)
}

func (m *msgRingPlaceholder) MsgToOtherReplicas(msg ring.Msg, partition uint32, timeout time.Duration) {
	m.lock.Lock()
	m.msgToPartitions = append(m.msgToPartitions, partition)
	m.lock.Unlock()
	msg.Free(0, 0)
}

type testErrorWriter struct {
	goodBytes int
}

func (w *testErrorWriter) Write(p []byte) (int, error) {
	if w.goodBytes >= len(p) {
		w.goodBytes -= len(p)
		return len(p), nil
	}
	if w.goodBytes > 0 {
		n := w.goodBytes
		w.goodBytes = 0
		return n, io.EOF
	}
	return 0, io.EOF
}

type memFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
	sys     interface{}
}

func (m *memFileInfo) Name() string {
	return m.name
}

func (m *memFileInfo) Size() int64 {
	return m.size
}

func (m *memFileInfo) Mode() os.FileMode {
	return m.mode
}

func (m *memFileInfo) ModTime() time.Time {
	return m.modTime
}

func (m *memFileInfo) IsDir() bool {
	return m.isDir
}

func (m *memFileInfo) Sys() interface{} {
	return m.sys
}
