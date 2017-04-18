package store

import (
	"io"
	"os"

	"github.com/gholt/locmap"
)

func newTestValueStore(c *ValueStoreConfig) (*defaultValueStore, chan error) {
	if c == nil {
		c = newTestValueStoreConfig()
	}
	s, err := NewValueStore(c)
	ds := s.(*defaultValueStore)
	return ds, err
}

func newTestValueStoreConfig() *ValueStoreConfig {
	locmap := locmap.NewValueLocMap(&locmap.ValueLocMapConfig{
		Roots:    1,
		PageSize: 1,
	})
	return &ValueStoreConfig{
		ValueCap:                  1024,
		Workers:                   2,
		ChecksumInterval:          1024,
		PageSize:                  1,
		WritePagesPerWorker:       1,
		ValueLocMap:               locmap,
		MsgCap:                    1,
		FileCap:                   1024 * 1024,
		FileReaders:               2,
		RecoveryBatchSize:         1024,
		TombstoneDiscardBatchSize: 1024,
		OutPullReplicationBloomN:  1000,

		openReadSeeker: func(fullPath string) (io.ReadSeeker, error) {
			return &memFile{buf: &memBuf{}}, nil
		},
		openWriteSeeker: func(fullPath string) (io.WriteSeeker, error) {
			return &memFile{buf: &memBuf{}}, nil
		},
		readdirnames: func(fullPath string) ([]string, error) {
			return nil, nil
		},
		createWriteCloser: func(fullPath string) (io.WriteCloser, error) {
			return &memFile{buf: &memBuf{}}, nil
		},
		stat: func(fullPath string) (os.FileInfo, error) {
			return &memFileInfo{}, nil
		},
		remove: func(fullPath string) error {
			return nil
		},
		rename: func(oldFullPath string, newFullPath string) error {
			return nil
		},
		isNotExist: func(err error) bool {
			return false
		},
	}
}
