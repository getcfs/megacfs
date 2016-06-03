package store

import (
	"io"
	"os"

	"github.com/gholt/locmap"
)

func newTestGroupStore(c *GroupStoreConfig) (*defaultGroupStore, chan error) {
	if c == nil {
		c = newTestGroupStoreConfig()
	}
	s, err := NewGroupStore(c)
	ds := s.(*defaultGroupStore)
	return ds, err
}

func newTestGroupStoreConfig() *GroupStoreConfig {
	locmap := locmap.NewGroupLocMap(&locmap.GroupLocMapConfig{
		Roots:    1,
		PageSize: 1,
	})
	return &GroupStoreConfig{
		ValueCap:                  1024,
		Workers:                   2,
		ChecksumInterval:          1024,
		PageSize:                  1,
		WritePagesPerWorker:       1,
		GroupLocMap:               locmap,
		MsgCap:                    1,
		FileCap:                   1024 * 1024,
		FileReaders:               2,
		RecoveryBatchSize:         1024,
		TombstoneDiscardBatchSize: 1024,
		OutPullReplicationBloomN:  1000,

		OpenReadSeeker: func(fullPath string) (io.ReadSeeker, error) {
			return &memFile{buf: &memBuf{}}, nil
		},
		OpenWriteSeeker: func(fullPath string) (io.WriteSeeker, error) {
			return &memFile{buf: &memBuf{}}, nil
		},
		Readdirnames: func(fullPath string) ([]string, error) {
			return nil, nil
		},
		CreateWriteCloser: func(fullPath string) (io.WriteCloser, error) {
			return &memFile{buf: &memBuf{}}, nil
		},
		Stat: func(fullPath string) (os.FileInfo, error) {
			return &memFileInfo{}, nil
		},
		Remove: func(fullPath string) error {
			return nil
		},
		Rename: func(oldFullPath string, newFullPath string) error {
			return nil
		},
		IsNotExist: func(err error) bool {
			return false
		},
	}
}
