package server

import (
	"fmt"
	"log"
	"time"

	"github.com/getcfs/megacfs/formic/formicproto"
	"github.com/gholt/store"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type updateItem struct {
	id        []byte
	block     uint64
	blocksize uint64
	size      uint64
	mtime     int64
}

type updatinator struct {
	in chan *updateItem
	fs *oortFS
}

// newUpdatinator ...
func newUpdatinator(in chan *updateItem, fs *oortFS) *updatinator {
	return &updatinator{
		in: in,
		fs: fs,
	}
}

func (u *updatinator) Run() {
	// TODO: Add fan-out based on the id of the update
	for {
		toupdate := <-u.in
		log.Println("Updating: ", toupdate)
		// TODO: Need better context
		ctx := context.Background()
		err := u.fs.update(ctx, toupdate.id, toupdate.block, toupdate.blocksize, toupdate.size, toupdate.mtime)
		if err != nil {
			log.Println("Update failed, requeing: ", err)
			u.in <- toupdate
		}
	}
}

type dirtyItem struct {
	dirty *formicproto.Dirty
}

// TODO: Crawl the dirty folders to look for dirty objects to cleanup

type cleaninator struct {
	in    chan *dirtyItem
	fs    *oortFS
	comms *storeComms
	log   *zap.Logger
}

func newCleaninator(in chan *dirtyItem, fs *oortFS, comms *storeComms, logger *zap.Logger) *cleaninator {
	return &cleaninator{
		in:    in,
		fs:    fs,
		log:   logger,
		comms: comms,
	}
}

func (c *cleaninator) Run() {
	// TODO: Parallelize?
	for {
		toclean := <-c.in
		dirty := toclean.dirty
		c.log.Debug("Cleaning", zap.Any("item", dirty))
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		fails := 0
		for b := dirty.Blocks + 1; b > 0; b-- {
			// Try to delete the old block
			id := getID(dirty.FSID, dirty.Inode, b)
			err := c.fs.deleteChunk(ctx, id, dirty.Dtime)
			if err == errStoreHasNewerValue {
				// Something has already been writte, so we are good
				break
			} else if store.IsNotFound(err) {
				continue
			} else if err != nil {
				fails++
			}
		}
		if fails > 0 {
			// Not everything could be cleaned, so queue to try again later
			c.in <- toclean
		} else {
			// All orphaned data is deleted so remove the tombstone
			c.log.Debug("Done Cleaning", zap.Any("item", dirty))
			err := c.comms.DeleteGroupItem(ctx, getDirtyID(dirty.FSID), []byte(fmt.Sprintf("%d", dirty.Inode)))
			if err != nil && !store.IsNotFound(err) {
				// Failed to remove so queue again to retry later
				c.in <- toclean
			}
		}
	}
}

type deleteItem struct {
	ts *formicproto.Tombstone
}

// TODO: Crawl the deleted folders to look for deletes to cleanup
// TODO: We should have sort of backoff in case of failures, so it isn't trying a delete over and over again if there are failures

type deletinator struct {
	in    chan *deleteItem
	fs    *oortFS
	comms *storeComms
	log   *zap.Logger
}

func newDeletinator(in chan *deleteItem, fs *oortFS, comms *storeComms, logger *zap.Logger) *deletinator {
	return &deletinator{
		in:    in,
		fs:    fs,
		comms: comms,
		log:   logger,
	}
}

func (d *deletinator) Run() {
	// TODO: Parallelize this thing?
	for {
		todelete := <-d.in
		ts := todelete.ts
		d.log.Debug("Deleting", zap.Any("tombstone", ts))
		deleted := uint64(0)
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		for b := uint64(0); b < ts.Blocks; b++ {
			// Delete each block
			id := getID(ts.FSID, ts.Inode, b+1)
			err := d.fs.deleteChunk(ctx, id, ts.Dtime)
			if err != nil && !store.IsNotFound(err) && err != errStoreHasNewerValue {
				continue
			}
			deleted++
		}
		if deleted == ts.Blocks {
			// Everything is deleted so delete the entry
			err := d.fs.deleteChunk(ctx, getID(ts.FSID, ts.Inode, 0), ts.Dtime)
			if err != nil && !store.IsNotFound(err) && err != errStoreHasNewerValue {
				// Couldn't delete the inode entry so try again later
				d.in <- todelete
				continue
			}
		} else {
			// If all artifacts are not deleted requeue for later
			d.in <- todelete
		}
		// All artifacts are deleted so remove the delete tombstone
		d.log.Debug("Done Deleting", zap.Any("tombstone", ts))
		err := d.comms.DeleteGroupItem(ctx, getDeletedID(ts.FSID), []byte(fmt.Sprintf("%d", ts.Inode)))
		if err != nil && !store.IsNotFound(err) {
			// Failed to remove so queue again to retry later
			d.in <- todelete
		}
	}
}
