package main

import (
	"fmt"
	"log"
	"time"

	"github.com/getcfs/megacfs/formic"
	"github.com/gholt/store"
	"github.com/uber-go/zap"

	pb "github.com/getcfs/megacfs/formic/proto"
	"golang.org/x/net/context"
)

type UpdateItem struct {
	id        []byte
	block     uint64
	blocksize uint64
	size      uint64
	mtime     int64
}

type Updatinator struct {
	in chan *UpdateItem
	fs FileService
}

func newUpdatinator(in chan *UpdateItem, fs FileService) *Updatinator {
	return &Updatinator{
		in: in,
		fs: fs,
	}
}

func (u *Updatinator) run() {
	// TODO: Add fan-out based on the id of the update
	for {
		toupdate := <-u.in
		log.Println("Updating: ", toupdate)
		// TODO: Need better context
		ctx := context.Background()
		err := u.fs.Update(ctx, toupdate.id, toupdate.block, toupdate.blocksize, toupdate.size, toupdate.mtime)
		if err != nil {
			log.Println("Update failed, requeing: ", err)
			u.in <- toupdate
		}
	}
}

type DeleteItem struct {
	ts *pb.Tombstone
}

// TODO: Crawl the deleted folders to look for deletes to cleanup
// TODO: We should have sort of backoff in case of failures, so it isn't trying a delete over and over again if there are failures

type Deletinator struct {
	in    chan *DeleteItem
	fs    FileService
	comms *StoreComms
	log   zap.Logger
}

func newDeletinator(in chan *DeleteItem, fs FileService, comms *StoreComms, logger zap.Logger) *Deletinator {
	return &Deletinator{
		in:    in,
		fs:    fs,
		comms: comms,
		log:   logger,
	}
}

func (d *Deletinator) run() {
	// TODO: Parallelize this thing?
	for {
		todelete := <-d.in
		ts := todelete.ts
		d.log.Debug("Deleting", zap.Object("tombstone", ts))
		deleted := uint64(0)
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		for b := uint64(0); b < ts.Blocks; b++ {
			// Delete each block
			id := formic.GetID(ts.FsId, ts.Inode, b+1)
			err := d.fs.DeleteChunk(ctx, id, ts.Dtime)
			if err != nil && !store.IsNotFound(err) && err != ErrStoreHasNewerValue {
				continue
			}
			deleted++
		}
		if deleted == ts.Blocks {
			// Everything is deleted so delete the entry
			err := d.fs.DeleteChunk(ctx, formic.GetID(ts.FsId, ts.Inode, 0), ts.Dtime)
			if err != nil && !store.IsNotFound(err) && err != ErrStoreHasNewerValue {
				// Couldn't delete the inode entry so try again later
				d.in <- todelete
				continue
			}
		} else {
			// If all artifacts are not deleted requeue for later
			d.in <- todelete
		}
		// All artifacts are deleted so remove the delete tombstone
		d.log.Debug("Done Deleting", zap.Object("tombstone", ts))
		err := d.comms.DeleteGroupItem(ctx, formic.GetDeletedID(ts.FsId), []byte(fmt.Sprintf("%d", ts.Inode)))
		if err != nil && !store.IsNotFound(err) {
			// Failed to remove so queue again to retry later
			d.in <- todelete
		}
	}
}
