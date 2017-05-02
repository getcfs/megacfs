package oort

import (
	"errors"
	"fmt"
	"sync"

	"github.com/getcfs/megacfs/ftls"
	pb "github.com/getcfs/megacfs/oort/groupproto"
	"github.com/getcfs/megacfs/oort/proto"
	"github.com/gholt/store"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type groupStore struct {
	lock             sync.Mutex
	logger           *zap.Logger
	addr             string
	ftlsc            *ftls.Config
	opts             []grpc.DialOption
	conn             *grpc.ClientConn
	client           pb.GroupStoreClient
	handlersDoneChan chan struct{}

	pendingDeleteReqChan chan *asyncGroupDeleteRequest
	freeDeleteReqChan    chan *asyncGroupDeleteRequest
	freeDeleteResChan    chan *asyncGroupDeleteResponse

	pendingLookupGroupReqChan chan *asyncGroupLookupGroupRequest
	freeLookupGroupReqChan    chan *asyncGroupLookupGroupRequest
	freeLookupGroupResChan    chan *asyncGroupLookupGroupResponse

	pendingLookupReqChan chan *asyncGroupLookupRequest
	freeLookupReqChan    chan *asyncGroupLookupRequest
	freeLookupResChan    chan *asyncGroupLookupResponse

	pendingReadGroupReqChan chan *asyncGroupReadGroupRequest
	freeReadGroupReqChan    chan *asyncGroupReadGroupRequest
	freeReadGroupResChan    chan *asyncGroupReadGroupResponse

	pendingReadReqChan chan *asyncGroupReadRequest
	freeReadReqChan    chan *asyncGroupReadRequest
	freeReadResChan    chan *asyncGroupReadResponse

	pendingWriteReqChan chan *asyncGroupWriteRequest
	freeWriteReqChan    chan *asyncGroupWriteRequest
	freeWriteResChan    chan *asyncGroupWriteResponse
}

// NewGroupStore creates a GroupStore connection via grpc to the given
// address.
func newGroupStore(logger *zap.Logger, addr string, concurrency int, ftlsConfig *ftls.Config, opts ...grpc.DialOption) store.GroupStore {
	stor := &groupStore{
		logger:           logger,
		addr:             addr,
		ftlsc:            ftlsConfig,
		opts:             opts,
		handlersDoneChan: make(chan struct{}),
	}

	stor.pendingDeleteReqChan = make(chan *asyncGroupDeleteRequest, concurrency)
	stor.freeDeleteReqChan = make(chan *asyncGroupDeleteRequest, concurrency)
	stor.freeDeleteResChan = make(chan *asyncGroupDeleteResponse, concurrency)
	for i := 0; i < cap(stor.freeDeleteReqChan); i++ {
		stor.freeDeleteReqChan <- &asyncGroupDeleteRequest{resChan: make(chan *asyncGroupDeleteResponse, 1)}
	}
	for i := 0; i < cap(stor.freeDeleteResChan); i++ {
		stor.freeDeleteResChan <- &asyncGroupDeleteResponse{}
	}
	go stor.handleDelete()

	stor.pendingLookupGroupReqChan = make(chan *asyncGroupLookupGroupRequest, concurrency)
	stor.freeLookupGroupReqChan = make(chan *asyncGroupLookupGroupRequest, concurrency)
	stor.freeLookupGroupResChan = make(chan *asyncGroupLookupGroupResponse, concurrency)
	for i := 0; i < cap(stor.freeLookupGroupReqChan); i++ {
		stor.freeLookupGroupReqChan <- &asyncGroupLookupGroupRequest{resChan: make(chan *asyncGroupLookupGroupResponse, 1)}
	}
	for i := 0; i < cap(stor.freeLookupGroupResChan); i++ {
		stor.freeLookupGroupResChan <- &asyncGroupLookupGroupResponse{}
	}
	go stor.handleLookupGroup()

	stor.pendingLookupReqChan = make(chan *asyncGroupLookupRequest, concurrency)
	stor.freeLookupReqChan = make(chan *asyncGroupLookupRequest, concurrency)
	stor.freeLookupResChan = make(chan *asyncGroupLookupResponse, concurrency)
	for i := 0; i < cap(stor.freeLookupReqChan); i++ {
		stor.freeLookupReqChan <- &asyncGroupLookupRequest{resChan: make(chan *asyncGroupLookupResponse, 1)}
	}
	for i := 0; i < cap(stor.freeLookupResChan); i++ {
		stor.freeLookupResChan <- &asyncGroupLookupResponse{}
	}
	go stor.handleLookup()

	stor.pendingReadGroupReqChan = make(chan *asyncGroupReadGroupRequest, concurrency)
	stor.freeReadGroupReqChan = make(chan *asyncGroupReadGroupRequest, concurrency)
	stor.freeReadGroupResChan = make(chan *asyncGroupReadGroupResponse, concurrency)
	for i := 0; i < cap(stor.freeReadGroupReqChan); i++ {
		stor.freeReadGroupReqChan <- &asyncGroupReadGroupRequest{resChan: make(chan *asyncGroupReadGroupResponse, 1)}
	}
	for i := 0; i < cap(stor.freeReadGroupResChan); i++ {
		stor.freeReadGroupResChan <- &asyncGroupReadGroupResponse{}
	}
	go stor.handleReadGroup()

	stor.pendingReadReqChan = make(chan *asyncGroupReadRequest, concurrency)
	stor.freeReadReqChan = make(chan *asyncGroupReadRequest, concurrency)
	stor.freeReadResChan = make(chan *asyncGroupReadResponse, concurrency)
	for i := 0; i < cap(stor.freeReadReqChan); i++ {
		stor.freeReadReqChan <- &asyncGroupReadRequest{resChan: make(chan *asyncGroupReadResponse, 1)}
	}
	for i := 0; i < cap(stor.freeReadResChan); i++ {
		stor.freeReadResChan <- &asyncGroupReadResponse{}
	}
	go stor.handleRead()

	stor.pendingWriteReqChan = make(chan *asyncGroupWriteRequest, concurrency)
	stor.freeWriteReqChan = make(chan *asyncGroupWriteRequest, concurrency)
	stor.freeWriteResChan = make(chan *asyncGroupWriteResponse, concurrency)
	for i := 0; i < cap(stor.freeWriteReqChan); i++ {
		stor.freeWriteReqChan <- &asyncGroupWriteRequest{resChan: make(chan *asyncGroupWriteResponse, 1)}
	}
	for i := 0; i < cap(stor.freeWriteResChan); i++ {
		stor.freeWriteResChan <- &asyncGroupWriteResponse{}
	}
	go stor.handleWrite()

	return stor
}

func (stor *groupStore) Startup(ctx context.Context) error {
	stor.lock.Lock()
	err := stor.startup()
	stor.lock.Unlock()
	return err
}

func (stor *groupStore) startup() error {
	if stor.conn != nil {
		return nil
	}
	var err error
	creds, err := ftls.NewGRPCClientDialOpt(stor.ftlsc)
	if err != nil {
		stor.conn = nil
		return err
	}
	opts := make([]grpc.DialOption, len(stor.opts))
	copy(opts, stor.opts)
	opts = append(opts, creds)
	stor.conn, err = grpc.Dial(stor.addr, opts...)
	if err != nil {
		stor.conn = nil
		return err
	}
	stor.client = pb.NewGroupStoreClient(stor.conn)
	return nil
}

// Shutdown will close any existing connections; note that Startup may
// automatically get called with any further activity, but it will use a new
// connection. To ensure the groupStore has no further activity, use Close.
func (stor *groupStore) Shutdown(ctx context.Context) error {
	stor.lock.Lock()
	err := stor.shutdown()
	stor.lock.Unlock()
	return err
}

func (stor *groupStore) shutdown() error {
	if stor.conn == nil {
		return nil
	}
	stor.conn.Close()
	stor.conn = nil
	stor.client = nil
	return nil
}

// Close will shutdown outgoing connectivity and stop all background
// goroutines; note that the groupStore is no longer usable after a call to
// Close, including using Startup.
func (stor *groupStore) Close() {
	stor.lock.Lock()
	stor.shutdown()
	close(stor.handlersDoneChan)
	stor.lock.Unlock()
}

func (stor *groupStore) EnableWrites(ctx context.Context) error {
	return nil
}

func (stor *groupStore) DisableWrites(ctx context.Context) error {
	// TODO: I suppose we could implement toggling writes from this client;
	// I'll leave that for later.
	return errors.New("cannot disable writes with this client at this time")
}

func (stor *groupStore) Flush(ctx context.Context) error {
	// Nothing cached on this end, so nothing to flush.
	return nil
}

func (stor *groupStore) AuditPass(ctx context.Context) error {
	return errors.New("audit passes not available with this client at this time")
}

func (stor *groupStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	return noStats, nil
}

func (stor *groupStore) ValueCap(ctx context.Context) (uint32, error) {
	// TODO: This should be a (cached) value from the server. Servers don't
	// change their value caps on the fly, so the cache can be kept until
	// disconnect.
	return 0xffffffff, nil
}

type asyncGroupDeleteRequest struct {
	req          pb.DeleteRequest
	resChan      chan *asyncGroupDeleteResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncGroupDeleteResponse struct {
	res *pb.DeleteResponse
	err error
}

func (stor *groupStore) handleDelete() {
	resChan := make(chan *asyncGroupDeleteResponse, cap(stor.freeDeleteReqChan))
	resFunc := func(stream pb.GroupStore_DeleteClient) {
		var err error
		var res *asyncGroupDeleteResponse
		for {
			select {
			case res = <-stor.freeDeleteResChan:
			case <-stor.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-stor.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.GroupStore_DeleteClient
	waitingMax := uint32(cap(stor.freeDeleteReqChan)) - 1
	waiting := make([]*asyncGroupDeleteRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-stor.pendingDeleteReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingDeleteReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				stor.lock.Lock()
				if stor.client == nil {
					if err = stor.startup(); err != nil {
						stor.lock.Unlock()
						res := <-stor.freeDeleteResChan
						res.err = err
						res.res = &pb.DeleteResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = stor.client.Delete(context.Background())
				stor.lock.Unlock()
				if err != nil {
					res := <-stor.freeDeleteResChan
					res.err = err
					res.res = &pb.DeleteResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-stor.freeDeleteResChan
				res.err = err
				res.res = &pb.DeleteResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncGroupDeleteRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				stor.freeDeleteResChan <- res
				go func(reqs []*asyncGroupDeleteRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-stor.freeDeleteResChan
						res.err = err
						res.res = &pb.DeleteResponse{RPCID: req.req.RPCID}
						resChan <- res
					}
				}(wereWaiting, err)
				break
			}
			if res.res.RPCID < 0 || res.res.RPCID > waitingMax {
				// TODO: Debug log error?
				break
			}
			req := waiting[res.res.RPCID]
			if req == nil {
				// TODO: Debug log error?
				break
			}
			waiting[res.res.RPCID] = nil
			req.canceledLock.Lock()
			if !req.canceled {
				req.resChan <- res
			} else {
				stor.freeDeleteReqChan <- req
				stor.freeDeleteResChan <- res
			}
			req.canceledLock.Unlock()
		case <-stor.handlersDoneChan:
			return
		}
	}
}

func (stor *groupStore) Delete(ctx context.Context, keyA, keyB uint64, childKeyA, childKeyB uint64, timestampMicro int64) (oldTimestampMicro int64, err error) {

	var req *asyncGroupDeleteRequest
	select {
	case req = <-stor.freeDeleteReqChan:
	case <-ctx.Done():

		return 0, ctx.Err()

	}
	req.canceled = false

	req.req.KeyA = keyA
	req.req.KeyB = keyB

	req.req.ChildKeyA = childKeyA
	req.req.ChildKeyB = childKeyB

	req.req.TimestampMicro = timestampMicro

	select {
	case stor.pendingDeleteReqChan <- req:
	case <-ctx.Done():
		stor.freeDeleteReqChan <- req

		return 0, ctx.Err()

	}
	var res *asyncGroupDeleteResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			stor.freeDeleteResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return 0, ctx.Err()

	}
	stor.freeDeleteReqChan <- req
	if res.err != nil {
		err = res.err
		stor.freeDeleteResChan <- res

		return 0, err

	}

	oldTimestampMicro = res.res.TimestampMicro

	if res.res.Err == "" {
		err = nil
	} else {
		err = proto.TranslateErrorString(res.res.Err)
	}
	stor.freeDeleteResChan <- res

	return oldTimestampMicro, err

}

type asyncGroupLookupGroupRequest struct {
	req          pb.LookupGroupRequest
	resChan      chan *asyncGroupLookupGroupResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncGroupLookupGroupResponse struct {
	res *pb.LookupGroupResponse
	err error
}

func (stor *groupStore) handleLookupGroup() {
	resChan := make(chan *asyncGroupLookupGroupResponse, cap(stor.freeLookupGroupReqChan))
	resFunc := func(stream pb.GroupStore_LookupGroupClient) {
		var err error
		var res *asyncGroupLookupGroupResponse
		for {
			select {
			case res = <-stor.freeLookupGroupResChan:
			case <-stor.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-stor.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.GroupStore_LookupGroupClient
	waitingMax := uint32(cap(stor.freeLookupGroupReqChan)) - 1
	waiting := make([]*asyncGroupLookupGroupRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-stor.pendingLookupGroupReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingLookupGroupReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				stor.lock.Lock()
				if stor.client == nil {
					if err = stor.startup(); err != nil {
						stor.lock.Unlock()
						res := <-stor.freeLookupGroupResChan
						res.err = err
						res.res = &pb.LookupGroupResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = stor.client.LookupGroup(context.Background())
				stor.lock.Unlock()
				if err != nil {
					res := <-stor.freeLookupGroupResChan
					res.err = err
					res.res = &pb.LookupGroupResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-stor.freeLookupGroupResChan
				res.err = err
				res.res = &pb.LookupGroupResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncGroupLookupGroupRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				stor.freeLookupGroupResChan <- res
				go func(reqs []*asyncGroupLookupGroupRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-stor.freeLookupGroupResChan
						res.err = err
						res.res = &pb.LookupGroupResponse{RPCID: req.req.RPCID}
						resChan <- res
					}
				}(wereWaiting, err)
				break
			}
			if res.res.RPCID < 0 || res.res.RPCID > waitingMax {
				// TODO: Debug log error?
				break
			}
			req := waiting[res.res.RPCID]
			if req == nil {
				// TODO: Debug log error?
				break
			}
			waiting[res.res.RPCID] = nil
			req.canceledLock.Lock()
			if !req.canceled {
				req.resChan <- res
			} else {
				stor.freeLookupGroupReqChan <- req
				stor.freeLookupGroupResChan <- res
			}
			req.canceledLock.Unlock()
		case <-stor.handlersDoneChan:
			return
		}
	}
}

func (stor *groupStore) LookupGroup(ctx context.Context, parentKeyA, parentKeyB uint64) (items []store.LookupGroupItem, err error) {

	var req *asyncGroupLookupGroupRequest
	select {
	case req = <-stor.freeLookupGroupReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.KeyA = parentKeyA
	req.req.KeyB = parentKeyB

	select {
	case stor.pendingLookupGroupReqChan <- req:
	case <-ctx.Done():
		stor.freeLookupGroupReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncGroupLookupGroupResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			stor.freeLookupGroupResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	stor.freeLookupGroupReqChan <- req
	if res.err != nil {
		err = res.err
		stor.freeLookupGroupResChan <- res

		return nil, err

	}

	items = make([]store.LookupGroupItem, len(res.res.Items))
	for i, v := range res.res.Items {
		items[i].ChildKeyA = v.ChildKeyA
		items[i].ChildKeyB = v.ChildKeyB
		items[i].TimestampMicro = v.TimestampMicro
		items[i].Length = v.Length
	}

	if res.res.Err == "" {
		err = nil
	} else {
		err = proto.TranslateErrorString(res.res.Err)
	}
	stor.freeLookupGroupResChan <- res

	return items, err

}

type asyncGroupLookupRequest struct {
	req          pb.LookupRequest
	resChan      chan *asyncGroupLookupResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncGroupLookupResponse struct {
	res *pb.LookupResponse
	err error
}

func (stor *groupStore) handleLookup() {
	resChan := make(chan *asyncGroupLookupResponse, cap(stor.freeLookupReqChan))
	resFunc := func(stream pb.GroupStore_LookupClient) {
		var err error
		var res *asyncGroupLookupResponse
		for {
			select {
			case res = <-stor.freeLookupResChan:
			case <-stor.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-stor.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.GroupStore_LookupClient
	waitingMax := uint32(cap(stor.freeLookupReqChan)) - 1
	waiting := make([]*asyncGroupLookupRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-stor.pendingLookupReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingLookupReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				stor.lock.Lock()
				if stor.client == nil {
					if err = stor.startup(); err != nil {
						stor.lock.Unlock()
						res := <-stor.freeLookupResChan
						res.err = err
						res.res = &pb.LookupResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = stor.client.Lookup(context.Background())
				stor.lock.Unlock()
				if err != nil {
					res := <-stor.freeLookupResChan
					res.err = err
					res.res = &pb.LookupResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-stor.freeLookupResChan
				res.err = err
				res.res = &pb.LookupResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncGroupLookupRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				stor.freeLookupResChan <- res
				go func(reqs []*asyncGroupLookupRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-stor.freeLookupResChan
						res.err = err
						res.res = &pb.LookupResponse{RPCID: req.req.RPCID}
						resChan <- res
					}
				}(wereWaiting, err)
				break
			}
			if res.res.RPCID < 0 || res.res.RPCID > waitingMax {
				// TODO: Debug log error?
				break
			}
			req := waiting[res.res.RPCID]
			if req == nil {
				// TODO: Debug log error?
				break
			}
			waiting[res.res.RPCID] = nil
			req.canceledLock.Lock()
			if !req.canceled {
				req.resChan <- res
			} else {
				stor.freeLookupReqChan <- req
				stor.freeLookupResChan <- res
			}
			req.canceledLock.Unlock()
		case <-stor.handlersDoneChan:
			return
		}
	}
}

func (stor *groupStore) Lookup(ctx context.Context, keyA, keyB uint64, childKeyA, childKeyB uint64) (timestampMicro int64, length uint32, err error) {

	var req *asyncGroupLookupRequest
	select {
	case req = <-stor.freeLookupReqChan:
	case <-ctx.Done():

		return 0, 0, ctx.Err()

	}
	req.canceled = false

	req.req.KeyA = keyA
	req.req.KeyB = keyB

	req.req.ChildKeyA = childKeyA
	req.req.ChildKeyB = childKeyB

	select {
	case stor.pendingLookupReqChan <- req:
	case <-ctx.Done():
		stor.freeLookupReqChan <- req

		return 0, 0, ctx.Err()

	}
	var res *asyncGroupLookupResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			stor.freeLookupResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return 0, 0, ctx.Err()

	}
	stor.freeLookupReqChan <- req
	if res.err != nil {
		err = res.err
		stor.freeLookupResChan <- res

		return 0, 0, err

	}

	timestampMicro = res.res.TimestampMicro
	length = res.res.Length

	if res.res.Err == "" {
		err = nil
	} else {
		err = proto.TranslateErrorString(res.res.Err)
	}
	stor.freeLookupResChan <- res

	return timestampMicro, length, err

}

type asyncGroupReadGroupRequest struct {
	req          pb.ReadGroupRequest
	resChan      chan *asyncGroupReadGroupResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncGroupReadGroupResponse struct {
	res *pb.ReadGroupResponse
	err error
}

func (stor *groupStore) handleReadGroup() {
	resChan := make(chan *asyncGroupReadGroupResponse, cap(stor.freeReadGroupReqChan))
	resFunc := func(stream pb.GroupStore_ReadGroupClient) {
		var err error
		var res *asyncGroupReadGroupResponse
		for {
			select {
			case res = <-stor.freeReadGroupResChan:
			case <-stor.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-stor.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.GroupStore_ReadGroupClient
	waitingMax := uint32(cap(stor.freeReadGroupReqChan)) - 1
	waiting := make([]*asyncGroupReadGroupRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-stor.pendingReadGroupReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingReadGroupReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				stor.lock.Lock()
				if stor.client == nil {
					if err = stor.startup(); err != nil {
						stor.lock.Unlock()
						res := <-stor.freeReadGroupResChan
						res.err = err
						res.res = &pb.ReadGroupResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = stor.client.ReadGroup(context.Background())
				stor.lock.Unlock()
				if err != nil {
					res := <-stor.freeReadGroupResChan
					res.err = err
					res.res = &pb.ReadGroupResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-stor.freeReadGroupResChan
				res.err = err
				res.res = &pb.ReadGroupResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncGroupReadGroupRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				stor.freeReadGroupResChan <- res
				go func(reqs []*asyncGroupReadGroupRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-stor.freeReadGroupResChan
						res.err = err
						res.res = &pb.ReadGroupResponse{RPCID: req.req.RPCID}
						resChan <- res
					}
				}(wereWaiting, err)
				break
			}
			if res.res.RPCID < 0 || res.res.RPCID > waitingMax {
				// TODO: Debug log error?
				break
			}
			req := waiting[res.res.RPCID]
			if req == nil {
				// TODO: Debug log error?
				break
			}
			waiting[res.res.RPCID] = nil
			req.canceledLock.Lock()
			if !req.canceled {
				req.resChan <- res
			} else {
				stor.freeReadGroupReqChan <- req
				stor.freeReadGroupResChan <- res
			}
			req.canceledLock.Unlock()
		case <-stor.handlersDoneChan:
			return
		}
	}
}

func (stor *groupStore) ReadGroup(ctx context.Context, parentKeyA, parentKeyB uint64) (items []store.ReadGroupItem, err error) {

	var req *asyncGroupReadGroupRequest
	select {
	case req = <-stor.freeReadGroupReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.KeyA = parentKeyA
	req.req.KeyB = parentKeyB

	select {
	case stor.pendingReadGroupReqChan <- req:
	case <-ctx.Done():
		stor.freeReadGroupReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncGroupReadGroupResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			stor.freeReadGroupResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	stor.freeReadGroupReqChan <- req
	if res.err != nil {
		err = res.err
		stor.freeReadGroupResChan <- res

		return nil, err

	}

	items = make([]store.ReadGroupItem, len(res.res.Items))
	for i, v := range res.res.Items {
		items[i].ChildKeyA = v.ChildKeyA
		items[i].ChildKeyB = v.ChildKeyB
		items[i].TimestampMicro = v.TimestampMicro
		items[i].Value = v.Value
	}

	if res.res.Err == "" {
		err = nil
	} else {
		err = proto.TranslateErrorString(res.res.Err)
	}
	stor.freeReadGroupResChan <- res

	return items, err

}

type asyncGroupReadRequest struct {
	req          pb.ReadRequest
	resChan      chan *asyncGroupReadResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncGroupReadResponse struct {
	res *pb.ReadResponse
	err error
}

func (stor *groupStore) handleRead() {
	resChan := make(chan *asyncGroupReadResponse, cap(stor.freeReadReqChan))
	resFunc := func(stream pb.GroupStore_ReadClient) {
		var err error
		var res *asyncGroupReadResponse
		for {
			select {
			case res = <-stor.freeReadResChan:
			case <-stor.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-stor.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.GroupStore_ReadClient
	waitingMax := uint32(cap(stor.freeReadReqChan)) - 1
	waiting := make([]*asyncGroupReadRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-stor.pendingReadReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingReadReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				stor.lock.Lock()
				if stor.client == nil {
					if err = stor.startup(); err != nil {
						stor.lock.Unlock()
						res := <-stor.freeReadResChan
						res.err = err
						res.res = &pb.ReadResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = stor.client.Read(context.Background())
				stor.lock.Unlock()
				if err != nil {
					res := <-stor.freeReadResChan
					res.err = err
					res.res = &pb.ReadResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-stor.freeReadResChan
				res.err = err
				res.res = &pb.ReadResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncGroupReadRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				stor.freeReadResChan <- res
				go func(reqs []*asyncGroupReadRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-stor.freeReadResChan
						res.err = err
						res.res = &pb.ReadResponse{RPCID: req.req.RPCID}
						resChan <- res
					}
				}(wereWaiting, err)
				break
			}
			if res.res.RPCID < 0 || res.res.RPCID > waitingMax {
				// TODO: Debug log error?
				break
			}
			req := waiting[res.res.RPCID]
			if req == nil {
				// TODO: Debug log error?
				break
			}
			waiting[res.res.RPCID] = nil
			req.canceledLock.Lock()
			if !req.canceled {
				req.resChan <- res
			} else {
				stor.freeReadReqChan <- req
				stor.freeReadResChan <- res
			}
			req.canceledLock.Unlock()
		case <-stor.handlersDoneChan:
			return
		}
	}
}

func (stor *groupStore) Read(ctx context.Context, keyA, keyB uint64, childKeyA, childKeyB uint64, value []byte) (timestampMicro int64, rvalue []byte, err error) {

	var req *asyncGroupReadRequest
	select {
	case req = <-stor.freeReadReqChan:
	case <-ctx.Done():

		return 0, rvalue, ctx.Err()

	}
	req.canceled = false

	req.req.KeyA = keyA
	req.req.KeyB = keyB

	req.req.ChildKeyA = childKeyA
	req.req.ChildKeyB = childKeyB

	select {
	case stor.pendingReadReqChan <- req:
	case <-ctx.Done():
		stor.freeReadReqChan <- req

		return 0, rvalue, ctx.Err()

	}
	var res *asyncGroupReadResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			stor.freeReadResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return 0, rvalue, ctx.Err()

	}
	stor.freeReadReqChan <- req
	if res.err != nil {
		err = res.err
		stor.freeReadResChan <- res

		return 0, rvalue, err

	}

	timestampMicro = res.res.TimestampMicro
	rvalue = append(rvalue, res.res.Value...)

	if res.res.Err == "" {
		err = nil
	} else {
		err = proto.TranslateErrorString(res.res.Err)
	}
	stor.freeReadResChan <- res

	return timestampMicro, rvalue, err

}

type asyncGroupWriteRequest struct {
	req          pb.WriteRequest
	resChan      chan *asyncGroupWriteResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncGroupWriteResponse struct {
	res *pb.WriteResponse
	err error
}

func (stor *groupStore) handleWrite() {
	resChan := make(chan *asyncGroupWriteResponse, cap(stor.freeWriteReqChan))
	resFunc := func(stream pb.GroupStore_WriteClient) {
		var err error
		var res *asyncGroupWriteResponse
		for {
			select {
			case res = <-stor.freeWriteResChan:
			case <-stor.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-stor.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.GroupStore_WriteClient
	waitingMax := uint32(cap(stor.freeWriteReqChan)) - 1
	waiting := make([]*asyncGroupWriteRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-stor.pendingWriteReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingWriteReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				stor.lock.Lock()
				if stor.client == nil {
					if err = stor.startup(); err != nil {
						stor.lock.Unlock()
						res := <-stor.freeWriteResChan
						res.err = err
						res.res = &pb.WriteResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = stor.client.Write(context.Background())
				stor.lock.Unlock()
				if err != nil {
					res := <-stor.freeWriteResChan
					res.err = err
					res.res = &pb.WriteResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-stor.freeWriteResChan
				res.err = err
				res.res = &pb.WriteResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncGroupWriteRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				stor.freeWriteResChan <- res
				go func(reqs []*asyncGroupWriteRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-stor.freeWriteResChan
						res.err = err
						res.res = &pb.WriteResponse{RPCID: req.req.RPCID}
						resChan <- res
					}
				}(wereWaiting, err)
				break
			}
			if res.res.RPCID < 0 || res.res.RPCID > waitingMax {
				// TODO: Debug log error?
				break
			}
			req := waiting[res.res.RPCID]
			if req == nil {
				// TODO: Debug log error?
				break
			}
			waiting[res.res.RPCID] = nil
			req.canceledLock.Lock()
			if !req.canceled {
				req.resChan <- res
			} else {
				stor.freeWriteReqChan <- req
				stor.freeWriteResChan <- res
			}
			req.canceledLock.Unlock()
		case <-stor.handlersDoneChan:
			return
		}
	}
}

func (stor *groupStore) Write(ctx context.Context, keyA, keyB uint64, childKeyA, childKeyB uint64, timestampMicro int64, value []byte) (oldTimestampMicro int64, err error) {

	var req *asyncGroupWriteRequest
	select {
	case req = <-stor.freeWriteReqChan:
	case <-ctx.Done():

		return 0, ctx.Err()

	}
	req.canceled = false

	req.req.KeyA = keyA
	req.req.KeyB = keyB

	req.req.ChildKeyA = childKeyA
	req.req.ChildKeyB = childKeyB

	req.req.TimestampMicro = timestampMicro
	if len(value) == 0 {
		panic(fmt.Sprintf("REMOVEME %s asked to Write a zlv", stor.addr))
	}
	req.req.Value = value

	select {
	case stor.pendingWriteReqChan <- req:
	case <-ctx.Done():
		stor.freeWriteReqChan <- req

		return 0, ctx.Err()

	}
	var res *asyncGroupWriteResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			stor.freeWriteResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return 0, ctx.Err()

	}
	stor.freeWriteReqChan <- req
	if res.err != nil {
		err = res.err
		stor.freeWriteResChan <- res

		return 0, err

	}

	oldTimestampMicro = res.res.TimestampMicro

	if res.res.Err == "" {
		err = nil
	} else {
		err = proto.TranslateErrorString(res.res.Err)
	}
	stor.freeWriteResChan <- res

	return oldTimestampMicro, err

}
