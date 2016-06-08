package api

import (
	"errors"
	"fmt"
	"sync"

	"github.com/getcfs/megacfs/ftls"
	"github.com/getcfs/megacfs/oort/api/proto"
	pb "github.com/getcfs/megacfs/oort/api/valueproto"
	"github.com/gholt/store"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type valueStore struct {
	lock             sync.Mutex
	addr             string
	ftlsc            *ftls.Config
	opts             []grpc.DialOption
	conn             *grpc.ClientConn
	client           pb.ValueStoreClient
	handlersDoneChan chan struct{}

	pendingLookupReqChan chan *asyncValueLookupRequest
	freeLookupReqChan    chan *asyncValueLookupRequest
	freeLookupResChan    chan *asyncValueLookupResponse

	pendingReadReqChan chan *asyncValueReadRequest
	freeReadReqChan    chan *asyncValueReadRequest
	freeReadResChan    chan *asyncValueReadResponse

	pendingWriteReqChan chan *asyncValueWriteRequest
	freeWriteReqChan    chan *asyncValueWriteRequest
	freeWriteResChan    chan *asyncValueWriteResponse

	pendingDeleteReqChan chan *asyncValueDeleteRequest
	freeDeleteReqChan    chan *asyncValueDeleteRequest
	freeDeleteResChan    chan *asyncValueDeleteResponse
}

// NewValueStore creates a ValueStore connection via grpc to the given
// address.
func NewValueStore(addr string, concurrency int, ftlsConfig *ftls.Config, opts ...grpc.DialOption) (store.ValueStore, error) {
	stor := &valueStore{
		addr:             addr,
		ftlsc:            ftlsConfig,
		opts:             opts,
		handlersDoneChan: make(chan struct{}),
	}

	stor.pendingLookupReqChan = make(chan *asyncValueLookupRequest, concurrency)
	stor.freeLookupReqChan = make(chan *asyncValueLookupRequest, concurrency)
	stor.freeLookupResChan = make(chan *asyncValueLookupResponse, concurrency)
	for i := 0; i < cap(stor.freeLookupReqChan); i++ {
		stor.freeLookupReqChan <- &asyncValueLookupRequest{resChan: make(chan *asyncValueLookupResponse, 1)}
	}
	for i := 0; i < cap(stor.freeLookupResChan); i++ {
		stor.freeLookupResChan <- &asyncValueLookupResponse{}
	}
	go stor.handleLookupStream()

	stor.pendingReadReqChan = make(chan *asyncValueReadRequest, concurrency)
	stor.freeReadReqChan = make(chan *asyncValueReadRequest, concurrency)
	stor.freeReadResChan = make(chan *asyncValueReadResponse, concurrency)
	for i := 0; i < cap(stor.freeReadReqChan); i++ {
		stor.freeReadReqChan <- &asyncValueReadRequest{resChan: make(chan *asyncValueReadResponse, 1)}
	}
	for i := 0; i < cap(stor.freeReadResChan); i++ {
		stor.freeReadResChan <- &asyncValueReadResponse{}
	}
	go stor.handleReadStream()

	stor.pendingWriteReqChan = make(chan *asyncValueWriteRequest, concurrency)
	stor.freeWriteReqChan = make(chan *asyncValueWriteRequest, concurrency)
	stor.freeWriteResChan = make(chan *asyncValueWriteResponse, concurrency)
	for i := 0; i < cap(stor.freeWriteReqChan); i++ {
		stor.freeWriteReqChan <- &asyncValueWriteRequest{resChan: make(chan *asyncValueWriteResponse, 1)}
	}
	for i := 0; i < cap(stor.freeWriteResChan); i++ {
		stor.freeWriteResChan <- &asyncValueWriteResponse{}
	}
	go stor.handleWriteStream()

	stor.pendingDeleteReqChan = make(chan *asyncValueDeleteRequest, concurrency)
	stor.freeDeleteReqChan = make(chan *asyncValueDeleteRequest, concurrency)
	stor.freeDeleteResChan = make(chan *asyncValueDeleteResponse, concurrency)
	for i := 0; i < cap(stor.freeDeleteReqChan); i++ {
		stor.freeDeleteReqChan <- &asyncValueDeleteRequest{resChan: make(chan *asyncValueDeleteResponse, 1)}
	}
	for i := 0; i < cap(stor.freeDeleteResChan); i++ {
		stor.freeDeleteResChan <- &asyncValueDeleteResponse{}
	}
	go stor.handleDeleteStream()

	return stor, nil
}

func (stor *valueStore) Startup(ctx context.Context) error {
	stor.lock.Lock()
	err := stor.startup()
	stor.lock.Unlock()
	return err
}

func (stor *valueStore) startup() error {
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
	stor.client = pb.NewValueStoreClient(stor.conn)
	return nil
}

// Shutdown will close any existing connections; note that Startup may
// automatically get called with any further activity, but it will use a new
// connection. To ensure the valueStore has no further activity, use Close.
func (stor *valueStore) Shutdown(ctx context.Context) error {
	stor.lock.Lock()
	err := stor.shutdown()
	stor.lock.Unlock()
	return err
}

func (stor *valueStore) shutdown() error {
	if stor.conn == nil {
		return nil
	}
	stor.conn.Close()
	stor.conn = nil
	stor.client = nil
	return nil
}

// Close will shutdown outgoing connectivity and stop all background
// goroutines; note that the valueStore is no longer usable after a call to
// Close, including using Startup.
func (stor *valueStore) Close() {
	stor.lock.Lock()
	stor.shutdown()
	close(stor.handlersDoneChan)
	stor.lock.Unlock()
}

func (stor *valueStore) EnableWrites(ctx context.Context) error {
	return nil
}

func (stor *valueStore) DisableWrites(ctx context.Context) error {
	// TODO: I suppose we could implement toggling writes from this client;
	// I'll leave that for later.
	return errors.New("cannot disable writes with this client at this time")
}

func (stor *valueStore) Flush(ctx context.Context) error {
	// Nothing cached on this end, so nothing to flush.
	return nil
}

func (stor *valueStore) AuditPass(ctx context.Context) error {
	return errors.New("audit passes not available with this client at this time")
}

func (stor *valueStore) Stats(ctx context.Context, debug bool) (fmt.Stringer, error) {
	return noStats, nil
}

func (stor *valueStore) ValueCap(ctx context.Context) (uint32, error) {
	// TODO: This should be a (cached) value from the server. Servers don't
	// change their value caps on the fly, so the cache can be kept until
	// disconnect.
	return 0xffffffff, nil
}

type asyncValueLookupRequest struct {
	req          pb.LookupRequest
	resChan      chan *asyncValueLookupResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncValueLookupResponse struct {
	res *pb.LookupResponse
	err error
}

func (stor *valueStore) handleLookupStream() {
	resChan := make(chan *asyncValueLookupResponse, cap(stor.freeLookupReqChan))
	resFunc := func(stream pb.ValueStore_StreamLookupClient) {
		var err error
		var res *asyncValueLookupResponse
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
	var stream pb.ValueStore_StreamLookupClient
	waitingMax := uint32(cap(stor.freeLookupReqChan)) - 1
	waiting := make([]*asyncValueLookupRequest, waitingMax+1)
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
			req.req.Rpcid = waitingIndex
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
						res.res = &pb.LookupResponse{Rpcid: req.req.Rpcid}
						resChan <- res
						break
					}
				}
				stream, err = stor.client.StreamLookup(context.Background())
				stor.lock.Unlock()
				if err != nil {
					res := <-stor.freeLookupResChan
					res.err = err
					res.res = &pb.LookupResponse{Rpcid: req.req.Rpcid}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-stor.freeLookupResChan
				res.err = err
				res.res = &pb.LookupResponse{Rpcid: req.req.Rpcid}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncValueLookupRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				go func(reqs []*asyncValueLookupRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-stor.freeLookupResChan
						res.err = err
						res.res = &pb.LookupResponse{Rpcid: req.req.Rpcid}
						resChan <- res
					}
				}(wereWaiting, err)
				break
			}
			if res.res.Rpcid < 0 || res.res.Rpcid > waitingMax {
				// TODO: Debug log error?
				break
			}
			req := waiting[res.res.Rpcid]
			if req == nil {
				// TODO: Debug log error?
				break
			}
			waiting[res.res.Rpcid] = nil
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

func (stor *valueStore) Lookup(ctx context.Context, keyA, keyB uint64) (timestampMicro int64, length uint32, err error) {

	var req *asyncValueLookupRequest
	select {
	case req = <-stor.freeLookupReqChan:
	case <-ctx.Done():

		return 0, 0, ctx.Err()

	}
	req.canceled = false

	req.req.KeyA = keyA
	req.req.KeyB = keyB

	select {
	case stor.pendingLookupReqChan <- req:
	case <-ctx.Done():
		stor.freeLookupReqChan <- req

		return 0, 0, ctx.Err()

	}
	var res *asyncValueLookupResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case <-req.resChan:
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

type asyncValueReadRequest struct {
	req          pb.ReadRequest
	resChan      chan *asyncValueReadResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncValueReadResponse struct {
	res *pb.ReadResponse
	err error
}

func (stor *valueStore) handleReadStream() {
	resChan := make(chan *asyncValueReadResponse, cap(stor.freeReadReqChan))
	resFunc := func(stream pb.ValueStore_StreamReadClient) {
		var err error
		var res *asyncValueReadResponse
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
	var stream pb.ValueStore_StreamReadClient
	waitingMax := uint32(cap(stor.freeReadReqChan)) - 1
	waiting := make([]*asyncValueReadRequest, waitingMax+1)
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
			req.req.Rpcid = waitingIndex
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
						res.res = &pb.ReadResponse{Rpcid: req.req.Rpcid}
						resChan <- res
						break
					}
				}
				stream, err = stor.client.StreamRead(context.Background())
				stor.lock.Unlock()
				if err != nil {
					res := <-stor.freeReadResChan
					res.err = err
					res.res = &pb.ReadResponse{Rpcid: req.req.Rpcid}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-stor.freeReadResChan
				res.err = err
				res.res = &pb.ReadResponse{Rpcid: req.req.Rpcid}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncValueReadRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				go func(reqs []*asyncValueReadRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-stor.freeReadResChan
						res.err = err
						res.res = &pb.ReadResponse{Rpcid: req.req.Rpcid}
						resChan <- res
					}
				}(wereWaiting, err)
				break
			}
			if res.res.Rpcid < 0 || res.res.Rpcid > waitingMax {
				// TODO: Debug log error?
				break
			}
			req := waiting[res.res.Rpcid]
			if req == nil {
				// TODO: Debug log error?
				break
			}
			waiting[res.res.Rpcid] = nil
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

func (stor *valueStore) Read(ctx context.Context, keyA, keyB uint64, value []byte) (timestampMicro int64, rvalue []byte, err error) {

	var req *asyncValueReadRequest
	select {
	case req = <-stor.freeReadReqChan:
	case <-ctx.Done():

		return 0, rvalue, ctx.Err()

	}
	req.canceled = false

	req.req.KeyA = keyA
	req.req.KeyB = keyB

	select {
	case stor.pendingReadReqChan <- req:
	case <-ctx.Done():
		stor.freeReadReqChan <- req

		return 0, rvalue, ctx.Err()

	}
	var res *asyncValueReadResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case <-req.resChan:
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

type asyncValueWriteRequest struct {
	req          pb.WriteRequest
	resChan      chan *asyncValueWriteResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncValueWriteResponse struct {
	res *pb.WriteResponse
	err error
}

func (stor *valueStore) handleWriteStream() {
	resChan := make(chan *asyncValueWriteResponse, cap(stor.freeWriteReqChan))
	resFunc := func(stream pb.ValueStore_StreamWriteClient) {
		var err error
		var res *asyncValueWriteResponse
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
	var stream pb.ValueStore_StreamWriteClient
	waitingMax := uint32(cap(stor.freeWriteReqChan)) - 1
	waiting := make([]*asyncValueWriteRequest, waitingMax+1)
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
			req.req.Rpcid = waitingIndex
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
						res.res = &pb.WriteResponse{Rpcid: req.req.Rpcid}
						resChan <- res
						break
					}
				}
				stream, err = stor.client.StreamWrite(context.Background())
				stor.lock.Unlock()
				if err != nil {
					res := <-stor.freeWriteResChan
					res.err = err
					res.res = &pb.WriteResponse{Rpcid: req.req.Rpcid}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-stor.freeWriteResChan
				res.err = err
				res.res = &pb.WriteResponse{Rpcid: req.req.Rpcid}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncValueWriteRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				go func(reqs []*asyncValueWriteRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-stor.freeWriteResChan
						res.err = err
						res.res = &pb.WriteResponse{Rpcid: req.req.Rpcid}
						resChan <- res
					}
				}(wereWaiting, err)
				break
			}
			if res.res.Rpcid < 0 || res.res.Rpcid > waitingMax {
				// TODO: Debug log error?
				break
			}
			req := waiting[res.res.Rpcid]
			if req == nil {
				// TODO: Debug log error?
				break
			}
			waiting[res.res.Rpcid] = nil
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

func (stor *valueStore) Write(ctx context.Context, keyA, keyB uint64, timestampMicro int64, value []byte) (oldTimestampMicro int64, err error) {

	var req *asyncValueWriteRequest
	select {
	case req = <-stor.freeWriteReqChan:
	case <-ctx.Done():

		return 0, ctx.Err()

	}
	req.canceled = false

	req.req.KeyA = keyA
	req.req.KeyB = keyB

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
	var res *asyncValueWriteResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case <-req.resChan:
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

type asyncValueDeleteRequest struct {
	req          pb.DeleteRequest
	resChan      chan *asyncValueDeleteResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncValueDeleteResponse struct {
	res *pb.DeleteResponse
	err error
}

func (stor *valueStore) handleDeleteStream() {
	resChan := make(chan *asyncValueDeleteResponse, cap(stor.freeDeleteReqChan))
	resFunc := func(stream pb.ValueStore_StreamDeleteClient) {
		var err error
		var res *asyncValueDeleteResponse
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
	var stream pb.ValueStore_StreamDeleteClient
	waitingMax := uint32(cap(stor.freeDeleteReqChan)) - 1
	waiting := make([]*asyncValueDeleteRequest, waitingMax+1)
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
			req.req.Rpcid = waitingIndex
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
						res.res = &pb.DeleteResponse{Rpcid: req.req.Rpcid}
						resChan <- res
						break
					}
				}
				stream, err = stor.client.StreamDelete(context.Background())
				stor.lock.Unlock()
				if err != nil {
					res := <-stor.freeDeleteResChan
					res.err = err
					res.res = &pb.DeleteResponse{Rpcid: req.req.Rpcid}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-stor.freeDeleteResChan
				res.err = err
				res.res = &pb.DeleteResponse{Rpcid: req.req.Rpcid}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncValueDeleteRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				go func(reqs []*asyncValueDeleteRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-stor.freeDeleteResChan
						res.err = err
						res.res = &pb.DeleteResponse{Rpcid: req.req.Rpcid}
						resChan <- res
					}
				}(wereWaiting, err)
				break
			}
			if res.res.Rpcid < 0 || res.res.Rpcid > waitingMax {
				// TODO: Debug log error?
				break
			}
			req := waiting[res.res.Rpcid]
			if req == nil {
				// TODO: Debug log error?
				break
			}
			waiting[res.res.Rpcid] = nil
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

func (stor *valueStore) Delete(ctx context.Context, keyA, keyB uint64, timestampMicro int64) (oldTimestampMicro int64, err error) {

	var req *asyncValueDeleteRequest
	select {
	case req = <-stor.freeDeleteReqChan:
	case <-ctx.Done():

		return 0, ctx.Err()

	}
	req.canceled = false

	req.req.KeyA = keyA
	req.req.KeyB = keyB

	req.req.TimestampMicro = timestampMicro

	select {
	case stor.pendingDeleteReqChan <- req:
	case <-ctx.Done():
		stor.freeDeleteReqChan <- req

		return 0, ctx.Err()

	}
	var res *asyncValueDeleteResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case <-req.resChan:
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
