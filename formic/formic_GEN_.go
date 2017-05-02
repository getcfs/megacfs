package formic

import (
	"errors"
	"sync"

	pb "github.com/getcfs/megacfs/formic/formicproto"
	"github.com/getcfs/megacfs/ftls"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Formic struct {
	lock             sync.Mutex
	logger           *zap.Logger
	fsid             string
	addr             string
	ftlsc            *ftls.Config
	opts             []grpc.DialOption
	conn             *grpc.ClientConn
	client           pb.FormicClient
	handlersDoneChan chan struct{}

	pendingCheckReqChan chan *asyncFormicCheckRequest
	freeCheckReqChan    chan *asyncFormicCheckRequest
	freeCheckResChan    chan *asyncFormicCheckResponse

	pendingCreateFSReqChan chan *asyncFormicCreateFSRequest
	freeCreateFSReqChan    chan *asyncFormicCreateFSRequest
	freeCreateFSResChan    chan *asyncFormicCreateFSResponse

	pendingCreateReqChan chan *asyncFormicCreateRequest
	freeCreateReqChan    chan *asyncFormicCreateRequest
	freeCreateResChan    chan *asyncFormicCreateResponse

	pendingDeleteFSReqChan chan *asyncFormicDeleteFSRequest
	freeDeleteFSReqChan    chan *asyncFormicDeleteFSRequest
	freeDeleteFSResChan    chan *asyncFormicDeleteFSResponse

	pendingGetAttrReqChan chan *asyncFormicGetAttrRequest
	freeGetAttrReqChan    chan *asyncFormicGetAttrRequest
	freeGetAttrResChan    chan *asyncFormicGetAttrResponse

	pendingGetXAttrReqChan chan *asyncFormicGetXAttrRequest
	freeGetXAttrReqChan    chan *asyncFormicGetXAttrRequest
	freeGetXAttrResChan    chan *asyncFormicGetXAttrResponse

	pendingGrantAddressFSReqChan chan *asyncFormicGrantAddressFSRequest
	freeGrantAddressFSReqChan    chan *asyncFormicGrantAddressFSRequest
	freeGrantAddressFSResChan    chan *asyncFormicGrantAddressFSResponse

	pendingInitFSReqChan chan *asyncFormicInitFSRequest
	freeInitFSReqChan    chan *asyncFormicInitFSRequest
	freeInitFSResChan    chan *asyncFormicInitFSResponse

	pendingListFSReqChan chan *asyncFormicListFSRequest
	freeListFSReqChan    chan *asyncFormicListFSRequest
	freeListFSResChan    chan *asyncFormicListFSResponse

	pendingListXAttrReqChan chan *asyncFormicListXAttrRequest
	freeListXAttrReqChan    chan *asyncFormicListXAttrRequest
	freeListXAttrResChan    chan *asyncFormicListXAttrResponse

	pendingLookupReqChan chan *asyncFormicLookupRequest
	freeLookupReqChan    chan *asyncFormicLookupRequest
	freeLookupResChan    chan *asyncFormicLookupResponse

	pendingMkDirReqChan chan *asyncFormicMkDirRequest
	freeMkDirReqChan    chan *asyncFormicMkDirRequest
	freeMkDirResChan    chan *asyncFormicMkDirResponse

	pendingReadDirAllReqChan chan *asyncFormicReadDirAllRequest
	freeReadDirAllReqChan    chan *asyncFormicReadDirAllRequest
	freeReadDirAllResChan    chan *asyncFormicReadDirAllResponse

	pendingReadLinkReqChan chan *asyncFormicReadLinkRequest
	freeReadLinkReqChan    chan *asyncFormicReadLinkRequest
	freeReadLinkResChan    chan *asyncFormicReadLinkResponse

	pendingReadReqChan chan *asyncFormicReadRequest
	freeReadReqChan    chan *asyncFormicReadRequest
	freeReadResChan    chan *asyncFormicReadResponse

	pendingRemoveReqChan chan *asyncFormicRemoveRequest
	freeRemoveReqChan    chan *asyncFormicRemoveRequest
	freeRemoveResChan    chan *asyncFormicRemoveResponse

	pendingRemoveXAttrReqChan chan *asyncFormicRemoveXAttrRequest
	freeRemoveXAttrReqChan    chan *asyncFormicRemoveXAttrRequest
	freeRemoveXAttrResChan    chan *asyncFormicRemoveXAttrResponse

	pendingRenameReqChan chan *asyncFormicRenameRequest
	freeRenameReqChan    chan *asyncFormicRenameRequest
	freeRenameResChan    chan *asyncFormicRenameResponse

	pendingRevokeAddressFSReqChan chan *asyncFormicRevokeAddressFSRequest
	freeRevokeAddressFSReqChan    chan *asyncFormicRevokeAddressFSRequest
	freeRevokeAddressFSResChan    chan *asyncFormicRevokeAddressFSResponse

	pendingSetAttrReqChan chan *asyncFormicSetAttrRequest
	freeSetAttrReqChan    chan *asyncFormicSetAttrRequest
	freeSetAttrResChan    chan *asyncFormicSetAttrResponse

	pendingSetXAttrReqChan chan *asyncFormicSetXAttrRequest
	freeSetXAttrReqChan    chan *asyncFormicSetXAttrRequest
	freeSetXAttrResChan    chan *asyncFormicSetXAttrResponse

	pendingShowFSReqChan chan *asyncFormicShowFSRequest
	freeShowFSReqChan    chan *asyncFormicShowFSRequest
	freeShowFSResChan    chan *asyncFormicShowFSResponse

	pendingStatFSReqChan chan *asyncFormicStatFSRequest
	freeStatFSReqChan    chan *asyncFormicStatFSRequest
	freeStatFSResChan    chan *asyncFormicStatFSResponse

	pendingSymLinkReqChan chan *asyncFormicSymLinkRequest
	freeSymLinkReqChan    chan *asyncFormicSymLinkRequest
	freeSymLinkResChan    chan *asyncFormicSymLinkResponse

	pendingUpdateFSReqChan chan *asyncFormicUpdateFSRequest
	freeUpdateFSReqChan    chan *asyncFormicUpdateFSRequest
	freeUpdateFSResChan    chan *asyncFormicUpdateFSResponse

	pendingWriteReqChan chan *asyncFormicWriteRequest
	freeWriteReqChan    chan *asyncFormicWriteRequest
	freeWriteResChan    chan *asyncFormicWriteResponse
}

func NewFormic(logger *zap.Logger, fsid string, addr string, concurrency int, ftlsConfig *ftls.Config, opts ...grpc.DialOption) *Formic {
	f := &Formic{
		logger:           logger,
		fsid:             fsid,
		addr:             addr,
		ftlsc:            ftlsConfig,
		opts:             opts,
		handlersDoneChan: make(chan struct{}),
	}

	f.pendingCheckReqChan = make(chan *asyncFormicCheckRequest, concurrency)
	f.freeCheckReqChan = make(chan *asyncFormicCheckRequest, concurrency)
	f.freeCheckResChan = make(chan *asyncFormicCheckResponse, concurrency)
	for i := 0; i < cap(f.freeCheckReqChan); i++ {
		f.freeCheckReqChan <- &asyncFormicCheckRequest{resChan: make(chan *asyncFormicCheckResponse, 1)}
	}
	for i := 0; i < cap(f.freeCheckResChan); i++ {
		f.freeCheckResChan <- &asyncFormicCheckResponse{}
	}
	go f.handleCheck()

	f.pendingCreateFSReqChan = make(chan *asyncFormicCreateFSRequest, concurrency)
	f.freeCreateFSReqChan = make(chan *asyncFormicCreateFSRequest, concurrency)
	f.freeCreateFSResChan = make(chan *asyncFormicCreateFSResponse, concurrency)
	for i := 0; i < cap(f.freeCreateFSReqChan); i++ {
		f.freeCreateFSReqChan <- &asyncFormicCreateFSRequest{resChan: make(chan *asyncFormicCreateFSResponse, 1)}
	}
	for i := 0; i < cap(f.freeCreateFSResChan); i++ {
		f.freeCreateFSResChan <- &asyncFormicCreateFSResponse{}
	}
	go f.handleCreateFS()

	f.pendingCreateReqChan = make(chan *asyncFormicCreateRequest, concurrency)
	f.freeCreateReqChan = make(chan *asyncFormicCreateRequest, concurrency)
	f.freeCreateResChan = make(chan *asyncFormicCreateResponse, concurrency)
	for i := 0; i < cap(f.freeCreateReqChan); i++ {
		f.freeCreateReqChan <- &asyncFormicCreateRequest{resChan: make(chan *asyncFormicCreateResponse, 1)}
	}
	for i := 0; i < cap(f.freeCreateResChan); i++ {
		f.freeCreateResChan <- &asyncFormicCreateResponse{}
	}
	go f.handleCreate()

	f.pendingDeleteFSReqChan = make(chan *asyncFormicDeleteFSRequest, concurrency)
	f.freeDeleteFSReqChan = make(chan *asyncFormicDeleteFSRequest, concurrency)
	f.freeDeleteFSResChan = make(chan *asyncFormicDeleteFSResponse, concurrency)
	for i := 0; i < cap(f.freeDeleteFSReqChan); i++ {
		f.freeDeleteFSReqChan <- &asyncFormicDeleteFSRequest{resChan: make(chan *asyncFormicDeleteFSResponse, 1)}
	}
	for i := 0; i < cap(f.freeDeleteFSResChan); i++ {
		f.freeDeleteFSResChan <- &asyncFormicDeleteFSResponse{}
	}
	go f.handleDeleteFS()

	f.pendingGetAttrReqChan = make(chan *asyncFormicGetAttrRequest, concurrency)
	f.freeGetAttrReqChan = make(chan *asyncFormicGetAttrRequest, concurrency)
	f.freeGetAttrResChan = make(chan *asyncFormicGetAttrResponse, concurrency)
	for i := 0; i < cap(f.freeGetAttrReqChan); i++ {
		f.freeGetAttrReqChan <- &asyncFormicGetAttrRequest{resChan: make(chan *asyncFormicGetAttrResponse, 1)}
	}
	for i := 0; i < cap(f.freeGetAttrResChan); i++ {
		f.freeGetAttrResChan <- &asyncFormicGetAttrResponse{}
	}
	go f.handleGetAttr()

	f.pendingGetXAttrReqChan = make(chan *asyncFormicGetXAttrRequest, concurrency)
	f.freeGetXAttrReqChan = make(chan *asyncFormicGetXAttrRequest, concurrency)
	f.freeGetXAttrResChan = make(chan *asyncFormicGetXAttrResponse, concurrency)
	for i := 0; i < cap(f.freeGetXAttrReqChan); i++ {
		f.freeGetXAttrReqChan <- &asyncFormicGetXAttrRequest{resChan: make(chan *asyncFormicGetXAttrResponse, 1)}
	}
	for i := 0; i < cap(f.freeGetXAttrResChan); i++ {
		f.freeGetXAttrResChan <- &asyncFormicGetXAttrResponse{}
	}
	go f.handleGetXAttr()

	f.pendingGrantAddressFSReqChan = make(chan *asyncFormicGrantAddressFSRequest, concurrency)
	f.freeGrantAddressFSReqChan = make(chan *asyncFormicGrantAddressFSRequest, concurrency)
	f.freeGrantAddressFSResChan = make(chan *asyncFormicGrantAddressFSResponse, concurrency)
	for i := 0; i < cap(f.freeGrantAddressFSReqChan); i++ {
		f.freeGrantAddressFSReqChan <- &asyncFormicGrantAddressFSRequest{resChan: make(chan *asyncFormicGrantAddressFSResponse, 1)}
	}
	for i := 0; i < cap(f.freeGrantAddressFSResChan); i++ {
		f.freeGrantAddressFSResChan <- &asyncFormicGrantAddressFSResponse{}
	}
	go f.handleGrantAddressFS()

	f.pendingInitFSReqChan = make(chan *asyncFormicInitFSRequest, concurrency)
	f.freeInitFSReqChan = make(chan *asyncFormicInitFSRequest, concurrency)
	f.freeInitFSResChan = make(chan *asyncFormicInitFSResponse, concurrency)
	for i := 0; i < cap(f.freeInitFSReqChan); i++ {
		f.freeInitFSReqChan <- &asyncFormicInitFSRequest{resChan: make(chan *asyncFormicInitFSResponse, 1)}
	}
	for i := 0; i < cap(f.freeInitFSResChan); i++ {
		f.freeInitFSResChan <- &asyncFormicInitFSResponse{}
	}
	go f.handleInitFS()

	f.pendingListFSReqChan = make(chan *asyncFormicListFSRequest, concurrency)
	f.freeListFSReqChan = make(chan *asyncFormicListFSRequest, concurrency)
	f.freeListFSResChan = make(chan *asyncFormicListFSResponse, concurrency)
	for i := 0; i < cap(f.freeListFSReqChan); i++ {
		f.freeListFSReqChan <- &asyncFormicListFSRequest{resChan: make(chan *asyncFormicListFSResponse, 1)}
	}
	for i := 0; i < cap(f.freeListFSResChan); i++ {
		f.freeListFSResChan <- &asyncFormicListFSResponse{}
	}
	go f.handleListFS()

	f.pendingListXAttrReqChan = make(chan *asyncFormicListXAttrRequest, concurrency)
	f.freeListXAttrReqChan = make(chan *asyncFormicListXAttrRequest, concurrency)
	f.freeListXAttrResChan = make(chan *asyncFormicListXAttrResponse, concurrency)
	for i := 0; i < cap(f.freeListXAttrReqChan); i++ {
		f.freeListXAttrReqChan <- &asyncFormicListXAttrRequest{resChan: make(chan *asyncFormicListXAttrResponse, 1)}
	}
	for i := 0; i < cap(f.freeListXAttrResChan); i++ {
		f.freeListXAttrResChan <- &asyncFormicListXAttrResponse{}
	}
	go f.handleListXAttr()

	f.pendingLookupReqChan = make(chan *asyncFormicLookupRequest, concurrency)
	f.freeLookupReqChan = make(chan *asyncFormicLookupRequest, concurrency)
	f.freeLookupResChan = make(chan *asyncFormicLookupResponse, concurrency)
	for i := 0; i < cap(f.freeLookupReqChan); i++ {
		f.freeLookupReqChan <- &asyncFormicLookupRequest{resChan: make(chan *asyncFormicLookupResponse, 1)}
	}
	for i := 0; i < cap(f.freeLookupResChan); i++ {
		f.freeLookupResChan <- &asyncFormicLookupResponse{}
	}
	go f.handleLookup()

	f.pendingMkDirReqChan = make(chan *asyncFormicMkDirRequest, concurrency)
	f.freeMkDirReqChan = make(chan *asyncFormicMkDirRequest, concurrency)
	f.freeMkDirResChan = make(chan *asyncFormicMkDirResponse, concurrency)
	for i := 0; i < cap(f.freeMkDirReqChan); i++ {
		f.freeMkDirReqChan <- &asyncFormicMkDirRequest{resChan: make(chan *asyncFormicMkDirResponse, 1)}
	}
	for i := 0; i < cap(f.freeMkDirResChan); i++ {
		f.freeMkDirResChan <- &asyncFormicMkDirResponse{}
	}
	go f.handleMkDir()

	f.pendingReadDirAllReqChan = make(chan *asyncFormicReadDirAllRequest, concurrency)
	f.freeReadDirAllReqChan = make(chan *asyncFormicReadDirAllRequest, concurrency)
	f.freeReadDirAllResChan = make(chan *asyncFormicReadDirAllResponse, concurrency)
	for i := 0; i < cap(f.freeReadDirAllReqChan); i++ {
		f.freeReadDirAllReqChan <- &asyncFormicReadDirAllRequest{resChan: make(chan *asyncFormicReadDirAllResponse, 1)}
	}
	for i := 0; i < cap(f.freeReadDirAllResChan); i++ {
		f.freeReadDirAllResChan <- &asyncFormicReadDirAllResponse{}
	}
	go f.handleReadDirAll()

	f.pendingReadLinkReqChan = make(chan *asyncFormicReadLinkRequest, concurrency)
	f.freeReadLinkReqChan = make(chan *asyncFormicReadLinkRequest, concurrency)
	f.freeReadLinkResChan = make(chan *asyncFormicReadLinkResponse, concurrency)
	for i := 0; i < cap(f.freeReadLinkReqChan); i++ {
		f.freeReadLinkReqChan <- &asyncFormicReadLinkRequest{resChan: make(chan *asyncFormicReadLinkResponse, 1)}
	}
	for i := 0; i < cap(f.freeReadLinkResChan); i++ {
		f.freeReadLinkResChan <- &asyncFormicReadLinkResponse{}
	}
	go f.handleReadLink()

	f.pendingReadReqChan = make(chan *asyncFormicReadRequest, concurrency)
	f.freeReadReqChan = make(chan *asyncFormicReadRequest, concurrency)
	f.freeReadResChan = make(chan *asyncFormicReadResponse, concurrency)
	for i := 0; i < cap(f.freeReadReqChan); i++ {
		f.freeReadReqChan <- &asyncFormicReadRequest{resChan: make(chan *asyncFormicReadResponse, 1)}
	}
	for i := 0; i < cap(f.freeReadResChan); i++ {
		f.freeReadResChan <- &asyncFormicReadResponse{}
	}
	go f.handleRead()

	f.pendingRemoveReqChan = make(chan *asyncFormicRemoveRequest, concurrency)
	f.freeRemoveReqChan = make(chan *asyncFormicRemoveRequest, concurrency)
	f.freeRemoveResChan = make(chan *asyncFormicRemoveResponse, concurrency)
	for i := 0; i < cap(f.freeRemoveReqChan); i++ {
		f.freeRemoveReqChan <- &asyncFormicRemoveRequest{resChan: make(chan *asyncFormicRemoveResponse, 1)}
	}
	for i := 0; i < cap(f.freeRemoveResChan); i++ {
		f.freeRemoveResChan <- &asyncFormicRemoveResponse{}
	}
	go f.handleRemove()

	f.pendingRemoveXAttrReqChan = make(chan *asyncFormicRemoveXAttrRequest, concurrency)
	f.freeRemoveXAttrReqChan = make(chan *asyncFormicRemoveXAttrRequest, concurrency)
	f.freeRemoveXAttrResChan = make(chan *asyncFormicRemoveXAttrResponse, concurrency)
	for i := 0; i < cap(f.freeRemoveXAttrReqChan); i++ {
		f.freeRemoveXAttrReqChan <- &asyncFormicRemoveXAttrRequest{resChan: make(chan *asyncFormicRemoveXAttrResponse, 1)}
	}
	for i := 0; i < cap(f.freeRemoveXAttrResChan); i++ {
		f.freeRemoveXAttrResChan <- &asyncFormicRemoveXAttrResponse{}
	}
	go f.handleRemoveXAttr()

	f.pendingRenameReqChan = make(chan *asyncFormicRenameRequest, concurrency)
	f.freeRenameReqChan = make(chan *asyncFormicRenameRequest, concurrency)
	f.freeRenameResChan = make(chan *asyncFormicRenameResponse, concurrency)
	for i := 0; i < cap(f.freeRenameReqChan); i++ {
		f.freeRenameReqChan <- &asyncFormicRenameRequest{resChan: make(chan *asyncFormicRenameResponse, 1)}
	}
	for i := 0; i < cap(f.freeRenameResChan); i++ {
		f.freeRenameResChan <- &asyncFormicRenameResponse{}
	}
	go f.handleRename()

	f.pendingRevokeAddressFSReqChan = make(chan *asyncFormicRevokeAddressFSRequest, concurrency)
	f.freeRevokeAddressFSReqChan = make(chan *asyncFormicRevokeAddressFSRequest, concurrency)
	f.freeRevokeAddressFSResChan = make(chan *asyncFormicRevokeAddressFSResponse, concurrency)
	for i := 0; i < cap(f.freeRevokeAddressFSReqChan); i++ {
		f.freeRevokeAddressFSReqChan <- &asyncFormicRevokeAddressFSRequest{resChan: make(chan *asyncFormicRevokeAddressFSResponse, 1)}
	}
	for i := 0; i < cap(f.freeRevokeAddressFSResChan); i++ {
		f.freeRevokeAddressFSResChan <- &asyncFormicRevokeAddressFSResponse{}
	}
	go f.handleRevokeAddressFS()

	f.pendingSetAttrReqChan = make(chan *asyncFormicSetAttrRequest, concurrency)
	f.freeSetAttrReqChan = make(chan *asyncFormicSetAttrRequest, concurrency)
	f.freeSetAttrResChan = make(chan *asyncFormicSetAttrResponse, concurrency)
	for i := 0; i < cap(f.freeSetAttrReqChan); i++ {
		f.freeSetAttrReqChan <- &asyncFormicSetAttrRequest{resChan: make(chan *asyncFormicSetAttrResponse, 1)}
	}
	for i := 0; i < cap(f.freeSetAttrResChan); i++ {
		f.freeSetAttrResChan <- &asyncFormicSetAttrResponse{}
	}
	go f.handleSetAttr()

	f.pendingSetXAttrReqChan = make(chan *asyncFormicSetXAttrRequest, concurrency)
	f.freeSetXAttrReqChan = make(chan *asyncFormicSetXAttrRequest, concurrency)
	f.freeSetXAttrResChan = make(chan *asyncFormicSetXAttrResponse, concurrency)
	for i := 0; i < cap(f.freeSetXAttrReqChan); i++ {
		f.freeSetXAttrReqChan <- &asyncFormicSetXAttrRequest{resChan: make(chan *asyncFormicSetXAttrResponse, 1)}
	}
	for i := 0; i < cap(f.freeSetXAttrResChan); i++ {
		f.freeSetXAttrResChan <- &asyncFormicSetXAttrResponse{}
	}
	go f.handleSetXAttr()

	f.pendingShowFSReqChan = make(chan *asyncFormicShowFSRequest, concurrency)
	f.freeShowFSReqChan = make(chan *asyncFormicShowFSRequest, concurrency)
	f.freeShowFSResChan = make(chan *asyncFormicShowFSResponse, concurrency)
	for i := 0; i < cap(f.freeShowFSReqChan); i++ {
		f.freeShowFSReqChan <- &asyncFormicShowFSRequest{resChan: make(chan *asyncFormicShowFSResponse, 1)}
	}
	for i := 0; i < cap(f.freeShowFSResChan); i++ {
		f.freeShowFSResChan <- &asyncFormicShowFSResponse{}
	}
	go f.handleShowFS()

	f.pendingStatFSReqChan = make(chan *asyncFormicStatFSRequest, concurrency)
	f.freeStatFSReqChan = make(chan *asyncFormicStatFSRequest, concurrency)
	f.freeStatFSResChan = make(chan *asyncFormicStatFSResponse, concurrency)
	for i := 0; i < cap(f.freeStatFSReqChan); i++ {
		f.freeStatFSReqChan <- &asyncFormicStatFSRequest{resChan: make(chan *asyncFormicStatFSResponse, 1)}
	}
	for i := 0; i < cap(f.freeStatFSResChan); i++ {
		f.freeStatFSResChan <- &asyncFormicStatFSResponse{}
	}
	go f.handleStatFS()

	f.pendingSymLinkReqChan = make(chan *asyncFormicSymLinkRequest, concurrency)
	f.freeSymLinkReqChan = make(chan *asyncFormicSymLinkRequest, concurrency)
	f.freeSymLinkResChan = make(chan *asyncFormicSymLinkResponse, concurrency)
	for i := 0; i < cap(f.freeSymLinkReqChan); i++ {
		f.freeSymLinkReqChan <- &asyncFormicSymLinkRequest{resChan: make(chan *asyncFormicSymLinkResponse, 1)}
	}
	for i := 0; i < cap(f.freeSymLinkResChan); i++ {
		f.freeSymLinkResChan <- &asyncFormicSymLinkResponse{}
	}
	go f.handleSymLink()

	f.pendingUpdateFSReqChan = make(chan *asyncFormicUpdateFSRequest, concurrency)
	f.freeUpdateFSReqChan = make(chan *asyncFormicUpdateFSRequest, concurrency)
	f.freeUpdateFSResChan = make(chan *asyncFormicUpdateFSResponse, concurrency)
	for i := 0; i < cap(f.freeUpdateFSReqChan); i++ {
		f.freeUpdateFSReqChan <- &asyncFormicUpdateFSRequest{resChan: make(chan *asyncFormicUpdateFSResponse, 1)}
	}
	for i := 0; i < cap(f.freeUpdateFSResChan); i++ {
		f.freeUpdateFSResChan <- &asyncFormicUpdateFSResponse{}
	}
	go f.handleUpdateFS()

	f.pendingWriteReqChan = make(chan *asyncFormicWriteRequest, concurrency)
	f.freeWriteReqChan = make(chan *asyncFormicWriteRequest, concurrency)
	f.freeWriteResChan = make(chan *asyncFormicWriteResponse, concurrency)
	for i := 0; i < cap(f.freeWriteReqChan); i++ {
		f.freeWriteReqChan <- &asyncFormicWriteRequest{resChan: make(chan *asyncFormicWriteResponse, 1)}
	}
	for i := 0; i < cap(f.freeWriteResChan); i++ {
		f.freeWriteResChan <- &asyncFormicWriteResponse{}
	}
	go f.handleWrite()

	return f
}

func (f *Formic) Startup(ctx context.Context) error {
	f.lock.Lock()
	err := f.startup()
	f.lock.Unlock()
	return err
}

func (f *Formic) startup() error {
	if f.conn != nil {
		return nil
	}
	var err error
	creds, err := ftls.NewGRPCClientDialOpt(f.ftlsc)
	if err != nil {
		f.conn = nil
		return err
	}
	opts := make([]grpc.DialOption, len(f.opts))
	copy(opts, f.opts)
	opts = append(opts, creds)
	f.conn, err = grpc.Dial(f.addr, opts...)
	if err != nil {
		f.conn = nil
		return err
	}
	f.client = pb.NewFormicClient(f.conn)
	return nil
}

// Shutdown will close any existing connections; note that Startup may
// automatically get called with any further activity, but it will use a new
// connection. To ensure the Formic has no further activity, use Close.
func (f *Formic) Shutdown(ctx context.Context) error {
	f.lock.Lock()
	err := f.shutdown()
	f.lock.Unlock()
	return err
}

func (f *Formic) shutdown() error {
	if f.conn == nil {
		return nil
	}
	f.conn.Close()
	f.conn = nil
	f.client = nil
	return nil
}

// Close will shutdown outgoing connectivity and stop all background
// goroutines; note that the Formic is no longer usable after a call to
// Close, including using Startup.
func (f *Formic) Close() {
	f.lock.Lock()
	f.shutdown()
	close(f.handlersDoneChan)
	f.lock.Unlock()
}

type asyncFormicCheckRequest struct {
	req          pb.CheckRequest
	resChan      chan *asyncFormicCheckResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicCheckResponse struct {
	res *pb.CheckResponse
	err error
}

func (f *Formic) handleCheck() {
	resChan := make(chan *asyncFormicCheckResponse, cap(f.freeCheckReqChan))
	resFunc := func(stream pb.Formic_CheckClient) {
		var err error
		var res *asyncFormicCheckResponse
		for {
			select {
			case res = <-f.freeCheckResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_CheckClient
	waitingMax := uint32(cap(f.freeCheckReqChan)) - 1
	waiting := make([]*asyncFormicCheckRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingCheckReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingCheckReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeCheckResChan
						res.err = err
						res.res = &pb.CheckResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.Check(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeCheckResChan
					res.err = err
					res.res = &pb.CheckResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeCheckResChan
				res.err = err
				res.res = &pb.CheckResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicCheckRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeCheckResChan <- res
				go func(reqs []*asyncFormicCheckRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeCheckResChan
						res.err = err
						res.res = &pb.CheckResponse{RPCID: req.req.RPCID}
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
				f.freeCheckReqChan <- req
				f.freeCheckResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) Check(ctx context.Context, parentINode uint64, childName string) (response string, err error) {

	var req *asyncFormicCheckRequest
	select {
	case req = <-f.freeCheckReqChan:
	case <-ctx.Done():

		return "", ctx.Err()

	}
	req.canceled = false

	req.req.ParentINode = parentINode
	req.req.ChildName = childName

	select {
	case f.pendingCheckReqChan <- req:
	case <-ctx.Done():
		f.freeCheckReqChan <- req

		return "", ctx.Err()

	}
	var res *asyncFormicCheckResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeCheckResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return "", ctx.Err()

	}
	f.freeCheckReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeCheckResChan <- res

		return "", err

	}

	response = res.res.Response

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeCheckResChan <- res

	return response, err

}

type asyncFormicCreateFSRequest struct {
	req          pb.CreateFSRequest
	resChan      chan *asyncFormicCreateFSResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicCreateFSResponse struct {
	res *pb.CreateFSResponse
	err error
}

func (f *Formic) handleCreateFS() {
	resChan := make(chan *asyncFormicCreateFSResponse, cap(f.freeCreateFSReqChan))
	resFunc := func(stream pb.Formic_CreateFSClient) {
		var err error
		var res *asyncFormicCreateFSResponse
		for {
			select {
			case res = <-f.freeCreateFSResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_CreateFSClient
	waitingMax := uint32(cap(f.freeCreateFSReqChan)) - 1
	waiting := make([]*asyncFormicCreateFSRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingCreateFSReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingCreateFSReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeCreateFSResChan
						res.err = err
						res.res = &pb.CreateFSResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.CreateFS(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeCreateFSResChan
					res.err = err
					res.res = &pb.CreateFSResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeCreateFSResChan
				res.err = err
				res.res = &pb.CreateFSResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicCreateFSRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeCreateFSResChan <- res
				go func(reqs []*asyncFormicCreateFSRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeCreateFSResChan
						res.err = err
						res.res = &pb.CreateFSResponse{RPCID: req.req.RPCID}
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
				f.freeCreateFSReqChan <- req
				f.freeCreateFSResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) CreateFS(ctx context.Context, token string, name string) (fsid string, err error) {

	var req *asyncFormicCreateFSRequest
	select {
	case req = <-f.freeCreateFSReqChan:
	case <-ctx.Done():

		return "", ctx.Err()

	}
	req.canceled = false

	req.req.Token = token
	req.req.Name = name

	select {
	case f.pendingCreateFSReqChan <- req:
	case <-ctx.Done():
		f.freeCreateFSReqChan <- req

		return "", ctx.Err()

	}
	var res *asyncFormicCreateFSResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeCreateFSResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return "", ctx.Err()

	}
	f.freeCreateFSReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeCreateFSResChan <- res

		return "", err

	}

	fsid = res.res.FSID

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeCreateFSResChan <- res

	return fsid, err

}

type asyncFormicCreateRequest struct {
	req          pb.CreateRequest
	resChan      chan *asyncFormicCreateResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicCreateResponse struct {
	res *pb.CreateResponse
	err error
}

func (f *Formic) handleCreate() {
	resChan := make(chan *asyncFormicCreateResponse, cap(f.freeCreateReqChan))
	resFunc := func(stream pb.Formic_CreateClient) {
		var err error
		var res *asyncFormicCreateResponse
		for {
			select {
			case res = <-f.freeCreateResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_CreateClient
	waitingMax := uint32(cap(f.freeCreateReqChan)) - 1
	waiting := make([]*asyncFormicCreateRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingCreateReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingCreateReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeCreateResChan
						res.err = err
						res.res = &pb.CreateResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.Create(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeCreateResChan
					res.err = err
					res.res = &pb.CreateResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeCreateResChan
				res.err = err
				res.res = &pb.CreateResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicCreateRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeCreateResChan <- res
				go func(reqs []*asyncFormicCreateRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeCreateResChan
						res.err = err
						res.res = &pb.CreateResponse{RPCID: req.req.RPCID}
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
				f.freeCreateReqChan <- req
				f.freeCreateResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) Create(ctx context.Context, parentINode uint64, childName string, childAttr *pb.Attr) (resultingChildAttr *pb.Attr, err error) {

	var req *asyncFormicCreateRequest
	select {
	case req = <-f.freeCreateReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.ParentINode = parentINode
	req.req.ChildName = childName
	req.req.ChildAttr = childAttr

	select {
	case f.pendingCreateReqChan <- req:
	case <-ctx.Done():
		f.freeCreateReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncFormicCreateResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeCreateResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	f.freeCreateReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeCreateResChan <- res

		return nil, err

	}

	resultingChildAttr = res.res.ChildAttr

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeCreateResChan <- res

	return resultingChildAttr, err

}

type asyncFormicDeleteFSRequest struct {
	req          pb.DeleteFSRequest
	resChan      chan *asyncFormicDeleteFSResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicDeleteFSResponse struct {
	res *pb.DeleteFSResponse
	err error
}

func (f *Formic) handleDeleteFS() {
	resChan := make(chan *asyncFormicDeleteFSResponse, cap(f.freeDeleteFSReqChan))
	resFunc := func(stream pb.Formic_DeleteFSClient) {
		var err error
		var res *asyncFormicDeleteFSResponse
		for {
			select {
			case res = <-f.freeDeleteFSResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_DeleteFSClient
	waitingMax := uint32(cap(f.freeDeleteFSReqChan)) - 1
	waiting := make([]*asyncFormicDeleteFSRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingDeleteFSReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingDeleteFSReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeDeleteFSResChan
						res.err = err
						res.res = &pb.DeleteFSResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.DeleteFS(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeDeleteFSResChan
					res.err = err
					res.res = &pb.DeleteFSResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeDeleteFSResChan
				res.err = err
				res.res = &pb.DeleteFSResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicDeleteFSRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeDeleteFSResChan <- res
				go func(reqs []*asyncFormicDeleteFSRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeDeleteFSResChan
						res.err = err
						res.res = &pb.DeleteFSResponse{RPCID: req.req.RPCID}
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
				f.freeDeleteFSReqChan <- req
				f.freeDeleteFSResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) DeleteFS(ctx context.Context, token string, fsid string) (err error) {

	var req *asyncFormicDeleteFSRequest
	select {
	case req = <-f.freeDeleteFSReqChan:
	case <-ctx.Done():

		return ctx.Err()

	}
	req.canceled = false

	req.req.Token = token
	req.req.FSID = fsid

	select {
	case f.pendingDeleteFSReqChan <- req:
	case <-ctx.Done():
		f.freeDeleteFSReqChan <- req

		return ctx.Err()

	}
	var res *asyncFormicDeleteFSResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeDeleteFSResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return ctx.Err()

	}
	f.freeDeleteFSReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeDeleteFSResChan <- res

		return err

	}

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeDeleteFSResChan <- res

	return err

}

type asyncFormicGetAttrRequest struct {
	req          pb.GetAttrRequest
	resChan      chan *asyncFormicGetAttrResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicGetAttrResponse struct {
	res *pb.GetAttrResponse
	err error
}

func (f *Formic) handleGetAttr() {
	resChan := make(chan *asyncFormicGetAttrResponse, cap(f.freeGetAttrReqChan))
	resFunc := func(stream pb.Formic_GetAttrClient) {
		var err error
		var res *asyncFormicGetAttrResponse
		for {
			select {
			case res = <-f.freeGetAttrResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_GetAttrClient
	waitingMax := uint32(cap(f.freeGetAttrReqChan)) - 1
	waiting := make([]*asyncFormicGetAttrRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingGetAttrReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingGetAttrReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeGetAttrResChan
						res.err = err
						res.res = &pb.GetAttrResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.GetAttr(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeGetAttrResChan
					res.err = err
					res.res = &pb.GetAttrResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeGetAttrResChan
				res.err = err
				res.res = &pb.GetAttrResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicGetAttrRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeGetAttrResChan <- res
				go func(reqs []*asyncFormicGetAttrRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeGetAttrResChan
						res.err = err
						res.res = &pb.GetAttrResponse{RPCID: req.req.RPCID}
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
				f.freeGetAttrReqChan <- req
				f.freeGetAttrResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) GetAttr(ctx context.Context, iNode uint64) (attr *pb.Attr, err error) {

	var req *asyncFormicGetAttrRequest
	select {
	case req = <-f.freeGetAttrReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.INode = iNode

	select {
	case f.pendingGetAttrReqChan <- req:
	case <-ctx.Done():
		f.freeGetAttrReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncFormicGetAttrResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeGetAttrResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	f.freeGetAttrReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeGetAttrResChan <- res

		return nil, err

	}

	attr = res.res.Attr

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeGetAttrResChan <- res

	return attr, err

}

type asyncFormicGetXAttrRequest struct {
	req          pb.GetXAttrRequest
	resChan      chan *asyncFormicGetXAttrResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicGetXAttrResponse struct {
	res *pb.GetXAttrResponse
	err error
}

func (f *Formic) handleGetXAttr() {
	resChan := make(chan *asyncFormicGetXAttrResponse, cap(f.freeGetXAttrReqChan))
	resFunc := func(stream pb.Formic_GetXAttrClient) {
		var err error
		var res *asyncFormicGetXAttrResponse
		for {
			select {
			case res = <-f.freeGetXAttrResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_GetXAttrClient
	waitingMax := uint32(cap(f.freeGetXAttrReqChan)) - 1
	waiting := make([]*asyncFormicGetXAttrRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingGetXAttrReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingGetXAttrReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeGetXAttrResChan
						res.err = err
						res.res = &pb.GetXAttrResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.GetXAttr(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeGetXAttrResChan
					res.err = err
					res.res = &pb.GetXAttrResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeGetXAttrResChan
				res.err = err
				res.res = &pb.GetXAttrResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicGetXAttrRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeGetXAttrResChan <- res
				go func(reqs []*asyncFormicGetXAttrRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeGetXAttrResChan
						res.err = err
						res.res = &pb.GetXAttrResponse{RPCID: req.req.RPCID}
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
				f.freeGetXAttrReqChan <- req
				f.freeGetXAttrResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) GetXAttr(ctx context.Context, iNode uint64, name string, size uint32, position uint32) (xAttr []byte, err error) {

	var req *asyncFormicGetXAttrRequest
	select {
	case req = <-f.freeGetXAttrReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.INode = iNode
	req.req.Name = name
	req.req.Size = size
	req.req.Position = position

	select {
	case f.pendingGetXAttrReqChan <- req:
	case <-ctx.Done():
		f.freeGetXAttrReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncFormicGetXAttrResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeGetXAttrResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	f.freeGetXAttrReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeGetXAttrResChan <- res

		return nil, err

	}

	xAttr = res.res.XAttr

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeGetXAttrResChan <- res

	return xAttr, err

}

type asyncFormicGrantAddressFSRequest struct {
	req          pb.GrantAddressFSRequest
	resChan      chan *asyncFormicGrantAddressFSResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicGrantAddressFSResponse struct {
	res *pb.GrantAddressFSResponse
	err error
}

func (f *Formic) handleGrantAddressFS() {
	resChan := make(chan *asyncFormicGrantAddressFSResponse, cap(f.freeGrantAddressFSReqChan))
	resFunc := func(stream pb.Formic_GrantAddressFSClient) {
		var err error
		var res *asyncFormicGrantAddressFSResponse
		for {
			select {
			case res = <-f.freeGrantAddressFSResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_GrantAddressFSClient
	waitingMax := uint32(cap(f.freeGrantAddressFSReqChan)) - 1
	waiting := make([]*asyncFormicGrantAddressFSRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingGrantAddressFSReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingGrantAddressFSReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeGrantAddressFSResChan
						res.err = err
						res.res = &pb.GrantAddressFSResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.GrantAddressFS(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeGrantAddressFSResChan
					res.err = err
					res.res = &pb.GrantAddressFSResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeGrantAddressFSResChan
				res.err = err
				res.res = &pb.GrantAddressFSResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicGrantAddressFSRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeGrantAddressFSResChan <- res
				go func(reqs []*asyncFormicGrantAddressFSRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeGrantAddressFSResChan
						res.err = err
						res.res = &pb.GrantAddressFSResponse{RPCID: req.req.RPCID}
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
				f.freeGrantAddressFSReqChan <- req
				f.freeGrantAddressFSResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) GrantAddressFS(ctx context.Context, token string, fsid string, address string) (err error) {

	var req *asyncFormicGrantAddressFSRequest
	select {
	case req = <-f.freeGrantAddressFSReqChan:
	case <-ctx.Done():

		return ctx.Err()

	}
	req.canceled = false

	req.req.Token = token
	req.req.FSID = fsid
	req.req.Address = address

	select {
	case f.pendingGrantAddressFSReqChan <- req:
	case <-ctx.Done():
		f.freeGrantAddressFSReqChan <- req

		return ctx.Err()

	}
	var res *asyncFormicGrantAddressFSResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeGrantAddressFSResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return ctx.Err()

	}
	f.freeGrantAddressFSReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeGrantAddressFSResChan <- res

		return ctx.Err()

	}

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeGrantAddressFSResChan <- res

	return err

}

type asyncFormicInitFSRequest struct {
	req          pb.InitFSRequest
	resChan      chan *asyncFormicInitFSResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicInitFSResponse struct {
	res *pb.InitFSResponse
	err error
}

func (f *Formic) handleInitFS() {
	resChan := make(chan *asyncFormicInitFSResponse, cap(f.freeInitFSReqChan))
	resFunc := func(stream pb.Formic_InitFSClient) {
		var err error
		var res *asyncFormicInitFSResponse
		for {
			select {
			case res = <-f.freeInitFSResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_InitFSClient
	waitingMax := uint32(cap(f.freeInitFSReqChan)) - 1
	waiting := make([]*asyncFormicInitFSRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingInitFSReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingInitFSReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeInitFSResChan
						res.err = err
						res.res = &pb.InitFSResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.InitFS(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeInitFSResChan
					res.err = err
					res.res = &pb.InitFSResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeInitFSResChan
				res.err = err
				res.res = &pb.InitFSResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicInitFSRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeInitFSResChan <- res
				go func(reqs []*asyncFormicInitFSRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeInitFSResChan
						res.err = err
						res.res = &pb.InitFSResponse{RPCID: req.req.RPCID}
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
				f.freeInitFSReqChan <- req
				f.freeInitFSResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) InitFS(ctx context.Context) (err error) {

	var req *asyncFormicInitFSRequest
	select {
	case req = <-f.freeInitFSReqChan:
	case <-ctx.Done():

		return ctx.Err()

	}
	req.canceled = false

	select {
	case f.pendingInitFSReqChan <- req:
	case <-ctx.Done():
		f.freeInitFSReqChan <- req

		return ctx.Err()

	}
	var res *asyncFormicInitFSResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeInitFSResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return ctx.Err()

	}
	f.freeInitFSReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeInitFSResChan <- res

		return ctx.Err()

	}

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeInitFSResChan <- res

	return err

}

type asyncFormicListFSRequest struct {
	req          pb.ListFSRequest
	resChan      chan *asyncFormicListFSResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicListFSResponse struct {
	res *pb.ListFSResponse
	err error
}

func (f *Formic) handleListFS() {
	resChan := make(chan *asyncFormicListFSResponse, cap(f.freeListFSReqChan))
	resFunc := func(stream pb.Formic_ListFSClient) {
		var err error
		var res *asyncFormicListFSResponse
		for {
			select {
			case res = <-f.freeListFSResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_ListFSClient
	waitingMax := uint32(cap(f.freeListFSReqChan)) - 1
	waiting := make([]*asyncFormicListFSRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingListFSReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingListFSReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeListFSResChan
						res.err = err
						res.res = &pb.ListFSResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.ListFS(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeListFSResChan
					res.err = err
					res.res = &pb.ListFSResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeListFSResChan
				res.err = err
				res.res = &pb.ListFSResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicListFSRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeListFSResChan <- res
				go func(reqs []*asyncFormicListFSRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeListFSResChan
						res.err = err
						res.res = &pb.ListFSResponse{RPCID: req.req.RPCID}
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
				f.freeListFSReqChan <- req
				f.freeListFSResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) ListFS(ctx context.Context, token string) (list []*pb.FSIDName, err error) {

	var req *asyncFormicListFSRequest
	select {
	case req = <-f.freeListFSReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.Token = token

	select {
	case f.pendingListFSReqChan <- req:
	case <-ctx.Done():
		f.freeListFSReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncFormicListFSResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeListFSResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	f.freeListFSReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeListFSResChan <- res

		return nil, ctx.Err()

	}

	list = res.res.List

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeListFSResChan <- res

	return list, err

}

type asyncFormicListXAttrRequest struct {
	req          pb.ListXAttrRequest
	resChan      chan *asyncFormicListXAttrResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicListXAttrResponse struct {
	res *pb.ListXAttrResponse
	err error
}

func (f *Formic) handleListXAttr() {
	resChan := make(chan *asyncFormicListXAttrResponse, cap(f.freeListXAttrReqChan))
	resFunc := func(stream pb.Formic_ListXAttrClient) {
		var err error
		var res *asyncFormicListXAttrResponse
		for {
			select {
			case res = <-f.freeListXAttrResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_ListXAttrClient
	waitingMax := uint32(cap(f.freeListXAttrReqChan)) - 1
	waiting := make([]*asyncFormicListXAttrRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingListXAttrReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingListXAttrReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeListXAttrResChan
						res.err = err
						res.res = &pb.ListXAttrResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.ListXAttr(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeListXAttrResChan
					res.err = err
					res.res = &pb.ListXAttrResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeListXAttrResChan
				res.err = err
				res.res = &pb.ListXAttrResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicListXAttrRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeListXAttrResChan <- res
				go func(reqs []*asyncFormicListXAttrRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeListXAttrResChan
						res.err = err
						res.res = &pb.ListXAttrResponse{RPCID: req.req.RPCID}
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
				f.freeListXAttrReqChan <- req
				f.freeListXAttrResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) ListXAttr(ctx context.Context, iNode uint64, size uint32, position uint32) (xAttr []byte, err error) {

	var req *asyncFormicListXAttrRequest
	select {
	case req = <-f.freeListXAttrReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.INode = iNode
	req.req.Size = size
	req.req.Position = position

	select {
	case f.pendingListXAttrReqChan <- req:
	case <-ctx.Done():
		f.freeListXAttrReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncFormicListXAttrResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeListXAttrResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	f.freeListXAttrReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeListXAttrResChan <- res

		return nil, ctx.Err()

	}

	xAttr = res.res.XAttr

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeListXAttrResChan <- res

	return xAttr, err

}

type asyncFormicLookupRequest struct {
	req          pb.LookupRequest
	resChan      chan *asyncFormicLookupResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicLookupResponse struct {
	res *pb.LookupResponse
	err error
}

func (f *Formic) handleLookup() {
	resChan := make(chan *asyncFormicLookupResponse, cap(f.freeLookupReqChan))
	resFunc := func(stream pb.Formic_LookupClient) {
		var err error
		var res *asyncFormicLookupResponse
		for {
			select {
			case res = <-f.freeLookupResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_LookupClient
	waitingMax := uint32(cap(f.freeLookupReqChan)) - 1
	waiting := make([]*asyncFormicLookupRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingLookupReqChan:
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
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeLookupResChan
						res.err = err
						res.res = &pb.LookupResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.Lookup(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeLookupResChan
					res.err = err
					res.res = &pb.LookupResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeLookupResChan
				res.err = err
				res.res = &pb.LookupResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicLookupRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeLookupResChan <- res
				go func(reqs []*asyncFormicLookupRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeLookupResChan
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
				f.freeLookupReqChan <- req
				f.freeLookupResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) Lookup(ctx context.Context, parent uint64, name string) (attr *pb.Attr, err error) {

	var req *asyncFormicLookupRequest
	select {
	case req = <-f.freeLookupReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.Parent = parent
	req.req.Name = name

	select {
	case f.pendingLookupReqChan <- req:
	case <-ctx.Done():
		f.freeLookupReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncFormicLookupResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeLookupResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	f.freeLookupReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeLookupResChan <- res

		return nil, ctx.Err()

	}

	attr = res.res.Attr

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeLookupResChan <- res

	return attr, err

}

type asyncFormicMkDirRequest struct {
	req          pb.MkDirRequest
	resChan      chan *asyncFormicMkDirResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicMkDirResponse struct {
	res *pb.MkDirResponse
	err error
}

func (f *Formic) handleMkDir() {
	resChan := make(chan *asyncFormicMkDirResponse, cap(f.freeMkDirReqChan))
	resFunc := func(stream pb.Formic_MkDirClient) {
		var err error
		var res *asyncFormicMkDirResponse
		for {
			select {
			case res = <-f.freeMkDirResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_MkDirClient
	waitingMax := uint32(cap(f.freeMkDirReqChan)) - 1
	waiting := make([]*asyncFormicMkDirRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingMkDirReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingMkDirReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeMkDirResChan
						res.err = err
						res.res = &pb.MkDirResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.MkDir(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeMkDirResChan
					res.err = err
					res.res = &pb.MkDirResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeMkDirResChan
				res.err = err
				res.res = &pb.MkDirResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicMkDirRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeMkDirResChan <- res
				go func(reqs []*asyncFormicMkDirRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeMkDirResChan
						res.err = err
						res.res = &pb.MkDirResponse{RPCID: req.req.RPCID}
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
				f.freeMkDirReqChan <- req
				f.freeMkDirResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) MkDir(ctx context.Context, parent uint64, name string, attr *pb.Attr) (resultingAttr *pb.Attr, err error) {

	var req *asyncFormicMkDirRequest
	select {
	case req = <-f.freeMkDirReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.Parent = parent
	req.req.Name = name
	req.req.Attr = attr

	select {
	case f.pendingMkDirReqChan <- req:
	case <-ctx.Done():
		f.freeMkDirReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncFormicMkDirResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeMkDirResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	f.freeMkDirReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeMkDirResChan <- res

		return nil, ctx.Err()

	}

	resultingAttr = res.res.Attr

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeMkDirResChan <- res

	return resultingAttr, err

}

type asyncFormicReadDirAllRequest struct {
	req          pb.ReadDirAllRequest
	resChan      chan *asyncFormicReadDirAllResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicReadDirAllResponse struct {
	res *pb.ReadDirAllResponse
	err error
}

func (f *Formic) handleReadDirAll() {
	resChan := make(chan *asyncFormicReadDirAllResponse, cap(f.freeReadDirAllReqChan))
	resFunc := func(stream pb.Formic_ReadDirAllClient) {
		var err error
		var res *asyncFormicReadDirAllResponse
		for {
			select {
			case res = <-f.freeReadDirAllResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_ReadDirAllClient
	waitingMax := uint32(cap(f.freeReadDirAllReqChan)) - 1
	waiting := make([]*asyncFormicReadDirAllRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingReadDirAllReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingReadDirAllReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeReadDirAllResChan
						res.err = err
						res.res = &pb.ReadDirAllResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.ReadDirAll(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeReadDirAllResChan
					res.err = err
					res.res = &pb.ReadDirAllResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeReadDirAllResChan
				res.err = err
				res.res = &pb.ReadDirAllResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicReadDirAllRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeReadDirAllResChan <- res
				go func(reqs []*asyncFormicReadDirAllRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeReadDirAllResChan
						res.err = err
						res.res = &pb.ReadDirAllResponse{RPCID: req.req.RPCID}
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
				f.freeReadDirAllReqChan <- req
				f.freeReadDirAllResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) ReadDirAll(ctx context.Context, iNode uint64) (readDirAllEnts []*pb.ReadDirAllEnt, err error) {

	var req *asyncFormicReadDirAllRequest
	select {
	case req = <-f.freeReadDirAllReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.INode = iNode

	select {
	case f.pendingReadDirAllReqChan <- req:
	case <-ctx.Done():
		f.freeReadDirAllReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncFormicReadDirAllResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeReadDirAllResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	f.freeReadDirAllReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeReadDirAllResChan <- res

		return nil, ctx.Err()

	}

	readDirAllEnts = res.res.Ents

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeReadDirAllResChan <- res

	return readDirAllEnts, err

}

type asyncFormicReadLinkRequest struct {
	req          pb.ReadLinkRequest
	resChan      chan *asyncFormicReadLinkResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicReadLinkResponse struct {
	res *pb.ReadLinkResponse
	err error
}

func (f *Formic) handleReadLink() {
	resChan := make(chan *asyncFormicReadLinkResponse, cap(f.freeReadLinkReqChan))
	resFunc := func(stream pb.Formic_ReadLinkClient) {
		var err error
		var res *asyncFormicReadLinkResponse
		for {
			select {
			case res = <-f.freeReadLinkResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_ReadLinkClient
	waitingMax := uint32(cap(f.freeReadLinkReqChan)) - 1
	waiting := make([]*asyncFormicReadLinkRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingReadLinkReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingReadLinkReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeReadLinkResChan
						res.err = err
						res.res = &pb.ReadLinkResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.ReadLink(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeReadLinkResChan
					res.err = err
					res.res = &pb.ReadLinkResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeReadLinkResChan
				res.err = err
				res.res = &pb.ReadLinkResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicReadLinkRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeReadLinkResChan <- res
				go func(reqs []*asyncFormicReadLinkRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeReadLinkResChan
						res.err = err
						res.res = &pb.ReadLinkResponse{RPCID: req.req.RPCID}
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
				f.freeReadLinkReqChan <- req
				f.freeReadLinkResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) ReadLink(ctx context.Context, iNode uint64) (target string, err error) {

	var req *asyncFormicReadLinkRequest
	select {
	case req = <-f.freeReadLinkReqChan:
	case <-ctx.Done():

		return "", ctx.Err()

	}
	req.canceled = false

	req.req.INode = iNode

	select {
	case f.pendingReadLinkReqChan <- req:
	case <-ctx.Done():
		f.freeReadLinkReqChan <- req

		return "", ctx.Err()

	}
	var res *asyncFormicReadLinkResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeReadLinkResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return "", ctx.Err()

	}
	f.freeReadLinkReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeReadLinkResChan <- res

		return "", ctx.Err()

	}

	target = res.res.Target

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeReadLinkResChan <- res

	return target, err

}

type asyncFormicReadRequest struct {
	req          pb.ReadRequest
	resChan      chan *asyncFormicReadResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicReadResponse struct {
	res *pb.ReadResponse
	err error
}

func (f *Formic) handleRead() {
	resChan := make(chan *asyncFormicReadResponse, cap(f.freeReadReqChan))
	resFunc := func(stream pb.Formic_ReadClient) {
		var err error
		var res *asyncFormicReadResponse
		for {
			select {
			case res = <-f.freeReadResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_ReadClient
	waitingMax := uint32(cap(f.freeReadReqChan)) - 1
	waiting := make([]*asyncFormicReadRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingReadReqChan:
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
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeReadResChan
						res.err = err
						res.res = &pb.ReadResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.Read(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeReadResChan
					res.err = err
					res.res = &pb.ReadResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeReadResChan
				res.err = err
				res.res = &pb.ReadResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicReadRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeReadResChan <- res
				go func(reqs []*asyncFormicReadRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeReadResChan
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
				f.freeReadReqChan <- req
				f.freeReadResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) Read(ctx context.Context, iNode uint64, offset int64, size int64) (payload []byte, err error) {

	var req *asyncFormicReadRequest
	select {
	case req = <-f.freeReadReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.INode = iNode
	req.req.Offset = offset
	req.req.Size = size

	select {
	case f.pendingReadReqChan <- req:
	case <-ctx.Done():
		f.freeReadReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncFormicReadResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeReadResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	f.freeReadReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeReadResChan <- res

		return nil, ctx.Err()

	}

	payload = res.res.Payload

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeReadResChan <- res

	return payload, err

}

type asyncFormicRemoveRequest struct {
	req          pb.RemoveRequest
	resChan      chan *asyncFormicRemoveResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicRemoveResponse struct {
	res *pb.RemoveResponse
	err error
}

func (f *Formic) handleRemove() {
	resChan := make(chan *asyncFormicRemoveResponse, cap(f.freeRemoveReqChan))
	resFunc := func(stream pb.Formic_RemoveClient) {
		var err error
		var res *asyncFormicRemoveResponse
		for {
			select {
			case res = <-f.freeRemoveResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_RemoveClient
	waitingMax := uint32(cap(f.freeRemoveReqChan)) - 1
	waiting := make([]*asyncFormicRemoveRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingRemoveReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingRemoveReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeRemoveResChan
						res.err = err
						res.res = &pb.RemoveResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.Remove(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeRemoveResChan
					res.err = err
					res.res = &pb.RemoveResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeRemoveResChan
				res.err = err
				res.res = &pb.RemoveResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicRemoveRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeRemoveResChan <- res
				go func(reqs []*asyncFormicRemoveRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeRemoveResChan
						res.err = err
						res.res = &pb.RemoveResponse{RPCID: req.req.RPCID}
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
				f.freeRemoveReqChan <- req
				f.freeRemoveResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) Remove(ctx context.Context, parent uint64, name string) (err error) {

	var req *asyncFormicRemoveRequest
	select {
	case req = <-f.freeRemoveReqChan:
	case <-ctx.Done():

		return ctx.Err()

	}
	req.canceled = false

	req.req.Parent = parent
	req.req.Name = name

	select {
	case f.pendingRemoveReqChan <- req:
	case <-ctx.Done():
		f.freeRemoveReqChan <- req

		return ctx.Err()

	}
	var res *asyncFormicRemoveResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeRemoveResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return ctx.Err()

	}
	f.freeRemoveReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeRemoveResChan <- res

		return ctx.Err()

	}

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeRemoveResChan <- res

	return err

}

type asyncFormicRemoveXAttrRequest struct {
	req          pb.RemoveXAttrRequest
	resChan      chan *asyncFormicRemoveXAttrResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicRemoveXAttrResponse struct {
	res *pb.RemoveXAttrResponse
	err error
}

func (f *Formic) handleRemoveXAttr() {
	resChan := make(chan *asyncFormicRemoveXAttrResponse, cap(f.freeRemoveXAttrReqChan))
	resFunc := func(stream pb.Formic_RemoveXAttrClient) {
		var err error
		var res *asyncFormicRemoveXAttrResponse
		for {
			select {
			case res = <-f.freeRemoveXAttrResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_RemoveXAttrClient
	waitingMax := uint32(cap(f.freeRemoveXAttrReqChan)) - 1
	waiting := make([]*asyncFormicRemoveXAttrRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingRemoveXAttrReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingRemoveXAttrReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeRemoveXAttrResChan
						res.err = err
						res.res = &pb.RemoveXAttrResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.RemoveXAttr(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeRemoveXAttrResChan
					res.err = err
					res.res = &pb.RemoveXAttrResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeRemoveXAttrResChan
				res.err = err
				res.res = &pb.RemoveXAttrResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicRemoveXAttrRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeRemoveXAttrResChan <- res
				go func(reqs []*asyncFormicRemoveXAttrRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeRemoveXAttrResChan
						res.err = err
						res.res = &pb.RemoveXAttrResponse{RPCID: req.req.RPCID}
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
				f.freeRemoveXAttrReqChan <- req
				f.freeRemoveXAttrResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) RemoveXAttr(ctx context.Context, iNode uint64, name string) (err error) {

	var req *asyncFormicRemoveXAttrRequest
	select {
	case req = <-f.freeRemoveXAttrReqChan:
	case <-ctx.Done():

		return ctx.Err()

	}
	req.canceled = false

	req.req.INode = iNode
	req.req.Name = name

	select {
	case f.pendingRemoveXAttrReqChan <- req:
	case <-ctx.Done():
		f.freeRemoveXAttrReqChan <- req

		return ctx.Err()

	}
	var res *asyncFormicRemoveXAttrResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeRemoveXAttrResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return ctx.Err()

	}
	f.freeRemoveXAttrReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeRemoveXAttrResChan <- res

		return ctx.Err()

	}

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeRemoveXAttrResChan <- res

	return err

}

type asyncFormicRenameRequest struct {
	req          pb.RenameRequest
	resChan      chan *asyncFormicRenameResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicRenameResponse struct {
	res *pb.RenameResponse
	err error
}

func (f *Formic) handleRename() {
	resChan := make(chan *asyncFormicRenameResponse, cap(f.freeRenameReqChan))
	resFunc := func(stream pb.Formic_RenameClient) {
		var err error
		var res *asyncFormicRenameResponse
		for {
			select {
			case res = <-f.freeRenameResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_RenameClient
	waitingMax := uint32(cap(f.freeRenameReqChan)) - 1
	waiting := make([]*asyncFormicRenameRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingRenameReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingRenameReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeRenameResChan
						res.err = err
						res.res = &pb.RenameResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.Rename(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeRenameResChan
					res.err = err
					res.res = &pb.RenameResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeRenameResChan
				res.err = err
				res.res = &pb.RenameResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicRenameRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeRenameResChan <- res
				go func(reqs []*asyncFormicRenameRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeRenameResChan
						res.err = err
						res.res = &pb.RenameResponse{RPCID: req.req.RPCID}
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
				f.freeRenameReqChan <- req
				f.freeRenameResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) Rename(ctx context.Context, oldParent uint64, newParent uint64, oldName string, newName string) (err error) {

	var req *asyncFormicRenameRequest
	select {
	case req = <-f.freeRenameReqChan:
	case <-ctx.Done():

		return ctx.Err()

	}
	req.canceled = false

	req.req.OldParent = oldParent
	req.req.NewParent = newParent
	req.req.OldName = oldName
	req.req.NewName = newName

	select {
	case f.pendingRenameReqChan <- req:
	case <-ctx.Done():
		f.freeRenameReqChan <- req

		return ctx.Err()

	}
	var res *asyncFormicRenameResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeRenameResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return ctx.Err()

	}
	f.freeRenameReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeRenameResChan <- res

		return ctx.Err()

	}

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeRenameResChan <- res

	return err

}

type asyncFormicRevokeAddressFSRequest struct {
	req          pb.RevokeAddressFSRequest
	resChan      chan *asyncFormicRevokeAddressFSResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicRevokeAddressFSResponse struct {
	res *pb.RevokeAddressFSResponse
	err error
}

func (f *Formic) handleRevokeAddressFS() {
	resChan := make(chan *asyncFormicRevokeAddressFSResponse, cap(f.freeRevokeAddressFSReqChan))
	resFunc := func(stream pb.Formic_RevokeAddressFSClient) {
		var err error
		var res *asyncFormicRevokeAddressFSResponse
		for {
			select {
			case res = <-f.freeRevokeAddressFSResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_RevokeAddressFSClient
	waitingMax := uint32(cap(f.freeRevokeAddressFSReqChan)) - 1
	waiting := make([]*asyncFormicRevokeAddressFSRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingRevokeAddressFSReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingRevokeAddressFSReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeRevokeAddressFSResChan
						res.err = err
						res.res = &pb.RevokeAddressFSResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.RevokeAddressFS(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeRevokeAddressFSResChan
					res.err = err
					res.res = &pb.RevokeAddressFSResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeRevokeAddressFSResChan
				res.err = err
				res.res = &pb.RevokeAddressFSResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicRevokeAddressFSRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeRevokeAddressFSResChan <- res
				go func(reqs []*asyncFormicRevokeAddressFSRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeRevokeAddressFSResChan
						res.err = err
						res.res = &pb.RevokeAddressFSResponse{RPCID: req.req.RPCID}
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
				f.freeRevokeAddressFSReqChan <- req
				f.freeRevokeAddressFSResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) RevokeAddressFS(ctx context.Context, token string, fsid string, address string) (err error) {

	var req *asyncFormicRevokeAddressFSRequest
	select {
	case req = <-f.freeRevokeAddressFSReqChan:
	case <-ctx.Done():

		return ctx.Err()

	}
	req.canceled = false

	req.req.Token = token
	req.req.FSID = fsid
	req.req.Address = address

	select {
	case f.pendingRevokeAddressFSReqChan <- req:
	case <-ctx.Done():
		f.freeRevokeAddressFSReqChan <- req

		return ctx.Err()

	}
	var res *asyncFormicRevokeAddressFSResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeRevokeAddressFSResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return ctx.Err()

	}
	f.freeRevokeAddressFSReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeRevokeAddressFSResChan <- res

		return ctx.Err()

	}

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeRevokeAddressFSResChan <- res

	return err

}

type asyncFormicSetAttrRequest struct {
	req          pb.SetAttrRequest
	resChan      chan *asyncFormicSetAttrResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicSetAttrResponse struct {
	res *pb.SetAttrResponse
	err error
}

func (f *Formic) handleSetAttr() {
	resChan := make(chan *asyncFormicSetAttrResponse, cap(f.freeSetAttrReqChan))
	resFunc := func(stream pb.Formic_SetAttrClient) {
		var err error
		var res *asyncFormicSetAttrResponse
		for {
			select {
			case res = <-f.freeSetAttrResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_SetAttrClient
	waitingMax := uint32(cap(f.freeSetAttrReqChan)) - 1
	waiting := make([]*asyncFormicSetAttrRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingSetAttrReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingSetAttrReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeSetAttrResChan
						res.err = err
						res.res = &pb.SetAttrResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.SetAttr(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeSetAttrResChan
					res.err = err
					res.res = &pb.SetAttrResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeSetAttrResChan
				res.err = err
				res.res = &pb.SetAttrResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicSetAttrRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeSetAttrResChan <- res
				go func(reqs []*asyncFormicSetAttrRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeSetAttrResChan
						res.err = err
						res.res = &pb.SetAttrResponse{RPCID: req.req.RPCID}
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
				f.freeSetAttrReqChan <- req
				f.freeSetAttrResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) SetAttr(ctx context.Context, attr *pb.Attr, valid uint32) (resultingAttr *pb.Attr, err error) {

	var req *asyncFormicSetAttrRequest
	select {
	case req = <-f.freeSetAttrReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.Attr = attr
	req.req.Valid = valid

	select {
	case f.pendingSetAttrReqChan <- req:
	case <-ctx.Done():
		f.freeSetAttrReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncFormicSetAttrResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeSetAttrResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	f.freeSetAttrReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeSetAttrResChan <- res

		return nil, ctx.Err()

	}

	resultingAttr = res.res.Attr

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeSetAttrResChan <- res

	return resultingAttr, err

}

type asyncFormicSetXAttrRequest struct {
	req          pb.SetXAttrRequest
	resChan      chan *asyncFormicSetXAttrResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicSetXAttrResponse struct {
	res *pb.SetXAttrResponse
	err error
}

func (f *Formic) handleSetXAttr() {
	resChan := make(chan *asyncFormicSetXAttrResponse, cap(f.freeSetXAttrReqChan))
	resFunc := func(stream pb.Formic_SetXAttrClient) {
		var err error
		var res *asyncFormicSetXAttrResponse
		for {
			select {
			case res = <-f.freeSetXAttrResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_SetXAttrClient
	waitingMax := uint32(cap(f.freeSetXAttrReqChan)) - 1
	waiting := make([]*asyncFormicSetXAttrRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingSetXAttrReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingSetXAttrReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeSetXAttrResChan
						res.err = err
						res.res = &pb.SetXAttrResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.SetXAttr(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeSetXAttrResChan
					res.err = err
					res.res = &pb.SetXAttrResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeSetXAttrResChan
				res.err = err
				res.res = &pb.SetXAttrResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicSetXAttrRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeSetXAttrResChan <- res
				go func(reqs []*asyncFormicSetXAttrRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeSetXAttrResChan
						res.err = err
						res.res = &pb.SetXAttrResponse{RPCID: req.req.RPCID}
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
				f.freeSetXAttrReqChan <- req
				f.freeSetXAttrResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) SetXAttr(ctx context.Context, iNode uint64, name string, value []byte, position uint32, flags uint32) (err error) {

	var req *asyncFormicSetXAttrRequest
	select {
	case req = <-f.freeSetXAttrReqChan:
	case <-ctx.Done():

		return ctx.Err()

	}
	req.canceled = false

	req.req.INode = iNode
	req.req.Name = name
	req.req.Value = value
	req.req.Position = position
	req.req.Flags = flags

	select {
	case f.pendingSetXAttrReqChan <- req:
	case <-ctx.Done():
		f.freeSetXAttrReqChan <- req

		return ctx.Err()

	}
	var res *asyncFormicSetXAttrResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeSetXAttrResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return ctx.Err()

	}
	f.freeSetXAttrReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeSetXAttrResChan <- res

		return ctx.Err()

	}

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeSetXAttrResChan <- res

	return err

}

type asyncFormicShowFSRequest struct {
	req          pb.ShowFSRequest
	resChan      chan *asyncFormicShowFSResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicShowFSResponse struct {
	res *pb.ShowFSResponse
	err error
}

func (f *Formic) handleShowFS() {
	resChan := make(chan *asyncFormicShowFSResponse, cap(f.freeShowFSReqChan))
	resFunc := func(stream pb.Formic_ShowFSClient) {
		var err error
		var res *asyncFormicShowFSResponse
		for {
			select {
			case res = <-f.freeShowFSResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_ShowFSClient
	waitingMax := uint32(cap(f.freeShowFSReqChan)) - 1
	waiting := make([]*asyncFormicShowFSRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingShowFSReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingShowFSReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeShowFSResChan
						res.err = err
						res.res = &pb.ShowFSResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.ShowFS(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeShowFSResChan
					res.err = err
					res.res = &pb.ShowFSResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeShowFSResChan
				res.err = err
				res.res = &pb.ShowFSResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicShowFSRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeShowFSResChan <- res
				go func(reqs []*asyncFormicShowFSRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeShowFSResChan
						res.err = err
						res.res = &pb.ShowFSResponse{RPCID: req.req.RPCID}
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
				f.freeShowFSReqChan <- req
				f.freeShowFSResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) ShowFS(ctx context.Context, token string, fsid string) (name string, addresses []string, err error) {

	var req *asyncFormicShowFSRequest
	select {
	case req = <-f.freeShowFSReqChan:
	case <-ctx.Done():

		return "", nil, ctx.Err()

	}
	req.canceled = false

	req.req.Token = token
	req.req.FSID = fsid

	select {
	case f.pendingShowFSReqChan <- req:
	case <-ctx.Done():
		f.freeShowFSReqChan <- req

		return "", nil, ctx.Err()

	}
	var res *asyncFormicShowFSResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeShowFSResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return "", nil, ctx.Err()

	}
	f.freeShowFSReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeShowFSResChan <- res

		return "", nil, ctx.Err()

	}

	name = res.res.Name
	addresses = res.res.Addresses

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeShowFSResChan <- res

	return name, addresses, err

}

type asyncFormicStatFSRequest struct {
	req          pb.StatFSRequest
	resChan      chan *asyncFormicStatFSResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicStatFSResponse struct {
	res *pb.StatFSResponse
	err error
}

func (f *Formic) handleStatFS() {
	resChan := make(chan *asyncFormicStatFSResponse, cap(f.freeStatFSReqChan))
	resFunc := func(stream pb.Formic_StatFSClient) {
		var err error
		var res *asyncFormicStatFSResponse
		for {
			select {
			case res = <-f.freeStatFSResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_StatFSClient
	waitingMax := uint32(cap(f.freeStatFSReqChan)) - 1
	waiting := make([]*asyncFormicStatFSRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingStatFSReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingStatFSReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeStatFSResChan
						res.err = err
						res.res = &pb.StatFSResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.StatFS(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeStatFSResChan
					res.err = err
					res.res = &pb.StatFSResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeStatFSResChan
				res.err = err
				res.res = &pb.StatFSResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicStatFSRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeStatFSResChan <- res
				go func(reqs []*asyncFormicStatFSRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeStatFSResChan
						res.err = err
						res.res = &pb.StatFSResponse{RPCID: req.req.RPCID}
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
				f.freeStatFSReqChan <- req
				f.freeStatFSResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) StatFS(ctx context.Context) (blocks uint64, bFree uint64, bAvail uint64, files uint64, fFree uint64, bSize uint32, nameLen uint32, frSize uint32, err error) {

	var req *asyncFormicStatFSRequest
	select {
	case req = <-f.freeStatFSReqChan:
	case <-ctx.Done():

		return 0, 0, 0, 0, 0, 0, 0, 0, ctx.Err()

	}
	req.canceled = false

	select {
	case f.pendingStatFSReqChan <- req:
	case <-ctx.Done():
		f.freeStatFSReqChan <- req

		return 0, 0, 0, 0, 0, 0, 0, 0, ctx.Err()

	}
	var res *asyncFormicStatFSResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeStatFSResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return 0, 0, 0, 0, 0, 0, 0, 0, ctx.Err()

	}
	f.freeStatFSReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeStatFSResChan <- res

		return 0, 0, 0, 0, 0, 0, 0, 0, ctx.Err()

	}

	blocks = res.res.Blocks
	bFree = res.res.BFree
	bAvail = res.res.BAvail
	files = res.res.Files
	fFree = res.res.FFree
	bSize = res.res.BSize
	nameLen = res.res.NameLen
	frSize = res.res.FrSize

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeStatFSResChan <- res

	return blocks, bFree, bAvail, files, fFree, bSize, nameLen, frSize, err

}

type asyncFormicSymLinkRequest struct {
	req          pb.SymLinkRequest
	resChan      chan *asyncFormicSymLinkResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicSymLinkResponse struct {
	res *pb.SymLinkResponse
	err error
}

func (f *Formic) handleSymLink() {
	resChan := make(chan *asyncFormicSymLinkResponse, cap(f.freeSymLinkReqChan))
	resFunc := func(stream pb.Formic_SymLinkClient) {
		var err error
		var res *asyncFormicSymLinkResponse
		for {
			select {
			case res = <-f.freeSymLinkResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_SymLinkClient
	waitingMax := uint32(cap(f.freeSymLinkReqChan)) - 1
	waiting := make([]*asyncFormicSymLinkRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingSymLinkReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingSymLinkReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeSymLinkResChan
						res.err = err
						res.res = &pb.SymLinkResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.SymLink(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeSymLinkResChan
					res.err = err
					res.res = &pb.SymLinkResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeSymLinkResChan
				res.err = err
				res.res = &pb.SymLinkResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicSymLinkRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeSymLinkResChan <- res
				go func(reqs []*asyncFormicSymLinkRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeSymLinkResChan
						res.err = err
						res.res = &pb.SymLinkResponse{RPCID: req.req.RPCID}
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
				f.freeSymLinkReqChan <- req
				f.freeSymLinkResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) SymLink(ctx context.Context, parent uint64, name string, target string, uid uint32, gid uint32) (attr *pb.Attr, err error) {

	var req *asyncFormicSymLinkRequest
	select {
	case req = <-f.freeSymLinkReqChan:
	case <-ctx.Done():

		return nil, ctx.Err()

	}
	req.canceled = false

	req.req.Parent = parent
	req.req.Name = name
	req.req.Target = target
	req.req.UID = uid
	req.req.GID = gid

	select {
	case f.pendingSymLinkReqChan <- req:
	case <-ctx.Done():
		f.freeSymLinkReqChan <- req

		return nil, ctx.Err()

	}
	var res *asyncFormicSymLinkResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeSymLinkResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return nil, ctx.Err()

	}
	f.freeSymLinkReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeSymLinkResChan <- res

		return nil, ctx.Err()

	}

	attr = res.res.Attr

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeSymLinkResChan <- res

	return attr, err

}

type asyncFormicUpdateFSRequest struct {
	req          pb.UpdateFSRequest
	resChan      chan *asyncFormicUpdateFSResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicUpdateFSResponse struct {
	res *pb.UpdateFSResponse
	err error
}

func (f *Formic) handleUpdateFS() {
	resChan := make(chan *asyncFormicUpdateFSResponse, cap(f.freeUpdateFSReqChan))
	resFunc := func(stream pb.Formic_UpdateFSClient) {
		var err error
		var res *asyncFormicUpdateFSResponse
		for {
			select {
			case res = <-f.freeUpdateFSResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_UpdateFSClient
	waitingMax := uint32(cap(f.freeUpdateFSReqChan)) - 1
	waiting := make([]*asyncFormicUpdateFSRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingUpdateFSReqChan:
			j := waitingIndex
			for waiting[waitingIndex] != nil {
				waitingIndex++
				if waitingIndex > waitingMax {
					waitingIndex = 0
				}
				if waitingIndex == j {
					panic("coding error: got more concurrent requests from pendingUpdateFSReqChan than should be available")
				}
			}
			req.req.RPCID = waitingIndex
			waiting[waitingIndex] = req
			waitingIndex++
			if waitingIndex > waitingMax {
				waitingIndex = 0
			}
			if stream == nil {
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeUpdateFSResChan
						res.err = err
						res.res = &pb.UpdateFSResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.UpdateFS(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeUpdateFSResChan
					res.err = err
					res.res = &pb.UpdateFSResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeUpdateFSResChan
				res.err = err
				res.res = &pb.UpdateFSResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicUpdateFSRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeUpdateFSResChan <- res
				go func(reqs []*asyncFormicUpdateFSRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeUpdateFSResChan
						res.err = err
						res.res = &pb.UpdateFSResponse{RPCID: req.req.RPCID}
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
				f.freeUpdateFSReqChan <- req
				f.freeUpdateFSResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) UpdateFS(ctx context.Context, token string, fsid string, newName string) (err error) {

	var req *asyncFormicUpdateFSRequest
	select {
	case req = <-f.freeUpdateFSReqChan:
	case <-ctx.Done():

		return ctx.Err()

	}
	req.canceled = false

	req.req.Token = token
	req.req.FSID = fsid
	req.req.NewName = newName

	select {
	case f.pendingUpdateFSReqChan <- req:
	case <-ctx.Done():
		f.freeUpdateFSReqChan <- req

		return ctx.Err()

	}
	var res *asyncFormicUpdateFSResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeUpdateFSResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return ctx.Err()

	}
	f.freeUpdateFSReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeUpdateFSResChan <- res

		return ctx.Err()

	}

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeUpdateFSResChan <- res

	return err

}

type asyncFormicWriteRequest struct {
	req          pb.WriteRequest
	resChan      chan *asyncFormicWriteResponse
	canceledLock sync.Mutex
	canceled     bool
}

type asyncFormicWriteResponse struct {
	res *pb.WriteResponse
	err error
}

func (f *Formic) handleWrite() {
	resChan := make(chan *asyncFormicWriteResponse, cap(f.freeWriteReqChan))
	resFunc := func(stream pb.Formic_WriteClient) {
		var err error
		var res *asyncFormicWriteResponse
		for {
			select {
			case res = <-f.freeWriteResChan:
			case <-f.handlersDoneChan:
				return
			}
			res.res, res.err = stream.Recv()
			err = res.err
			if err != nil {
				res.res = nil
			}
			select {
			case resChan <- res:
			case <-f.handlersDoneChan:
				return
			}
			if err != nil {
				return
			}
		}
	}
	var err error
	var stream pb.Formic_WriteClient
	waitingMax := uint32(cap(f.freeWriteReqChan)) - 1
	waiting := make([]*asyncFormicWriteRequest, waitingMax+1)
	waitingIndex := uint32(0)
	for {
		select {
		case req := <-f.pendingWriteReqChan:
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
				f.lock.Lock()
				if f.client == nil {
					if err = f.startup(); err != nil {
						f.lock.Unlock()
						res := <-f.freeWriteResChan
						res.err = err
						res.res = &pb.WriteResponse{RPCID: req.req.RPCID}
						resChan <- res
						break
					}
				}
				stream, err = f.client.Write(metadata.NewContext(context.Background(), metadata.Pairs("fsid", f.fsid)))
				f.lock.Unlock()
				if err != nil {
					res := <-f.freeWriteResChan
					res.err = err
					res.res = &pb.WriteResponse{RPCID: req.req.RPCID}
					resChan <- res
					break
				}
				go resFunc(stream)
			}
			if err = stream.Send(&req.req); err != nil {
				stream = nil
				res := <-f.freeWriteResChan
				res.err = err
				res.res = &pb.WriteResponse{RPCID: req.req.RPCID}
				resChan <- res
			}
		case res := <-resChan:
			if res.res == nil {
				stream = nil
				// Receiver got unrecoverable error, so we'll have to
				// respond with errors to all waiting requests.
				wereWaiting := make([]*asyncFormicWriteRequest, len(waiting))
				for i, v := range waiting {
					wereWaiting[i] = v
				}
				err := res.err
				if err == nil {
					err = errors.New("receiver had error, had to close any other waiting requests")
				}
				f.freeWriteResChan <- res
				go func(reqs []*asyncFormicWriteRequest, err error) {
					for _, req := range reqs {
						if req == nil {
							continue
						}
						res := <-f.freeWriteResChan
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
				f.freeWriteReqChan <- req
				f.freeWriteResChan <- res
			}
			req.canceledLock.Unlock()
		case <-f.handlersDoneChan:
			return
		}
	}
}

func (f *Formic) Write(ctx context.Context, iNode uint64, offset int64, payload []byte) (err error) {

	var req *asyncFormicWriteRequest
	select {
	case req = <-f.freeWriteReqChan:
	case <-ctx.Done():

		return ctx.Err()

	}
	req.canceled = false

	req.req.INode = iNode
	req.req.Offset = offset
	req.req.Payload = payload

	select {
	case f.pendingWriteReqChan <- req:
	case <-ctx.Done():
		f.freeWriteReqChan <- req

		return ctx.Err()

	}
	var res *asyncFormicWriteResponse
	select {
	case res = <-req.resChan:
	case <-ctx.Done():
		req.canceledLock.Lock()
		select {
		case res = <-req.resChan:
			f.freeWriteResChan <- res
		default:
			req.canceled = true
		}
		req.canceledLock.Unlock()

		return ctx.Err()

	}
	f.freeWriteReqChan <- req
	if res.err != nil {
		err = res.err
		f.freeWriteResChan <- res

		return ctx.Err()

	}

	if res.res.Err == "" {
		err = nil
	} else {
		err = errors.New(res.res.Err)
	}
	f.freeWriteResChan <- res

	return err

}
