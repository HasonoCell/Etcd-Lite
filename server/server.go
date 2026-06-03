package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/HasonoCell/Etcd-Lite/api/etcdlitepb"
	"github.com/HasonoCell/Etcd-Lite/mvcc"
	"github.com/HasonoCell/Etcd-Lite/raft/core"
	"google.golang.org/grpc"
)

const (
	defaultRequestTimeout = 3 * time.Second
	maxCachedRequests     = 1024
	maxCachedApplyIndex   = 1024

	errorNotLeader     = "not_leader"
	errorApplyMismatch = "apply_mismatch"
)

var (
	errMissingStore  = errors.New("server: missing mvcc store")
	errNotLeader     = errors.New(errorNotLeader)
	errApplyMismatch = errors.New(errorApplyMismatch)
)

type Config struct {
	ID                core.MemberID
	Peers             []core.MemberID
	RaftStorage       core.Storage
	RaftTransport     core.Transport
	Store             mvcc.Store
	ApplyCh           chan core.ApplyMsg
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	RequestTimeout    time.Duration
}

type Server struct {
	etcdlitepb.UnimplementedKVServer
	etcdlitepb.UnimplementedMaintenanceServer

	raft           *core.Raft // raft 共识层
	store          mvcc.Store // mvcc 状态机
	applyCh        chan core.ApplyMsg
	requestTimeout time.Duration
	stopCh         chan struct{}

	mu sync.Mutex
	// raft index -> 等待该 index apply 的 channel，目的就是为了把全局 applyCh 分发成某个请求正在等的结果。
	waiters map[uint64]chan applyResult
	// 如果 apply 先发生，waiter 后注册，就能从 cache 里直接拿结果。
	indexCache map[uint64]applyResult
	indexOrder []uint64
	// 按 (client_id, request_id) 缓存最近请求结果，做幂等。
	requestCache map[requestKey]applyResult
	requestOrder []requestKey
	// 如果同一个 request 正在进行中，第二个重复请求不要再次提交，而是等同一个 pending chan。
	pending map[requestKey]chan applyResult
	// server 视角下已经 apply 到的最新 Raft index。
	appliedIndex uint64
}

type requestKey struct {
	ClientID  uint64 // 客户端 id
	RequestID uint64 // 请求 id
}

// 校验 ClientID 和 RequestID 是否为 0
func (k requestKey) valid() bool {
	return k.ClientID != 0 && k.RequestID != 0
}

// applyResult 是 server 层将 request，raft 和 mvcc 的信息整合在一起的结构体
type applyResult struct {
	// raft 层
	index uint64 // 这条结果来自哪条 raft log index，用来唤醒 waiters[index]。
	term  uint64 // 那条 raft log apply 时的任期号

	// request 层
	request requestKey

	// mvcc 层
	result mvcc.ApplyResult
	err    error
}

// 新建一个 server
func New(cfg Config) (*Server, error) {
	if cfg.Store == nil {
		return nil, errMissingStore
	}
	if cfg.ApplyCh == nil {
		cfg.ApplyCh = make(chan core.ApplyMsg, 256)
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}

	rf, err := core.New(core.Config{
		ID:                cfg.ID,
		Peers:             cfg.Peers,
		Storage:           cfg.RaftStorage,
		Transport:         cfg.RaftTransport,
		ApplyCh:           cfg.ApplyCh,
		ElectionTimeout:   cfg.ElectionTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
	})
	if err != nil {
		return nil, err
	}

	s := &Server{
		raft:           rf,
		store:          cfg.Store,
		applyCh:        cfg.ApplyCh,
		requestTimeout: cfg.RequestTimeout,
		stopCh:         make(chan struct{}),
		waiters:        make(map[uint64]chan applyResult),
		indexCache:     make(map[uint64]applyResult),
		requestCache:   make(map[requestKey]applyResult),
		pending:        make(map[requestKey]chan applyResult),
		appliedIndex:   rf.Status().AppliedIndex,
	}
	go s.applyLoop()
	return s, nil
}

// Register 将 KV 和 Maintenance service 注册到同一个 gRPC server 上。
func (s *Server) Register(grpcServer *grpc.Server) {
	etcdlitepb.RegisterKVServer(grpcServer, s)
	etcdlitepb.RegisterMaintenanceServer(grpcServer, s)
}

func (s *Server) Stop() {
	select {
	case <-s.stopCh:
		return
	default:
		close(s.stopCh)
		s.raft.Stop()
	}
}

func (s *Server) Raft() *core.Raft {
	return s.raft
}

// range 读请求
func (s *Server) Range(ctx context.Context, req *etcdlitepb.RangeRequest) (*etcdlitepb.RangeResponse, error) {
	ctx, cancel := s.withRequestTimeout(ctx)
	defer cancel()

	var readIndex uint64
	if !req.GetSerializable() {
		status := s.raft.Status()
		if status.State != core.StateLeader {
			return &etcdlitepb.RangeResponse{Header: s.errorHeader(errorNotLeader, 0)}, nil
		}
		// range 请求不走日志而是 readindex
		index, err := s.raft.ReadIndex(ctx)
		if err != nil {
			return &etcdlitepb.RangeResponse{Header: s.errorHeader(errorNotLeader, 0)}, nil
		}
		readIndex = index
		if err := s.waitApplied(ctx, readIndex); err != nil {
			return &etcdlitepb.RangeResponse{Header: s.errorHeader(err.Error(), readIndex)}, nil
		}
	}

	resp, err := s.store.Range(mvcc.RangeRequest{
		Key:      req.GetKey(),
		End:      req.GetEnd(),
		Limit:    req.GetLimit(),
		Revision: req.GetRevision(),
	})
	if err != nil {
		return &etcdlitepb.RangeResponse{Header: s.errorHeader(err.Error(), readIndex)}, nil
	}
	return &etcdlitepb.RangeResponse{
		Header: s.header(resp.Revision, readIndex, ""),
		Kvs:    keyValuesToProto(resp.KVs),
		Count:  resp.Count,
	}, nil
}

// put 写请求
func (s *Server) Put(ctx context.Context, req *etcdlitepb.PutRequest) (*etcdlitepb.PutResponse, error) {
	ctx, cancel := s.withRequestTimeout(ctx)
	defer cancel()

	// 先将 grpc request 转换为 command
	command := mvcc.Command{
		ID:   mvcc.RequestID{ClientID: req.GetClientId(), RequestID: req.GetRequestId()},
		Kind: mvcc.CommandPut,
		Put: &mvcc.PutCommand{
			Key:     req.GetKey(),
			Value:   req.GetValue(),
			LeaseID: req.GetLeaseId(),
			PrevKV:  req.GetPrevKv(),
		},
	}

	// 提交到 raft 并等待 apply
	result, err := s.submit(ctx, command)
	if err != nil {
		return &etcdlitepb.PutResponse{Header: s.errorHeader(errorString(err), 0)}, nil
	}
	if len(result.result.Responses) == 0 || result.result.Responses[0].Put == nil {
		return &etcdlitepb.PutResponse{Header: s.errorHeader(errorApplyMismatch, result.index)}, nil
	}
	put := result.result.Responses[0].Put

	// 把 mvcc.ApplyResult 转回 proto response
	return &etcdlitepb.PutResponse{
		Header: s.header(put.Revision, result.index, ""),
		PrevKv: keyValuePtrToProto(put.PrevKV),
	}, nil
}

// delete 写请求
func (s *Server) DeleteRange(ctx context.Context, req *etcdlitepb.DeleteRangeRequest) (*etcdlitepb.DeleteRangeResponse, error) {
	ctx, cancel := s.withRequestTimeout(ctx)
	defer cancel()

	command := mvcc.Command{
		ID:   mvcc.RequestID{ClientID: req.GetClientId(), RequestID: req.GetRequestId()},
		Kind: mvcc.CommandDeleteRange,
		DeleteRange: &mvcc.DeleteRangeCommand{
			Key:    req.GetKey(),
			End:    req.GetEnd(),
			PrevKV: req.GetPrevKv(),
		},
	}
	result, err := s.submit(ctx, command)
	if err != nil {
		return &etcdlitepb.DeleteRangeResponse{Header: s.errorHeader(errorString(err), 0)}, nil
	}
	if len(result.result.Responses) == 0 || result.result.Responses[0].DeleteRange == nil {
		return &etcdlitepb.DeleteRangeResponse{Header: s.errorHeader(errorApplyMismatch, result.index)}, nil
	}
	del := result.result.Responses[0].DeleteRange
	return &etcdlitepb.DeleteRangeResponse{
		Header:  s.header(del.Revision, result.index, ""),
		Deleted: del.Deleted,
		PrevKvs: keyValuesToProto(del.PrevKVs),
	}, nil
}

// txn 写请求，compare 和 success/failure branch 都通过 Raft log 达成全序一致。
func (s *Server) Txn(ctx context.Context, req *etcdlitepb.TxnRequest) (*etcdlitepb.TxnResponse, error) {
	ctx, cancel := s.withRequestTimeout(ctx)
	defer cancel()

	command, err := txnRequestToCommand(req)
	if err != nil {
		return &etcdlitepb.TxnResponse{Header: s.errorHeader(errorString(err), 0)}, nil
	}

	result, err := s.submit(ctx, command)
	if err != nil {
		return &etcdlitepb.TxnResponse{Header: s.errorHeader(errorString(err), 0)}, nil
	}
	return &etcdlitepb.TxnResponse{
		Header:    s.header(result.result.Revision, result.index, ""),
		Succeeded: result.result.Succeeded,
		Responses: s.opResponsesToProto(result.result.Responses, result.result.Revision, result.index),
	}, nil
}

func (s *Server) Status(context.Context, *etcdlitepb.StatusRequest) (*etcdlitepb.StatusResponse, error) {
	status := s.raft.Status()
	return &etcdlitepb.StatusResponse{
		Header:        s.header(s.store.CurrentRevision(), 0, ""),
		State:         status.State.String(),
		CommitIndex:   status.CommitIndex,
		AppliedIndex:  status.AppliedIndex,
		LastLogIndex:  status.LastLogIndex,
		SnapshotIndex: status.SnapshotIndex,
	}, nil
}

// 将一条请求的 command 提交给 raft
func (s *Server) submit(ctx context.Context, command mvcc.Command) (applyResult, error) {
	request := requestKey{ClientID: command.ID.ClientID, RequestID: command.ID.RequestID}
	var pendingChan chan applyResult
	ownsPending := false
	if request.valid() {
		s.mu.Lock()
		if cached, ok := s.requestCache[request]; ok {
			s.mu.Unlock()
			return cached, cached.err
		}
		if existing, ok := s.pending[request]; ok {
			s.mu.Unlock()
			return waitResult(ctx, existing)
		}
		pendingChan = make(chan applyResult, 1)
		s.pending[request] = pendingChan // 含义：该 server 对于这个 key 的操作在等待中
		ownsPending = true
		s.mu.Unlock()
	}

	data, err := mvcc.EncodeCommand(command)
	if err != nil {
		result := applyResult{request: request, err: err}
		if ownsPending {
			s.finishPending(request, pendingChan, result)
		}
		return result, err
	}

	// 进入 raft
	index, term, ok := s.raft.Start(data)
	if !ok {
		result := applyResult{request: request, err: errNotLeader}
		if ownsPending {
			s.finishPending(request, pendingChan, result)
		}
		return result, errNotLeader
	}

	// 进入 raft 后需要 raft 达成共识，所以注册一个 waiter 等待
	waiter := s.registerWaiter(index)
	// 等待有消息传入 waiter chan
	result, err := waitResult(ctx, waiter)
	if err != nil {
		s.removeWaiter(index, waiter)
		if ownsPending {
			s.removePending(request, pendingChan)
		}
		return result, err
	}
	if result.term != term || (request.valid() && result.request != request) {
		return result, errApplyMismatch
	}
	return result, result.err
}

// apply 协程，不停循环将 raft 已达成共识的 command 输送给上层 mvcc 状态机 apply，
// mvcc 再根据 command 不同类型进行分发。
func (s *Server) applyLoop() {
	for {
		select {
		case <-s.stopCh:
			return
		case msg := <-s.applyCh:
			if !msg.CommandValid {
				continue
			}
			result := applyResult{index: msg.CommandIndex, term: msg.CommandTerm}
			command, err := mvcc.DecodeCommand(msg.Command)
			if err != nil {
				result.err = err
			} else {
				result.request = requestKey{ClientID: command.ID.ClientID, RequestID: command.ID.RequestID}
				applyResult, applyErr := s.store.Apply(command)
				result.result = applyResult
				result.err = applyErr
			}
			// command 成功 apply 后走 finishApply
			s.finishApply(result)
		}
	}
}

// 对 applyResult 的一系列操作
func (s *Server) finishApply(result applyResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if result.index > s.appliedIndex {
		s.appliedIndex = result.index
	}

	// 缓存 result 到 indexCache
	s.indexCache[result.index] = result
	s.indexOrder = append(s.indexOrder, result.index)
	if len(s.indexOrder) > maxCachedApplyIndex {
		oldest := s.indexOrder[0]
		s.indexOrder = s.indexOrder[1:]
		delete(s.indexCache, oldest)
	}

	// 缓存 result 到 requestCache
	if result.request.valid() {
		s.requestCache[result.request] = result
		s.requestOrder = append(s.requestOrder, result.request)
		if len(s.requestOrder) > maxCachedRequests {
			oldest := s.requestOrder[0]
			s.requestOrder = s.requestOrder[1:]
			delete(s.requestCache, oldest)
		}
		if pending, ok := s.pending[result.request]; ok {
			delete(s.pending, result.request)
			pending <- result
			close(pending)
		}
	}

	// 将 result 传入 waiter chan
	if waiter, ok := s.waiters[result.index]; ok {
		delete(s.waiters, result.index)
		waiter <- result
		close(waiter)
	}
}

func (s *Server) registerWaiter(index uint64) chan applyResult {
	ch := make(chan applyResult, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	// 先看看针对该 index 是否已经有结果了
	// 这种情况会出现在 raft apply 的发生先于 waiter 的创建
	if result, ok := s.indexCache[index]; ok {
		ch <- result
		close(ch)
		return ch
	}
	s.waiters[index] = ch
	return ch
}

func (s *Server) removeWaiter(index uint64, ch chan applyResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.waiters[index]; ok && current == ch {
		delete(s.waiters, index)
	}
}

func (s *Server) finishPending(key requestKey, ch chan applyResult, result applyResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.pending[key]; ok && current == ch {
		delete(s.pending, key)
		ch <- result
		close(ch)
	}
}

func (s *Server) removePending(key requestKey, ch chan applyResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.pending[key]; ok && current == ch {
		delete(s.pending, key)
	}
}

func (s *Server) waitApplied(ctx context.Context, index uint64) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		applied := s.appliedIndex
		s.mu.Unlock()
		if applied >= index {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, s.requestTimeout)
}

func (s *Server) header(revision int64, raftIndex uint64, err string) *etcdlitepb.ResponseHeader {
	status := s.raft.Status()
	return &etcdlitepb.ResponseHeader{
		MemberId:  uint64(status.ID),
		LeaderId:  uint64(status.LeaderID),
		Term:      status.Term,
		RaftIndex: raftIndex,
		Revision:  revision,
		Error:     err,
	}
}

func (s *Server) errorHeader(err string, raftIndex uint64) *etcdlitepb.ResponseHeader {
	return s.header(s.store.CurrentRevision(), raftIndex, err)
}

func waitResult(ctx context.Context, ch chan applyResult) (applyResult, error) {
	select {
	case result := <-ch:
		return result, result.err
	case <-ctx.Done():
		return applyResult{}, ctx.Err()
	}
}

func keyValuesToProto(kvs []mvcc.KeyValue) []*etcdlitepb.KeyValue {
	out := make([]*etcdlitepb.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, keyValueToProto(kv))
	}
	return out
}

func keyValuePtrToProto(kv *mvcc.KeyValue) *etcdlitepb.KeyValue {
	if kv == nil {
		return nil
	}
	return keyValueToProto(*kv)
}

func keyValueToProto(kv mvcc.KeyValue) *etcdlitepb.KeyValue {
	return &etcdlitepb.KeyValue{
		Key:            append([]byte(nil), kv.Key...),
		Value:          append([]byte(nil), kv.Value...),
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		LeaseId:        kv.LeaseID,
		Tombstone:      kv.Tombstone,
	}
}

// 将一条 txn command 中的 compare，success 和 failure 拆出来
func txnRequestToCommand(req *etcdlitepb.TxnRequest) (mvcc.Command, error) {
	compare, err := comparesFromProto(req.GetCompare())
	if err != nil {
		return mvcc.Command{}, err
	}
	success, err := requestOpsFromProto(req.GetSuccess())
	if err != nil {
		return mvcc.Command{}, err
	}
	failure, err := requestOpsFromProto(req.GetFailure())
	if err != nil {
		return mvcc.Command{}, err
	}

	command := mvcc.Command{
		ID:   mvcc.RequestID{ClientID: req.GetClientId(), RequestID: req.GetRequestId()},
		Kind: mvcc.CommandTxn,
		Txn: &mvcc.TxnCommand{
			Compare: compare,
			Success: success,
			Failure: failure,
		},
	}
	return command, command.Validate()
}

func comparesFromProto(compares []*etcdlitepb.Compare) ([]mvcc.Compare, error) {
	out := make([]mvcc.Compare, 0, len(compares))
	for _, compare := range compares {
		target, err := compareTargetFromProto(compare.GetTarget())
		if err != nil {
			return nil, err
		}
		result, err := compareResultFromProto(compare.GetResult())
		if err != nil {
			return nil, err
		}
		out = append(out, mvcc.Compare{
			Key:            compare.GetKey(),
			Target:         target,
			Result:         result,
			Version:        compare.GetVersion(),
			CreateRevision: compare.GetCreateRevision(),
			ModRevision:    compare.GetModRevision(),
			Value:          compare.GetValue(),
			LeaseID:        compare.GetLeaseId(),
		})
	}
	return out, nil
}

func compareTargetFromProto(target etcdlitepb.CompareTarget) (mvcc.CompareTarget, error) {
	switch target {
	case etcdlitepb.CompareTarget_COMPARE_TARGET_VERSION:
		return mvcc.CompareVersion, nil
	case etcdlitepb.CompareTarget_COMPARE_TARGET_CREATE_REVISION:
		return mvcc.CompareCreateRevision, nil
	case etcdlitepb.CompareTarget_COMPARE_TARGET_MOD_REVISION:
		return mvcc.CompareModRevision, nil
	case etcdlitepb.CompareTarget_COMPARE_TARGET_VALUE:
		return mvcc.CompareValue, nil
	case etcdlitepb.CompareTarget_COMPARE_TARGET_LEASE:
		return mvcc.CompareLease, nil
	default:
		return "", mvcc.ErrInvalidCommand
	}
}

func compareResultFromProto(result etcdlitepb.CompareResult) (mvcc.CompareResult, error) {
	switch result {
	case etcdlitepb.CompareResult_COMPARE_RESULT_EQUAL:
		return mvcc.CompareEqual, nil
	case etcdlitepb.CompareResult_COMPARE_RESULT_NOT_EQUAL:
		return mvcc.CompareNotEqual, nil
	case etcdlitepb.CompareResult_COMPARE_RESULT_GREATER:
		return mvcc.CompareGreater, nil
	case etcdlitepb.CompareResult_COMPARE_RESULT_LESS:
		return mvcc.CompareLess, nil
	default:
		return "", mvcc.ErrInvalidCommand
	}
}

func requestOpsFromProto(ops []*etcdlitepb.RequestOp) ([]mvcc.Op, error) {
	out := make([]mvcc.Op, 0, len(ops))
	for _, op := range ops {
		converted, err := requestOpFromProto(op)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

func requestOpFromProto(op *etcdlitepb.RequestOp) (mvcc.Op, error) {
	switch request := op.GetRequest().(type) {
	case *etcdlitepb.RequestOp_RequestRange:
		req := request.RequestRange
		return mvcc.Op{
			Kind: mvcc.OpRange,
			Range: &mvcc.RangeRequest{
				Key:      req.GetKey(),
				End:      req.GetEnd(),
				Limit:    req.GetLimit(),
				Revision: req.GetRevision(),
			},
		}, nil
	case *etcdlitepb.RequestOp_RequestPut:
		req := request.RequestPut
		return mvcc.Op{
			Kind: mvcc.OpPut,
			Put: &mvcc.PutCommand{
				Key:     req.GetKey(),
				Value:   req.GetValue(),
				LeaseID: req.GetLeaseId(),
				PrevKV:  req.GetPrevKv(),
			},
		}, nil
	case *etcdlitepb.RequestOp_RequestDeleteRange:
		req := request.RequestDeleteRange
		return mvcc.Op{
			Kind: mvcc.OpDeleteRange,
			DeleteRange: &mvcc.DeleteRangeCommand{
				Key:    req.GetKey(),
				End:    req.GetEnd(),
				PrevKV: req.GetPrevKv(),
			},
		}, nil
	default:
		return mvcc.Op{}, mvcc.ErrInvalidCommand
	}
}

func (s *Server) opResponsesToProto(responses []mvcc.OpResponse, revision int64, raftIndex uint64) []*etcdlitepb.ResponseOp {
	out := make([]*etcdlitepb.ResponseOp, 0, len(responses))
	for _, response := range responses {
		out = append(out, s.opResponseToProto(response, revision, raftIndex))
	}
	return out
}

func (s *Server) opResponseToProto(response mvcc.OpResponse, revision int64, raftIndex uint64) *etcdlitepb.ResponseOp {
	switch response.Kind {
	case mvcc.OpRange:
		rangeResp := response.Range
		if rangeResp == nil {
			return &etcdlitepb.ResponseOp{}
		}
		return &etcdlitepb.ResponseOp{
			Response: &etcdlitepb.ResponseOp_ResponseRange{
				ResponseRange: &etcdlitepb.RangeResponse{
					Header: s.header(revision, raftIndex, ""),
					Kvs:    keyValuesToProto(rangeResp.KVs),
					Count:  rangeResp.Count,
				},
			},
		}
	case mvcc.OpPut:
		putResp := response.Put
		if putResp == nil {
			return &etcdlitepb.ResponseOp{}
		}
		return &etcdlitepb.ResponseOp{
			Response: &etcdlitepb.ResponseOp_ResponsePut{
				ResponsePut: &etcdlitepb.PutResponse{
					Header: s.header(revision, raftIndex, ""),
					PrevKv: keyValuePtrToProto(putResp.PrevKV),
				},
			},
		}
	case mvcc.OpDeleteRange:
		delResp := response.DeleteRange
		if delResp == nil {
			return &etcdlitepb.ResponseOp{}
		}
		return &etcdlitepb.ResponseOp{
			Response: &etcdlitepb.ResponseOp_ResponseDeleteRange{
				ResponseDeleteRange: &etcdlitepb.DeleteRangeResponse{
					Header:  s.header(revision, raftIndex, ""),
					Deleted: delResp.Deleted,
					PrevKvs: keyValuesToProto(delResp.PrevKVs),
				},
			},
		}
	default:
		return &etcdlitepb.ResponseOp{}
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, errNotLeader) {
		return errorNotLeader
	}
	if errors.Is(err, errApplyMismatch) {
		return errorApplyMismatch
	}
	return err.Error()
}
