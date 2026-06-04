package memory

import (
	"bytes"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/HasonoCell/Etcd-Lite/mvcc"
)

// Store 用内存 map 维护 current view，并用 append-only slice 维护 history view。
type Store struct {
	mu         sync.RWMutex
	revision   int64
	current    map[string]mvcc.KeyValue // key -> KeyValue Struct
	history    []mvcc.Event
	leases     map[int64]mvcc.LeaseRecord // leaseID -> LeaseRecord
	leaseKeys  map[string]int64           // key -> leaseID
	compactRev int64
}

// New 创建一个 in-memory Store，初始 revision 为 0。
func New() *Store {
	return &Store{
		current:   make(map[string]mvcc.KeyValue),
		leases:    make(map[int64]mvcc.LeaseRecord),
		leaseKeys: make(map[string]int64),
	}
}

// CurrentRevision 返回当前 Store 已经 apply 到的最新 main revision。
func (s *Store) CurrentRevision() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

// Range 从指定 revision 的 view 中读取单 key 或 [key, end) range。
// 当 req.Revision 为 0 时读取 current revision；否则通过 history view 还原历史视图。
// 如何理解 Range？举个例子，比如 ["/app/", "/app0") 这个区间，
// 就是所有字节序上大于等于 /app/、小于 /app0 的 key。这样就可以做 prefix scan，比如 /app/a、/app/b 都会被扫到。
// 因为字符 / 的编码是 47，数字 0 的编码是 48。所有以 /app/ 为前缀的字符串在排列时都会紧紧挨在一起，
// 并且一定位于 /app/ 之后，但在 /app0 之前。
func (s *Store) Range(req mvcc.RangeRequest) (mvcc.RangeResponse, error) {
	if err := mvcc.ValidateKeyRange(req.Key, req.End); err != nil {
		return mvcc.RangeResponse{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// 对传进来的 revision 参数做一个校验。
	revision, err := s.normalizeReadRevisionLocked(req.Revision)
	if err != nil {
		return mvcc.RangeResponse{}, err
	}
	view := s.viewAtLocked(revision)
	keys := sortedKeysInRange(view, req.Key, req.End)

	count := int64(len(keys))
	if req.Limit > 0 && int64(len(keys)) > req.Limit {
		keys = keys[:req.Limit]
	}
	kvs := make([]mvcc.KeyValue, 0, len(keys))
	for _, key := range keys {
		kvs = append(kvs, mvcc.CloneKeyValue(view[key]))
	}
	return mvcc.RangeResponse{Revision: revision, Count: count, KVs: kvs}, nil
}

// Put 写入一个 key，并同步更新 current view、history view 和全局 revision。
// 对已有 key 会保留 create_revision，并递增 version 与 mod_revision。
func (s *Store) Put(req mvcc.PutRequest) (mvcc.PutResponse, error) {
	if err := mvcc.ValidateKeyRange(req.Key, nil); err != nil {
		return mvcc.PutResponse{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// put 要做 lease 校验，如果不带 lease 则 leaseID 必须传 0
	if err := s.validateLeaseLocked(req.LeaseID); err != nil {
		return mvcc.PutResponse{}, err
	}

	nextRevision := s.revision + 1
	key := string(req.Key)
	prev, existed := s.current[key]

	createRevision := nextRevision
	version := int64(1)
	// 区分是新增 key 还是已有 key。
	if existed {
		createRevision = prev.CreateRevision
		version = prev.Version + 1
	}

	// 构造 KeyValue 和 Event。
	kv := mvcc.KeyValue{
		Key:            append([]byte(nil), req.Key...),
		Value:          append([]byte(nil), req.Value...),
		CreateRevision: createRevision,
		ModRevision:    nextRevision,
		Version:        version,
		LeaseID:        req.LeaseID,
	}
	event := mvcc.Event{
		Type:     mvcc.EventPut,
		Revision: mvcc.Revision{Main: nextRevision},
		KV:       mvcc.CloneKeyValue(kv),
	}
	// 如果已有 key，放入 PrevKV。
	if existed {
		prevCopy := mvcc.CloneKeyValue(prev)
		event.PrevKV = &prevCopy
	}

	s.revision = nextRevision
	s.current[key] = mvcc.CloneKeyValue(kv)

	// 先解除旧 lease，再绑定新 lease
	if existed {
		s.detachLeaseKeyLocked(prev.LeaseID, prev.Key)
	}
	s.attachLeaseKeyLocked(req.LeaseID, req.Key)
	s.history = append(s.history, mvcc.CloneEvent(event))

	resp := mvcc.PutResponse{
		Revision: nextRevision,
		Event:    mvcc.CloneEvent(event),
	}
	if req.PrevKV && existed {
		prevCopy := mvcc.CloneKeyValue(prev)
		resp.PrevKV = &prevCopy
	}
	return resp, nil
}

// DeleteRange 删除单 key 或 [key, end) range，并为每个被删除 key 写入 tombstone event。
// 同一次 DeleteRange 中的所有 delete event 共享同一个 main revision，用 sub revision 区分顺序。
func (s *Store) DeleteRange(req mvcc.DeleteRangeRequest) (mvcc.DeleteRangeResponse, error) {
	if err := mvcc.ValidateKeyRange(req.Key, req.End); err != nil {
		return mvcc.DeleteRangeResponse{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	keys := sortedKeysInRange(s.current, req.Key, req.End)
	if len(keys) == 0 {
		return mvcc.DeleteRangeResponse{Revision: s.revision}, nil
	}

	nextRevision := s.revision + 1
	events := make([]mvcc.Event, 0, len(keys))
	prevKVs := make([]mvcc.KeyValue, 0, len(keys))
	for i, key := range keys {
		prev := s.current[key]
		delete(s.current, key)

		// 删除 key 也要删除对应的 lease
		s.detachLeaseKeyLocked(prev.LeaseID, prev.Key)

		tombstone := mvcc.KeyValue{
			Key:            append([]byte(nil), prev.Key...),
			CreateRevision: prev.CreateRevision,
			ModRevision:    nextRevision,
			Version:        prev.Version + 1,
			LeaseID:        prev.LeaseID,
			Tombstone:      true,
		}
		prevCopy := mvcc.CloneKeyValue(prev)
		event := mvcc.Event{
			Type:     mvcc.EventDelete,
			Revision: mvcc.Revision{Main: nextRevision, Sub: int64(i)},
			KV:       tombstone,
			PrevKV:   &prevCopy,
		}
		events = append(events, mvcc.CloneEvent(event))
		if req.PrevKV {
			prevKVs = append(prevKVs, mvcc.CloneKeyValue(prev))
		}
	}

	s.revision = nextRevision
	s.history = append(s.history, mvcc.CloneEvents(events)...)
	return mvcc.DeleteRangeResponse{
		Revision: nextRevision,
		Deleted:  int64(len(keys)),
		PrevKVs:  prevKVs,
		Events:   mvcc.CloneEvents(events),
	}, nil
}

// History 返回指定 revision 区间内的 append-only events，供后续 Watch replay 使用。
func (s *Store) History(req mvcc.HistoryRequest) (mvcc.HistoryResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// ToRevision
	to := req.ToRevision
	if to == 0 {
		to = s.revision
	}
	if to > s.revision {
		return mvcc.HistoryResponse{}, mvcc.ErrFutureRevision
	}
	// FromRevision
	from := req.FromRevision
	if from <= 0 {
		from = 1
	}
	if from <= s.compactRev {
		return mvcc.HistoryResponse{}, mvcc.ErrInvalidRevision
	}
	if from > to {
		return mvcc.HistoryResponse{Revision: s.revision}, nil
	}

	var events []mvcc.Event
	for _, event := range s.history {
		if event.Revision.Main >= from && event.Revision.Main <= to {
			events = append(events, mvcc.CloneEvent(event))
		}
	}
	return mvcc.HistoryResponse{Revision: s.revision, Events: events}, nil
}

// Leases 返回当前全部 lease metadata，供 leader expiration loop 扫描。
func (s *Store) Leases() ([]mvcc.LeaseRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]int64, 0, len(s.leases))
	for id := range s.leases {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	records := make([]mvcc.LeaseRecord, 0, len(ids))
	for _, id := range ids {
		records = append(records, mvcc.CloneLeaseRecord(s.leases[id]))
	}
	return records, nil
}

// Apply 将 raft 已经 committed 的内部 Command 应用到 MVCC Store，并返回结构化 ApplyResult。
// Txn 会在同一个 working view 中执行 compare 和 branch ops，所有写事件共享同一个 main revision。
func (s *Store) Apply(command mvcc.Command) (mvcc.ApplyResult, error) {
	if err := command.Validate(); err != nil {
		return mvcc.ApplyResult{Succeeded: false, Err: err}, err
	}

	switch command.Kind {
	case mvcc.CommandPut:
		resp, err := s.Put(mvcc.PutRequest{
			Key:     command.Put.Key,
			Value:   command.Put.Value,
			LeaseID: command.Put.LeaseID,
			PrevKV:  command.Put.PrevKV,
		})
		if err != nil {
			return mvcc.ApplyResult{Succeeded: false, Err: err}, err
		}
		return mvcc.ApplyResult{
			Revision:  resp.Revision,
			Succeeded: true,
			Responses: []mvcc.OpResponse{{Kind: mvcc.OpPut, Put: &resp}},
			Events:    []mvcc.Event{mvcc.CloneEvent(resp.Event)},
		}, nil
	case mvcc.CommandDeleteRange:
		resp, err := s.DeleteRange(mvcc.DeleteRangeRequest{
			Key:    command.DeleteRange.Key,
			End:    command.DeleteRange.End,
			PrevKV: command.DeleteRange.PrevKV,
		})
		if err != nil {
			return mvcc.ApplyResult{Succeeded: false, Err: err}, err
		}
		return mvcc.ApplyResult{
			Revision:  resp.Revision,
			Succeeded: true,
			Responses: []mvcc.OpResponse{{Kind: mvcc.OpDeleteRange, DeleteRange: &resp}},
			Events:    mvcc.CloneEvents(resp.Events),
		}, nil
	case mvcc.CommandTxn:
		return s.applyTxn(*command.Txn)
	case mvcc.CommandLeaseGrant:
		resp, err := s.leaseGrant(mvcc.LeaseGrantRequest{
			LeaseID:     command.LeaseGrant.LeaseID,
			TTL:         command.LeaseGrant.TTL,
			NowUnixNano: command.LeaseGrant.NowUnixNano,
		})
		if err != nil {
			return mvcc.ApplyResult{Succeeded: false, Err: err}, err
		}
		return mvcc.ApplyResult{Revision: s.CurrentRevision(), Succeeded: true, LeaseGrant: &resp}, nil
	case mvcc.CommandLeaseKeepAlive:
		resp, err := s.leaseKeepAlive(mvcc.LeaseKeepAliveRequest{
			LeaseID:     command.LeaseKeepAlive.LeaseID,
			NowUnixNano: command.LeaseKeepAlive.NowUnixNano,
		})
		if err != nil {
			return mvcc.ApplyResult{Succeeded: false, Err: err}, err
		}
		return mvcc.ApplyResult{Revision: s.CurrentRevision(), Succeeded: true, LeaseKeepAlive: &resp}, nil
	case mvcc.CommandLeaseRevoke:
		resp, err := s.leaseRevoke(mvcc.LeaseRevokeRequest{LeaseID: command.LeaseRevoke.LeaseID})
		if err != nil {
			return mvcc.ApplyResult{Succeeded: false, Err: err}, err
		}
		return mvcc.ApplyResult{
			Revision:    resp.Revision,
			Succeeded:   true,
			Events:      mvcc.CloneEvents(resp.Events),
			LeaseRevoke: &resp,
		}, nil
	default:
		err := mvcc.ErrInvalidCommand
		return mvcc.ApplyResult{Succeeded: false, Err: err}, err
	}
}

func (s *Store) applyTxn(txn mvcc.TxnCommand) (mvcc.ApplyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	execution, err := mvcc.ExecuteTxnWithLeaseValidator(s.current, s.revision, txn, func(leaseID int64) bool {
		_, ok := s.leases[leaseID]
		return ok
	})
	if err != nil {
		return mvcc.ApplyResult{Succeeded: false, Err: err}, err
	}
	if len(execution.Events) > 0 {
		s.revision = execution.Revision
		s.current = execution.Current
		s.applyLeaseEventsLocked(execution.Events)
		s.history = append(s.history, mvcc.CloneEvents(execution.Events)...)
	}

	return mvcc.ApplyResult{
		Revision:  execution.Revision,
		Succeeded: execution.Succeeded,
		Responses: execution.Responses,
		Events:    mvcc.CloneEvents(execution.Events),
	}, nil
}

// 创建 lease
func (s *Store) leaseGrant(req mvcc.LeaseGrantRequest) (mvcc.LeaseGrantResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.leases[req.LeaseID]; ok {
		return mvcc.LeaseGrantResponse{}, mvcc.ErrLeaseAlreadyExists
	}
	// 计算过期时间
	expireAt := req.NowUnixNano + req.TTL*int64(time.Second)
	record := mvcc.LeaseRecord{
		LeaseID:          req.LeaseID,
		TTL:              req.TTL,
		ExpireAtUnixNano: expireAt,
	}
	s.leases[req.LeaseID] = record
	return mvcc.LeaseGrantResponse{LeaseID: req.LeaseID, TTL: req.TTL, ExpireAtUnixNano: expireAt}, nil
}

// 续租 lease
func (s *Store) leaseKeepAlive(req mvcc.LeaseKeepAliveRequest) (mvcc.LeaseKeepAliveResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.leases[req.LeaseID]
	if !ok {
		return mvcc.LeaseKeepAliveResponse{}, mvcc.ErrLeaseNotFound
	}
	// 重新计算过期时间
	record.ExpireAtUnixNano = req.NowUnixNano + record.TTL*int64(time.Second)
	s.leases[req.LeaseID] = mvcc.CloneLeaseRecord(record)
	return mvcc.LeaseKeepAliveResponse{
		LeaseID:          record.LeaseID,
		TTL:              record.TTL,
		ExpireAtUnixNano: record.ExpireAtUnixNano,
	}, nil
}

// 删除 lease 并删除绑定在上面的 keys
// 因为涉及到 kv 状态的修改，所以会产生 event 供 watcher 监听
func (s *Store) leaseRevoke(req mvcc.LeaseRevokeRequest) (mvcc.LeaseRevokeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.leases[req.LeaseID]
	if !ok {
		return mvcc.LeaseRevokeResponse{}, mvcc.ErrLeaseNotFound
	}

	keys := mvcc.LeaseRecordKeys(record)
	events := make([]mvcc.Event, 0, len(keys))
	nextRevision := s.revision + 1

	// 逐个删除 current view 中的 kv
	for _, keyBytes := range keys {
		key := string(keyBytes)
		prev, ok := s.current[key]
		if !ok || prev.LeaseID != req.LeaseID {
			delete(s.leaseKeys, key)
			continue
		}
		// 先删 kv
		delete(s.current, key)
		delete(s.leaseKeys, key)

		tombstone := mvcc.KeyValue{
			Key:            append([]byte(nil), prev.Key...),
			CreateRevision: prev.CreateRevision,
			ModRevision:    nextRevision,
			Version:        prev.Version + 1,
			LeaseID:        prev.LeaseID,
			Tombstone:      true,
		}
		prevCopy := mvcc.CloneKeyValue(prev)

		// 生成 delete event
		event := mvcc.Event{
			Type:     mvcc.EventDelete,
			Revision: mvcc.Revision{Main: nextRevision, Sub: int64(len(events))},
			KV:       tombstone,
			PrevKV:   &prevCopy,
		}
		events = append(events, mvcc.CloneEvent(event))
	}
	// 最后删 lease
	delete(s.leases, req.LeaseID)

	if len(events) > 0 {
		s.revision = nextRevision
		// 将 delete events 加入 history
		s.history = append(s.history, mvcc.CloneEvents(events)...)
	}
	return mvcc.LeaseRevokeResponse{
		Revision: s.revision,
		Deleted:  int64(len(events)),
		Events:   mvcc.CloneEvents(events),
	}, nil
}

func (s *Store) validateLeaseLocked(leaseID int64) error {
	if leaseID == 0 {
		return nil
	}
	if leaseID < 0 {
		return mvcc.ErrInvalidLease
	}
	if _, ok := s.leases[leaseID]; !ok {
		return mvcc.ErrLeaseNotFound
	}
	return nil
}

func (s *Store) applyLeaseEventsLocked(events []mvcc.Event) {
	for _, event := range events {
		switch event.Type {
		case mvcc.EventPut:
			if event.PrevKV != nil {
				s.detachLeaseKeyLocked(event.PrevKV.LeaseID, event.PrevKV.Key)
			}
			s.attachLeaseKeyLocked(event.KV.LeaseID, event.KV.Key)
		case mvcc.EventDelete:
			if event.PrevKV != nil {
				s.detachLeaseKeyLocked(event.PrevKV.LeaseID, event.PrevKV.Key)
			}
		}
	}
}

func (s *Store) attachLeaseKeyLocked(leaseID int64, key []byte) {
	if leaseID == 0 {
		return
	}
	record, ok := s.leases[leaseID]
	if !ok {
		return
	}
	record.Keys = mvcc.AddLeaseKey(record.Keys, key)
	s.leases[leaseID] = mvcc.CloneLeaseRecord(record)
	s.leaseKeys[string(key)] = leaseID
}

func (s *Store) detachLeaseKeyLocked(leaseID int64, key []byte) {
	if leaseID == 0 {
		return
	}
	record, ok := s.leases[leaseID]
	if ok {
		record.Keys = mvcc.RemoveLeaseKey(record.Keys, key)
		s.leases[leaseID] = mvcc.CloneLeaseRecord(record)
	}
	delete(s.leaseKeys, string(key))
}

// normalizeReadRevisionLocked 将用户传入的 read revision 规范化为可读取的 MVCC revision。
// 调用方必须已经持有 s.mu，因为它会读取 revision 和 compactRev。
func (s *Store) normalizeReadRevisionLocked(revision int64) (int64, error) {
	if revision < 0 {
		return 0, mvcc.ErrInvalidRevision
	}
	if revision == 0 {
		return s.revision, nil
	}
	if revision > s.revision {
		return 0, mvcc.ErrFutureRevision
	}
	if revision <= s.compactRev {
		return 0, mvcc.ErrInvalidRevision
	}
	return revision, nil
}

// viewAtLocked 根据 history events 重放包括指定 revision 及其之前的历史事件，将结果放入 view 中。
// 调用方必须已经持有 s.mu；current revision 会直接 clone current view。
func (s *Store) viewAtLocked(revision int64) map[string]mvcc.KeyValue {
	if revision == s.revision {
		return cloneCurrent(s.current)
	}
	view := make(map[string]mvcc.KeyValue)
	// 遍历历史事件。
	for _, event := range s.history {
		// 只找传入 revision 之前的 event。
		if event.Revision.Main > revision {
			break
		}
		key := string(event.KV.Key)
		switch event.Type {
		case mvcc.EventPut:
			view[key] = mvcc.CloneKeyValue(event.KV)
		case mvcc.EventDelete:
			delete(view, key)
		}
	}
	return view
}

// sortedKeysInRange 从 current-like view 中筛出 range 内的 key，并按 byte order 排序。
func sortedKeysInRange(current map[string]mvcc.KeyValue, key []byte, end []byte) []string {
	keys := make([]string, 0)
	for k, kv := range current {
		if mvcc.KeyInRange(kv.Key, key, end) {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0
	})
	return keys
}

// cloneCurrent 深拷贝 current view，避免调用方通过 []byte alias 修改 Store 内部状态。
func cloneCurrent(current map[string]mvcc.KeyValue) map[string]mvcc.KeyValue {
	out := make(map[string]mvcc.KeyValue, len(current))
	for key, kv := range current {
		out[key] = mvcc.CloneKeyValue(kv)
	}
	return out
}

var _ mvcc.Store = (*Store)(nil)
