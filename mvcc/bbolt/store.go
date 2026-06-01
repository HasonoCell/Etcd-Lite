package bbolt

/*
bbolt 和 memory 的实现语义一样，都是实现 Store 接口，维护 current view、history view 和 current revision。
区别是 memory 用 map/slice/mutex 存状态；bbolt 用 bucket/transaction/cursor 存状态。
所有 bbolt 读写都包在 db.View 或 db.Update 中，通过 tx 获取 bucket；
写入 bucket 时，需要把 KeyValue/Event 这类 struct 编码成 []byte，比如现在用 JSON，未来可换 protobuf。
*/

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"sort"
	"time"

	"github.com/HasonoCell/Etcd-Lite/mvcc"
	bolt "go.etcd.io/bbolt"
)

var (
	metaBucket    = []byte("meta")
	currentBucket = []byte("current_kv")
	historyBucket = []byte("history")
	revisionKey   = []byte("current_main_rev")
)

// Store 用 bbolt bucket 维护 current view 和 history view。
type Store struct {
	db *bolt.DB
}

// Open 打开 bbolt backend，并初始化 meta/current_kv/history bucket。
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	store := &Store{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

// Close 关闭底层 bbolt DB。
func (s *Store) Close() error {
	return s.db.Close()
}

// CurrentRevision 从 meta bucket 读取当前 Store 已经 apply 到的最新 main revision。
func (s *Store) CurrentRevision() int64 {
	var revision int64
	_ = s.db.View(func(tx *bolt.Tx) error {
		revision = currentRevision(tx)
		return nil
	})
	return revision
}

// Range 在一个 read transaction 中读取单 key 或 [key, end) range。
// current revision 直接扫描 current_kv；历史 revision 会 replay history bucket 还原 view。
func (s *Store) Range(req mvcc.RangeRequest) (mvcc.RangeResponse, error) {
	if err := mvcc.ValidateKeyRange(req.Key, req.End); err != nil {
		return mvcc.RangeResponse{}, err
	}

	var resp mvcc.RangeResponse
	err := s.db.View(func(tx *bolt.Tx) error {
		revision, err := normalizeReadRevision(tx, req.Revision)
		if err != nil {
			return err
		}

		var kvs []mvcc.KeyValue
		if revision == currentRevision(tx) {
			kvs, err = currentRange(tx, req.Key, req.End)
		} else {
			view, err := viewAt(tx, revision)
			if err != nil {
				return err
			}
			kvs = rangeFromView(view, req.Key, req.End)
		}
		count := int64(len(kvs))
		if req.Limit > 0 && int64(len(kvs)) > req.Limit {
			kvs = kvs[:req.Limit]
		}
		resp = mvcc.RangeResponse{Revision: revision, Count: count, KVs: mvcc.CloneKeyValues(kvs)}
		return nil
	})
	return resp, err
}

// Put 在一个 bbolt write transaction 中完成 current_kv 更新、history event 写入和 revision 推进。
// 这样 crash 后不会出现 current view 与 history view 只更新一半的状态。
func (s *Store) Put(req mvcc.PutRequest) (mvcc.PutResponse, error) {
	if err := mvcc.ValidateKeyRange(req.Key, nil); err != nil {
		return mvcc.PutResponse{}, err
	}

	var resp mvcc.PutResponse
	err := s.db.Update(func(tx *bolt.Tx) error {
		current := tx.Bucket(currentBucket)
		history := tx.Bucket(historyBucket)

		nextRevision := currentRevision(tx) + 1
		prev, existed, err := getCurrent(current, req.Key)
		if err != nil {
			return err
		}

		createRevision := nextRevision
		version := int64(1)
		if existed {
			createRevision = prev.CreateRevision
			version = prev.Version + 1
		}
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
		if existed {
			prevCopy := mvcc.CloneKeyValue(prev)
			event.PrevKV = &prevCopy
		}

		if err := putJSON(current, req.Key, kv); err != nil {
			return err
		}
		if err := putJSON(history, revisionKeyBytes(event.Revision), event); err != nil {
			return err
		}
		if err := setCurrentRevision(tx, nextRevision); err != nil {
			return err
		}

		resp = mvcc.PutResponse{Revision: nextRevision, Event: mvcc.CloneEvent(event)}
		if req.PrevKV && existed {
			prevCopy := mvcc.CloneKeyValue(prev)
			resp.PrevKV = &prevCopy
		}
		return nil
	})
	return resp, err
}

// DeleteRange 在一个 bbolt write transaction 中删除 range 内的 current_kv，并写入 tombstone events。
// 同一次 DeleteRange 使用同一个 main revision，多个 deleted keys 通过 sub revision 排序。
func (s *Store) DeleteRange(req mvcc.DeleteRangeRequest) (mvcc.DeleteRangeResponse, error) {
	if err := mvcc.ValidateKeyRange(req.Key, req.End); err != nil {
		return mvcc.DeleteRangeResponse{}, err
	}

	var resp mvcc.DeleteRangeResponse
	err := s.db.Update(func(tx *bolt.Tx) error {
		current := tx.Bucket(currentBucket)
		history := tx.Bucket(historyBucket)
		kvs, err := currentRange(tx, req.Key, req.End)
		if err != nil {
			return err
		}
		if len(kvs) == 0 {
			resp = mvcc.DeleteRangeResponse{Revision: currentRevision(tx)}
			return nil
		}

		nextRevision := currentRevision(tx) + 1
		events := make([]mvcc.Event, 0, len(kvs))
		prevKVs := make([]mvcc.KeyValue, 0, len(kvs))
		for i, prev := range kvs {
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
			if err := current.Delete(prev.Key); err != nil {
				return err
			}
			if err := putJSON(history, revisionKeyBytes(event.Revision), event); err != nil {
				return err
			}
			events = append(events, mvcc.CloneEvent(event))
			if req.PrevKV {
				prevKVs = append(prevKVs, mvcc.CloneKeyValue(prev))
			}
		}
		if err := setCurrentRevision(tx, nextRevision); err != nil {
			return err
		}

		resp = mvcc.DeleteRangeResponse{
			Revision: nextRevision,
			Deleted:  int64(len(kvs)),
			PrevKVs:  prevKVs,
			Events:   mvcc.CloneEvents(events),
		}
		return nil
	})
	return resp, err
}

// History 从 history bucket 中顺序扫描指定 revision 区间内的 events。
// history key 使用 big-endian (main, sub)，因此 bbolt cursor 的 byte order 就是 revision order。
func (s *Store) History(req mvcc.HistoryRequest) (mvcc.HistoryResponse, error) {
	var resp mvcc.HistoryResponse
	err := s.db.View(func(tx *bolt.Tx) error {
		currentRev := currentRevision(tx)
		to := req.ToRevision
		if to == 0 {
			to = currentRev
		}
		if to > currentRev {
			return mvcc.ErrFutureRevision
		}
		from := req.FromRevision
		if from <= 0 {
			from = 1
		}
		if from > to {
			resp = mvcc.HistoryResponse{Revision: currentRev}
			return nil
		}

		var events []mvcc.Event
		cursor := tx.Bucket(historyBucket).Cursor()
		for key, value := cursor.Seek(revisionKeyBytes(mvcc.Revision{Main: from})); key != nil; key, value = cursor.Next() {
			rev := decodeRevisionKey(key)
			if rev.Main > to {
				break
			}
			var event mvcc.Event
			if err := decodeJSON(value, &event); err != nil {
				return err
			}
			events = append(events, mvcc.CloneEvent(event))
		}
		resp = mvcc.HistoryResponse{Revision: currentRev, Events: events}
		return nil
	})
	return resp, err
}

// Apply 将已经 committed 的内部 Command 应用到 bbolt-backed MVCC Store。
// Put/DeleteRange 会复用对应的 transactional method；Txn 语义留到 M5。
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
		err := mvcc.ErrUnsupportedCommand
		return mvcc.ApplyResult{Succeeded: false, Err: err}, err
	default:
		err := mvcc.ErrInvalidCommand
		return mvcc.ApplyResult{Succeeded: false, Err: err}, err
	}
}

// init 创建 Store 需要的逻辑 buckets，重复调用保持 idempotent。
func (s *Store) init() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(metaBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(currentBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(historyBucket)
		return err
	})
}

// normalizeReadRevision 将 req.Revision 规范化为可读取的 MVCC revision。
func normalizeReadRevision(tx *bolt.Tx, revision int64) (int64, error) {
	if revision < 0 {
		return 0, mvcc.ErrInvalidRevision
	}
	currentRev := currentRevision(tx)
	if revision == 0 {
		return currentRev, nil
	}
	if revision > currentRev {
		return 0, mvcc.ErrFutureRevision
	}
	return revision, nil
}

// currentRevision 从 meta bucket 中读取 current_main_rev，缺省值为 0。
func currentRevision(tx *bolt.Tx) int64 {
	value := tx.Bucket(metaBucket).Get(revisionKey)
	if len(value) == 0 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(value))
}

// setCurrentRevision 将 current_main_rev 写回 meta bucket。
func setCurrentRevision(tx *bolt.Tx, revision int64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(revision))
	return tx.Bucket(metaBucket).Put(revisionKey, value)
}

// getCurrent 从 current_kv bucket 读取单个 key 的 latest KeyValue。
func getCurrent(bucket *bolt.Bucket, key []byte) (mvcc.KeyValue, bool, error) {
	value := bucket.Get(key)
	if value == nil {
		return mvcc.KeyValue{}, false, nil
	}
	var kv mvcc.KeyValue
	if err := decodeJSON(value, &kv); err != nil {
		return mvcc.KeyValue{}, false, err
	}
	return mvcc.CloneKeyValue(kv), true, nil
}

// currentRange 从 current_kv bucket 读取单 key 或 [key, end) range。
func currentRange(tx *bolt.Tx, key []byte, end []byte) ([]mvcc.KeyValue, error) {
	current := tx.Bucket(currentBucket)
	if len(end) == 0 {
		kv, ok, err := getCurrent(current, key)
		if err != nil || !ok {
			return nil, err
		}
		return []mvcc.KeyValue{kv}, nil
	}

	var kvs []mvcc.KeyValue
	cursor := current.Cursor()
	for k, value := cursor.Seek(key); k != nil && bytes.Compare(k, end) < 0; k, value = cursor.Next() {
		var kv mvcc.KeyValue
		if err := decodeJSON(value, &kv); err != nil {
			return nil, err
		}
		kvs = append(kvs, mvcc.CloneKeyValue(kv))
	}
	return kvs, nil
}

// viewAt 通过 replay history bucket 还原指定 revision 的 point-in-time view。
func viewAt(tx *bolt.Tx, revision int64) (map[string]mvcc.KeyValue, error) {
	view := make(map[string]mvcc.KeyValue)
	cursor := tx.Bucket(historyBucket).Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		rev := decodeRevisionKey(key)
		if rev.Main > revision {
			break
		}
		var event mvcc.Event
		if err := decodeJSON(value, &event); err != nil {
			return nil, err
		}
		eventKey := string(event.KV.Key)
		switch event.Type {
		case mvcc.EventPut:
			view[eventKey] = mvcc.CloneKeyValue(event.KV)
		case mvcc.EventDelete:
			delete(view, eventKey)
		}
	}
	return view, nil
}

// rangeFromView 从 replay 出来的 view 中筛选 range，并按 byte order 返回。
func rangeFromView(view map[string]mvcc.KeyValue, key []byte, end []byte) []mvcc.KeyValue {
	keys := make([]string, 0)
	for k, kv := range view {
		if mvcc.KeyInRange(kv.Key, key, end) {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0
	})
	kvs := make([]mvcc.KeyValue, 0, len(keys))
	for _, key := range keys {
		kvs = append(kvs, mvcc.CloneKeyValue(view[key]))
	}
	return kvs
}

// revisionKeyBytes 将 (main revision, sub revision) 编码成可排序的 bbolt key。
func revisionKeyBytes(revision mvcc.Revision) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[0:8], uint64(revision.Main))
	binary.BigEndian.PutUint64(key[8:16], uint64(revision.Sub))
	return key
}

// decodeRevisionKey 将 history bucket 的 bbolt key 解码回 Revision。
func decodeRevisionKey(key []byte) mvcc.Revision {
	return mvcc.Revision{
		Main: int64(binary.BigEndian.Uint64(key[0:8])),
		Sub:  int64(binary.BigEndian.Uint64(key[8:16])),
	}
}

// putJSON 将 value 编码为 JSON 后写入指定 bucket。
func putJSON(bucket *bolt.Bucket, key []byte, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put(key, data)
}

// decodeJSON 将 bucket value 解码到目标结构体。
func decodeJSON(data []byte, out any) error {
	return json.Unmarshal(data, out)
}

var _ mvcc.Store = (*Store)(nil)
