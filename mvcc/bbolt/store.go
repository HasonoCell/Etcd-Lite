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
	metaBucket         = []byte("meta")
	currentBucket      = []byte("current_kv")
	historyBucket      = []byte("history")
	compactBucket      = []byte("compact_kv")
	leaseBucket        = []byte("lease")
	revisionKey        = []byte("current_main_rev")
	compactRevisionKey = []byte("compact_main_rev")
	appliedIndexKey    = []byte("applied_index")
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

// AppliedIndex 从 meta bucket 读取状态机已经 apply 到的最新 Raft log index。
func (s *Store) AppliedIndex() uint64 {
	var index uint64
	_ = s.db.View(func(tx *bolt.Tx) error {
		index = appliedIndex(tx)
		return nil
	})
	return index
}

// SetAppliedIndex 单调推进状态机 apply 进度，避免 restart recovery 重复执行已 apply log。
func (s *Store) SetAppliedIndex(index uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if index <= appliedIndex(tx) {
			return nil
		}
		return setAppliedIndex(tx, index)
	})
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
		leases := tx.Bucket(leaseBucket)

		nextRevision := currentRevision(tx) + 1
		prev, existed, err := getCurrent(current, req.Key)
		if err != nil {
			return err
		}
		if err := validateLease(leases, req.LeaseID); err != nil {
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
		if existed {
			if err := detachLeaseKey(leases, prev.LeaseID, prev.Key); err != nil {
				return err
			}
		}
		if err := attachLeaseKey(leases, req.LeaseID, req.Key); err != nil {
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
		leases := tx.Bucket(leaseBucket)
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
			if err := detachLeaseKey(leases, prev.LeaseID, prev.Key); err != nil {
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
		if from <= compactRevision(tx) {
			resp = mvcc.HistoryResponse{Revision: compactRevision(tx)}
			return mvcc.ErrCompacted
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

// Compact 推进 compact revision，并清理不再可见的旧 history events。
func (s *Store) Compact(req mvcc.CompactRequest) (mvcc.CompactResponse, error) {
	if req.Revision <= 0 {
		return mvcc.CompactResponse{}, mvcc.ErrInvalidRevision
	}

	var resp mvcc.CompactResponse
	err := s.db.Update(func(tx *bolt.Tx) error {
		currentRev := currentRevision(tx)
		compactRev := compactRevision(tx)
		if req.Revision <= compactRev {
			resp = mvcc.CompactResponse{Revision: compactRev}
			return mvcc.ErrCompacted
		}
		if req.Revision > currentRev {
			return mvcc.ErrFutureRevision
		}

		base, err := viewAt(tx, req.Revision)
		if err != nil {
			return err
		}
		if err := replaceKeyValueBucket(tx, compactBucket, base); err != nil {
			return err
		}
		history := tx.Bucket(historyBucket)
		cursor := history.Cursor()
		for key, _ := cursor.First(); key != nil; {
			rev := decodeRevisionKey(key)
			if rev.Main > req.Revision {
				break
			}
			deleteKey := append([]byte(nil), key...)
			next, _ := cursor.Next()
			if err := history.Delete(deleteKey); err != nil {
				return err
			}
			key = next
		}
		if err := setCompactRevision(tx, req.Revision); err != nil {
			return err
		}
		resp = mvcc.CompactResponse{Revision: req.Revision}
		return nil
	})
	return resp, err
}

// Leases 返回当前全部 lease metadata，供 leader expiration loop 扫描。
func (s *Store) Leases() ([]mvcc.LeaseRecord, error) {
	var records []mvcc.LeaseRecord
	err := s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(leaseBucket).Cursor()
		for _, value := cursor.First(); value != nil; _, value = cursor.Next() {
			var record mvcc.LeaseRecord
			if err := decodeJSON(value, &record); err != nil {
				return err
			}
			records = append(records, mvcc.CloneLeaseRecord(record))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].LeaseID < records[j].LeaseID
	})
	return records, nil
}

// Apply 将已经 committed 的内部 Command 应用到 bbolt-backed MVCC Store。
// Txn 会在一个 db.Update 中完成 compare、branch ops、history 写入和 revision 推进。
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
	case mvcc.CommandCompact:
		resp, err := s.Compact(mvcc.CompactRequest{Revision: command.Compact.Revision})
		if err != nil {
			return mvcc.ApplyResult{Succeeded: false, Err: err}, err
		}
		return mvcc.ApplyResult{
			Revision:  s.CurrentRevision(),
			Succeeded: true,
			Compact:   &resp,
		}, nil
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

// Snapshot 导出 bbolt backend 的逻辑状态，作为 Raft snapshot data。
func (s *Store) Snapshot() ([]byte, error) {
	var snapshot mvcc.SnapshotData
	err := s.db.View(func(tx *bolt.Tx) error {
		current, err := currentView(tx)
		if err != nil {
			return err
		}
		compact, err := keyValueBucketView(tx, compactBucket)
		if err != nil {
			return err
		}
		history, err := historyEvents(tx)
		if err != nil {
			return err
		}
		leases, err := leaseRecords(tx)
		if err != nil {
			return err
		}
		snapshot = mvcc.SnapshotData{
			Revision:        currentRevision(tx),
			CompactRevision: compactRevision(tx),
			AppliedIndex:    appliedIndex(tx),
			Current:         mapValues(current),
			CompactBase:     mapValues(compact),
			History:         history,
			Leases:          leases,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return mvcc.EncodeSnapshot(snapshot)
}

// RestoreSnapshot 用 Raft snapshot data 覆盖当前 backend buckets。
func (s *Store) RestoreSnapshot(data []byte) error {
	snapshot, err := mvcc.DecodeSnapshot(data)
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		if err := clearBucket(tx, currentBucket); err != nil {
			return err
		}
		if err := clearBucket(tx, historyBucket); err != nil {
			return err
		}
		if err := clearBucket(tx, compactBucket); err != nil {
			return err
		}
		if err := clearBucket(tx, leaseBucket); err != nil {
			return err
		}
		if err := replaceKeyValueBucket(tx, currentBucket, viewFromKVs(snapshot.Current)); err != nil {
			return err
		}
		if err := replaceKeyValueBucket(tx, compactBucket, viewFromKVs(snapshot.CompactBase)); err != nil {
			return err
		}
		history := tx.Bucket(historyBucket)
		for _, event := range snapshot.History {
			if err := putJSON(history, revisionKeyBytes(event.Revision), event); err != nil {
				return err
			}
		}
		leases := tx.Bucket(leaseBucket)
		for _, record := range snapshot.Leases {
			if err := putJSON(leases, leaseKeyBytes(record.LeaseID), record); err != nil {
				return err
			}
		}
		if err := setCurrentRevision(tx, snapshot.Revision); err != nil {
			return err
		}
		if err := setCompactRevision(tx, snapshot.CompactRevision); err != nil {
			return err
		}
		return setAppliedIndex(tx, snapshot.AppliedIndex)
	})
}

func (s *Store) applyTxn(txn mvcc.TxnCommand) (mvcc.ApplyResult, error) {
	var result mvcc.ApplyResult
	err := s.db.Update(func(tx *bolt.Tx) error {
		currentView, err := currentView(tx)
		if err != nil {
			return err
		}

		leases := tx.Bucket(leaseBucket)
		execution, err := mvcc.ExecuteTxnWithLeaseValidator(currentView, currentRevision(tx), txn, func(leaseID int64) bool {
			return leases.Get(leaseKeyBytes(leaseID)) != nil
		})
		if err != nil {
			return err
		}
		if len(execution.Events) > 0 {
			current := tx.Bucket(currentBucket)
			history := tx.Bucket(historyBucket)
			leases := tx.Bucket(leaseBucket)
			for _, event := range execution.Events {
				switch event.Type {
				case mvcc.EventPut:
					if event.PrevKV != nil {
						if err := detachLeaseKey(leases, event.PrevKV.LeaseID, event.PrevKV.Key); err != nil {
							return err
						}
					}
					if err := attachLeaseKey(leases, event.KV.LeaseID, event.KV.Key); err != nil {
						return err
					}
					if err := putJSON(current, event.KV.Key, event.KV); err != nil {
						return err
					}
				case mvcc.EventDelete:
					if event.PrevKV != nil {
						if err := detachLeaseKey(leases, event.PrevKV.LeaseID, event.PrevKV.Key); err != nil {
							return err
						}
					}
					if err := current.Delete(event.KV.Key); err != nil {
						return err
					}
				default:
					return mvcc.ErrInvalidCommand
				}
				if err := putJSON(history, revisionKeyBytes(event.Revision), event); err != nil {
					return err
				}
			}
			if err := setCurrentRevision(tx, execution.Revision); err != nil {
				return err
			}
		}

		result = mvcc.ApplyResult{
			Revision:  execution.Revision,
			Succeeded: execution.Succeeded,
			Responses: execution.Responses,
			Events:    mvcc.CloneEvents(execution.Events),
		}
		return nil
	})
	if err != nil {
		return mvcc.ApplyResult{Succeeded: false, Err: err}, err
	}
	return result, nil
}

func (s *Store) leaseGrant(req mvcc.LeaseGrantRequest) (mvcc.LeaseGrantResponse, error) {
	var resp mvcc.LeaseGrantResponse
	err := s.db.Update(func(tx *bolt.Tx) error {
		leases := tx.Bucket(leaseBucket)
		key := leaseKeyBytes(req.LeaseID)
		if leases.Get(key) != nil {
			return mvcc.ErrLeaseAlreadyExists
		}
		expireAt := req.NowUnixNano + req.TTL*int64(time.Second)
		record := mvcc.LeaseRecord{
			LeaseID:          req.LeaseID,
			TTL:              req.TTL,
			ExpireAtUnixNano: expireAt,
		}
		if err := putJSON(leases, key, record); err != nil {
			return err
		}
		resp = mvcc.LeaseGrantResponse{LeaseID: req.LeaseID, TTL: req.TTL, ExpireAtUnixNano: expireAt}
		return nil
	})
	return resp, err
}

func (s *Store) leaseKeepAlive(req mvcc.LeaseKeepAliveRequest) (mvcc.LeaseKeepAliveResponse, error) {
	var resp mvcc.LeaseKeepAliveResponse
	err := s.db.Update(func(tx *bolt.Tx) error {
		leases := tx.Bucket(leaseBucket)
		record, err := getLease(leases, req.LeaseID)
		if err != nil {
			return err
		}
		record.ExpireAtUnixNano = req.NowUnixNano + record.TTL*int64(time.Second)
		if err := putJSON(leases, leaseKeyBytes(req.LeaseID), record); err != nil {
			return err
		}
		resp = mvcc.LeaseKeepAliveResponse{
			LeaseID:          record.LeaseID,
			TTL:              record.TTL,
			ExpireAtUnixNano: record.ExpireAtUnixNano,
		}
		return nil
	})
	return resp, err
}

func (s *Store) leaseRevoke(req mvcc.LeaseRevokeRequest) (mvcc.LeaseRevokeResponse, error) {
	var resp mvcc.LeaseRevokeResponse
	err := s.db.Update(func(tx *bolt.Tx) error {
		current := tx.Bucket(currentBucket)
		history := tx.Bucket(historyBucket)
		leases := tx.Bucket(leaseBucket)
		record, err := getLease(leases, req.LeaseID)
		if err != nil {
			return err
		}

		keys := mvcc.LeaseRecordKeys(record)
		events := make([]mvcc.Event, 0, len(keys))
		nextRevision := currentRevision(tx) + 1
		for _, key := range keys {
			prev, ok, err := getCurrent(current, key)
			if err != nil {
				return err
			}
			if !ok || prev.LeaseID != req.LeaseID {
				continue
			}

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
				Revision: mvcc.Revision{Main: nextRevision, Sub: int64(len(events))},
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
		}
		if err := leases.Delete(leaseKeyBytes(req.LeaseID)); err != nil {
			return err
		}
		revision := currentRevision(tx)
		if len(events) > 0 {
			revision = nextRevision
			if err := setCurrentRevision(tx, revision); err != nil {
				return err
			}
		}
		resp = mvcc.LeaseRevokeResponse{
			Revision: revision,
			Deleted:  int64(len(events)),
			Events:   mvcc.CloneEvents(events),
		}
		return nil
	})
	return resp, err
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
		if _, err := tx.CreateBucketIfNotExists(historyBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(compactBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(leaseBucket)
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
	if revision <= compactRevision(tx) {
		return 0, mvcc.ErrCompacted
	}
	return revision, nil
}

// currentRevision 从 meta bucket 中读取 current_main_rev，缺省值为 0。
func currentRevision(tx *bolt.Tx) int64 {
	return int64(metaUint64(tx, revisionKey))
}

func compactRevision(tx *bolt.Tx) int64 {
	return int64(metaUint64(tx, compactRevisionKey))
}

func appliedIndex(tx *bolt.Tx) uint64 {
	return metaUint64(tx, appliedIndexKey)
}

func metaUint64(tx *bolt.Tx, key []byte) uint64 {
	value := tx.Bucket(metaBucket).Get(key)
	if len(value) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(value)
}

// setCurrentRevision 将 current_main_rev 写回 meta bucket。
func setCurrentRevision(tx *bolt.Tx, revision int64) error {
	return setMetaUint64(tx, revisionKey, uint64(revision))
}

func setCompactRevision(tx *bolt.Tx, revision int64) error {
	return setMetaUint64(tx, compactRevisionKey, uint64(revision))
}

func setAppliedIndex(tx *bolt.Tx, index uint64) error {
	return setMetaUint64(tx, appliedIndexKey, index)
}

func setMetaUint64(tx *bolt.Tx, key []byte, number uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, number)
	return tx.Bucket(metaBucket).Put(key, value)
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

func validateLease(bucket *bolt.Bucket, leaseID int64) error {
	if leaseID == 0 {
		return nil
	}
	if leaseID < 0 {
		return mvcc.ErrInvalidLease
	}
	if bucket.Get(leaseKeyBytes(leaseID)) == nil {
		return mvcc.ErrLeaseNotFound
	}
	return nil
}

func getLease(bucket *bolt.Bucket, leaseID int64) (mvcc.LeaseRecord, error) {
	value := bucket.Get(leaseKeyBytes(leaseID))
	if value == nil {
		return mvcc.LeaseRecord{}, mvcc.ErrLeaseNotFound
	}
	var record mvcc.LeaseRecord
	if err := decodeJSON(value, &record); err != nil {
		return mvcc.LeaseRecord{}, err
	}
	return mvcc.CloneLeaseRecord(record), nil
}

func attachLeaseKey(bucket *bolt.Bucket, leaseID int64, key []byte) error {
	if leaseID == 0 {
		return nil
	}
	record, err := getLease(bucket, leaseID)
	if err != nil {
		return err
	}
	record.Keys = mvcc.AddLeaseKey(record.Keys, key)
	return putJSON(bucket, leaseKeyBytes(leaseID), record)
}

func detachLeaseKey(bucket *bolt.Bucket, leaseID int64, key []byte) error {
	if leaseID == 0 {
		return nil
	}
	record, err := getLease(bucket, leaseID)
	if err != nil {
		if err == mvcc.ErrLeaseNotFound {
			return nil
		}
		return err
	}
	record.Keys = mvcc.RemoveLeaseKey(record.Keys, key)
	return putJSON(bucket, leaseKeyBytes(leaseID), record)
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

// currentView 读取完整 current_kv bucket，供 Txn 在 working view 中做原子修改。
func currentView(tx *bolt.Tx) (map[string]mvcc.KeyValue, error) {
	view := make(map[string]mvcc.KeyValue)
	cursor := tx.Bucket(currentBucket).Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		var kv mvcc.KeyValue
		if err := decodeJSON(value, &kv); err != nil {
			return nil, err
		}
		view[string(key)] = mvcc.CloneKeyValue(kv)
	}
	return view, nil
}

func keyValueBucketView(tx *bolt.Tx, bucketName []byte) (map[string]mvcc.KeyValue, error) {
	view := make(map[string]mvcc.KeyValue)
	cursor := tx.Bucket(bucketName).Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		var kv mvcc.KeyValue
		if err := decodeJSON(value, &kv); err != nil {
			return nil, err
		}
		view[string(key)] = mvcc.CloneKeyValue(kv)
	}
	return view, nil
}

func replaceKeyValueBucket(tx *bolt.Tx, bucketName []byte, view map[string]mvcc.KeyValue) error {
	if err := clearBucket(tx, bucketName); err != nil {
		return err
	}
	bucket := tx.Bucket(bucketName)
	keys := sortedViewKeys(view)
	for _, key := range keys {
		if err := putJSON(bucket, []byte(key), view[key]); err != nil {
			return err
		}
	}
	return nil
}

func clearBucket(tx *bolt.Tx, bucketName []byte) error {
	bucket := tx.Bucket(bucketName)
	cursor := bucket.Cursor()
	for key, _ := cursor.First(); key != nil; {
		deleteKey := append([]byte(nil), key...)
		next, _ := cursor.Next()
		if err := bucket.Delete(deleteKey); err != nil {
			return err
		}
		key = next
	}
	return nil
}

func historyEvents(tx *bolt.Tx) ([]mvcc.Event, error) {
	events := make([]mvcc.Event, 0)
	cursor := tx.Bucket(historyBucket).Cursor()
	for _, value := cursor.First(); value != nil; _, value = cursor.Next() {
		var event mvcc.Event
		if err := decodeJSON(value, &event); err != nil {
			return nil, err
		}
		events = append(events, mvcc.CloneEvent(event))
	}
	return events, nil
}

func leaseRecords(tx *bolt.Tx) ([]mvcc.LeaseRecord, error) {
	records := make([]mvcc.LeaseRecord, 0)
	cursor := tx.Bucket(leaseBucket).Cursor()
	for _, value := cursor.First(); value != nil; _, value = cursor.Next() {
		var record mvcc.LeaseRecord
		if err := decodeJSON(value, &record); err != nil {
			return nil, err
		}
		records = append(records, mvcc.CloneLeaseRecord(record))
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].LeaseID < records[j].LeaseID
	})
	return records, nil
}

func mapValues(view map[string]mvcc.KeyValue) []mvcc.KeyValue {
	keys := sortedViewKeys(view)
	kvs := make([]mvcc.KeyValue, 0, len(keys))
	for _, key := range keys {
		kvs = append(kvs, mvcc.CloneKeyValue(view[key]))
	}
	return kvs
}

func viewFromKVs(kvs []mvcc.KeyValue) map[string]mvcc.KeyValue {
	view := make(map[string]mvcc.KeyValue, len(kvs))
	for _, kv := range kvs {
		view[string(kv.Key)] = mvcc.CloneKeyValue(kv)
	}
	return view
}

func sortedViewKeys(view map[string]mvcc.KeyValue) []string {
	keys := make([]string, 0, len(view))
	for key := range view {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0
	})
	return keys
}

// viewAt 通过 replay history bucket 还原指定 revision 的 point-in-time view。
func viewAt(tx *bolt.Tx, revision int64) (map[string]mvcc.KeyValue, error) {
	view := make(map[string]mvcc.KeyValue)
	compactRev := compactRevision(tx)
	if compactRev > 0 {
		compactView, err := keyValueBucketView(tx, compactBucket)
		if err != nil {
			return nil, err
		}
		view = compactView
	}
	cursor := tx.Bucket(historyBucket).Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		rev := decodeRevisionKey(key)
		if rev.Main <= compactRev {
			continue
		}
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

func leaseKeyBytes(leaseID int64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(leaseID))
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
