package mvcc

import (
	"bytes"
	"errors"
)

var (
	ErrKeyRequired        = errors.New("mvcc: key required")
	ErrInvalidRange       = errors.New("mvcc: invalid key range")
	ErrFutureRevision     = errors.New("mvcc: future revision")
	ErrInvalidRevision    = errors.New("mvcc: invalid revision")
	ErrInvalidCommand     = errors.New("mvcc: invalid command")
	ErrUnsupportedCommand = errors.New("mvcc: unsupported command")
)

// Revision 表示 MVCC 中的全局 revision 和同一 transaction 内的 sub revision。
type Revision struct {
	// 每次有效写操作提交，Main += 1。
	Main int64 `json:"main"`
	// 用来区分同一个 write 事务里多个 key 的顺序。
	// 比如一次 DeleteRange 删除两个 key，它们共享同一个 Main，但 Sub 分别是 0、1。
	Sub int64 `json:"sub"`
}

// KeyValue 保存 current view 或 history event 中的 key metadata。比如：
//
//	 revision 1: put /foo = v1
//		create_revision = 1
//		mod_revision = 1
//		version = 1
//
//	 revision 2: put /foo = v2
//		create_revision = 1
//		mod_revision = 2
//		version = 2
type KeyValue struct {
	Key            []byte `json:"key"`
	Value          []byte `json:"value,omitempty"`
	CreateRevision int64  `json:"create_revision"`     // 这个 key 第一次创建时的 revision。
	ModRevision    int64  `json:"mod_revision"`        // 这个 key 最近一次修改时的 revision。
	Version        int64  `json:"version"`             // 当前 key generation 中被修改过多少次。
	LeaseID        int64  `json:"lease_id,omitempty"`  // 这个 key 绑定的 lease id。
	Tombstone      bool   `json:"tombstone,omitempty"` // 只用于 delete event，表示这个 key 在某个 revision 被删除。
}

type EventType string

const (
	EventPut    EventType = "put"
	EventDelete EventType = "delete"
)

// Event 是写入 history view 的单条变更记录，后续 Watch 会从这里 replay history。
type Event struct {
	Type     EventType `json:"type"`
	Revision Revision  `json:"revision"`
	KV       KeyValue  `json:"kv"`
	PrevKV   *KeyValue `json:"prev_kv,omitempty"`
}

// 当 End == nil 或 len(End) == 0 时，RangeRequest 就是单 key Get
type RangeRequest struct {
	Key      []byte
	End      []byte
	Limit    int64
	Revision int64
}

type RangeResponse struct {
	Revision int64
	Count    int64
	KVs      []KeyValue
}

type PutRequest struct {
	Key     []byte
	Value   []byte
	LeaseID int64
	PrevKV  bool
}

type PutResponse struct {
	Revision int64
	PrevKV   *KeyValue
	Event    Event
}

type DeleteRangeRequest struct {
	Key    []byte
	End    []byte
	PrevKV bool
}

type DeleteRangeResponse struct {
	Revision int64
	Deleted  int64
	PrevKVs  []KeyValue
	Events   []Event
}

type HistoryRequest struct {
	FromRevision int64
	ToRevision   int64
}

type HistoryResponse struct {
	Revision int64
	Events   []Event
}

type Store interface {
	CurrentRevision() int64
	Range(RangeRequest) (RangeResponse, error)
	Put(PutRequest) (PutResponse, error)
	DeleteRange(DeleteRangeRequest) (DeleteRangeResponse, error)
	History(HistoryRequest) (HistoryResponse, error)
	Apply(Command) (ApplyResult, error)
}

func ValidateKeyRange(key []byte, end []byte) error {
	if len(key) == 0 {
		return ErrKeyRequired
	}
	if len(end) > 0 && bytes.Compare(key, end) >= 0 {
		return ErrInvalidRange
	}
	return nil
}

func KeyInRange(key []byte, start []byte, end []byte) bool {
	if len(end) == 0 {
		return bytes.Equal(key, start)
	}
	return bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0
}

func PrefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func CloneKeyValue(kv KeyValue) KeyValue {
	return KeyValue{
		Key:            append([]byte(nil), kv.Key...),
		Value:          append([]byte(nil), kv.Value...),
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		LeaseID:        kv.LeaseID,
		Tombstone:      kv.Tombstone,
	}
}

func CloneKeyValues(kvs []KeyValue) []KeyValue {
	out := make([]KeyValue, len(kvs))
	for i, kv := range kvs {
		out[i] = CloneKeyValue(kv)
	}
	return out
}

func CloneEvent(event Event) Event {
	out := Event{
		Type:     event.Type,
		Revision: event.Revision,
		KV:       CloneKeyValue(event.KV),
	}
	if event.PrevKV != nil {
		prev := CloneKeyValue(*event.PrevKV)
		out.PrevKV = &prev
	}
	return out
}

func CloneEvents(events []Event) []Event {
	out := make([]Event, len(events))
	for i, event := range events {
		out[i] = CloneEvent(event)
	}
	return out
}
