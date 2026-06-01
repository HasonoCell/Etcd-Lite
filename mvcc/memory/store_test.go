package memory

import (
	"errors"
	"testing"

	"github.com/HasonoCell/Etcd-Lite/mvcc"
)

func TestPutMaintainsRevisionMetadataAndHistory(t *testing.T) {
	store := New()

	first, err := store.Put(mvcc.PutRequest{
		Key:     []byte("/foo"),
		Value:   []byte("v1"),
		LeaseID: 10,
	})
	if err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if first.Revision != 1 {
		t.Fatalf("first revision = %d, want 1", first.Revision)
	}

	second, err := store.Put(mvcc.PutRequest{
		Key:     []byte("/foo"),
		Value:   []byte("v2"),
		LeaseID: 11,
		PrevKV:  true,
	})
	if err != nil {
		t.Fatalf("second Put: %v", err)
	}
	if second.Revision != 2 {
		t.Fatalf("second revision = %d, want 2", second.Revision)
	}
	if second.PrevKV == nil || string(second.PrevKV.Value) != "v1" {
		t.Fatalf("PrevKV = %+v, want v1", second.PrevKV)
	}

	current := mustRange(t, store, mvcc.RangeRequest{Key: []byte("/foo")})
	if current.Revision != 2 || len(current.KVs) != 1 {
		t.Fatalf("current range = %+v", current)
	}
	kv := current.KVs[0]
	if string(kv.Value) != "v2" || kv.CreateRevision != 1 || kv.ModRevision != 2 || kv.Version != 2 || kv.LeaseID != 11 {
		t.Fatalf("current kv = %+v", kv)
	}

	old := mustRange(t, store, mvcc.RangeRequest{Key: []byte("/foo"), Revision: 1})
	if len(old.KVs) != 1 || string(old.KVs[0].Value) != "v1" {
		t.Fatalf("revision 1 range = %+v", old)
	}

	history, err := store.History(mvcc.HistoryRequest{FromRevision: 1, ToRevision: 2})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history.Events) != 2 {
		t.Fatalf("history events = %d, want 2", len(history.Events))
	}
	if history.Events[1].PrevKV == nil || string(history.Events[1].PrevKV.Value) != "v1" {
		t.Fatalf("second history event = %+v", history.Events[1])
	}
}

func TestRangePrefixLimitAndSortedOrder(t *testing.T) {
	store := New()
	put(t, store, "/app/b", "b")
	put(t, store, "/db/a", "db")
	put(t, store, "/app/a", "a")

	resp := mustRange(t, store, mvcc.RangeRequest{
		Key:   []byte("/app/"),
		End:   mvcc.PrefixEnd([]byte("/app/")),
		Limit: 1,
	})
	if resp.Revision != 3 {
		t.Fatalf("revision = %d, want 3", resp.Revision)
	}
	if resp.Count != 2 {
		t.Fatalf("count = %d, want 2", resp.Count)
	}
	if len(resp.KVs) != 1 || string(resp.KVs[0].Key) != "/app/a" {
		t.Fatalf("limited kvs = %+v", resp.KVs)
	}
}

func TestDeleteRangeWritesTombstonesAndPreservesHistoricalReads(t *testing.T) {
	store := New()
	put(t, store, "/app/a", "a")
	put(t, store, "/app/b", "b")
	put(t, store, "/db/a", "db")

	resp, err := store.DeleteRange(mvcc.DeleteRangeRequest{
		Key:    []byte("/app/"),
		End:    mvcc.PrefixEnd([]byte("/app/")),
		PrevKV: true,
	})
	if err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if resp.Revision != 4 || resp.Deleted != 2 {
		t.Fatalf("delete response = %+v, want revision 4 deleted 2", resp)
	}
	if len(resp.PrevKVs) != 2 || string(resp.PrevKVs[0].Key) != "/app/a" || string(resp.PrevKVs[1].Key) != "/app/b" {
		t.Fatalf("PrevKVs = %+v", resp.PrevKVs)
	}
	if len(resp.Events) != 2 {
		t.Fatalf("delete events = %d, want 2", len(resp.Events))
	}
	for i, event := range resp.Events {
		if event.Type != mvcc.EventDelete || event.Revision.Main != 4 || event.Revision.Sub != int64(i) || !event.KV.Tombstone {
			t.Fatalf("delete event[%d] = %+v", i, event)
		}
	}

	current := mustRange(t, store, mvcc.RangeRequest{
		Key: []byte("/app/"),
		End: mvcc.PrefixEnd([]byte("/app/")),
	})
	if current.Count != 0 || len(current.KVs) != 0 {
		t.Fatalf("current app range = %+v, want empty", current)
	}

	old := mustRange(t, store, mvcc.RangeRequest{
		Key:      []byte("/app/"),
		End:      mvcc.PrefixEnd([]byte("/app/")),
		Revision: 3,
	})
	if old.Count != 2 {
		t.Fatalf("revision 3 app range count = %d, want 2", old.Count)
	}
}

func TestDeleteRangeNoopDoesNotAdvanceRevision(t *testing.T) {
	store := New()
	put(t, store, "/foo", "bar")

	resp, err := store.DeleteRange(mvcc.DeleteRangeRequest{Key: []byte("/missing")})
	if err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if resp.Revision != 1 || resp.Deleted != 0 || store.CurrentRevision() != 1 {
		t.Fatalf("noop delete response = %+v current revision=%d", resp, store.CurrentRevision())
	}
}

func TestApplyCommandAndFutureRevisionError(t *testing.T) {
	store := New()

	result, err := store.Apply(mvcc.Command{
		Kind: mvcc.CommandPut,
		Put:  &mvcc.PutCommand{Key: []byte("/apply"), Value: []byte("ok")},
	})
	if err != nil {
		t.Fatalf("Apply put: %v", err)
	}
	if !result.Succeeded || result.Revision != 1 || len(result.Events) != 1 {
		t.Fatalf("apply result = %+v", result)
	}

	_, err = store.Range(mvcc.RangeRequest{Key: []byte("/apply"), Revision: 99})
	if !errors.Is(err, mvcc.ErrFutureRevision) {
		t.Fatalf("future revision error = %v, want ErrFutureRevision", err)
	}
}

func put(t *testing.T, store *Store, key string, value string) {
	t.Helper()
	if _, err := store.Put(mvcc.PutRequest{Key: []byte(key), Value: []byte(value)}); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
}

func mustRange(t *testing.T, store *Store, req mvcc.RangeRequest) mvcc.RangeResponse {
	t.Helper()
	resp, err := store.Range(req)
	if err != nil {
		t.Fatalf("Range(%+v): %v", req, err)
	}
	return resp
}
