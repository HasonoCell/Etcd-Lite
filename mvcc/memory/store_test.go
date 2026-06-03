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

func TestApplyTxnComparesAllTargetsAndSelectsBranch(t *testing.T) {
	store := New()
	put(t, store, "/txn/key", "v1")
	if _, err := store.Put(mvcc.PutRequest{Key: []byte("/txn/key"), Value: []byte("v2"), LeaseID: 11}); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	result, err := store.Apply(mvcc.Command{
		Kind: mvcc.CommandTxn,
		Txn: &mvcc.TxnCommand{
			Compare: []mvcc.Compare{
				{Key: []byte("/txn/key"), Target: mvcc.CompareVersion, Result: mvcc.CompareEqual, Version: 2},
				{Key: []byte("/txn/key"), Target: mvcc.CompareCreateRevision, Result: mvcc.CompareEqual, CreateRevision: 1},
				{Key: []byte("/txn/key"), Target: mvcc.CompareModRevision, Result: mvcc.CompareEqual, ModRevision: 2},
				{Key: []byte("/txn/key"), Target: mvcc.CompareValue, Result: mvcc.CompareEqual, Value: []byte("v2")},
				{Key: []byte("/txn/key"), Target: mvcc.CompareLease, Result: mvcc.CompareEqual, LeaseID: 11},
			},
			Success: []mvcc.Op{{
				Kind: mvcc.OpPut,
				Put:  &mvcc.PutCommand{Key: []byte("/txn/success"), Value: []byte("ok")},
			}},
			Failure: []mvcc.Op{{
				Kind: mvcc.OpPut,
				Put:  &mvcc.PutCommand{Key: []byte("/txn/failure"), Value: []byte("bad")},
			}},
		},
	})
	if err != nil {
		t.Fatalf("Apply txn success: %v", err)
	}
	if !result.Succeeded || result.Revision != 3 || len(result.Events) != 1 {
		t.Fatalf("txn success result = %+v", result)
	}
	if got := mustRange(t, store, mvcc.RangeRequest{Key: []byte("/txn/success")}); got.Count != 1 {
		t.Fatalf("success branch range = %+v", got)
	}
	if got := mustRange(t, store, mvcc.RangeRequest{Key: []byte("/txn/failure")}); got.Count != 0 {
		t.Fatalf("failure branch range = %+v", got)
	}

	failed, err := store.Apply(mvcc.Command{
		Kind: mvcc.CommandTxn,
		Txn: &mvcc.TxnCommand{
			Compare: []mvcc.Compare{{
				Key:    []byte("/txn/key"),
				Target: mvcc.CompareValue,
				Result: mvcc.CompareEqual,
				Value:  []byte("not-v2"),
			}},
			Success: []mvcc.Op{{
				Kind: mvcc.OpPut,
				Put:  &mvcc.PutCommand{Key: []byte("/txn/should-not-exist"), Value: []byte("bad")},
			}},
			Failure: []mvcc.Op{{
				Kind: mvcc.OpPut,
				Put:  &mvcc.PutCommand{Key: []byte("/txn/failure"), Value: []byte("ok")},
			}},
		},
	})
	if err != nil {
		t.Fatalf("Apply txn failure: %v", err)
	}
	if failed.Succeeded || failed.Revision != 4 || len(failed.Events) != 1 {
		t.Fatalf("txn failure result = %+v", failed)
	}
	if got := mustRange(t, store, mvcc.RangeRequest{Key: []byte("/txn/failure")}); got.Count != 1 || string(got.KVs[0].Value) != "ok" {
		t.Fatalf("failure branch current range = %+v", got)
	}
}

func TestApplyTxnMultipleWritesShareOneMainRevision(t *testing.T) {
	store := New()
	put(t, store, "/old", "old")

	result, err := store.Apply(mvcc.Command{
		Kind: mvcc.CommandTxn,
		Txn: &mvcc.TxnCommand{
			Success: []mvcc.Op{
				{Kind: mvcc.OpPut, Put: &mvcc.PutCommand{Key: []byte("/app/a"), Value: []byte("a")}},
				{Kind: mvcc.OpPut, Put: &mvcc.PutCommand{Key: []byte("/app/b"), Value: []byte("b")}},
				{Kind: mvcc.OpDeleteRange, DeleteRange: &mvcc.DeleteRangeCommand{Key: []byte("/old"), PrevKV: true}},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply txn writes: %v", err)
	}
	if !result.Succeeded || result.Revision != 2 || len(result.Events) != 3 {
		t.Fatalf("txn writes result = %+v", result)
	}
	for i, event := range result.Events {
		if event.Revision.Main != 2 || event.Revision.Sub != int64(i) {
			t.Fatalf("event[%d] revision = %+v, want main 2 sub %d", i, event.Revision, i)
		}
	}
	app := mustRange(t, store, mvcc.RangeRequest{Key: []byte("/app/"), End: mvcc.PrefixEnd([]byte("/app/"))})
	if app.Count != 2 || app.KVs[0].ModRevision != 2 || app.KVs[1].ModRevision != 2 {
		t.Fatalf("app range = %+v", app)
	}
	old := mustRange(t, store, mvcc.RangeRequest{Key: []byte("/old")})
	if old.Count != 0 {
		t.Fatalf("old range = %+v, want deleted", old)
	}
}

func TestApplyTxnReadOnlyDoesNotAdvanceRevision(t *testing.T) {
	store := New()
	put(t, store, "/readonly/key", "v1")

	result, err := store.Apply(mvcc.Command{
		Kind: mvcc.CommandTxn,
		Txn: &mvcc.TxnCommand{
			Compare: []mvcc.Compare{{
				Key:     []byte("/missing"),
				Target:  mvcc.CompareVersion,
				Result:  mvcc.CompareEqual,
				Version: 0,
			}},
			Success: []mvcc.Op{{
				Kind:  mvcc.OpRange,
				Range: &mvcc.RangeRequest{Key: []byte("/readonly/key")},
			}},
		},
	})
	if err != nil {
		t.Fatalf("Apply read-only txn: %v", err)
	}
	if !result.Succeeded || result.Revision != 1 || store.CurrentRevision() != 1 || len(result.Events) != 0 {
		t.Fatalf("read-only txn result = %+v current revision=%d", result, store.CurrentRevision())
	}
	if len(result.Responses) != 1 || result.Responses[0].Range == nil || result.Responses[0].Range.Count != 1 {
		t.Fatalf("read-only txn responses = %+v", result.Responses)
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
