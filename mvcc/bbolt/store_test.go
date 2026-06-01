package bbolt

import (
	"path/filepath"
	"testing"

	"github.com/HasonoCell/Etcd-Lite/mvcc"
)

func TestStorePersistsCurrentAndHistoryViews(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backend.db")
	store := openStore(t, path)

	if _, err := store.Put(mvcc.PutRequest{Key: []byte("/foo"), Value: []byte("v1")}); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if _, err := store.Put(mvcc.PutRequest{Key: []byte("/foo"), Value: []byte("v2"), PrevKV: true}); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openStore(t, path)
	defer reopened.Close()

	current, err := reopened.Range(mvcc.RangeRequest{Key: []byte("/foo")})
	if err != nil {
		t.Fatalf("Range current: %v", err)
	}
	if current.Revision != 2 || len(current.KVs) != 1 || string(current.KVs[0].Value) != "v2" {
		t.Fatalf("current range = %+v", current)
	}

	old, err := reopened.Range(mvcc.RangeRequest{Key: []byte("/foo"), Revision: 1})
	if err != nil {
		t.Fatalf("Range old: %v", err)
	}
	if len(old.KVs) != 1 || string(old.KVs[0].Value) != "v1" {
		t.Fatalf("revision 1 range = %+v", old)
	}

	history, err := reopened.History(mvcc.HistoryRequest{FromRevision: 1, ToRevision: 2})
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

func TestStoreDeleteRangeUsesOneRevisionForMultipleKeys(t *testing.T) {
	store := openStore(t, filepath.Join(t.TempDir(), "backend.db"))
	defer store.Close()

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
		t.Fatalf("delete response = %+v", resp)
	}
	for i, event := range resp.Events {
		if event.Type != mvcc.EventDelete || event.Revision.Main != 4 || event.Revision.Sub != int64(i) {
			t.Fatalf("event[%d] = %+v", i, event)
		}
	}

	current, err := store.Range(mvcc.RangeRequest{
		Key: []byte("/app/"),
		End: mvcc.PrefixEnd([]byte("/app/")),
	})
	if err != nil {
		t.Fatalf("Range current: %v", err)
	}
	if current.Count != 0 {
		t.Fatalf("current count = %d, want 0", current.Count)
	}

	old, err := store.Range(mvcc.RangeRequest{
		Key:      []byte("/app/"),
		End:      mvcc.PrefixEnd([]byte("/app/")),
		Revision: 3,
	})
	if err != nil {
		t.Fatalf("Range old: %v", err)
	}
	if old.Count != 2 {
		t.Fatalf("revision 3 count = %d, want 2", old.Count)
	}
}

func openStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return store
}

func put(t *testing.T, store *Store, key string, value string) {
	t.Helper()
	if _, err := store.Put(mvcc.PutRequest{Key: []byte(key), Value: []byte(value)}); err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
}
