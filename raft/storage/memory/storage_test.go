package memory

import (
	"testing"

	"github.com/HasonoCell/Etcd-Lite/raft/core"
)

func TestStorageSavesAndLoadsCopy(t *testing.T) {
	storage := New()
	state := core.PersistentState{
		HardState: core.HardState{Term: 3, VotedFor: 2, Commit: 7},
		Entries: []core.Entry{
			{Index: 7, Term: 3, Command: []byte("put a b")},
		},
		Snapshot: core.Snapshot{Index: 6, Term: 2, Data: []byte("snapshot")},
	}
	if err := storage.Save(state); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := storage.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	loaded.Entries[0].Command[0] = 'x'
	loaded.Snapshot.Data[0] = 'x'

	reloaded, err := storage.Load()
	if err != nil {
		t.Fatalf("Load after mutation: %v", err)
	}
	if string(reloaded.Entries[0].Command) != "put a b" {
		t.Fatalf("entry command was mutated through Load: %q", reloaded.Entries[0].Command)
	}
	if string(reloaded.Snapshot.Data) != "snapshot" {
		t.Fatalf("snapshot was mutated through Load: %q", reloaded.Snapshot.Data)
	}
}
