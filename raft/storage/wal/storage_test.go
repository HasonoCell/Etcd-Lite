package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/HasonoCell/Etcd-Lite/raft/core"
)

func TestStorageReplaysLatestRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	storage, err := Open(path, WithSync(false))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	first := stateWithCommand(1, "first")
	second := stateWithCommand(2, "second")
	if err := storage.Save(first); err != nil {
		t.Fatalf("Save first: %v", err)
	}
	if err := storage.Save(second); err != nil {
		t.Fatalf("Save second: %v", err)
	}

	reopened, err := Open(path, WithSync(false))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	loaded, err := reopened.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.HardState.Term != 2 {
		t.Fatalf("term = %d, want 2", loaded.HardState.Term)
	}
	if got := string(loaded.Entries[1].Command); got != "second" {
		t.Fatalf("command = %q, want second", got)
	}
}

func TestStorageIgnoresTornTrailingRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	storage, err := Open(path, WithSync(false))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := storage.Save(stateWithCommand(1, "stable")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("append torn record: %v", err)
	}
	if _, err := file.Write([]byte{0x57, 0x4c}); err != nil {
		t.Fatalf("write torn record: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close torn record: %v", err)
	}

	reopened, err := Open(path, WithSync(false))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	loaded, err := reopened.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := string(loaded.Entries[1].Command); got != "stable" {
		t.Fatalf("command = %q, want stable", got)
	}
}

func stateWithCommand(term uint64, command string) core.PersistentState {
	return core.PersistentState{
		HardState: core.HardState{Term: term, VotedFor: 1, Commit: 1},
		Entries: []core.Entry{
			{Index: 0, Term: 0},
			{Index: 1, Term: term, Command: []byte(command)},
		},
	}
}
