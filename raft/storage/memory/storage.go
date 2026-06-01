package memory

import (
	"sync"

	"github.com/HasonoCell/Etcd-Lite/raft/core"
)

// Storage 将 Raft persistent state 保存在进程内存中。
// 它适合不需要 crash recovery 的测试和 demo 场景。
type Storage struct {
	mu    sync.Mutex
	state core.PersistentState
}

func New() *Storage {
	return &Storage{}
}

func NewWithState(state core.PersistentState) *Storage {
	return &Storage{state: clonePersistentState(state)}
}

func (s *Storage) Load() (core.PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clonePersistentState(s.state), nil
}

func (s *Storage) Save(state core.PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = clonePersistentState(state)
	return nil
}

func clonePersistentState(state core.PersistentState) core.PersistentState {
	return core.PersistentState{
		HardState: state.HardState,
		Entries:   cloneEntries(state.Entries),
		Snapshot:  cloneSnapshot(state.Snapshot),
	}
}

func cloneEntries(entries []core.Entry) []core.Entry {
	out := make([]core.Entry, len(entries))
	for i, entry := range entries {
		out[i] = core.Entry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: append([]byte(nil), entry.Command...),
		}
	}
	return out
}

func cloneSnapshot(snapshot core.Snapshot) core.Snapshot {
	return core.Snapshot{
		Index: snapshot.Index,
		Term:  snapshot.Term,
		Data:  append([]byte(nil), snapshot.Data...),
	}
}
