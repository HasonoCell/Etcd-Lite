package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type testStorage struct {
	mu    sync.Mutex
	state PersistentState
}

func (s *testStorage) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clonePersistentState(s.state), nil
}

func (s *testStorage) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = clonePersistentState(state)
	return nil
}

type localTransport struct {
	mu    sync.RWMutex
	nodes map[MemberID]*Raft
}

func newLocalTransport() *localTransport {
	return &localTransport{nodes: make(map[MemberID]*Raft)}
}

func (t *localTransport) register(rf *Raft) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[rf.ID()] = rf
}

func (t *localTransport) node(id MemberID) (*Raft, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	rf, ok := t.nodes[id]
	if !ok {
		return nil, errors.New("missing node")
	}
	return rf, nil
}

func (t *localTransport) SendRequestVote(ctx context.Context, to MemberID, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rf, err := t.node(to)
	if err != nil {
		return nil, err
	}
	resp := new(RequestVoteResponse)
	rf.RequestVote(req, resp)
	return resp, nil
}

func (t *localTransport) SendAppendEntries(ctx context.Context, to MemberID, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rf, err := t.node(to)
	if err != nil {
		return nil, err
	}
	resp := new(AppendEntriesResponse)
	rf.AppendEntries(req, resp)
	return resp, nil
}

func (t *localTransport) SendInstallSnapshot(ctx context.Context, to MemberID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rf, err := t.node(to)
	if err != nil {
		return nil, err
	}
	resp := new(InstallSnapshotResponse)
	rf.InstallSnapshot(req, resp)
	return resp, nil
}

func TestRaftElectsLeaderAndReplicatesCommand(t *testing.T) {
	nodes, applyChans := newTestCluster(t, []MemberID{1, 2, 3})
	defer stopCluster(nodes)

	leader := waitLeader(t, nodes)
	index, _, ok := leader.Start([]byte("set /foo bar"))
	if !ok {
		t.Fatalf("leader rejected Start")
	}
	waitApplied(t, applyChans, index, []byte("set /foo bar"))
}

func TestRaftReadIndexAndStepDown(t *testing.T) {
	nodes, applyChans := newTestCluster(t, []MemberID{1, 2, 3})
	defer stopCluster(nodes)

	leader := waitLeader(t, nodes)
	index, _, ok := leader.Start([]byte("put /ready true"))
	if !ok {
		t.Fatalf("leader rejected Start")
	}
	waitApplied(t, applyChans, index, []byte("put /ready true"))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	readIndex, err := leader.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex failed: %v", err)
	}
	if readIndex < index {
		t.Fatalf("ReadIndex=%d, want at least %d", readIndex, index)
	}

	status := leader.Status()
	leader.AppendEntries(&AppendEntriesRequest{
		Term:         status.Term + 1,
		LeaderID:     99,
		PrevLogIndex: status.LastLogIndex,
		PrevLogTerm:  status.Term,
	}, new(AppendEntriesResponse))
	if got := leader.Status().State; got != StateFollower {
		t.Fatalf("state after higher-term AppendEntries = %s, want follower", got)
	}
}

func TestRaftSnapshotCompactsLocalLog(t *testing.T) {
	nodes, applyChans := newTestCluster(t, []MemberID{1, 2, 3})
	defer stopCluster(nodes)

	leader := waitLeader(t, nodes)
	index, _, ok := leader.Start([]byte("compact me"))
	if !ok {
		t.Fatalf("leader rejected Start")
	}
	waitApplied(t, applyChans, index, []byte("compact me"))

	leader.Snapshot(index, []byte("snapshot-state"))
	status := leader.Status()
	if status.SnapshotIndex != index {
		t.Fatalf("snapshot index = %d, want %d", status.SnapshotIndex, index)
	}
}

func newTestCluster(t *testing.T, ids []MemberID) (map[MemberID]*Raft, map[MemberID]chan ApplyMsg) {
	t.Helper()
	transport := newLocalTransport()
	nodes := make(map[MemberID]*Raft)
	applyChans := make(map[MemberID]chan ApplyMsg)
	for _, id := range ids {
		applyCh := make(chan ApplyMsg, 64)
		rf, err := New(Config{
			ID:                id,
			Peers:             ids,
			Storage:           &testStorage{},
			Transport:         transport,
			ApplyCh:           applyCh,
			ElectionTimeout:   80 * time.Millisecond,
			HeartbeatInterval: 15 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("New(%d): %v", id, err)
		}
		nodes[id] = rf
		applyChans[id] = applyCh
		transport.register(rf)
	}
	return nodes, applyChans
}

func stopCluster(nodes map[MemberID]*Raft) {
	for _, rf := range nodes {
		rf.Stop()
	}
}

func waitLeader(t *testing.T, nodes map[MemberID]*Raft) *Raft {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var leader *Raft
		var leaders int
		for _, rf := range nodes {
			if rf.Status().State == StateLeader {
				leader = rf
				leaders++
			}
		}
		if leaders == 1 {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for one leader")
	return nil
}

func waitApplied(t *testing.T, applyChans map[MemberID]chan ApplyMsg, index uint64, command []byte) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	seen := make(map[MemberID]bool)
	for len(seen) < len(applyChans) {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for index %d on all nodes; seen=%v", index, seen)
		default:
		}
		for id, ch := range applyChans {
			if seen[id] {
				continue
			}
			select {
			case msg := <-ch:
				if msg.CommandValid && msg.CommandIndex == index && string(msg.Command) == string(command) {
					seen[id] = true
				}
			default:
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func clonePersistentState(state PersistentState) PersistentState {
	return PersistentState{
		HardState: state.HardState,
		Entries:   cloneEntries(state.Entries),
		Snapshot:  cloneSnapshot(state.Snapshot),
	}
}
