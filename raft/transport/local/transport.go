package local

import (
	"context"
	"errors"
	"sync"

	"github.com/HasonoCell/Etcd-Lite/raft/core"
)

var ErrPeerUnavailable = errors.New("raft local transport: peer unavailable")

// Transport 是进程内 Raft transport（即通过 nodes map 记录所有 rf 然后互相调用）。
// 它保留 RPC boundary，但不需要真正监听 network port。
type Transport struct {
	mu       sync.RWMutex
	nodes    map[core.MemberID]*core.Raft
	disabled map[core.MemberID]bool
}

func New() *Transport {
	return &Transport{
		nodes:    make(map[core.MemberID]*core.Raft),
		disabled: make(map[core.MemberID]bool),
	}
}

func (t *Transport) Register(rf *core.Raft) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[rf.ID()] = rf
	t.disabled[rf.ID()] = false
}

func (t *Transport) Unregister(id core.MemberID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.nodes, id)
	delete(t.disabled, id)
}

func (t *Transport) Connect(id core.MemberID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.disabled[id] = false
}

func (t *Transport) Disconnect(id core.MemberID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.disabled[id] = true
}

func (t *Transport) SendRequestVote(ctx context.Context, to core.MemberID, req *core.RequestVoteRequest) (*core.RequestVoteResponse, error) {
	rf, err := t.node(ctx, to)
	if err != nil {
		return nil, err
	}
	resp := new(core.RequestVoteResponse)
	rf.RequestVote(cloneRequestVoteRequest(req), resp)
	return resp, nil
}

func (t *Transport) SendAppendEntries(ctx context.Context, to core.MemberID, req *core.AppendEntriesRequest) (*core.AppendEntriesResponse, error) {
	rf, err := t.node(ctx, to)
	if err != nil {
		return nil, err
	}
	resp := new(core.AppendEntriesResponse)
	rf.AppendEntries(cloneAppendEntriesRequest(req), resp)
	return resp, nil
}

func (t *Transport) SendInstallSnapshot(ctx context.Context, to core.MemberID, req *core.InstallSnapshotRequest) (*core.InstallSnapshotResponse, error) {
	rf, err := t.node(ctx, to)
	if err != nil {
		return nil, err
	}
	resp := new(core.InstallSnapshotResponse)
	rf.InstallSnapshot(cloneInstallSnapshotRequest(req), resp)
	return resp, nil
}

func (t *Transport) node(ctx context.Context, id core.MemberID) (*core.Raft, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.disabled[id] {
		return nil, ErrPeerUnavailable
	}
	rf, ok := t.nodes[id]
	if !ok {
		return nil, ErrPeerUnavailable
	}
	return rf, nil
}

func cloneRequestVoteRequest(req *core.RequestVoteRequest) *core.RequestVoteRequest {
	if req == nil {
		return nil
	}
	cp := *req
	return &cp
}

func cloneAppendEntriesRequest(req *core.AppendEntriesRequest) *core.AppendEntriesRequest {
	if req == nil {
		return nil
	}
	cp := *req
	cp.Entries = cloneEntries(req.Entries)
	return &cp
}

func cloneInstallSnapshotRequest(req *core.InstallSnapshotRequest) *core.InstallSnapshotRequest {
	if req == nil {
		return nil
	}
	cp := *req
	cp.Data = append([]byte(nil), req.Data...)
	return &cp
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
