package core

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultElectionTimeout   = 900 * time.Millisecond
	defaultHeartbeatInterval = 120 * time.Millisecond
)

var (
	errMissingStorage   = errors.New("raft: missing storage")
	errMissingTransport = errors.New("raft: missing transport")
)

type Raft struct {
	mu        sync.Mutex
	id        MemberID
	peers     []MemberID
	storage   Storage
	transport Transport
	applyCh   chan ApplyMsg

	state    State
	leaderID MemberID
	dead     atomic.Bool

	currentTerm uint64
	votedFor    MemberID
	logs        []Entry
	snapshot    Snapshot

	commitIndex uint64
	lastApplied uint64
	nextIndex   map[MemberID]uint64
	matchIndex  map[MemberID]uint64

	electionTimeout   time.Duration
	heartbeatInterval time.Duration
	electionTimer     *time.Timer
	heartbeatTimer    *time.Timer
	applyCond         *sync.Cond
	stopCh            chan struct{}
}

func New(cfg Config) (*Raft, error) {
	if cfg.Storage == nil {
		return nil, errMissingStorage
	}
	if cfg.Transport == nil {
		return nil, errMissingTransport
	}
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = defaultElectionTimeout
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}
	if cfg.ApplyCh == nil {
		cfg.ApplyCh = make(chan ApplyMsg, 128)
	}

	ps, err := cfg.Storage.Load()
	if err != nil {
		return nil, err
	}
	logs := normalizeEntries(ps.Entries, ps.Snapshot)

	rf := &Raft{
		id:                cfg.ID,
		peers:             append([]MemberID(nil), cfg.Peers...),
		storage:           cfg.Storage,
		transport:         cfg.Transport,
		applyCh:           cfg.ApplyCh,
		state:             StateFollower,
		leaderID:          NoMember,
		currentTerm:       ps.HardState.Term,
		votedFor:          ps.HardState.VotedFor,
		logs:              logs,
		snapshot:          cloneSnapshot(ps.Snapshot),
		commitIndex:       max(ps.HardState.Commit, logs[0].Index),
		lastApplied:       logs[0].Index,
		nextIndex:         make(map[MemberID]uint64),
		matchIndex:        make(map[MemberID]uint64),
		electionTimeout:   cfg.ElectionTimeout,
		heartbeatInterval: cfg.HeartbeatInterval,
		electionTimer:     time.NewTimer(randomized(cfg.ElectionTimeout)),
		heartbeatTimer:    time.NewTimer(cfg.HeartbeatInterval),
		stopCh:            make(chan struct{}),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.heartbeatTimer.Stop()
	rf.resetLeaderProgressLocked()

	go rf.ticker()
	go rf.applier()
	return rf, nil
}

func (rf *Raft) ID() MemberID {
	return rf.id
}

// Stop 停止当前 peer。所有长时间运行的 goroutine 都会观察 dead/stopCh 退出。
func (rf *Raft) Stop() {
	if rf.dead.CompareAndSwap(false, true) {
		close(rf.stopCh)
		rf.mu.Lock()
		rf.applyCond.Broadcast()
		rf.mu.Unlock()
	}
}

// Status 返回当前 term、leader、commit/applied index 等运行状态。
func (rf *Raft) Status() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return Status{
		ID:            rf.id,
		LeaderID:      rf.leaderID,
		State:         rf.state,
		Term:          rf.currentTerm,
		CommitIndex:   rf.commitIndex,
		AppliedIndex:  rf.lastApplied,
		LastLogIndex:  rf.lastLogLocked().Index,
		SnapshotIndex: rf.logs[0].Index,
	}
}

// Start 由上层服务调用，用来把一条新 command 放入 Raft 日志。
// 如果当前 peer 不是 leader，则返回 isLeader=false。
func (rf *Raft) Start(command []byte) (uint64, uint64, bool) {
	rf.mu.Lock()
	// 必须是 Leader 才处理 command。
	if rf.state != StateLeader || rf.dead.Load() {
		term := rf.currentTerm
		rf.mu.Unlock()
		return 0, term, false
	}
	entry := Entry{
		Index: rf.lastLogLocked().Index + 1,
		Term:  rf.currentTerm,
		// command 不再像 lab 中那样明文存储，而是转换为字节数组。
		Command: append([]byte(nil), command...),
	}
	rf.logs = append(rf.logs, entry)
	rf.matchIndex[rf.id] = entry.Index
	rf.nextIndex[rf.id] = entry.Index + 1
	term := rf.currentTerm
	_ = rf.persistLocked()
	rf.mu.Unlock()

	rf.broadcastAppendEntries()
	return entry.Index, term, true
}

// ReadIndex 用一次多数派 heartbeat 确认当前 leader 身份，然后返回可线性一致读取的 commit index。
// ! ReadIndex 也是一种针对线性一致读的优化思路，不采用 lab 中的 ReadLog。
func (rf *Raft) ReadIndex(ctx context.Context) (uint64, error) {
	rf.mu.Lock()
	if rf.state != StateLeader || rf.dead.Load() {
		rf.mu.Unlock()
		return 0, ErrNotLeader
	}
	commit := rf.commitIndex
	term := rf.currentTerm
	rf.mu.Unlock()

	// 通过 confirmLeadership 确定处理读请求的 server 的 leader 地位。
	if rf.confirmLeadership(ctx, term) {
		return commit, nil
	}
	return 0, ErrNotLeader
}

// Snapshot 由上层状态机在创建快照后调用，表示 index 及之前的日志已经不再需要。
func (rf *Raft) Snapshot(index uint64, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.logs[0].Index || index > rf.lastLogLocked().Index {
		return
	}
	term := rf.termAtLocked(index)
	offset := index - rf.logs[0].Index
	newLogs := []Entry{{Index: index, Term: term}}
	newLogs = append(newLogs, cloneEntries(rf.logs[offset+1:])...)
	rf.logs = newLogs
	rf.snapshot = Snapshot{Index: index, Term: term, Data: append([]byte(nil), data...)}
	_ = rf.persistLocked()
}

// RequestVote 处理候选人的投票请求。
func (rf *Raft) RequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	resp.Term = rf.currentTerm
	if req.Term < rf.currentTerm {
		return
	}
	if req.Term > rf.currentTerm {
		rf.becomeFollowerLocked(req.Term, NoMember)
	}

	// 候选人的日志必须至少和本机一样新，本机才可能投票。
	last := rf.lastLogLocked()
	upToDate := req.LastLogTerm > last.Term || (req.LastLogTerm == last.Term && req.LastLogIndex >= last.Index)
	canVote := rf.votedFor == NoMember || rf.votedFor == req.CandidateID
	if canVote && upToDate {
		rf.votedFor = req.CandidateID
		resp.VoteGranted = true
		rf.resetElectionTimerLocked()
		_ = rf.persistLocked()
	}
	resp.Term = rf.currentTerm
}

// AppendEntries 同时承担 leader heartbeat、日志复制、commit index 推进三件事。
func (rf *Raft) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	resp.Term = rf.currentTerm
	if req.Term < rf.currentTerm {
		return
	}
	if req.Term > rf.currentTerm || rf.state != StateFollower {
		rf.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	rf.leaderID = req.LeaderID
	rf.resetElectionTimerLocked()

	first := rf.logs[0].Index
	last := rf.lastLogLocked().Index
	// PrevLogIndex 就是从 leader 传来的某 peer 的预期已匹配日志位置
	if req.PrevLogIndex < first {
		resp.ConflictIndex = first + 1
		return
	}
	if req.PrevLogIndex > last {
		resp.ConflictIndex = last + 1
		return
	}
	// 冲突逻辑：PrevLogIndex 上有日志但 term 不对时，返回冲突 term 的第一条位置。
	if rf.termAtLocked(req.PrevLogIndex) != req.PrevLogTerm {
		resp.ConflictTerm = rf.termAtLocked(req.PrevLogIndex)
		idx := req.PrevLogIndex
		for idx > first && rf.termAtLocked(idx-1) == resp.ConflictTerm {
			idx--
		}
		resp.ConflictIndex = idx
		return
	}

	// 如果 follower 没有这条日志，追加。
	// 如果 follower 有同 index 但 term 不同，说明从这里开始是错误分支：
	// 1. 截断 follower 从这个 index 开始的旧日志
	// 2. 追加 leader 从这个 index 开始的日志
	for i, entry := range req.Entries {
		if entry.Index <= rf.lastLogLocked().Index && rf.termAtLocked(entry.Index) == entry.Term {
			continue
		}
		cut := entry.Index - first
		rf.logs = append(cloneEntries(rf.logs[:cut]), cloneEntries(req.Entries[i:])...)
		break
	}
	// 复制完后根据 leaderCommit 推进 follower 的提交进度。
	if req.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(req.LeaderCommit, rf.lastLogLocked().Index)
		rf.applyCond.Signal()
	}
	resp.Term = rf.currentTerm
	resp.Success = true
	_ = rf.persistLocked()
}

// InstallSnapshot 在 follower 日志落后到 leader 已经 compact 时安装快照。
func (rf *Raft) InstallSnapshot(req *InstallSnapshotRequest, resp *InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	resp.Term = rf.currentTerm
	if req.Term < rf.currentTerm {
		return
	}
	if req.Term > rf.currentTerm || rf.state != StateFollower {
		rf.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	rf.leaderID = req.LeaderID
	rf.resetElectionTimerLocked()
	if req.LastIncludedIndex <= rf.commitIndex {
		resp.Term = rf.currentTerm
		return
	}

	// 如果快照边界之后的日志还能和 leader 对齐，就保留后续日志；否则全部丢弃。
	if req.LastIncludedIndex < rf.lastLogLocked().Index && rf.termAtLocked(req.LastIncludedIndex) == req.LastIncludedTerm {
		offset := req.LastIncludedIndex - rf.logs[0].Index
		rf.logs = append([]Entry{{Index: req.LastIncludedIndex, Term: req.LastIncludedTerm}}, cloneEntries(rf.logs[offset+1:])...)
	} else {
		rf.logs = []Entry{{Index: req.LastIncludedIndex, Term: req.LastIncludedTerm}}
	}
	rf.snapshot = Snapshot{Index: req.LastIncludedIndex, Term: req.LastIncludedTerm, Data: append([]byte(nil), req.Data...)}
	rf.commitIndex = req.LastIncludedIndex
	rf.lastApplied = req.LastIncludedIndex
	_ = rf.persistLocked()

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      append([]byte(nil), req.Data...),
			SnapshotTerm:  req.LastIncludedTerm,
			SnapshotIndex: req.LastIncludedIndex,
		}
	}()
	resp.Term = rf.currentTerm
}

// ticker 在选举超时后发起新一轮选举，leader 则周期性发送 heartbeat。
func (rf *Raft) ticker() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.electionTimer.C:
			rf.startElection()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			isLeader := rf.state == StateLeader && !rf.dead.Load()
			if isLeader {
				rf.heartbeatTimer.Reset(rf.heartbeatInterval)
			}
			rf.mu.Unlock()
			if isLeader {
				rf.broadcastAppendEntries()
			}
		}
	}
}

// startElection 切换到 candidate，给自己投票，并向其他 peer 并发发送 RequestVote。
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.dead.Load() {
		rf.mu.Unlock()
		return
	}
	rf.state = StateCandidate
	rf.currentTerm++
	rf.votedFor = rf.id
	rf.leaderID = NoMember
	term := rf.currentTerm
	last := rf.lastLogLocked()
	votes := 1
	_ = rf.persistLocked()
	rf.resetElectionTimerLocked()
	rf.mu.Unlock()

	req := &RequestVoteRequest{Term: term, CandidateID: rf.id, LastLogIndex: last.Index, LastLogTerm: last.Term}
	// 遍历 peer 发 RequestVote。
	for _, peer := range rf.peers {
		if peer == rf.id {
			continue
		}
		go func(peer MemberID) {
			ctx, cancel := context.WithTimeout(context.Background(), rf.heartbeatInterval)
			defer cancel()
			resp, err := rf.transport.SendRequestVote(ctx, peer, req)
			if err != nil {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.state != StateCandidate || rf.currentTerm != term {
				return
			}
			if resp.Term > rf.currentTerm {
				rf.becomeFollowerLocked(resp.Term, NoMember)
				return
			}
			if resp.VoteGranted {
				votes++
				// quorum 返回当前多数派人数
				if votes >= rf.quorum() {
					rf.becomeLeaderLocked()
					// 成为新 leader 后依旧走 AppendEntries 同步一遍日志。
					go rf.broadcastAppendEntries()
				}
			}
		}(peer)
	}
}

// broadcastAppendEntries 唤醒所有 follower 的复制流程（replicator 协程）。
func (rf *Raft) broadcastAppendEntries() {
	for _, peer := range rf.peers {
		if peer == rf.id {
			continue
		}
		go rf.replicator(peer)
	}
}

// replicator 对单个 follower 执行一轮日志复制；如果 follower 太落后则发送 snapshot。
func (rf *Raft) replicator(peer MemberID) {
	rf.mu.Lock()
	if rf.state != StateLeader || rf.dead.Load() {
		rf.mu.Unlock()
		return
	}
	// leader 认为 peer 的预期位置
	next := rf.nextIndex[peer]
	first := rf.logs[0].Index
	// 如果 follower 预期匹配的位置已经被 leader compact，走快照安装流程。
	if next <= first {
		req := &InstallSnapshotRequest{
			Term:              rf.currentTerm,
			LeaderID:          rf.id,
			LastIncludedIndex: rf.snapshot.Index,
			LastIncludedTerm:  rf.snapshot.Term,
			Data:              append([]byte(nil), rf.snapshot.Data...),
		}
		rf.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), rf.electionTimeout)
		defer cancel()
		resp, err := rf.transport.SendInstallSnapshot(ctx, peer, req)
		if err == nil {
			rf.handleInstallSnapshotResponse(peer, req, resp)
		}
		return
	}
	prev := next - 1
	entries := cloneEntries(rf.logs[next-first:])
	req := &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderID:     rf.id,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.termAtLocked(prev),
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	rf.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), rf.electionTimeout)
	defer cancel()
	resp, err := rf.transport.SendAppendEntries(ctx, peer, req)
	if err == nil {
		rf.handleAppendEntriesResponse(peer, req, resp)
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer MemberID, req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader || rf.currentTerm != req.Term {
		return
	}
	if resp.Term > rf.currentTerm {
		rf.becomeFollowerLocked(resp.Term, NoMember)
		return
	}
	if resp.Success {
		match := req.PrevLogIndex + uint64(len(req.Entries))
		rf.matchIndex[peer] = match
		rf.nextIndex[peer] = match + 1
		// 开始提交。
		rf.advanceCommitLocked()
		return
	}
	// leader 针对日志冲突的处理逻辑：优先使用 follower 返回的 ConflictIndex 回退 nextIndex。
	// ! ConflictIndex 也是一种优化思路，避免了 leader 一个个回退 index，避免不必要的网络请求。
	if resp.ConflictIndex > 0 {
		rf.nextIndex[peer] = resp.ConflictIndex
	} else if rf.nextIndex[peer] > 1 {
		rf.nextIndex[peer]--
	}
}

func (rf *Raft) handleInstallSnapshotResponse(peer MemberID, req *InstallSnapshotRequest, resp *InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader || rf.currentTerm != req.Term {
		return
	}
	if resp.Term > rf.currentTerm {
		rf.becomeFollowerLocked(resp.Term, NoMember)
		return
	}
	rf.matchIndex[peer] = req.LastIncludedIndex
	rf.nextIndex[peer] = req.LastIncludedIndex + 1
}

// confirmLeadership 通过一轮空 AppendEntries 确认当前 leader 仍能联系到多数派。
func (rf *Raft) confirmLeadership(ctx context.Context, term uint64) bool {
	var wg sync.WaitGroup
	var oks atomic.Int32
	oks.Store(1)

	for _, peer := range rf.peers {
		if peer == rf.id {
			continue
		}
		wg.Add(1)
		go func(peer MemberID) {
			defer wg.Done()
			rf.mu.Lock()
			if rf.state != StateLeader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			prev := rf.lastLogLocked().Index
			req := &AppendEntriesRequest{
				Term:         term,
				LeaderID:     rf.id,
				PrevLogIndex: prev,
				PrevLogTerm:  rf.termAtLocked(prev),
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			resp, err := rf.transport.SendAppendEntries(ctx, peer, req)
			if err == nil && resp.Success && resp.Term == term {
				oks.Add(1)
			}
		}(peer)
	}
	wg.Wait()
	return int(oks.Load()) >= rf.quorum()
}

// applier 由 applyCond 控制起停，将 committed entries 按顺序推给上层状态机。
// 一组 committed entries 的区间在 lastApplied 和 commitIndex 之间（减去 first 索引重置开始位置）。
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !rf.dead.Load() && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.dead.Load() {
			rf.mu.Unlock()
			return
		}
		first := rf.logs[0].Index
		start := rf.lastApplied + 1
		end := rf.commitIndex
		entries := cloneEntries(rf.logs[start-first : end-first+1])
		rf.mu.Unlock()

		// 日志复制可能一次提交一批 entries，这里逐条发送 ApplyMsg。
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      append([]byte(nil), entry.Command...),
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}

		rf.mu.Lock()
		if rf.lastApplied < end {
			rf.lastApplied = end
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) becomeFollowerLocked(term uint64, leader MemberID) {
	rf.state = StateFollower
	rf.currentTerm = term
	rf.votedFor = NoMember
	rf.leaderID = leader
	rf.heartbeatTimer.Stop()
	rf.resetElectionTimerLocked()
	_ = rf.persistLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = StateLeader
	rf.leaderID = rf.id
	rf.resetLeaderProgressLocked()
	rf.matchIndex[rf.id] = rf.lastLogLocked().Index
	rf.nextIndex[rf.id] = rf.lastLogLocked().Index + 1
	rf.electionTimer.Stop()
	rf.heartbeatTimer.Reset(1 * time.Millisecond)
}

func (rf *Raft) resetLeaderProgressLocked() {
	last := rf.lastLogLocked().Index
	for _, peer := range rf.peers {
		rf.matchIndex[peer] = 0
		rf.nextIndex[peer] = last + 1
	}
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionTimer.Reset(randomized(rf.electionTimeout))
}

// advanceCommitLocked 是 leader 的提交逻辑：
// 对所有 peers 的 matchIndex 从小到大排序，取多数派覆盖的位置作为候选 commit index。
func (rf *Raft) advanceCommitLocked() {
	// 将每个 peer 的 matchIndex 拿出来
	matched := make([]uint64, 0, len(rf.peers))
	for _, peer := range rf.peers {
		matched = append(matched, rf.matchIndex[peer])
	}
	insertionSort(matched)

	newCommitIndex := matched[len(matched)-rf.quorum()]
	if newCommitIndex > rf.commitIndex && rf.termAtLocked(newCommitIndex) == rf.currentTerm {
		rf.commitIndex = newCommitIndex
		_ = rf.persistLocked()
		rf.applyCond.Signal()
	}
}

func (rf *Raft) quorum() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) lastLogLocked() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) termAtLocked(index uint64) uint64 {
	if index == rf.logs[0].Index {
		return rf.logs[0].Term
	}
	first := rf.logs[0].Index
	if index < first || index-first >= uint64(len(rf.logs)) {
		return 0
	}
	return rf.logs[index-first].Term
}

// persistLocked 保存 Raft 的持久状态，避免 crash 后丢失 term/vote/log/snapshot。
func (rf *Raft) persistLocked() error {
	return rf.storage.Save(PersistentState{
		HardState: HardState{Term: rf.currentTerm, VotedFor: rf.votedFor, Commit: rf.commitIndex},
		Entries:   cloneEntries(rf.logs),
		Snapshot:  cloneSnapshot(rf.snapshot),
	})
}

func normalizeEntries(entries []Entry, snapshot Snapshot) []Entry {
	if len(entries) == 0 {
		return []Entry{{Index: snapshot.Index, Term: snapshot.Term}}
	}
	return cloneEntries(entries)
}

func cloneEntries(entries []Entry) []Entry {
	out := make([]Entry, len(entries))
	for i, entry := range entries {
		out[i] = Entry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: append([]byte(nil), entry.Command...),
		}
	}
	return out
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	return Snapshot{Index: snapshot.Index, Term: snapshot.Term, Data: append([]byte(nil), snapshot.Data...)}
}

func randomized(base time.Duration) time.Duration {
	if base <= 0 {
		base = defaultElectionTimeout
	}
	return base + time.Duration(rand.Int63n(int64(base)))
}

func insertionSort(values []uint64) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
