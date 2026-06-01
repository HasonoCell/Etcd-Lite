package core

import (
	"context"
	"errors"
	"time"
)

type MemberID uint64

const NoMember MemberID = 0

type State uint8

const (
	StateFollower State = iota
	StateCandidate
	StateLeader
)

func (s State) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return "unknown"
	}
}

var (
	ErrStopped   = errors.New("raft: stopped")
	ErrNotLeader = errors.New("raft: not leader")
)

type Entry struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// HardState 是 Raft crash 后必须恢复的核心状态。
type HardState struct {
	Term     uint64
	VotedFor MemberID
	Commit   uint64
}

// Snapshot 保存已经压缩进状态机快照的日志边界和快照数据。
type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// PersistentState 是 Storage 一次性读写的持久化视图。
type PersistentState struct {
	HardState HardState
	Entries   []Entry
	Snapshot  Snapshot
}

// Storage 负责保存 Raft 状态，替代 6.824 lab 中的内存 Persister。
type Storage interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

// Transport 负责节点间 RPC，替代 6.824 lab 中的 labrpc。
type Transport interface {
	SendRequestVote(ctx context.Context, to MemberID, req *RequestVoteRequest) (*RequestVoteResponse, error)
	SendAppendEntries(ctx context.Context, to MemberID, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	SendInstallSnapshot(ctx context.Context, to MemberID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  MemberID
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     MemberID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	LeaderCommit uint64
	Entries      []Entry
}

type AppendEntriesResponse struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64
}

type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          MemberID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type InstallSnapshotResponse struct {
	Term uint64
}

// ApplyMsg 在 Raft peer 感知到新的 committed entry 后发送给上层状态机。
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandTerm  uint64
	CommandIndex uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  uint64
	SnapshotIndex uint64
}

type Status struct {
	ID            MemberID
	LeaderID      MemberID
	State         State
	Term          uint64
	CommitIndex   uint64
	AppliedIndex  uint64
	LastLogIndex  uint64
	SnapshotIndex uint64
}

// Config 是创建单个 Raft peer 所需的依赖和运行参数。
type Config struct {
	ID                MemberID
	Peers             []MemberID
	Storage           Storage
	Transport         Transport
	ApplyCh           chan ApplyMsg
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
}
