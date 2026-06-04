package mvcc

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
)

const snapshotVersion = 1

var ErrInvalidSnapshot = errors.New("mvcc: invalid snapshot")

type SnapshotData struct {
	Version         int           `json:"version"`                // snapshot 格式版本号
	Revision        int64         `json:"revision"`               // 当前 mvcc 已经推进到的最新 revision
	CompactRevision int64         `json:"compact_revision"`       // 当前 mvcc history 已经 compact 到哪个 revision
	AppliedIndex    uint64        `json:"applied_index"`          // 状态机已经执行到哪条 raft log
	Current         []KeyValue    `json:"current"`                // current view
	CompactBase     []KeyValue    `json:"compact_base,omitempty"` // compact 瞬间的完整 view
	History         []Event       `json:"history,omitempty"`      // 还没有被 compact 掉的 mvcc event
	Leases          []LeaseRecord `json:"leases,omitempty"`       // 所有 lease 元信息
}

// EncodeSnapshot 将 Store 的逻辑视图编码成 Raft snapshot data。
func EncodeSnapshot(snapshot SnapshotData) ([]byte, error) {
	snapshot.Version = snapshotVersion
	return json.Marshal(cloneSnapshotData(snapshot))
}

// DecodeSnapshot 将 Raft snapshot data 还原为 Store 可以恢复的逻辑视图。
func DecodeSnapshot(data []byte) (SnapshotData, error) {
	if len(data) == 0 {
		return SnapshotData{}, ErrInvalidSnapshot
	}
	var snapshot SnapshotData
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return SnapshotData{}, err
	}
	if snapshot.Version != snapshotVersion {
		return SnapshotData{}, ErrInvalidSnapshot
	}
	return cloneSnapshotData(snapshot), nil
}

func cloneSnapshotData(snapshot SnapshotData) SnapshotData {
	return SnapshotData{
		Version:         snapshot.Version,
		Revision:        snapshot.Revision,
		CompactRevision: snapshot.CompactRevision,
		AppliedIndex:    snapshot.AppliedIndex,
		Current:         sortedKeyValues(CloneKeyValues(snapshot.Current)),
		CompactBase:     sortedKeyValues(CloneKeyValues(snapshot.CompactBase)),
		History:         CloneEvents(snapshot.History),
		Leases:          sortedLeaseRecords(CloneLeaseRecords(snapshot.Leases)),
	}
}

func sortedKeyValues(kvs []KeyValue) []KeyValue {
	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
	})
	return kvs
}

func sortedLeaseRecords(records []LeaseRecord) []LeaseRecord {
	sort.Slice(records, func(i, j int) bool {
		return records[i].LeaseID < records[j].LeaseID
	})
	return records
}
