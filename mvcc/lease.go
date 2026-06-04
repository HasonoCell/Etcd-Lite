package mvcc

import (
	"bytes"
	"sort"
)

// LeaseRecordKeys 返回按 byte order 排序后的 lease attached keys。
func LeaseRecordKeys(record LeaseRecord) [][]byte {
	keys := make([][]byte, len(record.Keys))
	for i, key := range record.Keys {
		keys[i] = append([]byte(nil), key...)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	return keys
}

// AddLeaseKey 将 key 加入 attached keys，并保持去重和稳定排序。
func AddLeaseKey(keys [][]byte, key []byte) [][]byte {
	for _, existing := range keys {
		if bytes.Equal(existing, key) {
			return CloneKeys(keys)
		}
	}
	out := CloneKeys(keys)
	out = append(out, append([]byte(nil), key...))
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i], out[j]) < 0
	})
	return out
}

// RemoveLeaseKey 从 attached keys 中移除 key。
func RemoveLeaseKey(keys [][]byte, key []byte) [][]byte {
	out := make([][]byte, 0, len(keys))
	for _, existing := range keys {
		if !bytes.Equal(existing, key) {
			out = append(out, append([]byte(nil), existing...))
		}
	}
	return out
}

func CloneKeys(keys [][]byte) [][]byte {
	out := make([][]byte, len(keys))
	for i, key := range keys {
		out[i] = append([]byte(nil), key...)
	}
	return out
}
