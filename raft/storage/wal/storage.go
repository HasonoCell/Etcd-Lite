package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/HasonoCell/Etcd-Lite/raft/core"
)

const (
	recordMagic   uint32 = 0x45544c57 // magic: "ETLW"
	maxRecordSize uint64 = 64 << 20
)

var ErrCorrupt = errors.New("raft wal: corrupt log")

type Option func(*Storage)

type Storage struct {
	mu       sync.Mutex
	path     string // 存储路径
	syncFile bool   // 是否立即同步文件
}

func Open(path string, opts ...Option) (*Storage, error) {
	s := &Storage{
		path:     path,
		syncFile: true,
	}
	for _, opt := range opts {
		opt(s)
	}
	// 先创建文件夹
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	// 在创建 wal 存储文件
	file, err := os.OpenFile(path, os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	return s, nil
}

func WithSync(syncFile bool) Option {
	return func(s *Storage) {
		s.syncFile = syncFile
	}
}

func (s *Storage) Load() (core.PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.OpenFile(s.path, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return core.PersistentState{}, err
	}
	defer file.Close()

	var state core.PersistentState
	for {
		// 解码存储文件
		next, ok, err := readRecord(file)
		if err != nil {
			return core.PersistentState{}, err
		}
		if !ok {
			return clonePersistentState(state), nil
		}
		state = next
	}
}

func (s *Storage) Save(state core.PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 编码存储文件
	payload, err := encodeState(clonePersistentState(state))
	if err != nil {
		return err
	}
	payloadLen := len(payload)

	// 校验 payload 大小
	if uint64(payloadLen) > maxRecordSize {
		return fmt.Errorf("raft wal: record size %d exceeds limit %d", len(payload), maxRecordSize)
	}

	file, err := os.OpenFile(s.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	header := make([]byte, 16)
	binary.LittleEndian.PutUint32(header[0:4], recordMagic)
	binary.LittleEndian.PutUint64(header[4:12], uint64(payloadLen))           // payload 大小
	binary.LittleEndian.PutUint32(header[12:16], crc32.ChecksumIEEE(payload)) // payload 内容校验
	if _, err := file.Write(header); err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		return err
	}
	if s.syncFile {
		return file.Sync()
	}
	return nil
}

func readRecord(r io.Reader) (core.PersistentState, bool, error) {
	header := make([]byte, 16)
	if _, err := io.ReadFull(r, header); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return core.PersistentState{}, false, nil
		}
		return core.PersistentState{}, false, err
	}
	if binary.LittleEndian.Uint32(header[0:4]) != recordMagic {
		return core.PersistentState{}, false, ErrCorrupt
	}
	size := binary.LittleEndian.Uint64(header[4:12])
	if size > maxRecordSize {
		return core.PersistentState{}, false, ErrCorrupt
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return core.PersistentState{}, false, nil
		}
		return core.PersistentState{}, false, err
	}
	if binary.LittleEndian.Uint32(header[12:16]) != crc32.ChecksumIEEE(payload) {
		return core.PersistentState{}, false, ErrCorrupt
	}
	state, err := decodeState(payload)
	if err != nil {
		return core.PersistentState{}, false, err
	}
	return state, true, nil
}

// 按 gob 格式编码
func encodeState(state core.PersistentState) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// 按 gob 格式解码
func decodeState(data []byte) (core.PersistentState, error) {
	var state core.PersistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&state); err != nil {
		return core.PersistentState{}, err
	}
	return clonePersistentState(state), nil
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
