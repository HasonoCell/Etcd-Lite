package mvcc

import (
	"encoding/json"
	"errors"
)

type RequestID struct {
	ClientID  uint64 `json:"client_id"`
	RequestID uint64 `json:"request_id"`
}

type CommandKind string

const (
	CommandPut         CommandKind = "put"
	CommandDeleteRange CommandKind = "delete_range"
	CommandTxn         CommandKind = "txn"
)

// Command 是写入 Raft log 的内部 command，不直接复用外部 RPC request。
type Command struct {
	ID          RequestID           `json:"id"`
	Kind        CommandKind         `json:"kind"`
	Put         *PutCommand         `json:"put,omitempty"`
	DeleteRange *DeleteRangeCommand `json:"delete_range,omitempty"`
	Txn         *TxnCommand         `json:"txn,omitempty"`
}

type PutCommand struct {
	Key     []byte `json:"key"`
	Value   []byte `json:"value"`
	LeaseID int64  `json:"lease_id,omitempty"`
	PrevKV  bool   `json:"prev_kv,omitempty"`
}

type DeleteRangeCommand struct {
	Key    []byte `json:"key"`
	End    []byte `json:"end,omitempty"`
	PrevKV bool   `json:"prev_kv,omitempty"`
}

// TxnCommand 表示 compare 成功时执行 success ops，否则执行 failure ops。
// 可以发现，Op 其实粒度比 Command 更小。
// ! Txn 即事务本质上是一种业务概念，CAS 是实现 Txn 冲突检测和原子提交的一种思路。
type TxnCommand struct {
	Compare []Compare `json:"compare,omitempty"`
	Success []Op      `json:"success,omitempty"`
	Failure []Op      `json:"failure,omitempty"`
}

type CompareTarget string

const (
	CompareVersion        CompareTarget = "version"
	CompareCreateRevision CompareTarget = "create_revision"
	CompareModRevision    CompareTarget = "mod_revision"
	CompareValue          CompareTarget = "value"
	CompareLease          CompareTarget = "lease"
)

type CompareResult string

const (
	CompareEqual    CompareResult = "="
	CompareNotEqual CompareResult = "!="
	CompareGreater  CompareResult = ">"
	CompareLess     CompareResult = "<"
)

type Compare struct {
	Key            []byte        `json:"key"`
	Target         CompareTarget `json:"target"`
	Result         CompareResult `json:"result"`
	Version        int64         `json:"version,omitempty"`
	CreateRevision int64         `json:"create_revision,omitempty"`
	ModRevision    int64         `json:"mod_revision,omitempty"`
	Value          []byte        `json:"value,omitempty"`
	LeaseID        int64         `json:"lease_id,omitempty"`
}

type OpKind string

const (
	OpRange       OpKind = "range"
	OpPut         OpKind = "put"
	OpDeleteRange OpKind = "delete_range"
)

type Op struct {
	Kind        OpKind              `json:"kind"`
	Range       *RangeRequest       `json:"range,omitempty"`
	Put         *PutCommand         `json:"put,omitempty"`
	DeleteRange *DeleteRangeCommand `json:"delete_range,omitempty"`
}

type OpResponse struct {
	Kind        OpKind               `json:"kind"`
	Range       *RangeResponse       `json:"range,omitempty"`
	Put         *PutResponse         `json:"put,omitempty"`
	DeleteRange *DeleteRangeResponse `json:"delete_range,omitempty"`
}

type ApplyResult struct {
	Revision  int64
	Succeeded bool
	Responses []OpResponse
	Events    []Event
	Err       error
}

func EncodeCommand(command Command) ([]byte, error) {
	if err := command.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(command)
}

func DecodeCommand(data []byte) (Command, error) {
	var command Command
	if err := json.Unmarshal(data, &command); err != nil {
		return Command{}, err
	}
	if err := command.Validate(); err != nil {
		return Command{}, err
	}
	return command, nil
}

func (c Command) Validate() error {
	switch c.Kind {
	case CommandPut:
		if c.Put == nil {
			return errors.Join(ErrInvalidCommand, ErrKeyRequired)
		}
		return ValidateKeyRange(c.Put.Key, nil)
	case CommandDeleteRange:
		if c.DeleteRange == nil {
			return errors.Join(ErrInvalidCommand, ErrKeyRequired)
		}
		return ValidateKeyRange(c.DeleteRange.Key, c.DeleteRange.End)
	case CommandTxn:
		if c.Txn == nil {
			return ErrInvalidCommand
		}
		return ValidateTxnCommand(*c.Txn)
	default:
		return ErrInvalidCommand
	}
}
