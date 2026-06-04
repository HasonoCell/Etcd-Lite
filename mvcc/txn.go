package mvcc

import (
	"bytes"
	"sort"
)

/*
txn 作为一个小型事务解释器，它的思路是：提供一个 current view 和一个 TxnCommand，
判断 compare，选择 success/failure 分支，在 working view 上顺序执行 Op，
最后产出新的 current view、events、responses。memory 和 bbolt 共享这个逻辑。
*/

// TxnExecution 是一次 Txn 在 working view 上执行后的结果。
// Store 可以直接把 Events 持久化到 history view，并用 Current 替换或更新 current view。
type TxnExecution struct {
	Revision  int64               // 这次 Txn 执行后的 main revision
	Succeeded bool                // compare 是否全部成立
	Responses []OpResponse        // 每个 Op 对应的 response
	Events    []Event             // 这次 Txn 产生的 history events
	Current   map[string]KeyValue // 执行 Txn 后的新 current view
}

// ExecuteTxn 在 current view 的副本上执行 compare/success/failure。
// 所有写操作共享一个 main revision；同一个 Txn 内的多个 event 用 sub revision 保持顺序。
func ExecuteTxn(current map[string]KeyValue, currentRevision int64, txn TxnCommand) (TxnExecution, error) {
	return ExecuteTxnWithLeaseValidator(current, currentRevision, txn, nil)
}

// ExecuteTxnWithLeaseValidator 在 ExecuteTxn 基础上校验被执行 branch 里的 lease attach。
// leaseExists 为 nil 时跳过 lease 存在性校验，主要用于不关心 lease 的测试或纯内存计算。
func ExecuteTxnWithLeaseValidator(current map[string]KeyValue, currentRevision int64, txn TxnCommand, leaseExists func(int64) bool) (TxnExecution, error) {
	if err := ValidateTxnCommand(txn); err != nil {
		return TxnExecution{}, err
	}

	// 注意：compare 的时候是直接在 current view 上比较的
	succeeded, err := txnComparesSucceeded(current, txn.Compare)
	if err != nil {
		return TxnExecution{}, err
	}

	// 确定分支
	branch := txn.Success
	if !succeeded {
		branch = txn.Failure
	}

	// 执行 successs/failure 分支时修改的是 current view 副本。
	currentClone := cloneView(current)
	responses, events, err := executeTxnOps(currentClone, currentRevision, branch, leaseExists)
	if err != nil {
		return TxnExecution{}, err
	}

	// txn command 推进一次 Revision 中的 main，而 ops 推进 sub
	revision := currentRevision
	if len(events) > 0 {
		revision = currentRevision + 1
	}
	// 统一子 ops 的 revision
	normalizeOpResponseRevisions(responses, revision)

	return TxnExecution{
		Revision:  revision,
		Succeeded: succeeded,
		Responses: cloneOpResponses(responses),
		Events:    CloneEvents(events),
		Current:   cloneView(currentClone),
	}, nil
}

// ValidateTxnCommand 校验 Txn 内部 compare 和 op 的基本形状。
// Txn 里的 Range 读取 working/current view，暂不支持指定历史 revision。
func ValidateTxnCommand(txn TxnCommand) error {
	for _, compare := range txn.Compare {
		if err := ValidateKeyRange(compare.Key, nil); err != nil {
			return err
		}
		if !validCompareTarget(compare.Target) || !validCompareResult(compare.Result) {
			return ErrInvalidCommand
		}
	}
	for _, op := range txn.Success {
		if err := ValidateTxnOp(op); err != nil {
			return err
		}
	}
	for _, op := range txn.Failure {
		if err := ValidateTxnOp(op); err != nil {
			return err
		}
	}
	return nil
}

// ValidateTxnOp 校验 Txn 分支中的单个 request op。
func ValidateTxnOp(op Op) error {
	switch op.Kind {
	case OpRange:
		if op.Range == nil {
			return ErrInvalidCommand
		}
		if op.Range.Revision != 0 {
			return ErrInvalidRevision
		}
		return ValidateKeyRange(op.Range.Key, op.Range.End)
	case OpPut:
		if op.Put == nil {
			return ErrInvalidCommand
		}
		return ValidateKeyRange(op.Put.Key, nil)
	case OpDeleteRange:
		if op.DeleteRange == nil {
			return ErrInvalidCommand
		}
		return ValidateKeyRange(op.DeleteRange.Key, op.DeleteRange.End)
	default:
		return ErrInvalidCommand
	}
}

func txnComparesSucceeded(current map[string]KeyValue, compares []Compare) (bool, error) {
	for _, compare := range compares {
		// 根据 compare 的 key 去 current view 中找到 KeyValue Struct
		kv := current[string(compare.Key)]
		// 再比较 KeyValue Struct 和 compare 的其余字段
		ok, err := evalCompare(compare, kv)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// 比较 current view 中的字段和 compare 预期状态
func evalCompare(compare Compare, kv KeyValue) (bool, error) {
	var cmp int
	switch compare.Target {
	case CompareVersion:
		cmp = compareInt64(kv.Version, compare.Version)
	case CompareCreateRevision:
		cmp = compareInt64(kv.CreateRevision, compare.CreateRevision)
	case CompareModRevision:
		cmp = compareInt64(kv.ModRevision, compare.ModRevision)
	case CompareValue:
		cmp = bytes.Compare(kv.Value, compare.Value)
	case CompareLease:
		cmp = compareInt64(kv.LeaseID, compare.LeaseID)
	default:
		return false, ErrInvalidCommand
	}

	switch compare.Result {
	case CompareEqual:
		return cmp == 0, nil
	case CompareNotEqual:
		return cmp != 0, nil
	case CompareGreater:
		return cmp > 0, nil
	case CompareLess:
		return cmp < 0, nil
	default:
		return false, ErrInvalidCommand
	}
}

// 执行 txn 分支，注意操作的都是 current 副本
func executeTxnOps(current map[string]KeyValue, currentRevision int64, ops []Op, leaseExists func(int64) bool) ([]OpResponse, []Event, error) {
	nextRevision := currentRevision + 1
	responses := make([]OpResponse, 0, len(ops))
	events := make([]Event, 0)

	// 分发 op
	for _, op := range ops {
		switch op.Kind {
		case OpRange:
			resp := txnRange(current, *op.Range, currentRevision)
			responses = append(responses, OpResponse{Kind: OpRange, Range: &resp})
		case OpPut:
			if err := validateTxnLease(op.Put.LeaseID, leaseExists); err != nil {
				return nil, nil, err
			}
			resp, event := txnPut(current, *op.Put, nextRevision, int64(len(events)))
			events = append(events, CloneEvent(event))
			responses = append(responses, OpResponse{Kind: OpPut, Put: &resp})
		case OpDeleteRange:
			resp, deleteEvents := txnDeleteRange(current, *op.DeleteRange, nextRevision, int64(len(events)), currentRevision)
			events = append(events, CloneEvents(deleteEvents)...)
			responses = append(responses, OpResponse{Kind: OpDeleteRange, DeleteRange: &resp})
		default:
			return nil, nil, ErrInvalidCommand
		}
	}
	return responses, events, nil
}

func validateTxnLease(leaseID int64, leaseExists func(int64) bool) error {
	if leaseID == 0 {
		return nil
	}
	if leaseID < 0 {
		return ErrInvalidLease
	}
	if leaseExists != nil && !leaseExists(leaseID) {
		return ErrLeaseNotFound
	}
	return nil
}

// 虽然单个 range command 走 readindex，但是 txn 中的 range op 一会走 raft log。
func txnRange(current map[string]KeyValue, req RangeRequest, revision int64) RangeResponse {
	keys := sortedViewKeysInRange(current, req.Key, req.End)
	count := int64(len(keys))
	if req.Limit > 0 && int64(len(keys)) > req.Limit {
		keys = keys[:req.Limit]
	}

	kvs := make([]KeyValue, 0, len(keys))
	for _, key := range keys {
		kvs = append(kvs, CloneKeyValue(current[key]))
	}
	return RangeResponse{Revision: revision, Count: count, KVs: kvs}
}

func txnPut(current map[string]KeyValue, req PutCommand, revision int64, sub int64) (PutResponse, Event) {
	key := string(req.Key)
	prev, existed := current[key]

	createRevision := revision
	version := int64(1)
	if existed {
		createRevision = prev.CreateRevision
		version = prev.Version + 1
	}

	kv := KeyValue{
		Key:            append([]byte(nil), req.Key...),
		Value:          append([]byte(nil), req.Value...),
		CreateRevision: createRevision,
		ModRevision:    revision,
		Version:        version,
		LeaseID:        req.LeaseID,
	}
	event := Event{
		Type:     EventPut,
		Revision: Revision{Main: revision, Sub: sub},
		KV:       CloneKeyValue(kv),
	}
	if existed {
		prevCopy := CloneKeyValue(prev)
		event.PrevKV = &prevCopy
	}

	current[key] = CloneKeyValue(kv)
	resp := PutResponse{Revision: revision, Event: CloneEvent(event)}
	if req.PrevKV && existed {
		prevCopy := CloneKeyValue(prev)
		resp.PrevKV = &prevCopy
	}
	return resp, event
}

func txnDeleteRange(current map[string]KeyValue, req DeleteRangeCommand, revision int64, firstSub int64, currentRevision int64) (DeleteRangeResponse, []Event) {
	keys := sortedViewKeysInRange(current, req.Key, req.End)
	if len(keys) == 0 {
		return DeleteRangeResponse{Revision: currentRevision}, nil
	}

	events := make([]Event, 0, len(keys))
	prevKVs := make([]KeyValue, 0, len(keys))
	for i, key := range keys {
		prev := current[key]
		delete(current, key)

		tombstone := KeyValue{
			Key:            append([]byte(nil), prev.Key...),
			CreateRevision: prev.CreateRevision,
			ModRevision:    revision,
			Version:        prev.Version + 1,
			LeaseID:        prev.LeaseID,
			Tombstone:      true,
		}
		prevCopy := CloneKeyValue(prev)
		event := Event{
			Type:     EventDelete,
			Revision: Revision{Main: revision, Sub: firstSub + int64(i)},
			KV:       tombstone,
			PrevKV:   &prevCopy,
		}
		events = append(events, CloneEvent(event))
		if req.PrevKV {
			prevKVs = append(prevKVs, CloneKeyValue(prev))
		}
	}

	return DeleteRangeResponse{
		Revision: revision,
		Deleted:  int64(len(keys)),
		PrevKVs:  prevKVs,
		Events:   CloneEvents(events),
	}, events
}

// 把所有子 response 的 revision 统一改成 Txn 最终 revision。
func normalizeOpResponseRevisions(responses []OpResponse, revision int64) {
	for i := range responses {
		switch responses[i].Kind {
		case OpRange:
			if responses[i].Range != nil {
				responses[i].Range.Revision = revision
			}
		case OpPut:
			if responses[i].Put != nil {
				responses[i].Put.Revision = revision
			}
		case OpDeleteRange:
			if responses[i].DeleteRange != nil {
				responses[i].DeleteRange.Revision = revision
			}
		}
	}
}

func sortedViewKeysInRange(current map[string]KeyValue, key []byte, end []byte) []string {
	keys := make([]string, 0)
	for k, kv := range current {
		if KeyInRange(kv.Key, key, end) {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0
	})
	return keys
}

func cloneView(current map[string]KeyValue) map[string]KeyValue {
	out := make(map[string]KeyValue, len(current))
	for key, kv := range current {
		out[key] = CloneKeyValue(kv)
	}
	return out
}

func cloneOpResponses(responses []OpResponse) []OpResponse {
	out := make([]OpResponse, len(responses))
	for i, response := range responses {
		out[i] = cloneOpResponse(response)
	}
	return out
}

func cloneOpResponse(response OpResponse) OpResponse {
	out := OpResponse{Kind: response.Kind}
	if response.Range != nil {
		out.Range = &RangeResponse{
			Revision: response.Range.Revision,
			Count:    response.Range.Count,
			KVs:      CloneKeyValues(response.Range.KVs),
		}
	}
	if response.Put != nil {
		put := PutResponse{
			Revision: response.Put.Revision,
			Event:    CloneEvent(response.Put.Event),
		}
		if response.Put.PrevKV != nil {
			prev := CloneKeyValue(*response.Put.PrevKV)
			put.PrevKV = &prev
		}
		out.Put = &put
	}
	if response.DeleteRange != nil {
		out.DeleteRange = &DeleteRangeResponse{
			Revision: response.DeleteRange.Revision,
			Deleted:  response.DeleteRange.Deleted,
			PrevKVs:  CloneKeyValues(response.DeleteRange.PrevKVs),
			Events:   CloneEvents(response.DeleteRange.Events),
		}
	}
	return out
}

func validCompareTarget(target CompareTarget) bool {
	switch target {
	case CompareVersion, CompareCreateRevision, CompareModRevision, CompareValue, CompareLease:
		return true
	default:
		return false
	}
}

func validCompareResult(result CompareResult) bool {
	switch result {
	case CompareEqual, CompareNotEqual, CompareGreater, CompareLess:
		return true
	default:
		return false
	}
}

func compareInt64(left int64, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
