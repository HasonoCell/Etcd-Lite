# etcd-lite 开发任务规划

## Summary

当前 `etcd-lite` 仓库还是干净新项目，只有 [设计文档](etcd-lite/docs/etcd-lite-design.md)。开发路线采用 MVP-first：先把 6.824 Raft 抽成独立包，再接真实 RPC、持久化、MVCC、Txn、Watch、Lease、Snapshot 和观测能力。

默认目标是做一个单 Raft 组、3 节点、gRPC API、可 crash recovery 的协调存储系统。父仓库只作为代码参考和复制来源，不参与 `etcd-lite` 的 Git 管理。

## Milestones

- `M0 Repo Bootstrap`: 初始化 Go module、目录结构、Makefile/脚本、基础 CI 命令、日志和配置框架。
- `M1 Raft Extraction`: 从 `src/raft` 复制并重构 Raft，去掉 `labrpc`、`Persister`、`labgob` 强绑定，保留现有 goroutine 架构、`Start()`、`applyCh` 和快照核心逻辑。
- `M2 Raft Runtime`: 实现真实 transport、内存 storage、WAL storage 的接口雏形，完成 3 节点本地进程内选举、复制、重启恢复测试。
- `M3 Command + MVCC`: 定义内部 `Command`、`ApplyResult`、`KeyValue`、`Event`、`Revision` 模型，实现 `Put/Range/DeleteRange` 和 current/history 双视图。
- `M4 gRPC KV Cluster`: 定义 proto，生成 Go 代码，实现 `KV.Range/Put/DeleteRange`、leader 判断、follower 转发或 leader hint、apply waiter、请求超时和幂等缓存。
- `M5 Txn + ReadIndex`: 实现 compare/success/failure 事务语义，新增 Raft `ReadIndex(ctx)`，默认 `Range` 走线性一致读屏障。
- `M6 Watch + Lease`: 实现 watch 历史回放、实时推送、有界缓冲、lease grant/keepalive/revoke、leader-driven expiration。
- `M7 Snapshot + Compaction`: 实现 Raft log snapshot、backend checkpoint 或状态机 snapshot、MVCC history compaction、启动恢复流程。
- `M8 Operability + Demo`: 实现 `/metrics`、`Status` RPC、基础 CLI、3 节点启动脚本、demo 文档、benchmark 和简历描述材料。

## Public APIs And Interfaces

- Go module 使用 `github.com/HasonoCell/Etcd-Lite`，目录以设计文档中的 `cmd/ api/ server/ raft/ backend/ mvcc/ lease/ watch/ wal/ snapshot/ client/ metrics/ tests/` 为准。
- Raft 对上层暴露 `Start(command)`, `ReadIndex(ctx)`, `Status()`, `Snapshot(index, data)`, `Stop()`；内部新增 `Transport` 与 `Storage` 接口。
- RPC API 第一版包含 `KV.Range`, `KV.Put`, `KV.DeleteRange`, `KV.Txn`, `KV.Compact`, `Watch.Watch`, `Lease.LeaseGrant`, `Lease.LeaseKeepAlive`, `Lease.LeaseRevoke`, `Maintenance.Status`。
- 状态机命令统一使用内部 `Command`，不要把 proto request 直接塞进 Raft log；每个写命令 apply 后返回 `ApplyResult`。
- 存储模型固定为 `meta`, `current_kv`, `history`, `lease`, `lease_keys` 五类逻辑 bucket，backend 默认选 bbolt。
- 请求幂等使用 `(client_id, request_id)`，leader apply 完成后缓存最近响应；读请求默认线性一致，`serializable=true` 才允许本地旧读。

## Task Breakdown

- `T0.1`: 新建项目骨架、`go.mod`、基础 README、开发命令和 package layout。
- `T0.2`: 复制设计文档到正式 docs 目录索引，补一份 MVP roadmap 文档。
- `T1.1`: 复制 `src/raft` 到 `etcd-lite/raft/core`，替换 module import，先保持行为不变。
- `T1.2`: 把 `int` node id/index 改造成明确的 `uint64 MemberID` 与 peer list 映射，保留内部数组优化也可以。
- `T1.3`: 抽出 `Transport`，把 `sendRequestVote/sendAppendEntries/sendInstallSnapshot` 改成接口调用。
- `T1.4`: 抽出 `Storage`，用 `HardState`、`Entry`、`SnapshotMeta` 替代内存 `Persister` 字节数组。
- `T1.5`: 增加 raft-only 单元和集成测试：选举、日志复制、leader stepdown、snapshot install。
- `T2.1`: 实现 `raft/storage/memory`，让 Raft 在没有 WAL 时也能跑测试。
- `T2.2`: 实现 `raft/transport/grpc` 或本地 TCP RPC，先覆盖 Raft 三类 RPC。
- `T2.3`: 实现最小 WAL：hard state、entry append、startup replay、fsync 策略。
- `T3.1`: 实现 `mvcc.Store` 的 in-memory 版本，覆盖 revision、metadata、range、delete、history event。
- `T3.2`: 实现 bbolt backend 版本，并保证一次 apply 使用一个 backend transaction。
- `T3.3`: 定义 `Command`、`TxnCommand`、`ApplyResult`、错误类型和序列化格式。
- `T4.1`: 写 proto 并接入生成流程，第一批只生成 KV、Maintenance。
- `T4.2`: 实现 `server`：启动 raft、backend、mvcc、gRPC，维护 apply waiter 和 request cache。
- `T4.3`: 实现 3 节点本地启动脚本，验证 client 连任意节点都能完成写读。
- `T5.1`: 实现 Txn compare 条件：version、create_revision、mod_revision、value、lease。
- `T5.2`: 实现 `ReadIndex`，默认 Range 等待 `appliedIndex >= readIndex` 后读 backend。
- `T6.1`: 实现 `watch.Hub`，支持历史扫描、实时事件、prefix/range matcher、compacted revision 错误。
- `T6.2`: 实现 `lease.Lessor`，所有 grant/keepalive/revoke 走 Raft，leader 本地调度过期 revoke。
- `T7.1`: 实现 backend snapshot/checkpoint 和 Raft log compaction 联动。
- `T7.2`: 实现 `Compact(revision)`，推进 `compact_revision` 并后台清理旧 history。
- `T7.3`: 实现完整 restart recovery：manifest、snapshot、WAL replay、backend、lease heap 重建。
- `T8.1`: 增加 Prometheus metrics、status endpoint、结构化日志。
- `T8.2`: 增加 `etcdctl-lite`：put/get/del/txn/watch/lease/status。
- `T8.3`: 增加 demo：服务发现、分布式锁、leader election client 示例。

## Test Plan

- 单元测试覆盖 MVCC revision、range/prefix、delete tombstone、txn compare、lease attach/revoke、watch matcher、compaction。
- Raft 测试覆盖 3 节点选举、leader 崩溃重选、日志冲突回退、follower 落后后追平、snapshot install。
- 存储测试覆盖 WAL replay、backend transaction 原子性、snapshot restore、corrupt WAL 基础错误处理。
- 端到端测试覆盖 3 节点 Put/Range/Delete/Txn、默认线性一致读、watch 从旧 revision 追平、lease 到期删除、compact 后 watch 报错。
- 故障测试覆盖 kill/restart、网络分区、leader 切换期间重试、慢 watcher 不阻塞 apply 主路径。

## Assumptions

- MVP 不做动态 membership、TLS、RBAC、多 Raft 组、完全 etcd API 兼容。
- Raft 改造保留现有 6.824 实现的主体结构，先不切到 etcd/raft 的 `Ready` 模型。
- backend 默认使用 bbolt；WAL 自己实现最小可用版本，不直接引入 etcd wal。
- 第一版 follower 请求可以先返回 leader hint，随后再补透明转发。
- `Watch` 和 `Lease` 在 `Put/Range/Delete/Txn/ReadIndex` 稳定后再实现，避免早期把调试面扩大。
