# etcd-lite Demo

这份文档用于快速展示 `etcd-lite` 的核心能力：3 节点 Raft cluster、线性一致 KV、Txn、Watch、Lease、Compaction、Prometheus metrics 和基础 CLI。

## 启动 3 节点 Cluster

```bash
./scripts/run-local-3.sh
```

默认端口：

- gRPC: `127.0.0.1:2379`, `127.0.0.1:2380`, `127.0.0.1:2381`
- HTTP metrics: `127.0.0.1:2389`, `127.0.0.1:2390`, `127.0.0.1:2391`

查看节点状态：

```bash
go run ./cmd/etcdctl-lite status
curl http://127.0.0.1:2389/status
```

## KV 与 ReadIndex

默认 `get` 使用线性一致读：leader 会先走 `ReadIndex` barrier，确认本节点已经 apply 到安全 index，再读取 MVCC store。

```bash
go run ./cmd/etcdctl-lite put /app/config/port 8080
go run ./cmd/etcdctl-lite get /app/config/port
go run ./cmd/etcdctl-lite get -prefix /app/
```

如果只需要 follower 本地旧读，可以加 `-serializable`：

```bash
go run ./cmd/etcdctl-lite get -serializable /app/config/port
```

## Txn: Create If Not Exists

`txn-create` 会比较目标 key 的 `version == 0`，只有 key 不存在时才执行 put。这是分布式锁和 leader election 的基础语义。

```bash
go run ./cmd/etcdctl-lite txn-create /locks/job-1 owner-a
go run ./cmd/etcdctl-lite txn-create /locks/job-1 owner-b
```

第一次会 `succeeded=true`，第二次会 `succeeded=false` 并返回当前持有者。

## Watch: 服务发现

在一个终端监听服务实例前缀：

```bash
go run ./cmd/etcdctl-lite watch -prefix -prev-kv /services/search/
```

另一个终端写入实例：

```bash
go run ./cmd/etcdctl-lite put /services/search/instance-1 10.0.0.1:8080
go run ./cmd/etcdctl-lite put /services/search/instance-2 10.0.0.2:8080
go run ./cmd/etcdctl-lite del /services/search/instance-1
```

客户端可以先 `get -prefix /services/search/` 获取当前全量视图，再从返回 revision 之后 `watch`，这就是常见 service discovery 的 current view + event stream 模式。

## Lease: 临时节点

创建 lease，并把服务实例绑定到 lease 上：

```bash
go run ./cmd/etcdctl-lite lease grant 1001 5
go run ./cmd/etcdctl-lite put -lease 1001 /services/api/instance-1 127.0.0.1:9000
go run ./cmd/etcdctl-lite lease keepalive 1001
```

如果 lease 不再 keepalive，leader 会提交 `LeaseRevoke` command 到 Raft log，所有节点按同样顺序删除绑定 key，Watch 端会收到 delete event。

## 分布式锁 Sketch

锁路径可以设计为 `/locks/<name>/<lease-id>`：

```bash
go run ./cmd/etcdctl-lite lease grant 2001 10
go run ./cmd/etcdctl-lite txn-create /locks/build owner-a
```

如果 `txn-create` 成功，客户端持有锁；失败则读取当前 owner 并 watch 该 key 的 delete event。真实客户端还需要循环 keepalive，退出时主动 `del /locks/build` 或 `lease revoke 2001`。

## Leader Election Client Sketch

候选者将自己的身份写入固定 election key：

```bash
go run ./cmd/etcdctl-lite lease grant 3001 10
go run ./cmd/etcdctl-lite txn-create /election/controller candidate-a
go run ./cmd/etcdctl-lite watch /election/controller
```

成功创建 key 的客户端成为业务 leader。业务 leader 持续 keepalive；其他客户端 watch key 删除事件，删除后再次尝试 `txn-create`。

## Metrics

每个节点暴露 Prometheus text format：

```bash
curl http://127.0.0.1:2389/metrics
```

常用指标：

- `etcdlite_requests_total`: gRPC request 数量，按 method 区分。
- `etcdlite_apply_total`: Raft command apply 数量，按 command kind 区分。
- `etcdlite_errors_total`: server 返回的业务错误数量。
- `etcdlite_mvcc_revision`: 当前 MVCC revision。
- `etcdlite_raft_commit_index` / `etcdlite_raft_applied_index`: Raft commit/apply 进度。
- `etcdlite_watch_events_total`: Watch 发布过的 event 数量。
- `etcdlite_snapshots_total`: state machine snapshot 创建次数。

## Benchmark

```bash
go test -run '^$' -bench BenchmarkPutAndRange ./server
```

这个 benchmark 会启动本地 3 节点 in-process cluster，每轮执行一次 Put 和一次默认线性一致 Range，用来观察完整路径的基础开销。

## Resume Bullets

- Built a Raft-backed coordination store in Go with gRPC APIs, MVCC storage, linearizable reads via ReadIndex, transactions, watch streams, leases, snapshots, compaction, WAL recovery, Prometheus metrics, and a demo CLI.
- Refactored a MIT 6.824-style Raft implementation into reusable `Transport` and `Storage` interfaces, then integrated it with a production-inspired KV state machine and crash recovery path.
- Implemented request idempotency, apply waiters, leader hints, historical revision reads, service discovery/watch semantics, and lease-driven ephemeral keys.
