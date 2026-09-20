# etcd-lite

`etcd-lite` 是一个基于 Raft 的分布式键值数据库，支持 gRPC API、MVCC state machine、Txn、ReadIndex、Watch、Lease、Snapshot/Compaction、WAL recovery 和 Prometheus metrics。

## Architecture

- `raft/core` 负责选举、日志复制、ReadIndex、提交应用和 snapshot install。
- `mvcc` 提供内存 / bbolt 两种状态机后端，以 revision、history 和 tombstone 支持历史读与 compaction。
- `server` 将 gRPC 请求转换为 Raft command，并用 apply waiter、幂等 request cache 和 leader hint 串起一致性读写。
- `watch` 对历史事件做 replay，再订阅未来事件；Hub 在 watcher 关闭与事件发布并发时保持 channel 生命周期安全。

开发阶段还提供 GitHub Actions，执行全量测试、`go vet` 和核心共识 / watch 路径的 race 检查。

## Quick Start

启动本地 3 节点集群：

```bash
./scripts/run-local-3.sh
```

写入和读取：

```bash
go run ./cmd/etcdctl-lite put /demo/key value
go run ./cmd/etcdctl-lite get /demo/key
go run ./cmd/etcdctl-lite status
```

查看 Prometheus metrics：

```bash
curl http://127.0.0.1:2389/metrics
curl http://127.0.0.1:2390/status
```

## Development

运行测试：

```bash
go test ./...
```

运行基础 benchmark：

```bash
go test -run '^$' -bench BenchmarkPutAndRange ./server
```
