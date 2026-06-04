# etcd-lite

`etcd-lite` 是一个基于 Raft 的分布式键值数据库，支持 gRPC API、MVCC state machine、Txn、ReadIndex、Watch、Lease、Snapshot/Compaction、WAL recovery 和 Prometheus metrics。

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
