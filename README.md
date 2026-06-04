# etcd-lite

`etcd-lite` 是一个面向学习和简历展示的 coordination store。它复用了 MIT 6.824 Raft lab 的主体实现风格，并在此基础上补齐更接近工业系统的模块：真实 gRPC API、MVCC state machine、Txn、ReadIndex、Watch、Lease、Snapshot/Compaction、WAL recovery 和 Prometheus metrics。

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

更多 demo 和简历描述见 [docs/demo.md](docs/demo.md)。
