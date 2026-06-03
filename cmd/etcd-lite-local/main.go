package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mvccmemory "github.com/HasonoCell/Etcd-Lite/mvcc/memory"
	"github.com/HasonoCell/Etcd-Lite/raft/core"
	raftmemory "github.com/HasonoCell/Etcd-Lite/raft/storage/memory"
	"github.com/HasonoCell/Etcd-Lite/raft/transport/local"
	"github.com/HasonoCell/Etcd-Lite/server"
	"google.golang.org/grpc"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:2379,127.0.0.1:2380,127.0.0.1:2381", "comma-separated gRPC listen addresses")
	flag.Parse()

	addresses := strings.Split(*listen, ",")
	if len(addresses) != 3 {
		log.Fatalf("expected 3 listen addresses, got %d", len(addresses))
	}

	ids := []core.MemberID{1, 2, 3}
	transport := local.New()
	nodes := make([]*server.Server, 0, len(ids))
	grpcServers := make([]*grpc.Server, 0, len(ids))
	listeners := make([]net.Listener, 0, len(ids))

	for i, id := range ids {
		node, err := server.New(server.Config{
			ID:                id,
			Peers:             ids,
			RaftStorage:       raftmemory.New(),
			RaftTransport:     transport,
			Store:             mvccmemory.New(),
			ElectionTimeout:   250 * time.Millisecond,
			HeartbeatInterval: 50 * time.Millisecond,
			RequestTimeout:    3 * time.Second,
		})
		if err != nil {
			log.Fatalf("start node %d: %v", id, err)
		}
		transport.Register(node.Raft())

		listener, err := net.Listen("tcp", strings.TrimSpace(addresses[i]))
		if err != nil {
			log.Fatalf("listen node %d: %v", id, err)
		}
		grpcServer := grpc.NewServer()
		node.Register(grpcServer)

		nodes = append(nodes, node)
		grpcServers = append(grpcServers, grpcServer)
		listeners = append(listeners, listener)

		go func(id core.MemberID, address string) {
			log.Printf("node %d serving gRPC at %s", id, address)
			if err := grpcServer.Serve(listener); err != nil {
				log.Printf("node %d gRPC server stopped: %v", id, err)
			}
		}(id, listener.Addr().String())
	}

	waitForShutdown()
	for _, grpcServer := range grpcServers {
		grpcServer.GracefulStop()
	}
	for _, listener := range listeners {
		_ = listener.Close()
	}
	for _, node := range nodes {
		node.Stop()
		transport.Unregister(node.Raft().ID())
	}
}

func waitForShutdown() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}
