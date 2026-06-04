package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/HasonoCell/Etcd-Lite/metrics"
	mvccmemory "github.com/HasonoCell/Etcd-Lite/mvcc/memory"
	"github.com/HasonoCell/Etcd-Lite/raft/core"
	raftmemory "github.com/HasonoCell/Etcd-Lite/raft/storage/memory"
	"github.com/HasonoCell/Etcd-Lite/raft/transport/local"
	"github.com/HasonoCell/Etcd-Lite/server"
	"google.golang.org/grpc"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:2379,127.0.0.1:2380,127.0.0.1:2381", "comma-separated gRPC listen addresses")
	metricsListen := flag.String("metrics-listen", "127.0.0.1:2389,127.0.0.1:2390,127.0.0.1:2391", "comma-separated HTTP metrics listen addresses, empty disables metrics")
	flag.Parse()

	addresses := strings.Split(*listen, ",")
	if len(addresses) != 3 {
		log.Fatalf("expected 3 listen addresses, got %d", len(addresses))
	}
	metricsAddresses := splitAddresses(*metricsListen)
	if len(metricsAddresses) != 0 && len(metricsAddresses) != len(addresses) {
		log.Fatalf("expected %d metrics listen addresses, got %d", len(addresses), len(metricsAddresses))
	}

	ids := []core.MemberID{1, 2, 3}
	transport := local.New()
	nodes := make([]*server.Server, 0, len(ids))
	grpcServers := make([]*grpc.Server, 0, len(ids))
	listeners := make([]net.Listener, 0, len(ids))
	httpServers := make([]*http.Server, 0, len(ids))
	httpListeners := make([]net.Listener, 0, len(ids))

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
			log.Printf("event=grpc_start member_id=%d addr=%s", id, address)
			if err := grpcServer.Serve(listener); err != nil {
				log.Printf("event=grpc_stop member_id=%d err=%q", id, err)
			}
		}(id, listener.Addr().String())

		if len(metricsAddresses) > 0 {
			httpListener, err := net.Listen("tcp", strings.TrimSpace(metricsAddresses[i]))
			if err != nil {
				log.Fatalf("listen node %d metrics: %v", id, err)
			}
			httpServer := newHTTPServer(node)
			httpServers = append(httpServers, httpServer)
			httpListeners = append(httpListeners, httpListener)
			go func(id core.MemberID, address string, srv *http.Server, ln net.Listener) {
				log.Printf("event=http_start member_id=%d addr=%s", id, address)
				if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
					log.Printf("event=http_stop member_id=%d err=%q", id, err)
				}
			}(id, httpListener.Addr().String(), httpServer, httpListener)
		}
	}

	waitForShutdown()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, httpServer := range httpServers {
		_ = httpServer.Shutdown(shutdownCtx)
	}
	for _, grpcServer := range grpcServers {
		grpcServer.GracefulStop()
	}
	for _, httpListener := range httpListeners {
		_ = httpListener.Close()
	}
	for _, listener := range listeners {
		_ = listener.Close()
	}
	for _, node := range nodes {
		node.Stop()
		transport.Unregister(node.Raft().ID())
	}
}

func splitAddresses(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func newHTTPServer(node *server.Server) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := metrics.WritePrometheus(w, node.MetricsSnapshot()); err != nil {
			log.Printf("write metrics: %v", err)
		}
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(node.MetricsSnapshot()); err != nil {
			log.Printf("write status: %v", err)
		}
	})
	return &http.Server{Handler: mux}
}

func waitForShutdown() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
}
