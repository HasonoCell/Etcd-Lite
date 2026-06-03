package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/HasonoCell/Etcd-Lite/api/etcdlitepb"
	mvccmemory "github.com/HasonoCell/Etcd-Lite/mvcc/memory"
	"github.com/HasonoCell/Etcd-Lite/raft/core"
	raftmemory "github.com/HasonoCell/Etcd-Lite/raft/storage/memory"
	"github.com/HasonoCell/Etcd-Lite/raft/transport/local"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testCluster struct {
	t           *testing.T
	ids         []core.MemberID
	transport   *local.Transport
	nodes       map[core.MemberID]*Server
	grpcServers map[core.MemberID]*grpc.Server
	conns       map[core.MemberID]*grpc.ClientConn
	kvClients   map[core.MemberID]etcdlitepb.KVClient
	mtClients   map[core.MemberID]etcdlitepb.MaintenanceClient
}

func TestKVRequestsFollowLeaderHintAndUseReadIndex(t *testing.T) {
	c := newTestCluster(t, []core.MemberID{1, 2, 3})
	defer c.stop()

	leader := c.waitLeader()
	follower := c.pickFollower(leader)

	put := c.put(follower, &etcdlitepb.PutRequest{
		Key:       []byte("/m4/key"),
		Value:     []byte("v1"),
		ClientId:  100,
		RequestId: 1,
	})
	if put.GetHeader().GetRevision() != 1 {
		t.Fatalf("put revision = %d, want 1", put.GetHeader().GetRevision())
	}

	duplicate := c.put(follower, &etcdlitepb.PutRequest{
		Key:       []byte("/m4/key"),
		Value:     []byte("ignored"),
		ClientId:  100,
		RequestId: 1,
	})
	if duplicate.GetHeader().GetRevision() != put.GetHeader().GetRevision() {
		t.Fatalf("duplicate revision = %d, want cached revision %d", duplicate.GetHeader().GetRevision(), put.GetHeader().GetRevision())
	}

	ranged := c.rangeKV(follower, &etcdlitepb.RangeRequest{Key: []byte("/m4/key")})
	if ranged.GetHeader().GetRevision() != 1 {
		t.Fatalf("range revision = %d, want 1", ranged.GetHeader().GetRevision())
	}
	if ranged.GetCount() != 1 || len(ranged.GetKvs()) != 1 || string(ranged.GetKvs()[0].GetValue()) != "v1" {
		t.Fatalf("range response = %+v", ranged)
	}

	deleted := c.deleteRange(follower, &etcdlitepb.DeleteRangeRequest{
		Key:       []byte("/m4/key"),
		PrevKv:    true,
		ClientId:  100,
		RequestId: 2,
	})
	if deleted.GetHeader().GetRevision() != 2 || deleted.GetDeleted() != 1 {
		t.Fatalf("delete response = %+v", deleted)
	}
	if len(deleted.GetPrevKvs()) != 1 || string(deleted.GetPrevKvs()[0].GetValue()) != "v1" {
		t.Fatalf("delete prev kvs = %+v", deleted.GetPrevKvs())
	}

	empty := c.rangeKV(follower, &etcdlitepb.RangeRequest{Key: []byte("/m4/key")})
	if empty.GetCount() != 0 {
		t.Fatalf("range after delete count = %d, want 0", empty.GetCount())
	}
}

func TestMaintenanceStatusReportsRaftAndMVCCState(t *testing.T) {
	c := newTestCluster(t, []core.MemberID{1, 2, 3})
	defer c.stop()

	leader := c.waitLeader()
	c.put(leader, &etcdlitepb.PutRequest{
		Key:       []byte("/status/key"),
		Value:     []byte("ok"),
		ClientId:  200,
		RequestId: 1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	status, err := c.mtClients[leader].Status(ctx, &etcdlitepb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.GetState() != core.StateLeader.String() {
		t.Fatalf("state = %q, want leader", status.GetState())
	}
	if status.GetHeader().GetRevision() != 1 {
		t.Fatalf("status revision = %d, want 1", status.GetHeader().GetRevision())
	}
	if status.GetCommitIndex() == 0 || status.GetAppliedIndex() == 0 {
		t.Fatalf("status indexes = commit %d applied %d, want non-zero", status.GetCommitIndex(), status.GetAppliedIndex())
	}
}

func newTestCluster(t *testing.T, ids []core.MemberID) *testCluster {
	t.Helper()
	c := &testCluster{
		t:           t,
		ids:         append([]core.MemberID(nil), ids...),
		transport:   local.New(),
		nodes:       make(map[core.MemberID]*Server),
		grpcServers: make(map[core.MemberID]*grpc.Server),
		conns:       make(map[core.MemberID]*grpc.ClientConn),
		kvClients:   make(map[core.MemberID]etcdlitepb.KVClient),
		mtClients:   make(map[core.MemberID]etcdlitepb.MaintenanceClient),
	}
	for _, id := range ids {
		node, err := New(Config{
			ID:                id,
			Peers:             ids,
			RaftStorage:       raftmemory.New(),
			RaftTransport:     c.transport,
			Store:             mvccmemory.New(),
			ElectionTimeout:   80 * time.Millisecond,
			HeartbeatInterval: 15 * time.Millisecond,
			RequestTimeout:    2 * time.Second,
		})
		if err != nil {
			t.Fatalf("New server %d: %v", id, err)
		}
		c.nodes[id] = node
		c.transport.Register(node.Raft())
		c.startGRPC(id, node)
	}
	return c
}

func (c *testCluster) startGRPC(id core.MemberID, node *Server) {
	c.t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatalf("listen %d: %v", id, err)
	}
	grpcServer := grpc.NewServer()
	node.Register(grpcServer)
	c.grpcServers[id] = grpcServer

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		c.t.Fatalf("dial %d: %v", id, err)
	}
	c.conns[id] = conn
	c.kvClients[id] = etcdlitepb.NewKVClient(conn)
	c.mtClients[id] = etcdlitepb.NewMaintenanceClient(conn)
}

func (c *testCluster) stop() {
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	for _, grpcServer := range c.grpcServers {
		grpcServer.Stop()
	}
	for id, node := range c.nodes {
		node.Stop()
		c.transport.Unregister(id)
	}
}

func (c *testCluster) waitLeader() core.MemberID {
	c.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var leader core.MemberID
		var leaders int
		for id, node := range c.nodes {
			if node.Raft().Status().State == core.StateLeader {
				leader = id
				leaders++
			}
		}
		if leaders == 1 {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for one leader")
	return core.NoMember
}

func (c *testCluster) pickFollower(leader core.MemberID) core.MemberID {
	c.t.Helper()
	for _, id := range c.ids {
		if id != leader {
			return id
		}
	}
	c.t.Fatalf("no follower found")
	return core.NoMember
}

func (c *testCluster) put(start core.MemberID, req *etcdlitepb.PutRequest) *etcdlitepb.PutResponse {
	c.t.Helper()
	current := start
	for i := 0; i < 8; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := c.kvClients[current].Put(ctx, req)
		cancel()
		if err != nil {
			c.t.Fatalf("Put through %d: %v", current, err)
		}
		if resp.GetHeader().GetError() == "" {
			return resp
		}
		current = c.nextAttempt(current, resp.GetHeader())
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("Put did not reach leader")
	return nil
}

func (c *testCluster) rangeKV(start core.MemberID, req *etcdlitepb.RangeRequest) *etcdlitepb.RangeResponse {
	c.t.Helper()
	current := start
	for i := 0; i < 8; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := c.kvClients[current].Range(ctx, req)
		cancel()
		if err != nil {
			c.t.Fatalf("Range through %d: %v", current, err)
		}
		if resp.GetHeader().GetError() == "" {
			return resp
		}
		current = c.nextAttempt(current, resp.GetHeader())
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("Range did not reach leader")
	return nil
}

func (c *testCluster) deleteRange(start core.MemberID, req *etcdlitepb.DeleteRangeRequest) *etcdlitepb.DeleteRangeResponse {
	c.t.Helper()
	current := start
	for i := 0; i < 8; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := c.kvClients[current].DeleteRange(ctx, req)
		cancel()
		if err != nil {
			c.t.Fatalf("DeleteRange through %d: %v", current, err)
		}
		if resp.GetHeader().GetError() == "" {
			return resp
		}
		current = c.nextAttempt(current, resp.GetHeader())
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("DeleteRange did not reach leader")
	return nil
}

func (c *testCluster) nextAttempt(current core.MemberID, header *etcdlitepb.ResponseHeader) core.MemberID {
	c.t.Helper()
	if header.GetError() != errorNotLeader {
		c.t.Fatalf("request through %d failed: %s", current, header.GetError())
	}
	if leader := core.MemberID(header.GetLeaderId()); leader != core.NoMember {
		return leader
	}
	return c.waitLeader()
}
