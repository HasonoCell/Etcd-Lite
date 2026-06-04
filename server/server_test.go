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
	t            *testing.T
	ids          []core.MemberID
	transport    *local.Transport
	nodes        map[core.MemberID]*Server
	grpcServers  map[core.MemberID]*grpc.Server
	conns        map[core.MemberID]*grpc.ClientConn
	kvClients    map[core.MemberID]etcdlitepb.KVClient
	watchClients map[core.MemberID]etcdlitepb.WatchClient
	leaseClients map[core.MemberID]etcdlitepb.LeaseClient
	mtClients    map[core.MemberID]etcdlitepb.MaintenanceClient
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

func TestKVTxnThroughLeaderHintAndReadOnlyTxn(t *testing.T) {
	c := newTestCluster(t, []core.MemberID{1, 2, 3})
	defer c.stop()

	leader := c.waitLeader()
	follower := c.pickFollower(leader)

	put := c.put(follower, &etcdlitepb.PutRequest{
		Key:       []byte("/m5/key"),
		Value:     []byte("v1"),
		ClientId:  300,
		RequestId: 1,
	})
	if put.GetHeader().GetRevision() != 1 {
		t.Fatalf("put revision = %d, want 1", put.GetHeader().GetRevision())
	}

	txn := c.txn(follower, &etcdlitepb.TxnRequest{
		ClientId:  300,
		RequestId: 2,
		Compare: []*etcdlitepb.Compare{{
			Key:    []byte("/m5/key"),
			Target: etcdlitepb.CompareTarget_COMPARE_TARGET_VALUE,
			Result: etcdlitepb.CompareResult_COMPARE_RESULT_EQUAL,
			Value:  []byte("v1"),
		}},
		Success: []*etcdlitepb.RequestOp{
			putOp("/m5/a", "a"),
			putOp("/m5/b", "b"),
		},
		Failure: []*etcdlitepb.RequestOp{
			putOp("/m5/failure", "bad"),
		},
	})
	if !txn.GetSucceeded() || txn.GetHeader().GetRevision() != 2 || len(txn.GetResponses()) != 2 {
		t.Fatalf("txn response = %+v", txn)
	}

	duplicate := c.txn(follower, &etcdlitepb.TxnRequest{
		ClientId:  300,
		RequestId: 2,
		Compare: []*etcdlitepb.Compare{{
			Key:    []byte("/m5/key"),
			Target: etcdlitepb.CompareTarget_COMPARE_TARGET_VALUE,
			Result: etcdlitepb.CompareResult_COMPARE_RESULT_EQUAL,
			Value:  []byte("changed"),
		}},
		Success: []*etcdlitepb.RequestOp{putOp("/m5/ignored", "ignored")},
	})
	if duplicate.GetHeader().GetRevision() != txn.GetHeader().GetRevision() || len(duplicate.GetResponses()) != 2 {
		t.Fatalf("duplicate txn = %+v, want cached revision %d", duplicate, txn.GetHeader().GetRevision())
	}

	readOnly := c.txn(follower, &etcdlitepb.TxnRequest{
		ClientId:  300,
		RequestId: 3,
		Compare: []*etcdlitepb.Compare{{
			Key:     []byte("/missing"),
			Target:  etcdlitepb.CompareTarget_COMPARE_TARGET_VERSION,
			Result:  etcdlitepb.CompareResult_COMPARE_RESULT_EQUAL,
			Version: 0,
		}},
		Success: []*etcdlitepb.RequestOp{rangeOp("/m5/a")},
	})
	if !readOnly.GetSucceeded() || readOnly.GetHeader().GetRevision() != 2 || len(readOnly.GetResponses()) != 1 {
		t.Fatalf("read-only txn = %+v", readOnly)
	}
	rangeResp := readOnly.GetResponses()[0].GetResponseRange()
	if rangeResp == nil || rangeResp.GetCount() != 1 || string(rangeResp.GetKvs()[0].GetValue()) != "a" {
		t.Fatalf("read-only txn range response = %+v", rangeResp)
	}
}

func TestWatchReceivesHistoryAndLiveEvents(t *testing.T) {
	c := newTestCluster(t, []core.MemberID{1, 2, 3})
	defer c.stop()

	leader := c.waitLeader()
	c.put(leader, &etcdlitepb.PutRequest{
		Key:       []byte("/watch/a"),
		Value:     []byte("v1"),
		ClientId:  400,
		RequestId: 1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := c.watchClients[leader].Watch(ctx, &etcdlitepb.WatchRequest{
		Key:           []byte("/watch/"),
		End:           []byte("/watch0"),
		StartRevision: 0,
		PrevKv:        true,
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	created := mustRecvWatch(t, stream)
	if !created.GetCreated() {
		t.Fatalf("first watch response = %+v, want created", created)
	}
	history := mustRecvWatch(t, stream)
	if len(history.GetEvents()) != 1 || string(history.GetEvents()[0].GetKv().GetValue()) != "v1" {
		t.Fatalf("history watch response = %+v", history)
	}

	c.put(leader, &etcdlitepb.PutRequest{
		Key:       []byte("/watch/a"),
		Value:     []byte("v2"),
		PrevKv:    true,
		ClientId:  400,
		RequestId: 2,
	})
	live := mustRecvWatch(t, stream)
	if len(live.GetEvents()) != 1 || string(live.GetEvents()[0].GetKv().GetValue()) != "v2" {
		t.Fatalf("live watch response = %+v", live)
	}
	if live.GetEvents()[0].GetPrevKv() == nil || string(live.GetEvents()[0].GetPrevKv().GetValue()) != "v1" {
		t.Fatalf("live PrevKV = %+v", live.GetEvents()[0].GetPrevKv())
	}
}

func TestLeaseGrantRevokeAndExpirationDeleteKeys(t *testing.T) {
	c := newTestCluster(t, []core.MemberID{1, 2, 3})
	defer c.stop()

	leader := c.waitLeader()
	grant := c.leaseGrant(leader, &etcdlitepb.LeaseGrantRequest{
		LeaseId:   500,
		Ttl:       5,
		ClientId:  500,
		RequestId: 1,
	})
	if grant.GetLeaseId() != 500 || grant.GetTtl() != 5 {
		t.Fatalf("grant = %+v", grant)
	}

	c.put(leader, &etcdlitepb.PutRequest{
		Key:       []byte("/lease/revoke"),
		Value:     []byte("v"),
		LeaseId:   500,
		ClientId:  500,
		RequestId: 2,
	})
	revoke := c.leaseRevoke(leader, &etcdlitepb.LeaseRevokeRequest{
		LeaseId:   500,
		ClientId:  500,
		RequestId: 3,
	})
	if revoke.GetDeleted() != 1 {
		t.Fatalf("revoke = %+v", revoke)
	}
	empty := c.rangeKV(leader, &etcdlitepb.RangeRequest{Key: []byte("/lease/revoke")})
	if empty.GetCount() != 0 {
		t.Fatalf("range after revoke = %+v", empty)
	}

	c.leaseGrant(leader, &etcdlitepb.LeaseGrantRequest{
		LeaseId:   501,
		Ttl:       1,
		ClientId:  500,
		RequestId: 4,
	})
	c.put(leader, &etcdlitepb.PutRequest{
		Key:       []byte("/lease/expire"),
		Value:     []byte("v"),
		LeaseId:   501,
		ClientId:  500,
		RequestId: 5,
	})
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		resp := c.rangeKV(leader, &etcdlitepb.RangeRequest{Key: []byte("/lease/expire")})
		if resp.GetCount() == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("lease expiration did not delete key")
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
		t:            t,
		ids:          append([]core.MemberID(nil), ids...),
		transport:    local.New(),
		nodes:        make(map[core.MemberID]*Server),
		grpcServers:  make(map[core.MemberID]*grpc.Server),
		conns:        make(map[core.MemberID]*grpc.ClientConn),
		kvClients:    make(map[core.MemberID]etcdlitepb.KVClient),
		watchClients: make(map[core.MemberID]etcdlitepb.WatchClient),
		leaseClients: make(map[core.MemberID]etcdlitepb.LeaseClient),
		mtClients:    make(map[core.MemberID]etcdlitepb.MaintenanceClient),
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
	c.watchClients[id] = etcdlitepb.NewWatchClient(conn)
	c.leaseClients[id] = etcdlitepb.NewLeaseClient(conn)
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

func (c *testCluster) txn(start core.MemberID, req *etcdlitepb.TxnRequest) *etcdlitepb.TxnResponse {
	c.t.Helper()
	current := start
	for i := 0; i < 8; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := c.kvClients[current].Txn(ctx, req)
		cancel()
		if err != nil {
			c.t.Fatalf("Txn through %d: %v", current, err)
		}
		if resp.GetHeader().GetError() == "" {
			return resp
		}
		current = c.nextAttempt(current, resp.GetHeader())
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("Txn did not reach leader")
	return nil
}

func (c *testCluster) leaseGrant(start core.MemberID, req *etcdlitepb.LeaseGrantRequest) *etcdlitepb.LeaseGrantResponse {
	c.t.Helper()
	current := start
	for i := 0; i < 8; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := c.leaseClients[current].LeaseGrant(ctx, req)
		cancel()
		if err != nil {
			c.t.Fatalf("LeaseGrant through %d: %v", current, err)
		}
		if resp.GetHeader().GetError() == "" {
			return resp
		}
		current = c.nextAttempt(current, resp.GetHeader())
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("LeaseGrant did not reach leader")
	return nil
}

func (c *testCluster) leaseRevoke(start core.MemberID, req *etcdlitepb.LeaseRevokeRequest) *etcdlitepb.LeaseRevokeResponse {
	c.t.Helper()
	current := start
	for i := 0; i < 8; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := c.leaseClients[current].LeaseRevoke(ctx, req)
		cancel()
		if err != nil {
			c.t.Fatalf("LeaseRevoke through %d: %v", current, err)
		}
		if resp.GetHeader().GetError() == "" {
			return resp
		}
		current = c.nextAttempt(current, resp.GetHeader())
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("LeaseRevoke did not reach leader")
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

func mustRecvWatch(t *testing.T, stream etcdlitepb.Watch_WatchClient) *etcdlitepb.WatchResponse {
	t.Helper()
	type watchResult struct {
		resp *etcdlitepb.WatchResponse
		err  error
	}
	ch := make(chan watchResult, 1)
	go func() {
		resp, err := stream.Recv()
		ch <- watchResult{resp: resp, err: err}
	}()
	select {
	case result := <-ch:
		if result.err != nil {
			t.Fatalf("Watch Recv: %v", result.err)
		}
		return result.resp
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for watch response")
		return nil
	}
}

func putOp(key string, value string) *etcdlitepb.RequestOp {
	return &etcdlitepb.RequestOp{
		Request: &etcdlitepb.RequestOp_RequestPut{
			RequestPut: &etcdlitepb.PutRequest{
				Key:   []byte(key),
				Value: []byte(value),
			},
		},
	}
}

func rangeOp(key string) *etcdlitepb.RequestOp {
	return &etcdlitepb.RequestOp{
		Request: &etcdlitepb.RequestOp_RequestRange{
			RequestRange: &etcdlitepb.RangeRequest{Key: []byte(key)},
		},
	}
}
