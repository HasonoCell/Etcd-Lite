package runtime_test

import (
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/HasonoCell/Etcd-Lite/raft/core"
	"github.com/HasonoCell/Etcd-Lite/raft/storage/memory"
	"github.com/HasonoCell/Etcd-Lite/raft/storage/wal"
	"github.com/HasonoCell/Etcd-Lite/raft/transport/local"
)

type cluster struct {
	t         *testing.T
	ids       []core.MemberID
	transport *local.Transport
	nodes     map[core.MemberID]*core.Raft
	applyChs  map[core.MemberID]chan core.ApplyMsg
	storages  map[core.MemberID]core.Storage
}

func TestThreeNodeClusterReplicatesWithMemoryStorage(t *testing.T) {
	ids := []core.MemberID{1, 2, 3}
	c := newCluster(t, ids, func(core.MemberID) core.Storage {
		return memory.New()
	})
	defer c.stop()

	leader := c.waitLeader()
	index, _, ok := leader.Start([]byte("put /m2 memory"))
	if !ok {
		t.Fatalf("leader rejected Start")
	}
	c.waitApplied(index, []byte("put /m2 memory"), ids...)
}

func TestFollowerRestartsFromWALAndCatchesUp(t *testing.T) {
	ids := []core.MemberID{1, 2, 3}
	dir := t.TempDir()
	c := newCluster(t, ids, func(id core.MemberID) core.Storage {
		storage, err := wal.Open(filepath.Join(dir, idString(id)+".wal"), wal.WithSync(false))
		if err != nil {
			t.Fatalf("open wal for %d: %v", id, err)
		}
		return storage
	})
	defer c.stop()

	leader := c.waitLeader()
	firstIndex, _, ok := leader.Start([]byte("put /m2 before-restart"))
	if !ok {
		t.Fatalf("leader rejected first Start")
	}
	c.waitApplied(firstIndex, []byte("put /m2 before-restart"), ids...)

	restartedID := pickFollowerID(t, c, leader.ID())
	c.stopNode(restartedID)

	secondIndex, _, ok := leader.Start([]byte("put /m2 after-restart"))
	if !ok {
		t.Fatalf("leader rejected second Start")
	}
	c.waitApplied(secondIndex, []byte("put /m2 after-restart"), leader.ID(), otherLiveID(ids, leader.ID(), restartedID))

	c.restartNode(restartedID)
	c.waitApplied(secondIndex, []byte("put /m2 after-restart"), restartedID)
}

func newCluster(t *testing.T, ids []core.MemberID, storageFor func(core.MemberID) core.Storage) *cluster {
	t.Helper()
	c := &cluster{
		t:         t,
		ids:       append([]core.MemberID(nil), ids...),
		transport: local.New(),
		nodes:     make(map[core.MemberID]*core.Raft),
		applyChs:  make(map[core.MemberID]chan core.ApplyMsg),
		storages:  make(map[core.MemberID]core.Storage),
	}
	for _, id := range ids {
		c.storages[id] = storageFor(id)
		c.startNode(id)
	}
	return c
}

func (c *cluster) startNode(id core.MemberID) {
	c.t.Helper()
	applyCh := make(chan core.ApplyMsg, 128)
	rf, err := core.New(core.Config{
		ID:                id,
		Peers:             c.ids,
		Storage:           c.storages[id],
		Transport:         c.transport,
		ApplyCh:           applyCh,
		ElectionTimeout:   80 * time.Millisecond,
		HeartbeatInterval: 15 * time.Millisecond,
	})
	if err != nil {
		c.t.Fatalf("New(%d): %v", id, err)
	}
	c.nodes[id] = rf
	c.applyChs[id] = applyCh
	c.transport.Register(rf)
}

func (c *cluster) restartNode(id core.MemberID) {
	c.t.Helper()
	c.startNode(id)
}

func (c *cluster) stopNode(id core.MemberID) {
	c.t.Helper()
	if rf, ok := c.nodes[id]; ok {
		rf.Stop()
	}
	c.transport.Unregister(id)
	delete(c.nodes, id)
	delete(c.applyChs, id)
}

func (c *cluster) stop() {
	for id, rf := range c.nodes {
		rf.Stop()
		c.transport.Unregister(id)
	}
}

func (c *cluster) waitLeader() *core.Raft {
	c.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var leader *core.Raft
		var leaders int
		for _, rf := range c.nodes {
			if rf.Status().State == core.StateLeader {
				leader = rf
				leaders++
			}
		}
		if leaders == 1 {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for one leader")
	return nil
}

func (c *cluster) waitApplied(index uint64, command []byte, ids ...core.MemberID) {
	c.t.Helper()
	deadline := time.After(3 * time.Second)
	seen := make(map[core.MemberID]bool)
	for len(seen) < len(ids) {
		select {
		case <-deadline:
			c.t.Fatalf("timed out waiting for index %d on %v; seen=%v", index, ids, seen)
		default:
		}
		for _, id := range ids {
			if seen[id] {
				continue
			}
			ch := c.applyChs[id]
			select {
			case msg := <-ch:
				if msg.CommandValid && msg.CommandIndex == index && string(msg.Command) == string(command) {
					seen[id] = true
				}
			default:
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func pickFollowerID(t *testing.T, c *cluster, leaderID core.MemberID) core.MemberID {
	t.Helper()
	for _, id := range c.ids {
		if id != leaderID {
			return id
		}
	}
	t.Fatalf("no follower found")
	return core.NoMember
}

func otherLiveID(ids []core.MemberID, leaderID core.MemberID, stoppedID core.MemberID) core.MemberID {
	for _, id := range ids {
		if id != leaderID && id != stoppedID {
			return id
		}
	}
	return core.NoMember
}

func idString(id core.MemberID) string {
	return strconv.FormatUint(uint64(id), 10)
}
