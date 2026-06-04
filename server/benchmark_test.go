package server

import (
	"fmt"
	"testing"

	"github.com/HasonoCell/Etcd-Lite/api/etcdlitepb"
	"github.com/HasonoCell/Etcd-Lite/raft/core"
)

func BenchmarkPutAndRange(b *testing.B) {
	c := newTestCluster(b, []core.MemberID{1, 2, 3})
	defer c.stop()

	leader := c.waitLeader()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("/bench/%08d", i))
		c.put(leader, &etcdlitepb.PutRequest{
			Key:       key,
			Value:     []byte("value"),
			ClientId:  900,
			RequestId: uint64(i*2 + 1),
		})
		c.rangeKV(leader, &etcdlitepb.RangeRequest{Key: key})
	}
}
