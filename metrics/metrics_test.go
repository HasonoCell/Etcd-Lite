package metrics

import (
	"strings"
	"testing"
)

func TestRecorderSnapshotAndPrometheusOutput(t *testing.T) {
	recorder := NewRecorder()
	recorder.IncRequest("put")
	recorder.IncRequest("put")
	recorder.IncApply("put")
	recorder.IncError("not_leader")
	recorder.AddWatchEvents(3)
	recorder.IncSnapshot()
	recorder.IncLeaseExpiration()

	snapshot := recorder.Snapshot()
	snapshot.MemberID = 1
	snapshot.LeaderID = 2
	snapshot.State = "leader"
	snapshot.Term = 3
	snapshot.Revision = 4
	snapshot.CommitIndex = 5
	snapshot.AppliedIndex = 6
	snapshot.LastLogIndex = 7
	snapshot.SnapshotIndex = 8

	var out strings.Builder
	if err := WritePrometheus(&out, snapshot); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	text := out.String()
	for _, want := range []string{
		`etcdlite_member_info{member_id="1",state="leader"} 1`,
		`etcdlite_leader_id 2`,
		`etcdlite_raft_term 3`,
		`etcdlite_mvcc_revision 4`,
		`etcdlite_requests_total{method="put"} 2`,
		`etcdlite_apply_total{kind="put"} 1`,
		`etcdlite_errors_total{error="not_leader"} 1`,
		`etcdlite_watch_events_total 3`,
		`etcdlite_snapshots_total 1`,
		`etcdlite_lease_expirations_total 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("prometheus output missing %q:\n%s", want, text)
		}
	}
}
