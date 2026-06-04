package metrics

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Snapshot 是一次 metrics scrape 时看到的运行状态。
type Snapshot struct {
	MemberID              uint64
	LeaderID              uint64
	State                 string
	Term                  uint64
	Revision              int64
	CommitIndex           uint64
	AppliedIndex          uint64
	LastLogIndex          uint64
	SnapshotIndex         uint64
	UptimeSeconds         float64
	Requests              map[string]uint64
	Applies               map[string]uint64
	Errors                map[string]uint64
	WatchEventsTotal      uint64
	SnapshotsTotal        uint64
	LeaseExpirationsTotal uint64
}

// Recorder 保存 server 运行期的计数器。
type Recorder struct {
	mu                    sync.Mutex
	startedAt             time.Time
	requests              map[string]uint64
	applies               map[string]uint64
	errors                map[string]uint64
	watchEventsTotal      uint64
	snapshotsTotal        uint64
	leaseExpirationsTotal uint64
}

func NewRecorder() *Recorder {
	return &Recorder{
		startedAt: time.Now(),
		requests:  make(map[string]uint64),
		applies:   make(map[string]uint64),
		errors:    make(map[string]uint64),
	}
}

func (r *Recorder) IncRequest(method string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests[method]++
}

func (r *Recorder) IncApply(kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applies[kind]++
}

func (r *Recorder) IncError(name string) {
	if name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors[name]++
}

func (r *Recorder) AddWatchEvents(n uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.watchEventsTotal += n
}

func (r *Recorder) IncSnapshot() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snapshotsTotal++
}

func (r *Recorder) IncLeaseExpiration() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leaseExpirationsTotal++
}

func (r *Recorder) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Snapshot{
		UptimeSeconds:         time.Since(r.startedAt).Seconds(),
		Requests:              cloneCounters(r.requests),
		Applies:               cloneCounters(r.applies),
		Errors:                cloneCounters(r.errors),
		WatchEventsTotal:      r.watchEventsTotal,
		SnapshotsTotal:        r.snapshotsTotal,
		LeaseExpirationsTotal: r.leaseExpirationsTotal,
	}
}

// WritePrometheus 将 snapshot 渲染为 Prometheus text format。
func WritePrometheus(w io.Writer, snapshot Snapshot) error {
	lines := []string{
		"# HELP etcdlite_uptime_seconds Server uptime in seconds.",
		"# TYPE etcdlite_uptime_seconds gauge",
		fmt.Sprintf("etcdlite_uptime_seconds %.3f", snapshot.UptimeSeconds),
		"# HELP etcdlite_member_info Static member information labelled by member/state.",
		"# TYPE etcdlite_member_info gauge",
		fmt.Sprintf("etcdlite_member_info{member_id=\"%d\",state=\"%s\"} 1", snapshot.MemberID, escapeLabel(snapshot.State)),
		"# HELP etcdlite_leader_id Current known leader member id.",
		"# TYPE etcdlite_leader_id gauge",
		fmt.Sprintf("etcdlite_leader_id %d", snapshot.LeaderID),
		"# HELP etcdlite_raft_term Current Raft term.",
		"# TYPE etcdlite_raft_term gauge",
		fmt.Sprintf("etcdlite_raft_term %d", snapshot.Term),
		"# HELP etcdlite_mvcc_revision Current MVCC revision.",
		"# TYPE etcdlite_mvcc_revision gauge",
		fmt.Sprintf("etcdlite_mvcc_revision %d", snapshot.Revision),
		"# HELP etcdlite_raft_commit_index Current Raft commit index.",
		"# TYPE etcdlite_raft_commit_index gauge",
		fmt.Sprintf("etcdlite_raft_commit_index %d", snapshot.CommitIndex),
		"# HELP etcdlite_raft_applied_index Current Raft applied index.",
		"# TYPE etcdlite_raft_applied_index gauge",
		fmt.Sprintf("etcdlite_raft_applied_index %d", snapshot.AppliedIndex),
		"# HELP etcdlite_raft_last_log_index Current Raft last log index.",
		"# TYPE etcdlite_raft_last_log_index gauge",
		fmt.Sprintf("etcdlite_raft_last_log_index %d", snapshot.LastLogIndex),
		"# HELP etcdlite_raft_snapshot_index Current Raft snapshot index.",
		"# TYPE etcdlite_raft_snapshot_index gauge",
		fmt.Sprintf("etcdlite_raft_snapshot_index %d", snapshot.SnapshotIndex),
	}

	appendCounterLines := func(help string, name string, label string, counters map[string]uint64) {
		lines = append(lines, "# HELP "+name+" "+help)
		lines = append(lines, "# TYPE "+name+" counter")
		for _, key := range sortedCounterKeys(counters) {
			lines = append(lines, fmt.Sprintf("%s{%s=\"%s\"} %d", name, label, escapeLabel(key), counters[key]))
		}
	}
	appendCounterLines("Total handled gRPC requests.", "etcdlite_requests_total", "method", snapshot.Requests)
	appendCounterLines("Total applied Raft commands.", "etcdlite_apply_total", "kind", snapshot.Applies)
	appendCounterLines("Total server errors by error string.", "etcdlite_errors_total", "error", snapshot.Errors)

	lines = append(lines,
		"# HELP etcdlite_watch_events_total Total watch events published.",
		"# TYPE etcdlite_watch_events_total counter",
		fmt.Sprintf("etcdlite_watch_events_total %d", snapshot.WatchEventsTotal),
		"# HELP etcdlite_snapshots_total Total state machine snapshots created.",
		"# TYPE etcdlite_snapshots_total counter",
		fmt.Sprintf("etcdlite_snapshots_total %d", snapshot.SnapshotsTotal),
		"# HELP etcdlite_lease_expirations_total Total leader-driven lease expiration revokes submitted.",
		"# TYPE etcdlite_lease_expirations_total counter",
		fmt.Sprintf("etcdlite_lease_expirations_total %d", snapshot.LeaseExpirationsTotal),
	)

	_, err := io.WriteString(w, strings.Join(lines, "\n")+"\n")
	return err
}

func cloneCounters(in map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func sortedCounterKeys(counters map[string]uint64) []string {
	keys := make([]string, 0, len(counters))
	for key := range counters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return value
}
