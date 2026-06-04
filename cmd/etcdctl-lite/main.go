package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/HasonoCell/Etcd-Lite/api/etcdlitepb"
	"github.com/HasonoCell/Etcd-Lite/mvcc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const errorNotLeader = "not_leader"

type cli struct {
	endpoints []string
	timeout   time.Duration
	clientID  uint64
	nextID    uint64
}

func main() {
	endpoints := flag.String("endpoints", "127.0.0.1:2379,127.0.0.1:2380,127.0.0.1:2381", "comma-separated gRPC endpoints")
	timeout := flag.Duration("timeout", 3*time.Second, "per-attempt request timeout")
	flag.Usage = usage
	flag.Parse()

	c := &cli{
		endpoints: splitEndpoints(*endpoints),
		timeout:   *timeout,
		clientID:  uint64(time.Now().UnixNano()),
	}
	if len(c.endpoints) == 0 {
		exitError(errors.New("at least one endpoint is required"))
	}
	if err := c.run(flag.Args()); err != nil {
		exitError(err)
	}
}

func usage() {
	fmt.Fprintf(flag.CommandLine.Output(), `etcdctl-lite is a small CLI for the etcd-lite demo cluster.

Usage:
  etcdctl-lite [global flags] <command> [command flags]

	Global flags:
	`)
	flag.PrintDefaults()
	fmt.Fprint(flag.CommandLine.Output(), `
Commands:
  put [-lease id] [-prev-kv] <key> <value>
  get [-prefix] [-rev revision] [-serializable] <key>
  del [-prefix] [-prev-kv] <key>
  txn-create <key> <value>
  compact <revision>
  watch [-prefix] [-rev revision] [-prev-kv] <key>
  lease grant <lease-id> <ttl-seconds>
  lease keepalive <lease-id>
  lease revoke <lease-id>
  status
`)
}

func (c *cli) run(args []string) error {
	if len(args) == 0 {
		usage()
		return nil
	}
	switch args[0] {
	case "put":
		return c.runPut(args[1:])
	case "get":
		return c.runGet(args[1:])
	case "del":
		return c.runDelete(args[1:])
	case "txn-create":
		return c.runTxnCreate(args[1:])
	case "compact":
		return c.runCompact(args[1:])
	case "watch":
		return c.runWatch(args[1:])
	case "lease":
		return c.runLease(args[1:])
	case "status":
		return c.runStatus(args[1:])
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func (c *cli) runPut(args []string) error {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	leaseID := fs.Int64("lease", 0, "lease id attached to this key")
	prevKV := fs.Bool("prev-kv", false, "return previous key-value")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: put [-lease id] [-prev-kv] <key> <value>")
	}

	req := &etcdlitepb.PutRequest{
		Key:       []byte(fs.Arg(0)),
		Value:     []byte(fs.Arg(1)),
		LeaseId:   *leaseID,
		PrevKv:    *prevKV,
		ClientId:  c.clientID,
		RequestId: c.requestID(),
	}
	var resp *etcdlitepb.PutResponse
	endpoint, err := c.withKV(func(ctx context.Context, client etcdlitepb.KVClient) (*etcdlitepb.ResponseHeader, error) {
		var callErr error
		resp, callErr = client.Put(ctx, req)
		if callErr != nil {
			return nil, callErr
		}
		return resp.GetHeader(), nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("OK endpoint=%s revision=%d raft_index=%d\n", endpoint, resp.GetHeader().GetRevision(), resp.GetHeader().GetRaftIndex())
	if resp.GetPrevKv() != nil {
		printKV(resp.GetPrevKv())
	}
	return nil
}

func (c *cli) runGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	prefix := fs.Bool("prefix", false, "read all keys with the given prefix")
	revision := fs.Int64("rev", 0, "read historical revision")
	serializable := fs.Bool("serializable", false, "allow local serializable read without ReadIndex")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: get [-prefix] [-rev revision] [-serializable] <key>")
	}

	key := []byte(fs.Arg(0))
	req := &etcdlitepb.RangeRequest{
		Key:          key,
		Revision:     *revision,
		Serializable: *serializable,
	}
	if *prefix {
		req.End = mvcc.PrefixEnd(key)
	}
	var resp *etcdlitepb.RangeResponse
	endpoint, err := c.withKV(func(ctx context.Context, client etcdlitepb.KVClient) (*etcdlitepb.ResponseHeader, error) {
		var callErr error
		resp, callErr = client.Range(ctx, req)
		if callErr != nil {
			return nil, callErr
		}
		return resp.GetHeader(), nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("endpoint=%s revision=%d count=%d\n", endpoint, resp.GetHeader().GetRevision(), resp.GetCount())
	for _, kv := range resp.GetKvs() {
		printKV(kv)
	}
	return nil
}

func (c *cli) runDelete(args []string) error {
	fs := flag.NewFlagSet("del", flag.ContinueOnError)
	prefix := fs.Bool("prefix", false, "delete all keys with the given prefix")
	prevKV := fs.Bool("prev-kv", false, "return previous key-values")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: del [-prefix] [-prev-kv] <key>")
	}

	key := []byte(fs.Arg(0))
	req := &etcdlitepb.DeleteRangeRequest{
		Key:       key,
		PrevKv:    *prevKV,
		ClientId:  c.clientID,
		RequestId: c.requestID(),
	}
	if *prefix {
		req.End = mvcc.PrefixEnd(key)
	}
	var resp *etcdlitepb.DeleteRangeResponse
	endpoint, err := c.withKV(func(ctx context.Context, client etcdlitepb.KVClient) (*etcdlitepb.ResponseHeader, error) {
		var callErr error
		resp, callErr = client.DeleteRange(ctx, req)
		if callErr != nil {
			return nil, callErr
		}
		return resp.GetHeader(), nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("endpoint=%s revision=%d deleted=%d\n", endpoint, resp.GetHeader().GetRevision(), resp.GetDeleted())
	for _, kv := range resp.GetPrevKvs() {
		printKV(kv)
	}
	return nil
}

func (c *cli) runTxnCreate(args []string) error {
	if len(args) != 2 {
		return errors.New("usage: txn-create <key> <value>")
	}
	key := []byte(args[0])
	req := &etcdlitepb.TxnRequest{
		ClientId:  c.clientID,
		RequestId: c.requestID(),
		Compare: []*etcdlitepb.Compare{{
			Key:     key,
			Target:  etcdlitepb.CompareTarget_COMPARE_TARGET_VERSION,
			Result:  etcdlitepb.CompareResult_COMPARE_RESULT_EQUAL,
			Version: 0,
		}},
		Success: []*etcdlitepb.RequestOp{{
			Request: &etcdlitepb.RequestOp_RequestPut{
				RequestPut: &etcdlitepb.PutRequest{Key: key, Value: []byte(args[1])},
			},
		}},
		Failure: []*etcdlitepb.RequestOp{{
			Request: &etcdlitepb.RequestOp_RequestRange{
				RequestRange: &etcdlitepb.RangeRequest{Key: key},
			},
		}},
	}
	var resp *etcdlitepb.TxnResponse
	endpoint, err := c.withKV(func(ctx context.Context, client etcdlitepb.KVClient) (*etcdlitepb.ResponseHeader, error) {
		var callErr error
		resp, callErr = client.Txn(ctx, req)
		if callErr != nil {
			return nil, callErr
		}
		return resp.GetHeader(), nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("endpoint=%s revision=%d succeeded=%t\n", endpoint, resp.GetHeader().GetRevision(), resp.GetSucceeded())
	for _, op := range resp.GetResponses() {
		if rangeResp := op.GetResponseRange(); rangeResp != nil {
			for _, kv := range rangeResp.GetKvs() {
				printKV(kv)
			}
		}
	}
	return nil
}

func (c *cli) runCompact(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: compact <revision>")
	}
	revision, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("parse revision: %w", err)
	}
	req := &etcdlitepb.CompactionRequest{
		Revision:  revision,
		ClientId:  c.clientID,
		RequestId: c.requestID(),
	}
	var resp *etcdlitepb.CompactionResponse
	endpoint, err := c.withKV(func(ctx context.Context, client etcdlitepb.KVClient) (*etcdlitepb.ResponseHeader, error) {
		var callErr error
		resp, callErr = client.Compact(ctx, req)
		if callErr != nil {
			return nil, callErr
		}
		return resp.GetHeader(), nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("endpoint=%s compact_revision=%d raft_index=%d\n", endpoint, resp.GetCompactRevision(), resp.GetHeader().GetRaftIndex())
	return nil
}

func (c *cli) runWatch(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	prefix := fs.Bool("prefix", false, "watch all keys with the given prefix")
	startRevision := fs.Int64("rev", 0, "start revision")
	prevKV := fs.Bool("prev-kv", false, "return previous key-values")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: watch [-prefix] [-rev revision] [-prev-kv] <key>")
	}

	key := []byte(fs.Arg(0))
	req := &etcdlitepb.WatchRequest{
		Key:           key,
		StartRevision: *startRevision,
		PrevKv:        *prevKV,
	}
	if *prefix {
		req.End = mvcc.PrefixEnd(key)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	endpoint, stream, conn, err := c.openWatch(ctx, req)
	if err != nil {
		return err
	}
	defer conn.Close()
	fmt.Printf("watching endpoint=%s\n", endpoint)
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return err
		}
		if resp.GetCanceled() {
			return fmt.Errorf("watch canceled: %s compact_revision=%d", resp.GetError(), resp.GetCompactRevision())
		}
		for _, event := range resp.GetEvents() {
			fmt.Printf("%s revision=%d.%d ", event.GetType().String(), event.GetRevision().GetMain(), event.GetRevision().GetSub())
			printKV(event.GetKv())
			if event.GetPrevKv() != nil {
				fmt.Print("prev ")
				printKV(event.GetPrevKv())
			}
		}
	}
}

func (c *cli) runLease(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: lease <grant|keepalive|revoke> ...")
	}
	switch args[0] {
	case "grant":
		return c.runLeaseGrant(args[1:])
	case "keepalive":
		return c.runLeaseKeepAlive(args[1:])
	case "revoke":
		return c.runLeaseRevoke(args[1:])
	default:
		return fmt.Errorf("unknown lease command %q", args[0])
	}
}

func (c *cli) runLeaseGrant(args []string) error {
	if len(args) != 2 {
		return errors.New("usage: lease grant <lease-id> <ttl-seconds>")
	}
	leaseID, ttl, err := parseLeaseIDAndTTL(args)
	if err != nil {
		return err
	}
	req := &etcdlitepb.LeaseGrantRequest{
		LeaseId:   leaseID,
		Ttl:       ttl,
		ClientId:  c.clientID,
		RequestId: c.requestID(),
	}
	var resp *etcdlitepb.LeaseGrantResponse
	endpoint, err := c.withLease(func(ctx context.Context, client etcdlitepb.LeaseClient) (*etcdlitepb.ResponseHeader, error) {
		var callErr error
		resp, callErr = client.LeaseGrant(ctx, req)
		if callErr != nil {
			return nil, callErr
		}
		return resp.GetHeader(), nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("endpoint=%s lease_id=%d ttl=%d\n", endpoint, resp.GetLeaseId(), resp.GetTtl())
	return nil
}

func (c *cli) runLeaseKeepAlive(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: lease keepalive <lease-id>")
	}
	leaseID, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("parse lease id: %w", err)
	}
	req := &etcdlitepb.LeaseKeepAliveRequest{
		LeaseId:   leaseID,
		ClientId:  c.clientID,
		RequestId: c.requestID(),
	}
	var resp *etcdlitepb.LeaseKeepAliveResponse
	endpoint, err := c.withLeaseKeepAlive(req, &resp)
	if err != nil {
		return err
	}
	fmt.Printf("endpoint=%s lease_id=%d ttl=%d\n", endpoint, resp.GetLeaseId(), resp.GetTtl())
	return nil
}

func (c *cli) runLeaseRevoke(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: lease revoke <lease-id>")
	}
	leaseID, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return fmt.Errorf("parse lease id: %w", err)
	}
	req := &etcdlitepb.LeaseRevokeRequest{
		LeaseId:   leaseID,
		ClientId:  c.clientID,
		RequestId: c.requestID(),
	}
	var resp *etcdlitepb.LeaseRevokeResponse
	endpoint, err := c.withLease(func(ctx context.Context, client etcdlitepb.LeaseClient) (*etcdlitepb.ResponseHeader, error) {
		var callErr error
		resp, callErr = client.LeaseRevoke(ctx, req)
		if callErr != nil {
			return nil, callErr
		}
		return resp.GetHeader(), nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("endpoint=%s deleted=%d\n", endpoint, resp.GetDeleted())
	return nil
}

func (c *cli) runStatus(args []string) error {
	if len(args) != 0 {
		return errors.New("usage: status")
	}
	for _, endpoint := range c.endpoints {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		conn, err := grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			cancel()
			fmt.Fprintf(os.Stderr, "%s unavailable: %v\n", endpoint, err)
			continue
		}
		resp, err := etcdlitepb.NewMaintenanceClient(conn).Status(ctx, &etcdlitepb.StatusRequest{})
		_ = conn.Close()
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s status error: %v\n", endpoint, err)
			continue
		}
		header := resp.GetHeader()
		fmt.Printf("%s member=%d state=%s leader=%d term=%d revision=%d commit=%d applied=%d last_log=%d snapshot=%d\n",
			endpoint,
			header.GetMemberId(),
			resp.GetState(),
			header.GetLeaderId(),
			header.GetTerm(),
			header.GetRevision(),
			resp.GetCommitIndex(),
			resp.GetAppliedIndex(),
			resp.GetLastLogIndex(),
			resp.GetSnapshotIndex(),
		)
	}
	return nil
}

func (c *cli) withKV(call func(context.Context, etcdlitepb.KVClient) (*etcdlitepb.ResponseHeader, error)) (string, error) {
	return c.withUnary(func(ctx context.Context, conn *grpc.ClientConn) (*etcdlitepb.ResponseHeader, error) {
		return call(ctx, etcdlitepb.NewKVClient(conn))
	})
}

func (c *cli) withLease(call func(context.Context, etcdlitepb.LeaseClient) (*etcdlitepb.ResponseHeader, error)) (string, error) {
	return c.withUnary(func(ctx context.Context, conn *grpc.ClientConn) (*etcdlitepb.ResponseHeader, error) {
		return call(ctx, etcdlitepb.NewLeaseClient(conn))
	})
}

func (c *cli) withUnary(call func(context.Context, *grpc.ClientConn) (*etcdlitepb.ResponseHeader, error)) (string, error) {
	var lastErr error
	endpoints := append([]string(nil), c.endpoints...)
	for attempt := 0; attempt < len(c.endpoints)*3; attempt++ {
		endpoint := endpoints[0]
		endpoints = rotateEndpoints(endpoints)
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		conn, err := grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			cancel()
			lastErr = fmt.Errorf("dial %s: %w", endpoint, err)
			continue
		}
		header, err := call(ctx, conn)
		_ = conn.Close()
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", endpoint, err)
			continue
		}
		if header == nil {
			lastErr = fmt.Errorf("%s: missing response header", endpoint)
			continue
		}
		if header.GetError() == "" {
			return endpoint, nil
		}
		lastErr = fmt.Errorf("%s: %s", endpoint, header.GetError())
		if header.GetError() == errorNotLeader {
			endpoints = c.preferLeaderEndpoint(endpoints, header.GetLeaderId())
			continue
		}
		return endpoint, lastErr
	}
	if lastErr == nil {
		lastErr = errors.New("no endpoint attempted")
	}
	return "", lastErr
}

func (c *cli) withLeaseKeepAlive(req *etcdlitepb.LeaseKeepAliveRequest, resp **etcdlitepb.LeaseKeepAliveResponse) (string, error) {
	var lastErr error
	endpoints := append([]string(nil), c.endpoints...)
	for attempt := 0; attempt < len(c.endpoints)*3; attempt++ {
		endpoint := endpoints[0]
		endpoints = rotateEndpoints(endpoints)
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		conn, err := grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			cancel()
			lastErr = fmt.Errorf("dial %s: %w", endpoint, err)
			continue
		}
		stream, err := etcdlitepb.NewLeaseClient(conn).LeaseKeepAlive(ctx)
		if err == nil {
			err = stream.Send(req)
		}
		if err == nil {
			*resp, err = stream.Recv()
		}
		_ = conn.Close()
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", endpoint, err)
			continue
		}
		if *resp == nil || (*resp).GetHeader() == nil {
			lastErr = fmt.Errorf("%s: missing lease keepalive response", endpoint)
			continue
		}
		if (*resp).GetHeader().GetError() == "" {
			return endpoint, nil
		}
		lastErr = fmt.Errorf("%s: %s", endpoint, (*resp).GetHeader().GetError())
		if (*resp).GetHeader().GetError() == errorNotLeader {
			endpoints = c.preferLeaderEndpoint(endpoints, (*resp).GetHeader().GetLeaderId())
			continue
		}
		return endpoint, lastErr
	}
	return "", lastErr
}

func (c *cli) openWatch(ctx context.Context, req *etcdlitepb.WatchRequest) (string, etcdlitepb.Watch_WatchClient, *grpc.ClientConn, error) {
	var lastErr error
	endpoints := append([]string(nil), c.endpoints...)
	for attempt := 0; attempt < len(c.endpoints)*3; attempt++ {
		endpoint := endpoints[0]
		endpoints = rotateEndpoints(endpoints)
		dialCtx, cancel := context.WithTimeout(ctx, c.timeout)
		conn, err := grpc.DialContext(dialCtx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err != nil {
			lastErr = fmt.Errorf("dial %s: %w", endpoint, err)
			continue
		}
		stream, err := etcdlitepb.NewWatchClient(conn).Watch(ctx, req)
		if err != nil {
			_ = conn.Close()
			lastErr = fmt.Errorf("%s: %w", endpoint, err)
			continue
		}
		created, err := stream.Recv()
		if err != nil {
			_ = conn.Close()
			lastErr = fmt.Errorf("%s: %w", endpoint, err)
			continue
		}
		if created.GetHeader() == nil {
			_ = conn.Close()
			lastErr = fmt.Errorf("%s: missing watch response header", endpoint)
			continue
		}
		if created.GetHeader().GetError() == "" && created.GetCreated() {
			fmt.Printf("created watch_id=%d revision=%d\n", created.GetWatchId(), created.GetHeader().GetRevision())
			return endpoint, stream, conn, nil
		}
		_ = conn.Close()
		lastErr = fmt.Errorf("%s: %s", endpoint, created.GetHeader().GetError())
		if created.GetHeader().GetError() == errorNotLeader {
			endpoints = c.preferLeaderEndpoint(endpoints, created.GetHeader().GetLeaderId())
			continue
		}
		return "", nil, nil, lastErr
	}
	return "", nil, nil, lastErr
}

// preferLeaderEndpoint 用 leader hint 调整下一次尝试顺序，MVP 默认 member 1..N 对应 endpoints 1..N。
func (c *cli) preferLeaderEndpoint(endpoints []string, leaderID uint64) []string {
	if leaderID == 0 || leaderID > uint64(len(c.endpoints)) {
		return endpoints
	}
	return prependEndpoint(endpoints, c.endpoints[leaderID-1])
}

func rotateEndpoints(endpoints []string) []string {
	if len(endpoints) <= 1 {
		return endpoints
	}
	return append(endpoints[1:], endpoints[0])
}

func prependEndpoint(endpoints []string, preferred string) []string {
	out := []string{preferred}
	for _, endpoint := range endpoints {
		if endpoint != preferred {
			out = append(out, endpoint)
		}
	}
	return out
}

func (c *cli) requestID() uint64 {
	c.nextID++
	return c.nextID
}

func splitEndpoints(value string) []string {
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

func parseLeaseIDAndTTL(args []string) (int64, int64, error) {
	leaseID, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse lease id: %w", err)
	}
	ttl, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse ttl: %w", err)
	}
	return leaseID, ttl, nil
}

func printKV(kv *etcdlitepb.KeyValue) {
	fmt.Printf("%s\t%s\tcreate=%d mod=%d version=%d lease=%d\n",
		string(kv.GetKey()),
		string(kv.GetValue()),
		kv.GetCreateRevision(),
		kv.GetModRevision(),
		kv.GetVersion(),
		kv.GetLeaseId(),
	)
}

func exitError(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
