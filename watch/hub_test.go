package watch

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/HasonoCell/Etcd-Lite/mvcc"
)

func TestHubPublishesMatchingEventsAndDropsPrevKVWhenDisabled(t *testing.T) {
	hub := New(Config{BufferSize: 4})
	watcher := hub.Register(context.Background(), []byte("/app/"), mvcc.PrefixEnd([]byte("/app/")), false)
	defer watcher.Close()

	prev := mvcc.KeyValue{Key: []byte("/app/a"), Value: []byte("old")}
	hub.Publish([]mvcc.Event{
		{Type: mvcc.EventPut, Revision: mvcc.Revision{Main: 1}, KV: mvcc.KeyValue{Key: []byte("/db/a"), Value: []byte("db")}},
		{Type: mvcc.EventPut, Revision: mvcc.Revision{Main: 2}, KV: mvcc.KeyValue{Key: []byte("/app/a"), Value: []byte("new")}, PrevKV: &prev},
	})

	select {
	case batch := <-watcher.Events():
		if batch.Revision != 2 || len(batch.Events) != 1 || string(batch.Events[0].KV.Key) != "/app/a" {
			t.Fatalf("batch = %+v", batch)
		}
		if batch.Events[0].PrevKV != nil {
			t.Fatalf("PrevKV = %+v, want nil when prevKV disabled", batch.Events[0].PrevKV)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for watch event")
	}
}

func TestHubClosesSlowWatcher(t *testing.T) {
	hub := New(Config{BufferSize: 1})
	watcher := hub.Register(context.Background(), []byte("/key"), nil, true)

	hub.Publish([]mvcc.Event{{Type: mvcc.EventPut, Revision: mvcc.Revision{Main: 1}, KV: mvcc.KeyValue{Key: []byte("/key")}}})
	hub.Publish([]mvcc.Event{{Type: mvcc.EventPut, Revision: mvcc.Revision{Main: 2}, KV: mvcc.KeyValue{Key: []byte("/key")}}})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		select {
		case _, ok := <-watcher.Events():
			if !ok {
				return
			}
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Fatalf("slow watcher was not closed")
}

func TestHubCloseUnblocksWatcher(t *testing.T) {
	hub := New(Config{BufferSize: 1})
	watcher := hub.Register(context.Background(), []byte("/key"), nil, false)
	hub.Close()

	select {
	case _, ok := <-watcher.Events():
		if ok {
			t.Fatalf("watcher channel is still open after hub close")
		}
	case <-time.After(time.Second):
		t.Fatalf("watcher was not closed by hub close")
	}
}

func TestHubPublishAndCloseAreSafeConcurrently(t *testing.T) {
	hub := New(Config{BufferSize: 4})
	watcher := hub.Register(context.Background(), []byte("/key"), nil, false)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			hub.Publish([]mvcc.Event{{
				Type:     mvcc.EventPut,
				Revision: mvcc.Revision{Main: int64(i + 1)},
				KV:       mvcc.KeyValue{Key: []byte("/key")},
			}})
		}
	}()
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		watcher.Close()
	}()
	wg.Wait()

	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-watcher.Events():
			if !ok {
				return
			}
		case <-deadline:
			t.Fatalf("watcher cleanup did not complete")
		}
	}
}
