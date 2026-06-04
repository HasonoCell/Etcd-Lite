package watch

import (
	"context"
	"errors"
	"sync"

	"github.com/HasonoCell/Etcd-Lite/mvcc"
)

const defaultBufferSize = 128

var ErrWatcherSlow = errors.New("watch: watcher is too slow")

type Config struct {
	BufferSize int
}

type EventBatch struct {
	Revision int64
	Events   []mvcc.Event
}

// 客户端对一个 key 或者 prefix 的订阅
type Watcher struct {
	id     int64
	key    []byte             // key range
	end    []byte             // key range
	prevKV bool               // 客户端是否想看到旧值
	ch     chan EventBatch    // 这个 watcher 接收事件的 channel
	cancel context.CancelFunc // 清理 watcher 的开关
}

func (w *Watcher) ID() int64 {
	return w.id
}

func (w *Watcher) Events() <-chan EventBatch {
	return w.ch
}

func (w *Watcher) Close() {
	w.cancel()
}

// 管理所有 Watcher
type Hub struct {
	mu         sync.Mutex
	nextID     int64
	bufferSize int // 控制每个 watcher 的 channel 缓冲区大小
	watchers   map[int64]*Watcher
}

func New(cfg Config) *Hub {
	bufferSize := cfg.BufferSize
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	return &Hub{
		nextID:     1,
		bufferSize: bufferSize,
		watchers:   make(map[int64]*Watcher),
	}
}

// 注册一个 watcher
func (h *Hub) Register(ctx context.Context, key []byte, end []byte, prevKV bool) *Watcher {
	h.mu.Lock()
	defer h.mu.Unlock()

	watchCtx, cancel := context.WithCancel(ctx)
	watcher := &Watcher{
		id:     h.nextID,
		key:    append([]byte(nil), key...),
		end:    append([]byte(nil), end...),
		prevKV: prevKV,
		ch:     make(chan EventBatch, h.bufferSize),
		cancel: cancel,
	}
	h.nextID++
	h.watchers[watcher.id] = watcher

	// 启动协程监听 watchCtx，如果 cancel 被调用，自动清理 watcher
	go func() {
		<-watchCtx.Done()
		h.unregister(watcher.id)
	}()
	return watcher
}

// 将 mvcc apply 的一批事件通知给所有匹配的 watcher
func (h *Hub) Publish(events []mvcc.Event) {
	if len(events) == 0 {
		return
	}

	// 复制 watchers
	h.mu.Lock()
	watchers := make([]*Watcher, 0, len(h.watchers))
	for _, watcher := range h.watchers {
		watchers = append(watchers, watcher)
	}
	h.mu.Unlock()

	// 遍历每个 watcher，看 events 中的事件和 watcher 能否匹配
	for _, watcher := range watchers {
		matched := matchEvents(watcher, events)
		if len(matched) == 0 {
			continue
		}
		// 构造 batch 传给 ch
		batch := EventBatch{
			Revision: matched[len(matched)-1].Revision.Main,
			Events:   matched,
		}
		select {
		case watcher.ch <- batch:
		default:
			watcher.cancel()
		}
	}
}

func (h *Hub) unregister(id int64) {
	h.mu.Lock()
	watcher, ok := h.watchers[id]
	if ok {
		delete(h.watchers, id)
		close(watcher.ch)
	}
	h.mu.Unlock()
}

func matchEvents(watcher *Watcher, events []mvcc.Event) []mvcc.Event {
	matched := make([]mvcc.Event, 0)
	for _, event := range events {
		if !mvcc.KeyInRange(event.KV.Key, watcher.key, watcher.end) {
			continue
		}
		out := mvcc.CloneEvent(event)
		if !watcher.prevKV {
			out.PrevKV = nil
		}
		matched = append(matched, out)
	}
	return matched
}
