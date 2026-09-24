package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

const sinkStateVersion = 1

func resultKey(r WindowResult) string {
	return fmt.Sprintf("%s@%d", r.Key, r.WindowID)
}

type sinkStateJSON struct {
	Version   int            `json:"version"`
	Seen      []WindowResult `json:"seen"`
	Queue     []WindowResult `json:"queue"`
	Processed int            `json:"processed"`
	FailCount int            `json:"failCount"`
	Retrying  bool           `json:"retrying"`
}

// SlowSink 模拟一个可能变慢甚至周期性失败的下游。
//
// 背压：内部维护容量上限的在途队列；队列满时 Offer 阻塞等待下游腾出空间，
// 绝不静默丢数据、也不允许内存无上限增长；等待超过调用方给定 deadline 时
// 返回可区分的 ErrBackpressure。
//
// 故障注入与隔离：failEvery>0 时，每处理 N 条记录注入一次“瞬时失败”
// （归类 ErrOperatorFailed 的 *SinkError，携带触发失败的输出记录）。
// 失败的记录保留在队首不丢弃，下次 Drain 重试成功——故障被隔离在单条记录，
// 恢复后链路最终结果仍然完整正确。
//
// 幂等输出：按 (key,windowStart) 去重，检查点回放导致的重复投递不会产生
// 重复输出（effectively-once）。
type SlowSink struct {
	mu        sync.Mutex
	cond      *sync.Cond
	capacity  int
	delay     time.Duration
	queue     []WindowResult
	queued    map[string]bool
	seen      map[string]WindowResult
	processed int
	failEvery int
	maxFails  int
	failCount int
	retrying  bool // 队首记录是否正处于“失败待重试”状态
	log       Logger
}

// NewSlowSink 创建下游。capacity 为在途结果上限；delay 为每条处理延迟（模拟变慢）。
func NewSlowSink(capacity int, delay time.Duration, log Logger) *SlowSink {
	if capacity <= 0 {
		panic("sink capacity must be positive")
	}
	s := &SlowSink{
		capacity: capacity,
		delay:    delay,
		queued:   map[string]bool{},
		seen:     map[string]WindowResult{},
		log:      log,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// SetFault 配置故障注入：每 failEvery 条成功记录之间注入一次瞬时失败，最多 maxFails 次。
func (s *SlowSink) SetFault(failEvery, maxFails int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failEvery = failEvery
	s.maxFails = maxFails
}

// Offer 将一条结果放入在途队列。
//   - 已确认输出的重复结果：幂等跳过（去重，不重复入队）；
//   - 队列满：阻塞等待 Drain 腾出空间；ctx 超时 → ErrBackpressure；
//     ctx 被取消 → 返回 ctx.Err()。
func (s *SlowSink) Offer(ctx context.Context, r WindowResult) error {
	key := resultKey(r)

	s.mu.Lock()
	defer s.mu.Unlock()
	stop := watchCond(ctx, s.cond)
	defer stop()

	if _, ok := s.seen[key]; ok {
		if s.log != nil {
			s.log.Logf("SINK-DEDUP key=%s window=%d 已确认输出，重复投递跳过", r.Key, r.WindowID)
		}
		return nil
	}
	for len(s.queue) >= s.capacity {
		if err := ctx.Err(); err != nil {
			if err == context.DeadlineExceeded {
				return fmt.Errorf("%w: queue=%d capacity=%d key=%s window=%d",
					ErrBackpressure, len(s.queue), s.capacity, r.Key, r.WindowID)
			}
			return err
		}
		if s.log != nil {
			s.log.Logf("BACKPRESSURE-WAIT key=%s window=%d queue=%d/%d 下游慢，阻塞等待",
				r.Key, r.WindowID, len(s.queue), s.capacity)
		}
		s.cond.Wait()
	}
	if !s.queued[key] {
		s.queue = append(s.queue, r)
		s.queued[key] = true
		if s.log != nil {
			s.log.Logf("SINK-ENQUEUE key=%s window=%d queue=%d/%d",
				r.Key, r.WindowID, len(s.queue), s.capacity)
		}
	}
	// 唤醒后台 drainer（有新工作）或等待容量的上游。
	s.cond.Broadcast()
	return nil
}

// waitCtxLocked 的调用者须持有锁；为 cond.Wait 安装 ctx 取消唤醒。
// 返回一个停止监听的清理函数（调用时无需持锁）。
func watchCond(ctx context.Context, cond *sync.Cond) func() {
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cond.Broadcast()
		case <-stop:
		}
	}()
	return func() { close(stop) }
}

// WaitForWork 阻塞直到队列中出现待处理记录；ctx 结束返回 false。供后台 drainer 使用。
func (s *SlowSink) WaitForWork(ctx context.Context) bool {
	stop := watchCond(ctx, s.cond)
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.queue) == 0 {
		if err := ctx.Err(); err != nil {
			return false
		}
		s.cond.Wait()
	}
	return true
}

// WaitUntilIdle 阻塞直到在途队列被全部确认排空（用于检查点前的屏障）。
func (s *SlowSink) WaitUntilIdle(ctx context.Context) error {
	stop := watchCond(ctx, s.cond)
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.queue) != 0 {
		if err := ctx.Err(); err != nil {
			if err == context.DeadlineExceeded {
				return fmt.Errorf("%w: 等待下游排空超时 queue=%d", ErrBackpressure, len(s.queue))
			}
			return err
		}
		s.cond.Wait()
	}
	return nil
}

// shouldFailLocked 判断队首记录本次处理是否应被注入瞬时失败。
func (s *SlowSink) shouldFailLocked() bool {
	if s.retrying {
		return false // 上次失败的重试：放行，模拟“故障已恢复”
	}
	if s.failEvery <= 0 || s.failCount >= s.maxFails {
		return false
	}
	return (s.processed+1)%s.failEvery == 0
}

// Drain 处理在途队列：FIFO，每条处理前等待 delay（模拟下游变慢）。
// 若某条记录触发注入失败，立即返回 *SinkError，该记录留在队首等待重试；
// 已成功处理的结果随返回值给出。ctx 取消/超时时中断等待。
func (s *SlowSink) Drain(ctx context.Context) ([]WindowResult, error) {
	var done []WindowResult
	for {
		// 取出队首（不立即删除），先做可取消的延迟。
		s.mu.Lock()
		if len(s.queue) == 0 {
			s.mu.Unlock()
			return done, nil
		}
		head := s.queue[0]
		willFail := s.shouldFailLocked()
		s.mu.Unlock()

		if s.delay > 0 {
			t := time.NewTimer(s.delay)
			select {
			case <-t.C:
			case <-ctx.Done():
				return done, ctx.Err()
			}
		}

		s.mu.Lock()
		// 双重确认：延迟期间状态可能被恢复替换。
		if len(s.queue) == 0 || resultKey(s.queue[0]) != resultKey(head) {
			s.mu.Unlock()
			return done, nil
		}
		if willFail {
			// 瞬时失败：记录保留在队首，标记待重试。
			s.retrying = true
			s.failCount++
			s.mu.Unlock()
			err := &SinkError{Result: head, Inner: fmt.Errorf("%w: 注入故障 #%d（下游瞬时不可用）",
				ErrOperatorFailed, s.failCount)}
			if s.log != nil {
				s.log.Logf("SINK-FAIL key=%s window=%d 隔离单条失败，保留队首待重试: %v",
					head.Key, head.WindowID, err)
			}
			return done, err
		}
		// 成功：出队、确认去重。
		s.queue = s.queue[1:]
		delete(s.queued, resultKey(head))
		s.retrying = false
		if _, dup := s.seen[resultKey(head)]; !dup {
			s.seen[resultKey(head)] = head
			done = append(done, head)
		}
		s.processed++
		s.cond.Broadcast()
		if s.log != nil {
			s.log.Logf("SINK-OUT key=%s window=[%d,%d) sum=%d count=%d confirmed=%d",
				head.Key, head.WindowID, head.WindowEnd, head.Sum, head.Count, len(s.seen))
		}
		s.mu.Unlock()
	}
}

// Outputs 返回去重后的最终输出，按 (windowStart, key) 稳定排序。
func (s *SlowSink) Outputs() []WindowResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]WindowResult, 0, len(s.seen))
	for _, r := range s.seen {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].WindowID != out[j].WindowID {
			return out[i].WindowID < out[j].WindowID
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// QueueLen 返回当前在途数量（观察背压用）。
func (s *SlowSink) QueueLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

// ConfirmedLen 返回已确认输出数量。
func (s *SlowSink) ConfirmedLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

// Snapshot 持久化去重集合、在途队列与故障计数，保证恢复后不重复确认。
func (s *SlowSink) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seenKeys := make([]string, 0, len(s.seen))
	for k := range s.seen {
		seenKeys = append(seenKeys, k)
	}
	sort.Strings(seenKeys)
	seen := make([]WindowResult, 0, len(s.seen))
	for _, k := range seenKeys {
		seen = append(seen, s.seen[k])
	}
	st := sinkStateJSON{
		Version:   sinkStateVersion,
		Seen:      seen,
		Queue:     append([]WindowResult(nil), s.queue...),
		Processed: s.processed,
		FailCount: s.failCount,
		Retrying:  s.retrying,
	}
	return json.Marshal(&st)
}

// Restore 恢复下游状态；版本不符→ErrCheckpointIncompat，损坏→ErrCheckpointCorrupt。
func (s *SlowSink) Restore(data []byte) error {
	var st sinkStateJSON
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&st); err != nil {
		return fmt.Errorf("%w: sink state: %v", ErrCheckpointCorrupt, err)
	}
	if st.Version != sinkStateVersion {
		return fmt.Errorf("%w: sink state version %d != %d", ErrCheckpointIncompat, st.Version, sinkStateVersion)
	}
	seen := map[string]WindowResult{}
	queued := map[string]bool{}
	for _, r := range st.Queue {
		queued[resultKey(r)] = true
	}
	for _, r := range st.Seen {
		seen[resultKey(r)] = r
	}
	// 全部解析成功后整体替换。
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = seen
	s.queue = append([]WindowResult(nil), st.Queue...)
	s.queued = queued
	s.processed = st.Processed
	s.failCount = st.FailCount
	s.retrying = st.Retrying
	s.cond.Broadcast()
	return nil
}
