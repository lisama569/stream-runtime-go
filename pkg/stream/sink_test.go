package stream

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func res(key string, start, sum, count int64) WindowResult {
	return WindowResult{Key: key, WindowID: start, WindowEnd: start + 1000, Sum: sum, Count: count}
}

// TestSinkBackpressure 队列满时 Offer 必须阻塞（背压），不丢数据、不超容量；
// drainer 腾位后恢复；超时归类为 ErrBackpressure。
func TestSinkBackpressure(t *testing.T) {
	var buf bytes.Buffer
	log := NewTextLogger(&buf)
	s := NewSlowSink(2, 0, log) // 容量 2、零延迟，方便手控

	// 填满容量。
	for i := 0; i < 2; i++ {
		if err := s.Offer(context.Background(), res("a", int64(i*1000), int64(i), 1)); err != nil {
			t.Fatal(err)
		}
	}
	if s.QueueLen() != 2 {
		t.Fatalf("队列应有 2 条，实际 %d", s.QueueLen())
	}

	// 第三条：短超时必须触发 ErrBackpressure（证明阻塞而非丢弃/扩容）。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := s.Offer(ctx, res("a", 2000, 2, 1))
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("队列满应触发 ErrBackpressure，得到 %v", err)
	}
	if s.QueueLen() != 2 {
		t.Fatalf("背压下不得丢数据也不得扩容，队列仍应为 2，实际 %d", s.QueueLen())
	}
	t.Logf("背压已触发并归类: %v", err)

	// 排空整个队列后（2 条全部确认），第三条应能立即放入（阻塞被解除）。
	if _, err := s.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.QueueLen() != 0 || len(s.Outputs()) != 2 {
		t.Fatalf("排空后队列应为 0、已确认 2，得到 queue=%d outputs=%d", s.QueueLen(), len(s.Outputs()))
	}
	third := res("a", 2000, 2, 1)
	if err := s.Offer(context.Background(), third); err != nil {
		t.Fatalf("腾位后 Offer 应成功: %v", err)
	}
	// 再次填满至容量，确认内存始终不超过 capacity。
	if err := s.Offer(context.Background(), res("a", 3000, 3, 1)); err != nil {
		t.Fatal(err)
	}
	if s.QueueLen() != 2 {
		t.Fatalf("队列容量上限应为 2，实际 %d", s.QueueLen())
	}

	// 全部确认，输出完整（无丢失，共 4 条）。
	if _, err := s.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(s.Outputs()) != 4 {
		t.Fatalf("应确认 4 条输出，实际 %d", len(s.Outputs()))
	}
	t.Logf("背压恢复后输出完整: %d 条；日志如下：", len(s.Outputs()))
	for _, l := range log.Lines() {
		t.Log(l)
	}
}

// TestSinkDedup 重复投递同一窗口必须幂等，不产生重复输出。
func TestSinkDedup(t *testing.T) {
	s := NewSlowSink(4, 0, nil)
	r := res("a", 0, 10, 2)
	for i := 0; i < 5; i++ {
		if err := s.Offer(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 确认后再次投递（模拟检查点回放）：必须跳过。
	if err := s.Offer(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	if len(s.Outputs()) != 1 {
		t.Fatalf("去重后应只有 1 条输出，实际 %d", len(s.Outputs()))
	}
}

// TestSinkFaultIsolation 注入瞬时失败：失败被归类为 ErrOperatorFailed 的 *SinkError，
// 隔离在队首且不丢数据，重试后成功，最终结果完整。
func TestSinkFaultIsolation(t *testing.T) {
	var buf bytes.Buffer
	log := NewTextLogger(&buf)
	s := NewSlowSink(4, 0, log)
	s.SetFault(2, 2) // 每 2 条注入一次，最多 2 次

	// 放入 4 条。
	for i := 0; i < 4; i++ {
		if err := s.Offer(context.Background(), res("a", int64(i*1000), int64(i), 1)); err != nil {
			t.Fatal(err)
		}
	}

	// 反复 Drain：遇到 SinkError 重试，直到全部确认。
	var isolated int
	for s.QueueLen() > 0 {
		_, err := s.Drain(context.Background())
		if err != nil {
			var se *SinkError
			if !errors.As(err, &se) || !errors.Is(err, ErrOperatorFailed) {
				t.Fatalf("失败必须归类为 *SinkError(ErrOperatorFailed)，得到 %T %v", err, err)
			}
			if se.Result.WindowID != s.queue[0].WindowID {
				t.Fatal("失败上下文必须定位到队首记录")
			}
			isolated++
			continue // 重试
		}
	}
	if isolated == 0 {
		t.Fatal("应至少观察到 1 次被隔离的失败")
	}
	if outs := s.Outputs(); len(outs) != 4 {
		t.Fatalf("重试恢复后应输出 4 条，实际 %d", len(outs))
	}
	t.Logf("被隔离单条失败 %d 次，全部重试成功，最终输出 %d 条；失败流水：", isolated, len(s.Outputs()))
	for _, l := range log.Lines() {
		t.Log(l)
	}
}

// TestSinkSnapshotRestore 恢复后已确认集合与在途队列延续，不重复确认、不丢失。
func TestSinkSnapshotRestore(t *testing.T) {
	s1 := NewSlowSink(4, 0, nil)
	s1.Offer(context.Background(), res("a", 0, 1, 1))
	s1.Drain(context.Background())                       // 先确认 1 条
	s1.Offer(context.Background(), res("a", 1000, 2, 1)) // 再放入 1 条但不确认
	data, err := s1.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	s2 := NewSlowSink(4, 0, nil)
	if err := s2.Restore(data); err != nil {
		t.Fatal(err)
	}
	if s2.QueueLen() != 1 {
		t.Fatalf("恢复后在途应为 1（1000 那条），实际 %d", s2.QueueLen())
	}
	if len(s2.Outputs()) != 1 {
		t.Fatalf("恢复后已确认应为 1，实际 %d", len(s2.Outputs()))
	}
	// 已确认的重复投递被去重，不改变在途与确认集合。
	if err := s2.Offer(context.Background(), res("a", 0, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if s2.QueueLen() != 1 {
		t.Fatalf("重复投递不得入队，在途仍应为 1，实际 %d", s2.QueueLen())
	}
	s2.Drain(context.Background())
	if len(s2.Outputs()) != 2 {
		t.Fatalf("恢复并确认后应输出 2 条（不重复），实际 %d", len(s2.Outputs()))
	}
	// 版本/损坏拒绝。
	if err := s2.Restore([]byte("{xx")); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("损坏 sink 状态应拒绝: %v", err)
	}
}
