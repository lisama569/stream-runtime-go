package stream

import (
	"bytes"
	"testing"
)

func ev(key string, ts, v int64) Event { return Event{Key: key, EventTime: ts, Value: v} }

func resultsToMap(rs []WindowResult) map[string]WindowResult {
	m := map[string]WindowResult{}
	for _, r := range rs {
		m[resultKey(r)] = r
	}
	return m
}

// TestWindowOutOfOrderAndLate 覆盖乱序与迟到语义：
// 同一批事件按固定顺序处理，窗口结果确定；窗口触发后到达的老事件被稳定丢弃，
// 重复执行得到完全一致的结果（不重复聚合、不漏算存活窗口内的记录）。
func TestWindowOutOfOrderAndLate(t *testing.T) {
	var buf bytes.Buffer
	log := NewTextLogger(&buf)
	op := NewWindowOperator(1000, log)
	wm := NewWatermarkGenerator(500)

	events := []Event{
		ev("a", 100, 1),
		ev("a", 200, 2),
		ev("a", 600, 4),
		ev("a", 400, 8), // 乱序老事件，窗口未触发，必须计入
		ev("a", 1500, 16),
		ev("b", 1500, 64),
		ev("a", 2500, 32),  // wm 到达 2000 → 触发 [1000,2000)
		ev("a", 1200, 128), // 迟到：[1000,2000) 已触发 → 丢弃
		ev("a", 3500, 256), // 落入 [3000,4000)，并把 wm 推到 3000 以闭合 [2000,3000)
	}
	var fired []WindowResult
	for _, e := range events {
		late := op.Process(e)
		if e.EventTime == 1200 {
			if late == nil || late.Reason != "window-fired" {
				t.Fatalf("eventTime=1200 应被判定为 window-fired 迟到，得到 %+v", late)
			}
		}
		if w, adv := wm.Observe(e.EventTime); adv {
			fired = append(fired, op.OnWatermark(w)...)
		}
	}
	// 收尾触发最后一个窗口。
	fired = append(fired, op.OnWatermark(wm.MaxSeen()+1)...)

	m := resultsToMap(fired)
	// [0,1000): 1+2+4+8=15 count4；[1000,2000): a=16,b=64；[2000,3000): a=32
	if r := m["a@0"]; r.Sum != 15 || r.Count != 4 {
		t.Fatalf("窗口 a@0 期望 sum=15,count=4，得到 sum=%d,count=%d", r.Sum, r.Count)
	}
	if r := m["a@1000"]; r.Sum != 16 || r.Count != 1 {
		t.Fatalf("窗口 a@1000 期望 sum=16,count=1，得到 sum=%d,count=%d", r.Sum, r.Count)
	}
	if r := m["b@1000"]; r.Sum != 64 || r.Count != 1 {
		t.Fatalf("窗口 b@1000 期望 sum=64,count=1，得到 sum=%d,count=%d", r.Sum, r.Count)
	}
	if r := m["a@2000"]; r.Sum != 32 || r.Count != 1 {
		t.Fatalf("窗口 a@2000 期望 sum=32,count=1，得到 sum=%d,count=%d", r.Sum, r.Count)
	}
	// 3500 落入 [3000,4000)，收尾(3501)不闭合，故只触发 4 个。
	if len(fired) != 4 {
		t.Fatalf("期望触发 4 个窗口，实际 %d", len(fired))
	}
	late := op.LateRecords()
	if len(late) != 1 || late[0].Event.Value != 128 {
		t.Fatalf("期望恰好 1 条迟到记录(value=128)，得到 %+v", late)
	}

	// 打印水位线/记录处理流水（测试报告可见）。
	t.Logf("窗口算子状态摘要:\n%s", op.StateDigest())
	for _, line := range log.Lines() {
		t.Log(line)
	}
}

// TestWindowFireExactlyOnce 保证窗口只触发一次：重复推进已越过窗口的水位线不重复输出。
func TestWindowFireExactlyOnce(t *testing.T) {
	op := NewWindowOperator(1000, nil)
	op.Process(ev("a", 100, 1))
	r1 := op.OnWatermark(1000)
	r2 := op.OnWatermark(2000)
	r3 := op.OnWatermark(3000)
	if len(r1) != 1 || len(r2) != 0 || len(r3) != 0 {
		t.Fatalf("窗口必须只触发一次，得到 %d/%d/%d", len(r1), len(r2), len(r3))
	}
	// 触发后再来老记录：迟到丢弃。
	if late := op.Process(ev("a", 200, 5)); late == nil {
		t.Fatal("触发后到达的老记录必须被判定迟到")
	}
	if op.StateDigest() == "" {
		// fired 集合应保留在摘要中。
		t.Fatal("fired 集合应反映在状态摘要中")
	}
}

// TestWindowSnapshotRestoreRoundTrip 校验窗口状态可序列化/恢复，且恢复后迟到判定延续。
func TestWindowSnapshotRestoreRoundTrip(t *testing.T) {
	op1 := NewWindowOperator(1000, nil)
	op1.Process(ev("a", 100, 1))
	data, err := op1.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	op2 := NewWindowOperator(1000, nil)
	if err := op2.Restore(data); err != nil {
		t.Fatal(err)
	}
	// 恢复后推进水位线触发，聚合值应延续。
	rs := op2.OnWatermark(1000)
	if len(rs) != 1 || rs[0].Sum != 1 {
		t.Fatalf("恢复后触发结果错误: %+v", rs)
	}
	// 不同窗口大小 → 不兼容，明确拒绝。
	op3 := NewWindowOperator(500, nil)
	if err := op3.Restore(data); err == nil {
		t.Fatal("窗口大小不同的状态必须被拒绝(ErrCheckpointIncompat)")
	}
	// 损坏字节 → ErrCheckpointCorrupt。
	if err := op2.Restore([]byte("{bad-json")); err == nil {
		t.Fatal("损坏状态必须被拒绝(ErrCheckpointCorrupt)")
	}
}

// TestWatermarkMonotonic 校验水位线单调不减且确定性。
func TestWatermarkMonotonic(t *testing.T) {
	g := NewWatermarkGenerator(500)
	seq := []int64{100, 1000, 600, 2000, 1500}
	var prev int64 = MinWatermark
	var advCount int
	for _, ts := range seq {
		wm, adv := g.Observe(ts)
		if wm < prev {
			t.Fatalf("水位线倒退: %d < %d", wm, prev)
		}
		if adv {
			advCount++
		}
		prev = wm
	}
	if g.Current() != 1500 { // maxSeen=2000, bound=500
		t.Fatalf("期望最终水位线 1500，得到 %d", g.Current())
	}
	if advCount != 3 { // -400, 500, 1500
		t.Fatalf("期望推进 3 次，实际 %d", advCount)
	}
}
