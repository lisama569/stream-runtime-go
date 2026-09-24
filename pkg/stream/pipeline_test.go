package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// pipelineEvents 构造一批乱序 + 真正“窗口触发后迟到”的输入。
// win=1000, bound=500。
func pipelineEvents() []Event {
	return []Event{
		{Key: "a", EventTime: 100, Value: 1},
		{Key: "a", EventTime: 200, Value: 2},
		{Key: "a", EventTime: 600, Value: 4},
		{Key: "a", EventTime: 400, Value: 8}, // 乱序：仍计入 [0,1000)
		{Key: "b", EventTime: 800, Value: 10},
		{Key: "a", EventTime: 1500, Value: 16},  // wm=1000 → 触发 [0,1000)
		{Key: "a", EventTime: 300, Value: 32},   // 迟到：[0,1000) 已触发 → 丢弃
		{Key: "a", EventTime: 2600, Value: 64},  // wm=2100 → 触发 [1000,2000)
		{Key: "a", EventTime: 1300, Value: 128}, // 迟到：[1000,2000) 已触发 → 丢弃
		{Key: "a", EventTime: 3600, Value: 256}, // wm=3100 → 触发 [2000,3000)
	}
}

func baseCfg(dir string, log Logger) PipelineConfig {
	return PipelineConfig{
		TopologyID:      "test-topo",
		Source:          NewLocalSource(pipelineEvents()),
		WatermarkBound:  500,
		WindowSizeMs:    1000,
		SinkCapacity:    2,
		SinkDelay:       time.Millisecond,
		CheckpointDir:   dir,
		OfferTimeout:    5 * time.Second,
		IdleTimeout:     10 * time.Second,
		CheckpointEvery: 1,
		Logger:          log,
	}
}

func expectFinal(t *testing.T, outs []WindowResult) {
	t.Helper()
	m := resultsToMap(outs)
	want := []struct {
		key        string
		start      int64
		sum, count int64
	}{
		{"a", 0, 15, 4}, // 1+2+4+8；300(value32) 迟到丢弃
		{"b", 0, 10, 1},
		{"a", 1000, 16, 1}, // 1300(value128) 迟到丢弃
		{"a", 2000, 64, 1},
	}
	if len(outs) != len(want) {
		t.Fatalf("期望 %d 个窗口结果，得到 %d: %+v", len(want), len(outs), outs)
	}
	for _, w := range want {
		r, ok := m[fmt.Sprintf("%s@%d", w.key, w.start)]
		if !ok {
			t.Fatalf("缺少结果 %s@%d", w.key, w.start)
		}
		if r.Sum != w.sum || r.Count != w.count {
			t.Fatalf("%s@%d 期望 sum=%d,count=%d，得到 sum=%d,count=%d",
				w.key, w.start, w.sum, w.count, r.Sum, r.Count)
		}
	}
}

// TestPipelineDeterministicReplay 同一批输入独立跑两遍，最终结果与迟到集合必须逐字节一致。
func TestPipelineDeterministicReplay(t *testing.T) {
	run := func() ([]WindowResult, []LateRecord) {
		log := NewTextLogger(nil)
		p, err := NewPipeline(baseCfg(t.TempDir(), log))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		return p.Outputs(), p.LateRecords()
	}
	o1, l1 := run()
	o2, l2 := run()
	if fmt.Sprint(o1) != fmt.Sprint(o2) {
		t.Fatalf("两次运行结果不一致:\n%+v\n%+v", o1, o2)
	}
	if fmt.Sprint(l1) != fmt.Sprint(l2) {
		t.Fatalf("两次运行迟到集合不一致:\n%+v\n%+v", l1, l2)
	}
	expectFinal(t, o1)
}

// TestPipelineCrashRecovery 处理到一半模拟崩溃：用中途检查点构造新流水线恢复，
// 最终输出必须与一次跑全量完全一致（不重复、不漏算）。
func TestPipelineCrashRecovery(t *testing.T) {
	var buf bytes.Buffer
	log := NewTextLogger(&buf)
	dir := t.TempDir()

	// 阶段一：处理前 6 条后崩溃（此时 [0,1000) 已触发）。
	p1, err := NewPipeline(baseCfg(dir, log))
	if err != nil {
		t.Fatal(err)
	}
	p1.startDrainer()
	for i := 0; i < 6; i++ {
		e, ok, _ := p1.source.Poll(context.Background())
		if !ok {
			t.Fatal("需要更多事件")
		}
		p1.window.Process(e)
		if wm, adv := p1.wm.Observe(e.EventTime); adv {
			for _, r := range p1.window.OnWatermark(wm) {
				if err := p1.offer(r); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	mid, err := p1.checkpoint(p1.wm.MaxSeen(), p1.wm.Current())
	if err != nil {
		t.Fatal(err)
	}
	p1.stopDrainer()
	t.Logf("模拟崩溃点检查点: offset=%d wm=%d", mid.SourceOffset, mid.WMCurrent)

	// 阶段二：全新流水线从同一检查点恢复，继续跑完整源（源从头构造，靠 offset 跳过已处理）。
	p2, err := NewPipeline(baseCfg(dir, log))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p2.Run(context.Background()); err != nil {
		t.Fatalf("恢复后继续运行失败: %v", err)
	}
	expectFinal(t, p2.Outputs())
	if len(p2.LateRecords()) != 2 {
		t.Fatalf("期望 2 条迟到记录，得到 %d: %+v", len(p2.LateRecords()), p2.LateRecords())
	}
	t.Logf("崩溃恢复后输出 %d 条、迟到 %d 条，无重复无丢失", len(p2.Outputs()), len(p2.LateRecords()))

	// 阶段三：在阶段二最终检查点上再次恢复并运行（重复恢复），结果不得变化。
	p3, err := NewPipeline(baseCfg(dir, log))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p3.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	expectFinal(t, p3.Outputs())

	t.Log("==== 水位线/检查点/记录处理流水（节选）====")
	printed := 0
	for _, line := range log.Lines() {
		if printed < 60 {
			t.Log(line)
			printed++
		}
	}
}

// TestPipelineRejectBadCheckpoints 损坏 / 拓扑不兼容 / 过期的检查点必须被明确拒绝，
// 错误原因可区分，且 Run 不得从中间状态继续。
func TestPipelineRejectBadCheckpoints(t *testing.T) {
	dir := t.TempDir()

	// 先产生一个有效检查点。
	p, err := NewPipeline(baseCfg(dir, NewTextLogger(nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	cpPath := p.CheckpointStore().Path()

	// 1) 拓扑不兼容：构造一个 topo 不同的流水线加载同一（有效）检查点。
	cfg2 := baseCfg(dir, NewTextLogger(nil))
	cfg2.TopologyID = "different-topo"
	pDiff, err := NewPipeline(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pDiff.Run(context.Background()); !errors.Is(err, ErrCheckpointIncompat) {
		t.Fatalf("拓扑不兼容应拒绝，得到 %v", err)
	}
	t.Logf("拓扑不兼容检查点已拒绝")

	// 2) 过期：用 TTL 极短的 store 加载同一（有效）检查点。
	expiredStore := NewCheckpointStore(cpPath, time.Nanosecond)
	if _, err := expiredStore.Load("test-topo"); !errors.Is(err, ErrCheckpointExpired) {
		t.Fatalf("过期检查点应拒绝，得到 %v", err)
	}
	t.Logf("过期检查点已拒绝")

	// 3) 损坏：改写文件字节后必须归类为 ErrCheckpointCorrupt。
	raw, _ := os.ReadFile(cpPath)
	raw = append(raw[:len(raw)-5], 'X', 'X', 'X')
	if err := os.WriteFile(cpPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	pCorrupt, err := NewPipeline(baseCfg(dir, NewTextLogger(nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pCorrupt.Run(context.Background()); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("损坏检查点应拒绝(ErrCheckpointCorrupt)，得到 %v", err)
	}
	t.Logf("损坏检查点已拒绝")
}

// TestPipelineBackpressureEndToEnd 极小容量 + 较高延迟的下游不会丢数据，
// 最终结果完整；主循环在投递期间真实经历阻塞背压。
func TestPipelineBackpressureEndToEnd(t *testing.T) {
	log := NewTextLogger(nil)
	cfg := baseCfg(t.TempDir(), log)
	cfg.SinkCapacity = 1
	cfg.SinkDelay = 2 * time.Millisecond
	cfg.OfferTimeout = 10 * time.Second
	p, err := NewPipeline(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatalf("背压下运行不应失败: %v", err)
	}
	expectFinal(t, p.Outputs())
}

// TestPipelineFaultInjection 下游注入瞬时失败后，pipeline 自动重试恢复，最终结果完整，
// 且失败被归类并保留定位上下文。
func TestPipelineFaultInjection(t *testing.T) {
	log := NewTextLogger(nil)
	cfg := baseCfg(t.TempDir(), log)
	cfg.SinkDelay = 0
	p, err := NewPipeline(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p.SetFault(1, 3)
	if _, err := p.Run(context.Background()); err != nil {
		t.Fatalf("故障应被隔离并恢复: %v", err)
	}
	expectFinal(t, p.Outputs())
	fails := p.Failures()
	if len(fails) == 0 {
		t.Fatal("应记录被隔离的下游失败")
	}
	for _, f := range fails {
		if !errors.Is(f, ErrOperatorFailed) || f.Result.WindowID < 0 {
			t.Fatalf("失败归类/上下文错误: %v", f)
		}
	}
	t.Logf("下游瞬时失败 %d 次，全部隔离并重试成功，输出 %d 条", len(fails), len(p.Outputs()))
}

// TestSourceReplayOffset 校验源偏移量在 SeekOffset 后的重放边界。
func TestSourceReplayOffset(t *testing.T) {
	evs := pipelineEvents()
	s := NewLocalSource(evs)
	for i := 0; i < 4; i++ {
		if _, ok, err := s.Poll(context.Background()); err != nil || !ok {
			t.Fatal("poll")
		}
	}
	if s.Offset() != 4 {
		t.Fatalf("offset 应为 4，得到 %d", s.Offset())
	}
	if err := s.SeekOffset(4); err != nil {
		t.Fatal(err)
	}
	e, ok, _ := s.Poll(context.Background())
	if !ok || e.EventTime != evs[4].EventTime {
		t.Fatalf("SeekOffset 后应从第 5 条重放，得到 %+v ok=%v", e, ok)
	}
	if err := s.SeekOffset(int64(len(evs) + 1)); err == nil {
		t.Fatal("越界 offset 必须报错，拒绝中间状态")
	}
}

// 确保临时目录路径风格可移植。
var _ = filepath.Join
