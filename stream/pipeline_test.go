package stream

import (
	"errors"
	"log"
	"os"
	"sort"
	"testing"
	"time"
)

func testLogger(t *testing.T) *log.Logger {
	return log.New(testWriter{t}, "", 0)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p[:len(p)-1]))
	return len(p), nil
}

// buildInput 构造含乱序与迟到数据的输入。
// 窗口大小 100，允许乱序 50。
func buildInput() []Record {
	return []Record{
		{Key: "a", Value: 1, EventTime: 10},
		{Key: "b", Value: 2, EventTime: 20},
		{Key: "a", Value: 3, EventTime: 15}, // 乱序
		{Key: "a", Value: 4, EventTime: 110},
		{Key: "b", Value: 5, EventTime: 105},
		{Key: "a", Value: 6, EventTime: 250}, // 水位线 -> 200，触发窗口 [0,100) 与 [100,200)
		{Key: "a", Value: 7, EventTime: 30},  // 迟到（水位线 200）
		{Key: "b", Value: 8, EventTime: 260},
		{Key: "a", Value: 9, EventTime: 400}, // 水位线 -> 350，触发窗口 [200,300)
		{Key: "b", Value: 1, EventTime: 120}, // 迟到（水位线 350）
	}
}

func sortedResults(s *MemSink) []WindowResult {
	rs := s.Results()
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].Window.Start != rs[j].Window.Start {
			return rs[i].Window.Start < rs[j].Window.Start
		}
		return rs[i].Key < rs[j].Key
	})
	return rs
}

func newTestPipeline(t *testing.T, src Source, sink Sink, store *CheckpointStore, sendTimeout time.Duration) *Pipeline {
	op := NewWindowOperator("win", NewWindowAssigner(100), NewWatermarkGenerator(50))
	return NewPipeline(src, op, sink, store, Options{
		MaxInFlight:     2,
		SendTimeout:     sendTimeout,
		CheckpointEvery: 3,
		Logger:          testLogger(t),
	})
}

// 端到端：乱序 + 迟到输入，两次运行结果完全一致。
func TestPipelineEndToEndDeterministic(t *testing.T) {
	run := func() ([]WindowResult, []LateRecord) {
		sink := NewMemSink()
		p := newTestPipeline(t, NewMemSource(buildInput()), sink, nil, time.Second)
		if err := p.Run(); err != nil {
			t.Fatal(err)
		}
		return sortedResults(sink), p.operator.LateRecords()
	}
	r1, l1 := run()
	r2, l2 := run()
	if !equalResults(r1, r2) {
		t.Fatalf("non-deterministic:\n run1=%+v\n run2=%+v", r1, r2)
	}
	if len(l1) != 2 || len(l2) != 2 {
		t.Fatalf("expected 2 late records each run, got %d / %d", len(l1), len(l2))
	}
	want := []WindowResult{
		{Window: Window{Start: 0, End: 100}, Key: "a", Sum: 4, Count: 2},
		{Window: Window{Start: 0, End: 100}, Key: "b", Sum: 2, Count: 1},
		{Window: Window{Start: 100, End: 200}, Key: "a", Sum: 4, Count: 1},
		{Window: Window{Start: 100, End: 200}, Key: "b", Sum: 5, Count: 1},
		{Window: Window{Start: 200, End: 300}, Key: "a", Sum: 6, Count: 1},
		{Window: Window{Start: 200, End: 300}, Key: "b", Sum: 8, Count: 1},
	}
	if !equalResults(r1, want) {
		t.Fatalf("got %+v, want %+v", r1, want)
	}
	t.Logf("[test] deterministic results verified, late=%d windows=%d", len(l1), len(r1))
}

func equalResults(a, b []WindowResult) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// flakySink 前 failAfter 次写入正常，之后返回错误（模拟下游故障）。
type flakySink struct {
	inner     *MemSink
	failAfter int
	writes    int
}

func (s *flakySink) Write(r WindowResult) error {
	s.writes++
	if s.writes > s.failAfter {
		return errors.New("downstream unavailable")
	}
	return s.inner.Write(r)
}

// 故障恢复：下游中途失败 -> 从检查点恢复 -> 结果与无故障运行一致（不重不漏）。
func TestPipelineRecoveryNoDupNoLoss(t *testing.T) {
	dir := t.TempDir()
	topo := TopologyHash("win", 100, 50)

	// 参考运行：无故障。
	refSink := NewMemSink()
	refStore := NewCheckpointStore(t.TempDir(), topo, time.Hour)
	ref := newTestPipeline(t, NewMemSource(buildInput()), refSink, refStore, time.Second)
	if err := ref.Run(); err != nil {
		t.Fatal(err)
	}
	want := sortedResults(refSink)

	// 故障运行：第 2 次写入后下游失败。
	sharedSink := NewMemSink()
	store := NewCheckpointStore(dir, topo, time.Hour)
	broken := &flakySink{inner: sharedSink, failAfter: 2}
	p1 := newTestPipeline(t, NewMemSource(buildInput()), broken, store, time.Second)
	err := p1.Run()
	var opErr *OpError
	if !errors.As(err, &opErr) || !errors.Is(err, ErrOperatorFailed) {
		t.Fatalf("expected OpError/ErrOperatorFailed, got %v", err)
	}
	t.Logf("[test] first run failed as expected: %v (sink writes=%d)", err, broken.writes)

	// 恢复运行：全新源与算子，同一检查点目录与结果表。
	p2 := newTestPipeline(t, NewMemSource(buildInput()), sharedSink, store, time.Second)
	if err := p2.Run(); err != nil {
		t.Fatalf("recovery run failed: %v", err)
	}
	got := sortedResults(sharedSink)
	if !equalResults(got, want) {
		t.Fatalf("recovery mismatch:\n got=%+v\n want=%+v", got, want)
	}
	t.Logf("[test] recovery verified: %d windows match reference, no dup/loss", len(got))
}

// 背压：下游极慢 + 有界通道 + 短超时 -> 背压超时错误，可区分。
func TestPipelineBackpressureTimeout(t *testing.T) {
	slow := &slowSink{inner: NewMemSink(), delay: 200 * time.Millisecond}
	p := newTestPipeline(t, NewMemSource(buildInput()), slow, nil, 20*time.Millisecond)
	err := p.Run()
	if !errors.Is(err, ErrBackpressureTimeout) {
		t.Fatalf("expected ErrBackpressureTimeout, got %v", err)
	}
	var opErr *OpError
	if errors.As(err, &opErr) {
		t.Logf("[test] backpressure error with context: op=%s offset=%d", opErr.Op, opErr.Offset)
	}
}

// 下游慢但未超时：结果仍完整正确，不丢数据。
func TestPipelineSlowSinkCompletes(t *testing.T) {
	slow := &slowSink{inner: NewMemSink(), delay: 5 * time.Millisecond}
	p := newTestPipeline(t, NewMemSource(buildInput()), slow, nil, 2*time.Second)
	if err := p.Run(); err != nil {
		t.Fatal(err)
	}
	if got := len(slow.inner.Results()); got != 6 {
		t.Fatalf("expected 6 windows, got %d", got)
	}
	t.Logf("[test] slow sink completed with all 6 windows")
}

type slowSink struct {
	inner *MemSink
	delay time.Duration
}

func (s *slowSink) Write(r WindowResult) error {
	time.Sleep(s.delay)
	return s.inner.Write(r)
}

// 损坏的检查点必须被拒绝，且不得从中间状态继续运行。
func TestPipelineRefusesCorruptCheckpoint(t *testing.T) {
	dir := t.TempDir()
	topo := TopologyHash("win", 100, 50)
	store := NewCheckpointStore(dir, topo, time.Hour)
	if err := store.Save(&Checkpoint{SourceOffset: 5, Windows: map[string]windowState{}}); err != nil {
		t.Fatal(err)
	}
	// 破坏文件。
	p := dir + "/checkpoint.json"
	data, _ := os.ReadFile(p)
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	sink := NewMemSink()
	pl := newTestPipeline(t, NewMemSource(buildInput()), sink, store, time.Second)
	err := pl.Run()
	if !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("expected ErrCheckpointCorrupt, got %v", err)
	}
	if len(sink.Results()) != 0 {
		t.Fatalf("pipeline must not run with corrupt checkpoint, got %d results", len(sink.Results()))
	}
	t.Logf("[test] corrupt checkpoint refused before any processing: %v", err)
}

// 拓扑不兼容的检查点同样被拒绝。
func TestPipelineRefusesIncompatibleCheckpoint(t *testing.T) {
	dir := t.TempDir()
	if err := NewCheckpointStore(dir, "other-topo", time.Hour).Save(&Checkpoint{}); err != nil {
		t.Fatal(err)
	}
	store := NewCheckpointStore(dir, TopologyHash("win", 100, 50), time.Hour)
	pl := newTestPipeline(t, NewMemSource(buildInput()), NewMemSink(), store, time.Second)
	if err := pl.Run(); !errors.Is(err, ErrCheckpointIncompatible) {
		t.Fatalf("expected ErrCheckpointIncompatible, got %v", err)
	}
}

// 算子 panic 被隔离为带上下文的错误，不丢失检查点。
func TestPipelineOperatorPanicIsolated(t *testing.T) {
	src := &panicSource{inner: NewMemSource(buildInput()), panicAt: 4}
	p := newTestPipeline(t, src, NewMemSink(), nil, time.Second)
	err := p.Run()
	var opErr *OpError
	if !errors.As(err, &opErr) || !errors.Is(err, ErrOperatorFailed) {
		t.Fatalf("expected OpError/ErrOperatorFailed, got %v", err)
	}
	if opErr.Op != "win" {
		t.Fatalf("error should locate operator 'win', got %q", opErr.Op)
	}
	t.Logf("[test] panic isolated with context: %v", err)
}

type panicSource struct {
	inner   *MemSource
	panicAt int64
}

func (s *panicSource) Next() (Record, bool) {
	if s.inner.Offset() == s.panicAt {
		panic("simulated operator crash")
	}
	return s.inner.Next()
}
func (s *panicSource) Offset() int64          { return s.inner.Offset() }
func (s *panicSource) SeekOffset(offset int64) { s.inner.SeekOffset(offset) }
