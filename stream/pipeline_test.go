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
func (s *panicSource) Offset() int64            { return s.inner.Offset() }
func (s *panicSource) SeekOffset(offset int64)  { s.inner.SeekOffset(offset) }
func (s *panicSource) Bounds() (min, max int64) { return s.inner.Bounds() }

// 连续两次故障、两次从检查点恢复，最终结果仍与无故障运行一致。
func TestPipelineRepeatedRecovery(t *testing.T) {
	dir := t.TempDir()
	topo := TopologyHash("win", 100, 50)
	sharedSink := NewMemSink()

	// 第一次运行：第 1 次写入后失败。
	p1 := newTestPipeline(t, NewMemSource(buildInput()),
		&flakySink{inner: sharedSink, failAfter: 1},
		NewCheckpointStore(dir, topo, time.Hour), time.Second)
	if err := p1.Run(); !errors.Is(err, ErrOperatorFailed) {
		t.Fatalf("run1 expected ErrOperatorFailed, got %v", err)
	}
	// 第二次运行（恢复）：第 2 次写入后再次失败。
	p2 := newTestPipeline(t, NewMemSource(buildInput()),
		&flakySink{inner: sharedSink, failAfter: 2},
		NewCheckpointStore(dir, topo, time.Hour), time.Second)
	if err := p2.Run(); !errors.Is(err, ErrOperatorFailed) {
		t.Fatalf("run2 expected ErrOperatorFailed, got %v", err)
	}
	// 第三次运行（再次恢复）：完整跑完。
	p3 := newTestPipeline(t, NewMemSource(buildInput()), sharedSink,
		NewCheckpointStore(dir, topo, time.Hour), time.Second)
	if err := p3.Run(); err != nil {
		t.Fatalf("run3 failed: %v", err)
	}

	refSink := NewMemSink()
	ref := newTestPipeline(t, NewMemSource(buildInput()), refSink,
		NewCheckpointStore(t.TempDir(), topo, time.Hour), time.Second)
	if err := ref.Run(); err != nil {
		t.Fatal(err)
	}
	got, want := sortedResults(sharedSink), sortedResults(refSink)
	if !equalResults(got, want) {
		t.Fatalf("repeated recovery mismatch:\n got=%+v\n want=%+v", got, want)
	}
	t.Logf("[test] repeated recovery verified: %d windows, sink writes=%d (idempotent overwrite)",
		len(got), sharedSink.writes)
}

// newConfigPipeline 用运行配置构建存储与流水线（窗口大小、乱序度可变）。
func newConfigPipeline(t *testing.T, src Source, sink Sink, dir string, windowSize, lateness int64) *Pipeline {
	cfg := RuntimeConfig{OperatorName: "win", WindowSize: windowSize, AllowedLateness: lateness}
	op := NewWindowOperator("win", NewWindowAssigner(windowSize), NewWatermarkGenerator(lateness))
	store := NewCheckpointStoreWithConfig(dir, cfg, time.Hour)
	return NewPipeline(src, op, sink, store, Options{
		MaxInFlight:     2,
		SendTimeout:     time.Second,
		CheckpointEvery: 3,
		Logger:          testLogger(t),
	})
}

// 调整窗口大小后重启：旧检查点与当前生效配置对不上，必须明确拒绝，
// 且不得产出任何结果（不能默默接受后一个结果都不出）。
func TestPipelineRefusesConfigMismatch(t *testing.T) {
	dir := t.TempDir()
	// 第一次运行：窗口 100，正常跑完并留下检查点。
	sink1 := NewMemSink()
	p1 := newConfigPipeline(t, NewMemSource(buildInput()), sink1, dir, 100, 50)
	if err := p1.Run(); err != nil {
		t.Fatal(err)
	}
	if len(sink1.Results()) == 0 {
		t.Fatal("first run should produce results")
	}
	// 重启：窗口调成 200，存储声明的配置与算子一致（都是 200），
	// 但旧检查点记录的是 100 -> 拒绝恢复。
	sink2 := NewMemSink()
	p2 := newConfigPipeline(t, NewMemSource(buildInput()), sink2, dir, 200, 50)
	err := p2.Run()
	if !errors.Is(err, ErrCheckpointConfigMismatch) {
		t.Fatalf("expected ErrCheckpointConfigMismatch, got %v", err)
	}
	if errors.Is(err, ErrCheckpointCorrupt) || errors.Is(err, ErrCheckpointVersionMismatch) {
		t.Fatalf("config mismatch misclassified: %v", err)
	}
	if len(sink2.Results()) != 0 {
		t.Fatalf("pipeline must not run with mismatched config, got %d results", len(sink2.Results()))
	}
	t.Logf("[test] config change refused before any processing: %v", err)

	// 存储声明的配置与算子实际生效配置不一致时，同样拒绝启动。
	sink3 := NewMemSink()
	cfgStore := NewCheckpointStoreWithConfig(dir,
		RuntimeConfig{OperatorName: "win", WindowSize: 100, AllowedLateness: 50}, time.Hour)
	op3 := NewWindowOperator("win", NewWindowAssigner(200), NewWatermarkGenerator(50))
	p3 := NewPipeline(NewMemSource(buildInput()), op3, sink3, cfgStore, Options{Logger: testLogger(t)})
	if err := p3.Run(); !errors.Is(err, ErrCheckpointConfigMismatch) {
		t.Fatalf("store/operator config divergence expected ErrCheckpointConfigMismatch, got %v", err)
	}
	if len(sink3.Results()) != 0 {
		t.Fatal("pipeline must not run when store config diverges from operator")
	}
	t.Logf("[test] store/operator config divergence refused")
}

// 检查点记录的消费位置超出当前数据范围（本地数据被裁过）：
// 必须明确拒绝并给出原因，不能悄悄从最近位置续读。
func TestPipelineRefusesOffsetOutOfRange(t *testing.T) {
	dir := t.TempDir()
	// 第一次运行：完整数据（10 条），跑完后检查点 offset=10。
	sink1 := NewMemSink()
	p1 := newConfigPipeline(t, NewMemSource(buildInput()), sink1, dir, 100, 50)
	if err := p1.Run(); err != nil {
		t.Fatal(err)
	}
	// 重启：数据被裁到只剩 5 条，检查点 offset=10 落在范围之外。
	sink2 := NewMemSink()
	p2 := newConfigPipeline(t, NewMemSource(buildInput()[:5]), sink2, dir, 100, 50)
	err := p2.Run()
	if !errors.Is(err, ErrCheckpointOffsetOutOfRange) {
		t.Fatalf("expected ErrCheckpointOffsetOutOfRange, got %v", err)
	}
	if errors.Is(err, ErrCheckpointCorrupt) || errors.Is(err, ErrCheckpointConfigMismatch) {
		t.Fatalf("offset-out-of-range misclassified: %v", err)
	}
	if len(sink2.Results()) != 0 {
		t.Fatalf("pipeline must not resume out of range, got %d results", len(sink2.Results()))
	}
	t.Logf("[test] out-of-range checkpoint offset refused: %v", err)
}

// transientSink 前 failFirst 次写入失败，之后恢复（模拟下游偶发抖动）。
type transientSink struct {
	inner     *MemSink
	failFirst int
	writes    int
}

func (s *transientSink) Write(r WindowResult) error {
	s.writes++
	if s.writes <= s.failFirst {
		return errors.New("downstream temporarily unavailable")
	}
	return s.inner.Write(r)
}

// 下游连续失败后重试成功：同一次运行内隔离重试，结果与无故障参考一致，
// 不重不漏；重试次数可从写入计数观察。
func TestPipelineSinkRetryRecovers(t *testing.T) {
	// 参考运行：无故障。
	refSink := NewMemSink()
	ref := newTestPipeline(t, NewMemSource(buildInput()), refSink, nil, time.Second)
	if err := ref.Run(); err != nil {
		t.Fatal(err)
	}
	want := sortedResults(refSink)

	// 下游前 2 次写入失败，重试额度 3 -> 应恢复。
	shared := NewMemSink()
	flaky := &transientSink{inner: shared, failFirst: 2}
	op := NewWindowOperator("win", NewWindowAssigner(100), NewWatermarkGenerator(50))
	p := NewPipeline(NewMemSource(buildInput()), op, flaky, nil, Options{
		MaxInFlight:      2,
		SendTimeout:      time.Second,
		CheckpointEvery:  3,
		SinkRetries:      3,
		SinkRetryBackoff: time.Millisecond,
		Logger:           testLogger(t),
	})
	if err := p.Run(); err != nil {
		t.Fatalf("run with retries should succeed, got %v", err)
	}
	got := sortedResults(shared)
	if !equalResults(got, want) {
		t.Fatalf("retry run mismatch:\n got=%+v\n want=%+v", got, want)
	}
	if flaky.writes != len(want)+2 {
		t.Fatalf("expected %d committed + 2 failed writes, got %d", len(want), flaky.writes)
	}
	t.Logf("[test] sink retry recovered: %d windows, writes=%d (2 retried)", len(got), flaky.writes)
}

// 重试额度用完：本次运行失败，错误类别可区分且能定位到出问题的记录。
func TestPipelineSinkRetryExhausted(t *testing.T) {
	alwaysFail := &transientSink{inner: NewMemSink(), failFirst: 1 << 30}
	op := NewWindowOperator("win", NewWindowAssigner(100), NewWatermarkGenerator(50))
	p := NewPipeline(NewMemSource(buildInput()), op, alwaysFail, nil, Options{
		MaxInFlight:      2,
		SendTimeout:      time.Second,
		SinkRetries:      2,
		SinkRetryBackoff: time.Millisecond,
		Logger:           testLogger(t),
	})
	err := p.Run()
	if !errors.Is(err, ErrSinkRetryExhausted) {
		t.Fatalf("expected ErrSinkRetryExhausted, got %v", err)
	}
	if errors.Is(err, ErrOperatorFailed) {
		t.Fatalf("retry-exhausted must be distinguishable from plain operator failure: %v", err)
	}
	var opErr *OpError
	if !errors.As(err, &opErr) || opErr.Op != "sink" {
		t.Fatalf("error should locate sink, got %v", err)
	}
	// 每条结果最多尝试 1+2 次，第一条即耗尽。
	if alwaysFail.writes != 3 {
		t.Fatalf("expected 3 write attempts (1+2 retries), got %d", alwaysFail.writes)
	}
	t.Logf("[test] retry exhausted with context: %v (attempts=%d)", err, alwaysFail.writes)
}
