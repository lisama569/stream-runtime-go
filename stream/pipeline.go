package stream

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// Options 流水线运行参数。
type Options struct {
	// MaxInFlight 背压上限：算子与汇之间允许堆积的最大结果数，超出即阻塞上游。
	MaxInFlight int
	// SendTimeout 向下游发送的最长等待时间，超时判定为背压超时错误。
	SendTimeout time.Duration
	// CheckpointEvery 每处理多少条记录做一次检查点。
	CheckpointEvery int
	// SinkRetries 下游写失败时的隔离重试次数（同一次运行内）。
	// 0 表示不重试，失败即终止（保持旧行为）。
	SinkRetries int
	// SinkRetryBackoff 每次重试前的等待时间。
	SinkRetryBackoff time.Duration
	// Logger 处理流水日志（水位线、检查点、记录），nil 则用默认。
	Logger *log.Logger
}

func (o *Options) withDefaults() Options {
	out := *o
	if out.MaxInFlight <= 0 {
		out.MaxInFlight = 64
	}
	if out.SendTimeout <= 0 {
		out.SendTimeout = 5 * time.Second
	}
	if out.CheckpointEvery <= 0 {
		out.CheckpointEvery = 100
	}
	if out.SinkRetries < 0 {
		out.SinkRetries = 0
	}
	if out.SinkRetryBackoff <= 0 {
		out.SinkRetryBackoff = 100 * time.Millisecond
	}
	if out.Logger == nil {
		out.Logger = log.Default()
	}
	return out
}

// TopologyHash 计算拓扑指纹：算子名、窗口大小、允许乱序度任一变化都视为不兼容。
// 等价于 RuntimeConfig{opName, windowSize, allowedLateness}.Fingerprint()。
func TopologyHash(opName string, windowSize, allowedLateness int64) string {
	return RuntimeConfig{
		OperatorName:    opName,
		WindowSize:      windowSize,
		AllowedLateness: allowedLateness,
	}.Fingerprint()
}

// windowMsg 是算子发往汇的消息：普通结果或同步屏障。
type windowMsg struct {
	result  *WindowResult
	barrier chan struct{} // 非 nil 时表示屏障：汇处理完此前所有结果后关闭它
}

// Pipeline 串联 源 -> 窗口算子 -> 结果汇，提供背压、检查点与故障隔离。
type Pipeline struct {
	source   Source
	operator *WindowOperator
	sink     Sink
	store    *CheckpointStore
	opts     Options
}

// NewPipeline 组装流水线。
func NewPipeline(src Source, op *WindowOperator, sink Sink, store *CheckpointStore, opts Options) *Pipeline {
	return &Pipeline{
		source:   src,
		operator: op,
		sink:     sink,
		store:    store,
		opts:     opts.withDefaults(),
	}
}

// Run 运行流水线直到源耗尽。
//
// 恢复语义：若 store 中存在合法检查点则先恢复（源位移、水位线、窗口状态），
// 检查点非法时返回可区分错误并拒绝运行，不会从中间状态继续：
//   - 内容损坏/无法解析：ErrCheckpointCorrupt
//   - 来自更早状态结构（格式版本过旧）：ErrCheckpointVersionMismatch
//   - 运行配置（窗口大小、允许乱序度等）对不上：ErrCheckpointConfigMismatch
//   - 检查点记录的源位移超出当前数据范围：ErrCheckpointOffsetOutOfRange
//
// 配置诚实性：若存储按 NewCheckpointStoreWithConfig 创建，运行前还会核对
// 存储声明的配置与算子实际生效的配置，不一致同样拒绝启动。
//
// 背压：算子与汇之间是有界通道（MaxInFlight），下游变慢会阻塞上游，
// 超过 SendTimeout 返回 ErrBackpressureTimeout，不会静默丢数据或无上限堆积。
//
// 下游抖动：SinkRetries > 0 时，汇写失败在同一次运行内做有限次隔离重试
// （重试期间有界通道不变，内存仍封顶）；额度用完返回 ErrSinkRetryExhausted，
// 并定位到失败的窗口结果。重试不重复、不丢失已算出的结果（汇幂等）。
//
// 故障隔离：算子 panic 与汇写失败都被归类为带上下文的 OpError，
// 已提交的检查点不受影响，恢复后结果仍正确（汇按窗口+键幂等去重）。
func (p *Pipeline) Run() (err error) {
	logg := p.opts.Logger
	// 1. 尝试从检查点恢复。
	if p.store != nil {
		// 配置守卫：存储声明的配置必须与算子实际生效配置一致，
		// 否则即使检查点校验通过也是在用错误语义续跑，必须拒绝。
		if p.store.hasConfig && p.store.config != p.operator.Config() {
			return fmt.Errorf("refuse to run: %w: store declares [%s] but operator runs [%s]",
				ErrCheckpointConfigMismatch, p.store.config, p.operator.Config())
		}
		cp, lerr := p.store.Load()
		if lerr != nil {
			return fmt.Errorf("refuse to run: %w", lerr)
		}
		if cp != nil {
			// 位移守卫：检查点记录的消费位置必须落在当前数据范围内，
			// 否则续读会悄悄吐出一批空窗口结果，必须拒绝。
			min, max := p.source.Bounds()
			if cp.SourceOffset < min || cp.SourceOffset > max {
				return fmt.Errorf("refuse to run: %w: checkpoint offset=%d source range=[%d,%d]",
					ErrCheckpointOffsetOutOfRange, cp.SourceOffset, min, max)
			}
			p.operator.restore(cp.Windows, cp.MaxSeen)
			p.source.SeekOffset(cp.SourceOffset)
			logg.Printf("[checkpoint] restored offset=%d watermark=%d windows=%d",
				cp.SourceOffset, cp.Watermark, len(cp.Windows))
		} else {
			logg.Printf("[checkpoint] none found, start from beginning")
		}
	}

	// 2. 启动有界结果通道 + 独立写汇协程（背压边界）。
	results := make(chan windowMsg, p.opts.MaxInFlight)
	sinkErr := make(chan error, 1)
	sinkDone := make(chan struct{})
	var closeOnce sync.Once
	go func() {
		defer close(sinkDone)
		for msg := range results {
			if msg.barrier != nil {
				close(msg.barrier)
				continue
			}
			r := msg.result
			if werr := p.writeWithRetry(*r, logg); werr != nil {
				sinkErr <- werr
				return
			}
			logg.Printf("[sink] committed window=[%d,%d) key=%s sum=%d count=%d",
				r.Window.Start, r.Window.End, r.Key, r.Sum, r.Count)
		}
	}()

	// 3. 主循环：读源 -> 算子处理 -> 发送结果（带背压超时）。
	defer func() {
		if rec := recover(); rec != nil {
			err = &OpError{Op: p.operator.name, Offset: p.source.Offset(),
				Kind: ErrOperatorFailed, Msg: fmt.Sprintf("panic: %v", rec)}
		}
		closeOnce.Do(func() { close(results) })
		<-sinkDone
	}()

	processed := int64(0)
	for {
		rec, ok := p.source.Next()
		if !ok {
			break
		}
		out := p.operator.Process(rec)
		logg.Printf("[record] offset=%d key=%s ts=%d value=%d watermark=%d emitted=%d",
			rec.Offset, rec.Key, rec.EventTime, rec.Value, p.operator.Watermark(), len(out))
		for i := range out {
			select {
			case results <- windowMsg{result: &out[i]}:
			case serr := <-sinkErr:
				return serr
			case <-time.After(p.opts.SendTimeout):
				return &OpError{Op: p.operator.name, Offset: rec.Offset,
					Kind: ErrBackpressureTimeout,
					Msg:  fmt.Sprintf("sink not draining within %s (in-flight cap=%d)", p.opts.SendTimeout, cap(results))}
			}
		}
		processed++
		if p.store != nil && processed%int64(p.opts.CheckpointEvery) == 0 {
			if cerr := p.syncAndCheckpoint(results, sinkErr, logg); cerr != nil {
				return cerr
			}
		}
	}
	// 4. 结束前做最终屏障 + 检查点，保证重启后可精确续跑。
	if p.store != nil {
		if cerr := p.syncAndCheckpoint(results, sinkErr, logg); cerr != nil {
			return cerr
		}
	}
	closeOnce.Do(func() { close(results) })
	<-sinkDone
	select {
	case serr := <-sinkErr:
		return serr
	default:
	}
	return nil
}

// writeWithRetry 写入单条窗口结果，失败时在额度内隔离重试。
// 重试发生在汇协程内：有界通道容量不变，重试期间上游自然阻塞，
// 背压与内存封顶不被破坏；结果本身不复制、不重排，汇按 (窗口, key) 幂等，
// 因此重试不会重复也不会丢。每次重试都带上下文日志。
func (p *Pipeline) writeWithRetry(r WindowResult, logg *log.Logger) error {
	maxAttempts := 1 + p.opts.SinkRetries
	var werr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if werr = p.sink.Write(r); werr == nil {
			if attempt > 1 {
				logg.Printf("[sink-retry] recovered attempt=%d/%d window=[%d,%d) key=%s",
					attempt, maxAttempts, r.Window.Start, r.Window.End, r.Key)
			}
			return nil
		}
		if attempt < maxAttempts {
			logg.Printf("[sink-retry] attempt=%d/%d window=[%d,%d) key=%s err=%v backoff=%s",
				attempt, maxAttempts, r.Window.Start, r.Window.End, r.Key, werr, p.opts.SinkRetryBackoff)
			time.Sleep(p.opts.SinkRetryBackoff)
		}
	}
	kind := error(ErrOperatorFailed)
	if p.opts.SinkRetries > 0 {
		kind = ErrSinkRetryExhausted
	}
	return &OpError{Op: "sink", Offset: p.source.Offset(), Kind: kind,
		Msg: fmt.Sprintf("window=[%d,%d) key=%s attempts=%d: %v",
			r.Window.Start, r.Window.End, r.Key, maxAttempts, werr)}
}

// syncAndCheckpoint 先发屏障确保此前所有结果已写入汇，再保存检查点，
// 保证检查点只覆盖已落汇的结果，崩溃恢复不重不漏。
func (p *Pipeline) syncAndCheckpoint(results chan<- windowMsg, sinkErr <-chan error, logg *log.Logger) error {
	barrier := make(chan struct{})
	select {
	case results <- windowMsg{barrier: barrier}:
	case serr := <-sinkErr:
		return serr
	case <-time.After(p.opts.SendTimeout):
		return &OpError{Op: "checkpoint-barrier", Offset: p.source.Offset(),
			Kind: ErrBackpressureTimeout, Msg: "sink not draining, cannot checkpoint"}
	}
	select {
	case <-barrier:
	case serr := <-sinkErr:
		return serr
	case <-time.After(p.opts.SendTimeout):
		return &OpError{Op: "checkpoint-barrier", Offset: p.source.Offset(),
			Kind: ErrBackpressureTimeout, Msg: "sink not draining, cannot checkpoint"}
	}
	return p.checkpoint(logg)
}

// checkpoint 保存当前源位移、水位线与窗口状态。
func (p *Pipeline) checkpoint(logg *log.Logger) error {
	cp := &Checkpoint{
		SourceOffset: p.source.Offset(),
		Watermark:    p.operator.Watermark(),
		MaxSeen:      p.operator.wm.MaxSeen(),
		Windows:      p.operator.snapshot(),
	}
	if err := p.store.Save(cp); err != nil {
		return &OpError{Op: "checkpoint", Offset: cp.SourceOffset, Kind: ErrOperatorFailed, Msg: err.Error()}
	}
	logg.Printf("[checkpoint] saved offset=%d watermark=%d windows=%d",
		cp.SourceOffset, cp.Watermark, len(cp.Windows))
	return nil
}
