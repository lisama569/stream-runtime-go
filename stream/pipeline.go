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
	// SinkMaxRetries 下游写入失败时的隔离重试次数（同一次运行内）。
	// 重试在写汇协程内原位进行，不占用额外内存、不破坏有界背压；
	// 额度用完才以 ErrSinkRetriesExhausted 失败。0 表示不重试。
	SinkMaxRetries int
	// SinkRetryBackoff 每次重试前的退避等待时间。
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
	if out.SinkMaxRetries < 0 {
		out.SinkMaxRetries = 0
	}
	if out.SinkRetryBackoff <= 0 {
		out.SinkRetryBackoff = 20 * time.Millisecond
	}
	if out.Logger == nil {
		out.Logger = log.Default()
	}
	return out
}

// TopologyHash 计算拓扑指纹：算子名、窗口大小、允许乱序度任一变化都视为不兼容。
func TopologyHash(opName string, windowSize, allowedLateness int64) string {
	return fmt.Sprintf("%s/w%d/l%d", opName, windowSize, allowedLateness)
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
// 检查点非法时返回可区分错误并拒绝运行，不会从中间状态继续。
// 检查点记录的消费位置若落在源当前数据范围之外（数据被裁剪、回放区间变窄），
// 以 ErrCheckpointOffsetOutOfRange 拒绝恢复，不会悄悄从最近位置续读。
//
// 背压：算子与汇之间是有界通道（MaxInFlight），下游变慢会阻塞上游，
// 超过 SendTimeout 返回 ErrBackpressureTimeout，不会静默丢数据或无上限堆积。
//
// 下游抖动：汇写失败在同一次运行内做有限次数（SinkMaxRetries）的原位重试，
// 重试在有界通道内完成，不额外占用内存；额度用完才以
// ErrSinkRetriesExhausted（属 ErrOperatorFailed）失败，并定位到具体窗口与键。
//
// 故障隔离：算子 panic 与汇写失败都被归类为带上下文的 OpError，
// 已提交的检查点不受影响，恢复后结果仍正确（汇按窗口+键幂等去重）。
func (p *Pipeline) Run() (err error) {
	logg := p.opts.Logger
	// 1. 尝试从检查点恢复。
	if p.store != nil {
		cp, lerr := p.store.Load()
		if lerr != nil {
			return fmt.Errorf("refuse to run: %w", lerr)
		}
		if cp != nil {
			// 消费位置越界校验：源能报告数据范围时，检查点位移必须落在
			// [min, max] 内，否则拒绝恢复（不允许悄悄从最近位置续读）。
			if rs, ok := p.source.(RangedSource); ok {
				min, max := rs.OffsetRange()
				if cp.SourceOffset < min || cp.SourceOffset > max {
					return &OpError{Op: "restore", Offset: cp.SourceOffset,
						Kind: ErrCheckpointOffsetOutOfRange,
						Msg:  fmt.Sprintf("checkpoint offset %d outside source range [%d, %d]", cp.SourceOffset, min, max)}
				}
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

// writeWithRetry 在写汇协程内原位重试下游写入：
// 重试期间结果仍占着有界通道的位置，背压与内存上限不被破坏；
// 已算出的结果按原顺序逐条重试（汇按窗口+键幂等），不重不漏。
// 额度用完返回带窗口/键/位移上下文的 ErrSinkRetriesExhausted。
func (p *Pipeline) writeWithRetry(r WindowResult, logg *log.Logger) error {
	err := p.sink.Write(r)
	for attempt := 1; err != nil && attempt <= p.opts.SinkMaxRetries; attempt++ {
		logg.Printf("[sink] retry attempt=%d/%d window=[%d,%d) key=%s offset=%d err=%v",
			attempt, p.opts.SinkMaxRetries, r.Window.Start, r.Window.End, r.Key,
			p.source.Offset(), err)
		time.Sleep(p.opts.SinkRetryBackoff)
		err = p.sink.Write(r)
	}
	if err != nil {
		return &OpError{Op: "sink", Offset: p.source.Offset(),
			Kind: ErrSinkRetriesExhausted,
			Msg: fmt.Sprintf("window=[%d,%d) key=%s: %v (retries=%d)",
				r.Window.Start, r.Window.End, r.Key, err, p.opts.SinkMaxRetries)}
	}
	return nil
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
