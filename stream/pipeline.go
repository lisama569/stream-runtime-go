package stream

import (
	"fmt"
	"log"
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
	if out.Logger == nil {
		out.Logger = log.Default()
	}
	return out
}

// TopologyHash 计算拓扑指纹：算子名、窗口大小、允许乱序度任一变化都视为不兼容。
func TopologyHash(opName string, windowSize, allowedLateness int64) string {
	return fmt.Sprintf("%s/w%d/l%d", opName, windowSize, allowedLateness)
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
// 若 store 中存在合法检查点则先恢复；检查点非法时返回可区分错误并拒绝运行，
// 不会从中间状态继续。下游过慢触发背压阻塞，超时返回 ErrBackpressureTimeout；
// 算子或汇失败返回带上下文的 OpError（ErrOperatorFailed），已提交检查点不丢失。
func (p *Pipeline) Run() (err error) {
	logg := p.opts.Logger
	// 1. 尝试从检查点恢复。
	if p.store != nil {
		cp, lerr := p.store.Load()
		if lerr != nil {
			return fmt.Errorf("refuse to run: %w", lerr)
		}
		if cp != nil {
			p.operator.restore(cp.Windows, cp.MaxSeen)
			p.source.SeekOffset(cp.SourceOffset)
			logg.Printf("[checkpoint] restored offset=%d watermark=%d windows=%d",
				cp.SourceOffset, cp.Watermark, len(cp.Windows))
		} else {
			logg.Printf("[checkpoint] none found, start from beginning")
		}
	}

	// 2. 启动有界结果通道 + 独立写汇协程（背压边界）。
	results := make(chan WindowResult, p.opts.MaxInFlight)
	sinkErr := make(chan error, 1)
	sinkDone := make(chan struct{})
	go func() {
		defer close(sinkDone)
		for r := range results {
			if werr := p.sink.Write(r); werr != nil {
				sinkErr <- &OpError{Op: "sink", Kind: ErrOperatorFailed, Msg: werr.Error()}
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
		close(results)
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
		for _, r := range out {
			select {
			case results <- r:
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
			if cerr := p.checkpoint(logg); cerr != nil {
				return cerr
			}
		}
	}
	// 4. 结束前做最终检查点，保证重启后可精确续跑。
	if p.store != nil {
		if cerr := p.checkpoint(logg); cerr != nil {
			return cerr
		}
	}
	return nil
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
