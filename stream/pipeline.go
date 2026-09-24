package stream

import (
	"log"
	"time"
)

// Options 流水线运行参数。
type Options struct {
	// MaxInFlight 背压上限：源与算子间允许堆积的最大记录数，超出即阻塞。
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
// 若 store 中存在合法检查点则先恢复；检查点非法时返回可区分错误并拒绝运行。
func (p *Pipeline) Run() error {
	return nil // 骨架：待实现
}
