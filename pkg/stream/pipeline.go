package stream

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Pipeline 把 Source→Watermark→Window→Sink 串成一条可恢复的数据链路。
//
// 线程模型：Run 主循环（读取/水位线/窗口/投递）+ 一个后台 drainer
// （按下游延迟慢速确认）。两者通过有界队列 + 条件变量协作：
//   - 队列满时 Offer 真实阻塞 → 向上游传导背压，内存始终 <= capacity；
//   - drainer 处理慢/瞬时失败时，失败被隔离在队首并重试，不丢数据；
//   - 检查点前调用 WaitUntilIdle 形成屏障：只有下游确认完才提交快照。
//
// 一致性边界（effectively-once 输出）：
//   - 源读取 at-least-once：检查点记录已处理偏移，崩溃后从该偏移重放；
//   - 窗口算子持久化“已触发窗口集合”，恢复后绝不重复触发；
//   - 下游按 (key,window) 幂等去重，重放的重复投递不产生重复输出；
//   - 检查点只在下游把在途结果全部确认后提交，不会出现触发了却丢在半路的窗口。
type Pipeline struct {
	topologyID string
	source     *LocalSource
	wm         *WatermarkGenerator
	window     *WindowOperator
	sink       *SlowSink
	store      *CheckpointStore
	log        Logger

	offerTimeout time.Duration // Offer 等待下游腾位的上限，超时归类 ErrBackpressure
	idleTimeout  time.Duration // 检查点前等待下游排空的上限
	maxRetries   int           // 单条下游失败的最大重试次数
	cpEvery      int           // 每处理多少条源记录做一次检查点

	drainerStop chan struct{}
	drainerWG   sync.WaitGroup
	drainMu     sync.Mutex
	drainErr    error // drainer 因超过重试上限等原因致命退出时记录
	failures    []*SinkError
}

// PipelineConfig 构造流水线所需的全部参数。
type PipelineConfig struct {
	TopologyID      string
	Source          *LocalSource
	WatermarkBound  int64
	WindowSizeMs    int64
	SinkCapacity    int
	SinkDelay       time.Duration
	CheckpointDir   string
	CheckpointTTL   time.Duration
	OfferTimeout    time.Duration
	IdleTimeout     time.Duration
	MaxRetries      int
	CheckpointEvery int
	Logger          Logger
}

// SuggestTopologyID 依据窗口大小与乱序容忍度生成稳定拓扑指纹，
// 用于检查点兼容校验（拓扑改了就必须拒绝旧检查点）。
func SuggestTopologyID(windowSizeMs, watermarkBound int64) string {
	return fmt.Sprintf("topo:win=%d:bound=%d:v=%d", windowSizeMs, watermarkBound, checkpointFormatVersion)
}

// NewPipeline 组装流水线。
func NewPipeline(cfg PipelineConfig) (*Pipeline, error) {
	if cfg.Source == nil {
		return nil, fmt.Errorf("pipeline: source is required")
	}
	if cfg.WindowSizeMs <= 0 {
		return nil, fmt.Errorf("pipeline: WindowSizeMs must be positive")
	}
	if cfg.TopologyID == "" {
		cfg.TopologyID = SuggestTopologyID(cfg.WindowSizeMs, cfg.WatermarkBound)
	}
	if cfg.SinkCapacity <= 0 {
		cfg.SinkCapacity = 4
	}
	if cfg.OfferTimeout == 0 {
		cfg.OfferTimeout = 5 * time.Second
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 10 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 1000
	}
	if cfg.CheckpointEvery <= 0 {
		cfg.CheckpointEvery = 1
	}
	store := NewCheckpointStore(cfg.CheckpointDir+"/checkpoint.json", cfg.CheckpointTTL)
	p := &Pipeline{
		topologyID:   cfg.TopologyID,
		source:       cfg.Source,
		wm:           NewWatermarkGenerator(cfg.WatermarkBound),
		window:       NewWindowOperator(cfg.WindowSizeMs, cfg.Logger),
		sink:         NewSlowSink(cfg.SinkCapacity, cfg.SinkDelay, cfg.Logger),
		store:        store,
		log:          cfg.Logger,
		offerTimeout: cfg.OfferTimeout,
		idleTimeout:  cfg.IdleTimeout,
		maxRetries:   cfg.MaxRetries,
		cpEvery:      cfg.CheckpointEvery,
		drainerStop:  make(chan struct{}),
	}
	return p, nil
}

// SetFault 透传下游故障注入配置。
func (p *Pipeline) SetFault(failEvery, maxFails int) { p.sink.SetFault(failEvery, maxFails) }

// startDrainer 启动后台确认协程：循环等待在途结果并确认，
// 单条瞬时失败隔离在队首自动重试；超过重试上限才致命退出。
func (p *Pipeline) startDrainer() {
	p.drainerWG.Add(1)
	go func() {
		defer p.drainerWG.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			<-p.drainerStop
			cancel()
		}()
		for {
			if !p.sink.WaitForWork(ctx) {
				return // 停止信号
			}
			_, err := p.sink.Drain(ctx)
			if err != nil {
				if se, ok := err.(*SinkError); ok {
					p.drainMu.Lock()
					p.failures = append(p.failures, se)
					n := len(p.failures)
					p.drainMu.Unlock()
					if n > p.maxRetries {
						p.drainMu.Lock()
						p.drainErr = fmt.Errorf("sink 单条失败重试超过上限 %d: %w", p.maxRetries, err)
						p.drainMu.Unlock()
						return
					}
					continue // 失败记录仍在队首，下一轮重试
				}
				if ctx.Err() != nil {
					return
				}
				p.drainMu.Lock()
				p.drainErr = err
				p.drainMu.Unlock()
				return
			}
		}
	}()
}

func (p *Pipeline) stopDrainer() {
	close(p.drainerStop)
	p.drainerWG.Wait()
}

func (p *Pipeline) drainerError() error {
	p.drainMu.Lock()
	defer p.drainMu.Unlock()
	return p.drainErr
}

// barrier 检查点前屏障：等待下游把在途结果全部确认，并检查 drainer 是否致命退出。
func (p *Pipeline) barrier() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.idleTimeout)
	defer cancel()
	if err := p.sink.WaitUntilIdle(ctx); err != nil {
		return err
	}
	return p.drainerError()
}

// checkpoint 在屏障之后收集各组件状态并原子落盘。
// wmMaxSeen/wmCurrent 为要持久化的水位线状态（流末尾的收尾水位线可能高于
// WatermarkGenerator 当前值，故由调用方显式给出）。
func (p *Pipeline) checkpoint(wmMaxSeen, wmCurrent int64) (*Checkpoint, error) {
	if err := p.barrier(); err != nil {
		return nil, err
	}
	win, err := p.window.Snapshot()
	if err != nil {
		return nil, err
	}
	sinkState, err := p.sink.Snapshot()
	if err != nil {
		return nil, err
	}
	cp := &Checkpoint{
		Version:      checkpointFormatVersion,
		TopologyID:   p.topologyID,
		SourceOffset: p.source.Offset(),
		WMMaxSeen:    wmMaxSeen,
		WMCurrent:    wmCurrent,
		WindowState:  win,
		SinkState:    sinkState,
	}
	if err := p.store.Save(cp); err != nil {
		return nil, err
	}
	if p.log != nil {
		p.log.Logf("CHECKPOINT-SAVED offset=%d wm=%d confirmed=%d topo=%s",
			cp.SourceOffset, cp.WMCurrent, p.sink.ConfirmedLen(), cp.TopologyID)
	}
	return cp, nil
}

// Recover 从已通过校验的检查点恢复各组件状态。任何一步失败都返回可区分错误，
// 调用方不得继续运行；全部成功才整体生效。
func (p *Pipeline) Recover(cp *Checkpoint) error {
	if err := cp.Verify(); err != nil {
		return err
	}
	if cp.TopologyID != p.topologyID {
		return fmt.Errorf("%w: checkpoint topology=%q current=%q",
			ErrCheckpointIncompat, cp.TopologyID, p.topologyID)
	}
	if err := p.source.SeekOffset(cp.SourceOffset); err != nil {
		return fmt.Errorf("%w: %v", ErrCheckpointCorrupt, err)
	}
	if err := p.wm.AdvanceTo(cp.WMMaxSeen, cp.WMCurrent); err != nil {
		return err
	}
	if err := p.window.Restore(cp.WindowState); err != nil {
		return err
	}
	if err := p.sink.Restore(cp.SinkState); err != nil {
		return err
	}
	if p.log != nil {
		p.log.Logf("RECOVERED offset=%d wm=%d confirmed=%d",
			cp.SourceOffset, cp.WMCurrent, p.sink.ConfirmedLen())
	}
	return nil
}

// maybeRecover 尝试加载检查点；不存在→全新启动；其余错误（过期/损坏/不兼容）原样上抛。
func (p *Pipeline) maybeRecover() (bool, error) {
	cp, err := p.store.Load(p.topologyID)
	if err != nil {
		if err == ErrCheckpointMissing {
			if p.log != nil {
				p.log.Logf("NO-CHECKPOINT 全新启动 topo=%s", p.topologyID)
			}
			return false, nil
		}
		return false, err
	}
	if err := p.Recover(cp); err != nil {
		return false, err
	}
	return true, nil
}

// offer 投递一条触发结果；队列满时阻塞背压，超过 OfferTimeout 归类 ErrBackpressure。
func (p *Pipeline) offer(r WindowResult) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.offerTimeout)
	defer cancel()
	if err := p.sink.Offer(ctx, r); err != nil {
		return err
	}
	return p.drainerError()
}

// Run 执行整条链路。启动时若存在检查点则先恢复，否则全新启动。返回最后一次有效检查点。
func (p *Pipeline) Run(ctx context.Context) (*Checkpoint, error) {
	recovered, err := p.maybeRecover()
	if err != nil {
		return nil, err
	}
	p.startDrainer()
	defer p.stopDrainer()
	if recovered && p.sink.QueueLen() > 0 {
		// 恢复后若有残留的在途结果（非标准边界），先等待确认。
		if err := p.barrier(); err != nil {
			return nil, err
		}
	}

	var lastCP *Checkpoint
	processed := 0
	for {
		if err := ctx.Err(); err != nil {
			return lastCP, err
		}
		e, ok, err := p.source.Poll(ctx)
		if err != nil {
			return lastCP, err
		}
		if !ok {
			break
		}

		// 先计入窗口，再据观察到的事件时间推进水位线并触发闭合窗口。
		p.window.Process(e)
		if wm, advanced := p.wm.Observe(e.EventTime); advanced {
			for _, r := range p.window.OnWatermark(wm) {
				if err := p.offer(r); err != nil {
					return lastCP, err
				}
			}
		}

		processed++
		if p.cpEvery > 0 && processed%p.cpEvery == 0 {
			cp, err := p.checkpoint(p.wm.MaxSeen(), p.wm.Current())
			if err != nil {
				return lastCP, err
			}
			lastCP = cp
		}
	}

	// 流结束：以 maxSeen+1 的水位线强制闭合所有存活窗口（确定性收尾）。
	finalWM := p.wm.MaxSeen() + 1
	for _, r := range p.window.OnWatermark(finalWM) {
		if err := p.offer(r); err != nil {
			return lastCP, err
		}
	}
	cp, err := p.checkpoint(p.wm.MaxSeen(), p.wm.Current())
	if err != nil {
		return lastCP, err
	}
	lastCP = cp
	if p.log != nil {
		p.log.Logf("RUN-COMPLETE processed=%d outputs=%d late=%d isolatedFailures=%d",
			processed, len(p.sink.Outputs()), len(p.window.LateRecords()), len(p.Failures()))
	}
	return lastCP, nil
}

// Outputs 返回下游最终输出（稳定顺序）。
func (p *Pipeline) Outputs() []WindowResult { return p.sink.Outputs() }

// LateRecords 返回被判定迟到的记录流水。
func (p *Pipeline) LateRecords() []LateRecord { return p.window.LateRecords() }

// Failures 返回运行期被隔离并最终重试成功的下游失败记录。
func (p *Pipeline) Failures() []*SinkError {
	p.drainMu.Lock()
	defer p.drainMu.Unlock()
	out := make([]*SinkError, len(p.failures))
	copy(out, p.failures)
	return out
}

// CheckpointStore 暴露底层存储（测试构造损坏/过期/不兼容文件用）。
func (p *Pipeline) CheckpointStore() *CheckpointStore { return p.store }
