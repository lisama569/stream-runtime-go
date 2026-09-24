package stream

import "math"

// MinWatermark 是初始水位线，表示“尚未确认任何事件时间”。
const MinWatermark = math.MinInt64

// WatermarkGenerator 基于观察到的最大事件时间戳与固定乱序容忍度生成水位线。
//
// 语义：wm = maxSeenEventTime - maxOutOfOrderness，且全局单调不减。
// 含义为“事件时间 <= wm 的记录理论上都已到达”，此后再到达更老时间戳的记录
// 即被判定为迟到。容忍度越大，越能吸收乱序，但窗口触发越晚。
type WatermarkGenerator struct {
	maxSeen    int64 // 观察到的最大事件时间
	outOfOrder int64 // 允许的最大乱序（毫秒）
	current    int64 // 当前已发出的水位线（单调不减）
}

// NewWatermarkGenerator 创建生成器；bound 为允许的最大乱序时间（毫秒，>=0）。
func NewWatermarkGenerator(bound int64) *WatermarkGenerator {
	if bound < 0 {
		bound = 0
	}
	return &WatermarkGenerator{maxSeen: math.MinInt64, outOfOrder: bound, current: MinWatermark}
}

// Observe 观察一条记录的事件时间，返回新水位线以及是否发生了推进。
// 纯确定性：同一输入序列必然得到同一水位线序列，可安全重放。
func (g *WatermarkGenerator) Observe(eventTime int64) (wm int64, advanced bool) {
	if eventTime > g.maxSeen {
		g.maxSeen = eventTime
	}
	candidate := g.maxSeen - g.outOfOrder
	if candidate > g.current {
		g.current = candidate
		return g.current, true
	}
	return g.current, false
}

// Current 返回当前水位线（单调不减）。
func (g *WatermarkGenerator) Current() int64 { return g.current }

// MaxSeen 返回观察到的最大事件时间（检查点用）。
func (g *WatermarkGenerator) MaxSeen() int64 { return g.maxSeen }

// AdvanceTo 从检查点恢复水位线状态，并做一致性校验：
// current 不得超过 maxSeen-bound，否则状态自相矛盾，拒绝恢复。
func (g *WatermarkGenerator) AdvanceTo(maxSeen, current int64) error {
	if current != MinWatermark {
		if maxSeen == math.MinInt64 || current > maxSeen-g.outOfOrder {
			return ErrCheckpointCorrupt
		}
	}
	g.maxSeen = maxSeen
	g.current = current
	return nil
}
