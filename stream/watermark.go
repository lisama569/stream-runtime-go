package stream

// WatermarkGenerator 基于最大事件时间减允许乱序度推进水位线。
type WatermarkGenerator struct {
	maxOutOfOrderness int64
	maxSeen           int64
	initialized       bool
}

// NewWatermarkGenerator 创建水位线生成器，allowedLateness 为允许的最大乱序毫秒数。
func NewWatermarkGenerator(allowedLateness int64) *WatermarkGenerator {
	return &WatermarkGenerator{maxOutOfOrderness: allowedLateness}
}

// Observe 观察一条记录的事件时间，返回当前水位线。
// 水位线 = 已见最大事件时间 - 允许乱序度，单调不减。
func (g *WatermarkGenerator) Observe(eventTime int64) int64 {
	if !g.initialized || eventTime > g.maxSeen {
		g.maxSeen = eventTime
		g.initialized = true
	}
	return g.Current()
}

// Current 返回当前水位线；未观察到任何事件时返回最小值。
func (g *WatermarkGenerator) Current() int64 {
	if !g.initialized {
		return -1 << 62
	}
	return g.maxSeen - g.maxOutOfOrderness
}

// IsLate 判断事件时间是否落后于水位线（严格小于视为迟到）。
func (g *WatermarkGenerator) IsLate(eventTime int64) bool {
	return g.initialized && eventTime < g.Current()
}
