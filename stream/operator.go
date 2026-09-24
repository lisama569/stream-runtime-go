package stream

// WindowOperator 按事件时间做窗口聚合，并推进水位线。
// 迟到数据（事件时间 < 水位线）被确定性地拒绝并记录到迟到流。
type WindowOperator struct {
	name     string
	assigner *WindowAssigner
	wm       *WatermarkGenerator
	states   map[windowKey]windowState
	lates    []LateRecord
	emitted  map[windowKey]bool // 已输出窗口，防止重复计算
}

// NewWindowOperator 创建窗口聚合算子。
func NewWindowOperator(name string, assigner *WindowAssigner, wm *WatermarkGenerator) *WindowOperator {
	return &WindowOperator{
		name:     name,
		assigner: assigner,
		wm:       wm,
		states:   make(map[windowKey]windowState),
		emitted:  make(map[windowKey]bool),
	}
}

// Process 处理一条记录：累加窗口状态并推进水位线，
// 返回因水位线推进而完成、可输出的窗口结果。
func (o *WindowOperator) Process(r Record) []WindowResult {
	return nil // 骨架：待实现
}

// Watermark 返回当前水位线。
func (o *WindowOperator) Watermark() int64 { return o.wm.Current() }

// LateRecords 返回被水位线拒绝的迟到记录列表。
func (o *WindowOperator) LateRecords() []LateRecord {
	out := make([]LateRecord, len(o.lates))
	copy(out, o.lates)
	return out
}

// snapshot 导出内部状态用于检查点。
func (o *WindowOperator) snapshot() map[string]windowState { return nil }

// restore 从检查点恢复内部状态。
func (o *WindowOperator) restore(windows map[string]windowState, maxSeen int64) {}
