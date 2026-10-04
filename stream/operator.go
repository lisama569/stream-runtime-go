package stream

import (
	"sort"
	"strconv"
)

// WindowOperator 按事件时间做窗口聚合，并推进水位线。
// 迟到数据（事件时间 < 水位线）被确定性地拒绝并记录到迟到流，
// 同一批输入重复执行不会产生重复或遗漏的窗口结果。
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
// 返回因水位线推进而完成、可输出的窗口结果（按窗口起始时间排序，保证输出顺序确定）。
func (o *WindowOperator) Process(r Record) []WindowResult {
	wm := o.wm.Observe(r.EventTime)
	if o.wm.IsLate(r.EventTime) {
		o.lates = append(o.lates, LateRecord{
			Record:    r,
			Watermark: wm,
			Reason:    "event time below watermark",
		})
		return nil
	}
	w := o.assigner.Assign(r.EventTime)
	k := windowKey{Start: w.Start, End: w.End, Key: r.Key}
	st := o.states[k]
	st.Sum += r.Value
	st.Count++
	o.states[k] = st
	return o.fireWindows()
}

// fireWindows 输出所有 End <= 当前水位线且尚未输出的窗口。
func (o *WindowOperator) fireWindows() []WindowResult {
	wm := o.wm.Current()
	var ready []windowKey
	for k := range o.states {
		if k.End <= wm && !o.emitted[k] {
			ready = append(ready, k)
		}
	}
	if len(ready) == 0 {
		return nil
	}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].Start != ready[j].Start {
			return ready[i].Start < ready[j].Start
		}
		return ready[i].Key < ready[j].Key
	})
	out := make([]WindowResult, 0, len(ready))
	for _, k := range ready {
		st := o.states[k]
		out = append(out, WindowResult{
			Window: Window{Start: k.Start, End: k.End},
			Key:    k.Key,
			Sum:    st.Sum,
			Count:  st.Count,
		})
		o.emitted[k] = true
		delete(o.states, k) // 输出后清理状态，避免重复计算
	}
	return out
}

// Watermark 返回当前水位线。
func (o *WindowOperator) Watermark() int64 { return o.wm.Current() }

// Config 返回算子实际生效的运行配置（算子名、窗口大小、允许乱序度），
// 用于检查点兼容性判定——以算子真实参数为准，而不是外部声明。
func (o *WindowOperator) Config() RuntimeConfig {
	return RuntimeConfig{
		OperatorName:    o.name,
		WindowSize:      o.assigner.Size,
		AllowedLateness: o.wm.maxOutOfOrderness,
	}
}

// LateRecords 返回被水位线拒绝的迟到记录列表。
func (o *WindowOperator) LateRecords() []LateRecord {
	out := make([]LateRecord, len(o.lates))
	copy(out, o.lates)
	return out
}

// snapshot 导出内部状态用于检查点。键格式 "start,end,key"。
func (o *WindowOperator) snapshot() map[string]windowState {
	out := make(map[string]windowState, len(o.states))
	for k, st := range o.states {
		out[encodeWindowKey(k)] = st
	}
	return out
}

// restore 从检查点恢复内部状态。
func (o *WindowOperator) restore(windows map[string]windowState, maxSeen int64) {
	o.states = make(map[windowKey]windowState, len(windows))
	for ks, st := range windows {
		if k, ok := decodeWindowKey(ks); ok {
			o.states[k] = st
		}
	}
	o.wm.Restore(maxSeen)
}

func encodeWindowKey(k windowKey) string {
	return strconv.FormatInt(k.Start, 10) + "," + strconv.FormatInt(k.End, 10) + "," + k.Key
}

func decodeWindowKey(s string) (windowKey, bool) {
	var k windowKey
	i := indexByte(s, ',')
	if i < 0 {
		return k, false
	}
	j := indexByte(s[i+1:], ',')
	if j < 0 {
		return k, false
	}
	j += i + 1
	start, err1 := strconv.ParseInt(s[:i], 10, 64)
	end, err2 := strconv.ParseInt(s[i+1:j], 10, 64)
	if err1 != nil || err2 != nil {
		return k, false
	}
	k.Start, k.End, k.Key = start, end, s[j+1:]
	return k, true
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
