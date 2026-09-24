package stream

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// 窗口算子内部状态序列化版本，用于恢复时识别不兼容状态。
const windowStateVersion = 1

type aggState struct {
	Sum   int64 `json:"sum"`
	Count int64 `json:"count"`
}

type windowStateJSON struct {
	Version int                           `json:"version"`
	SizeMs  int64                         `json:"sizeMs"`
	WM      int64                         `json:"wm"`
	Agg     map[string]map[int64]aggState `json:"agg"`
	Fired   map[string][]int64            `json:"fired"`
	Late    []LateRecord                  `json:"late"`
}

// WindowOperator 实现按 key、按事件时间的滚动窗口聚合（sum/count）。
//
// 触发语义：窗口 [start, start+size) 在水位线推进到 wm >= start+size 时
// 恰好触发一次（on-watermark timer），结果按 key、窗口起始时间的稳定顺序输出。
//
// 迟到语义（确定且稳定、可重放）：
//   - 记录到达时其窗口尚未触发（wm < windowEnd）：正常聚合。即使记录的
//     eventTime 已落后于当前水位线，只要窗口还存活就仍然计入——这正是乱序
//     容忍度覆盖的范围，日志标记为 behind-watermark-accepted。
//   - 窗口已触发后到达的记录：判定为迟到（reason=window-fired），丢弃并记入
//     LateRecords，绝不二次聚合，也不会因此再次触发窗口。
type WindowOperator struct {
	size  int64
	log   Logger
	agg   map[string]map[int64]*aggState // key -> windowStart -> 聚合
	fired map[string]map[int64]bool      // key -> 已触发窗口集合（只触发一次）
	late  []LateRecord
	wm    int64
}

// NewWindowOperator 创建窗口大小为 sizeMs 毫秒的滚动窗口算子。
func NewWindowOperator(sizeMs int64, log Logger) *WindowOperator {
	if sizeMs <= 0 {
		panic("window size must be positive")
	}
	return &WindowOperator{
		size:  sizeMs,
		log:   log,
		agg:   map[string]map[int64]*aggState{},
		fired: map[string]map[int64]bool{},
		wm:    MinWatermark,
	}
}

func windowStart(ts, size int64) int64 {
	if ts >= 0 {
		return (ts / size) * size
	}
	// 负时间戳（理论上不会出现）也给出数学正确的向下取整分桶。
	return ((ts - size + 1) / size) * size
}

// Process 处理一条记录。返回非 nil 的 *LateRecord 表示该记录迟到被丢弃；
// 返回 nil 表示已正常计入某个存活窗口。
func (op *WindowOperator) Process(e Event) *LateRecord {
	start := windowStart(e.EventTime, op.size)
	end := start + op.size

	// 1) 窗口已触发过：迟到，稳定丢弃。
	if op.fired[e.Key] != nil && op.fired[e.Key][start] {
		rec := LateRecord{
			Event:     e,
			Watermark: op.wm,
			Reason:    "window-fired",
		}
		op.late = append(op.late, rec)
		if op.log != nil {
			op.log.Logf("LATE-DROP key=%s eventTime=%d value=%d window=[%d,%d) wm=%d reason=window-fired",
				e.Key, e.EventTime, e.Value, start, end, op.wm)
		}
		return &rec
	}

	// 2) 窗口存活：聚合。记录 eventTime 落后水位线时仅标注，不改变结果。
	behind := op.wm != MinWatermark && e.EventTime < op.wm
	if op.agg[e.Key] == nil {
		op.agg[e.Key] = map[int64]*aggState{}
	}
	st := op.agg[e.Key][start]
	if st == nil {
		st = &aggState{}
		op.agg[e.Key][start] = st
	}
	st.Sum += e.Value
	st.Count++
	if op.log != nil {
		note := "on-time"
		if behind {
			note = "behind-watermark-accepted"
		}
		op.log.Logf("REC-IN key=%s eventTime=%d value=%d window=[%d,%d) agg(sum=%d,count=%d) wm=%d note=%s",
			e.Key, e.EventTime, e.Value, start, end, st.Sum, st.Count, op.wm, note)
	}
	return nil
}

// OnWatermark 推进水位线，返回所有跨过结束时间、且此前未触发的窗口结果。
// 同一窗口在任意重放/恢复序列中只输出一次。
func (op *WindowOperator) OnWatermark(wm int64) []WindowResult {
	if wm <= op.wm {
		// 水位线单调，不允许倒退。
		return nil
	}
	op.wm = wm
	if op.log != nil {
		op.log.Logf("WATERMARK -> %d", wm)
	}

	var out []WindowResult
	keys := make([]string, 0, len(op.agg))
	for k := range op.agg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		starts := make([]int64, 0, len(op.agg[k]))
		for st := range op.agg[k] {
			starts = append(starts, st)
		}
		sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
		for _, start := range starts {
			end := start + op.size
			if wm < end {
				continue // 窗口尚未闭合
			}
			if op.fired[k] != nil && op.fired[k][start] {
				continue // 防御：已触发绝不重复
			}
			a := op.agg[k][start]
			res := WindowResult{Key: k, WindowID: start, WindowEnd: end, Sum: a.Sum, Count: a.Count}
			out = append(out, res)
			if op.fired[k] == nil {
				op.fired[k] = map[int64]bool{}
			}
			op.fired[k][start] = true
			delete(op.agg[k], start)
			if op.log != nil {
				op.log.Logf("WINDOW-FIRE key=%s window=[%d,%d) sum=%d count=%d",
					k, start, end, res.Sum, res.Count)
			}
		}
		if len(op.agg[k]) == 0 {
			delete(op.agg, k)
		}
	}
	return out
}

// LateRecords 返回自启动以来被判定迟到的记录（按到达顺序，可解释）。
func (op *WindowOperator) LateRecords() []LateRecord {
	out := make([]LateRecord, len(op.late))
	copy(out, op.late)
	return out
}

// Snapshot 序列化算子状态（带版本与窗口大小，恢复时校验）。
func (op *WindowOperator) Snapshot() ([]byte, error) {
	st := windowStateJSON{
		Version: windowStateVersion,
		SizeMs:  op.size,
		WM:      op.wm,
		Agg:     map[string]map[int64]aggState{},
		Fired:   map[string][]int64{},
		Late:    op.LateRecords(),
	}
	for k, m := range op.agg {
		st.Agg[k] = map[int64]aggState{}
		for start, a := range m {
			st.Agg[k][start] = *a
		}
	}
	for k, m := range op.fired {
		for start, ok := range m {
			if ok {
				st.Fired[k] = append(st.Fired[k], start)
			}
		}
	}
	return json.Marshal(&st)
}

// Restore 恢复算子状态；版本不符或窗口大小不同返回 ErrCheckpointIncompat，
// JSON 损坏返回 ErrCheckpointCorrupt。绝不从半截状态继续运行。
func (op *WindowOperator) Restore(data []byte) error {
	var st windowStateJSON
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&st); err != nil {
		return fmt.Errorf("%w: window state: %v", ErrCheckpointCorrupt, err)
	}
	if st.Version != windowStateVersion {
		return fmt.Errorf("%w: window state version %d != %d", ErrCheckpointIncompat, st.Version, windowStateVersion)
	}
	if st.SizeMs != op.size {
		return fmt.Errorf("%w: window size %d != current %d", ErrCheckpointIncompat, st.SizeMs, op.size)
	}

	agg := map[string]map[int64]*aggState{}
	for k, m := range st.Agg {
		agg[k] = map[int64]*aggState{}
		for start, a := range m {
			aa := a
			agg[k][start] = &aa
		}
	}
	fired := map[string]map[int64]bool{}
	for k, starts := range st.Fired {
		fired[k] = map[int64]bool{}
		for _, s := range starts {
			fired[k][s] = true
		}
	}
	late := make([]LateRecord, len(st.Late))
	copy(late, st.Late)

	// 全部解析成功后才整体替换，避免失败时留下中间状态。
	op.agg = agg
	op.fired = fired
	op.late = late
	op.wm = st.WM
	return nil
}

// StateDigest 返回聚合+已触发状态的稳定摘要，供测试断言重复运行结果一致。
func (op *WindowOperator) StateDigest() string {
	var b bytes.Buffer
	keys := make([]string, 0, len(op.agg))
	for k := range op.agg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		starts := make([]int64, 0, len(op.agg[k]))
		for s := range op.agg[k] {
			starts = append(starts, s)
		}
		sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
		for _, s := range starts {
			a := op.agg[k][s]
			fmt.Fprintf(&b, "%s@%d=%d:%d;", k, s, a.Sum, a.Count)
		}
	}
	fkeys := make([]string, 0, len(op.fired))
	for k := range op.fired {
		fkeys = append(fkeys, k)
	}
	sort.Strings(fkeys)
	for _, k := range fkeys {
		starts := make([]int64, 0, len(op.fired[k]))
		for s := range op.fired[k] {
			starts = append(starts, s)
		}
		sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
		for _, s := range starts {
			fmt.Fprintf(&b, "fired:%s@%d;", k, s)
		}
	}
	return b.String()
}

// CurrentWM 返回算子上次收到的水位线（测试用）。
func (op *WindowOperator) CurrentWM() int64 { return op.wm }
