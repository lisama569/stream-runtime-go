// Package stream 提供本地可运行的实时流处理运行时。
package stream

// Record 是一条输入事件。
type Record struct {
	Offset    int64  // 在数据源中的位移，用于恢复后重放
	Key       string // 分组键
	Value     int64  // 聚合值
	EventTime int64  // 事件时间（毫秒）
}

// Window 表示一个按事件时间划分的滚动窗口 [Start, End)。
type Window struct {
	Start int64
	End   int64
}

// Contains 判断事件时间是否属于该窗口。
func (w Window) Contains(ts int64) bool { return ts >= w.Start && ts < w.End }

// WindowResult 是一个窗口的最终聚合输出。
type WindowResult struct {
	Window Window
	Key    string
	Sum    int64
	Count  int64
}

// LateRecord 记录一条被水位线拒绝的迟到数据。
type LateRecord struct {
	Record    Record
	Watermark int64
	Reason    string
}
