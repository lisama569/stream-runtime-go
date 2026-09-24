package stream

// Event 是流经运行时的最小数据单元，携带业务事件时间（毫秒）与负载。
type Event struct {
	Key       string `json:"key"`
	EventTime int64  `json:"eventTime"` // 毫秒，epoch
	Value     int64  `json:"value"`
}

// Watermark 表示事件时间水位线，语义：事件时间 <= T 的记录理论上均已到达。
type Watermark struct {
	T int64 `json:"t"`
}

// WindowResult 是一个窗口聚合的最终输出。
type WindowResult struct {
	Key       string `json:"key"`
	WindowID  int64  `json:"windowId"`  // 窗口起始时间（含）
	WindowEnd int64  `json:"windowEnd"` // 窗口结束时间（不含）
	Sum       int64  `json:"sum"`
	Count     int64  `json:"count"`
}

// LateRecord 描述一条在水位线之后到达、被判定为迟到的记录及其处置方式。
type LateRecord struct {
	Event     Event  `json:"event"`
	Watermark int64  `json:"watermark"`
	Reason    string `json:"reason"`
}

// Logger 是运行时使用的最小日志接口，测试中用它捕获水位线/检查点/记录流水。
type Logger interface {
	Logf(format string, args ...any)
}
