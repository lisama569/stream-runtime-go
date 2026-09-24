package stream

// WindowAssigner 将事件时间分配到固定大小的滚动窗口。
type WindowAssigner struct {
	Size int64 // 窗口大小（毫秒）
}

// NewWindowAssigner 创建滚动窗口分配器。
func NewWindowAssigner(size int64) *WindowAssigner {
	return &WindowAssigner{Size: size}
}

// Assign 返回事件时间所属的窗口 [floor(ts/size)*size, +size)。
func (a *WindowAssigner) Assign(eventTime int64) Window {
	start := eventTime - mod(eventTime, a.Size)
	return Window{Start: start, End: start + a.Size}
}

func mod(v, m int64) int64 {
	r := v % m
	if r < 0 {
		r += m
	}
	return r
}

// windowState 保存单个 (window, key) 的中间聚合值。
type windowState struct {
	Sum   int64 `json:"sum"`
	Count int64 `json:"count"`
}

// windowKey 是窗口状态表的键。
type windowKey struct {
	Start int64
	End   int64
	Key   string
}
