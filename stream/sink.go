package stream

import "sync"

// Sink 接收窗口结果，写入必须是幂等的（按窗口+键去重）。
type Sink interface {
	Write(result WindowResult) error
}

// MemSink 内存结果表，按 (window, key) 幂等覆盖写，天然去重。
type MemSink struct {
	mu      sync.Mutex
	results map[windowKey]WindowResult
	writes  int // 总写入次数（含重复覆盖），用于测试观察
}

// NewMemSink 创建内存结果表。
func NewMemSink() *MemSink {
	return &MemSink{results: make(map[windowKey]WindowResult)}
}

// Write 幂等写入：同一 (window, key) 重复写结果一致，不产生重复记录。
func (s *MemSink) Write(r WindowResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[windowKey{Start: r.Window.Start, End: r.Window.End, Key: r.Key}] = r
	s.writes++
	return nil
}

// Results 返回当前所有窗口结果的快照。
func (s *MemSink) Results() []WindowResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]WindowResult, 0, len(s.results))
	for _, r := range s.results {
		out = append(out, r)
	}
	return out
}

// Writes 返回累计写入次数。
func (s *MemSink) Writes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}
