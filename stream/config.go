package stream

import "fmt"

// RuntimeConfig 影响状态语义的运行参数。
// 任一字段变化都会改变窗口划分或水位线推进，旧检查点必须拒绝恢复。
// 注意：MaxInFlight、SendTimeout 等背压参数不影响状态语义，不在此列，
// 调整它们不会使旧检查点失效。
type RuntimeConfig struct {
	OperatorName    string `json:"operatorName"`    // 算子名
	WindowSize      int64  `json:"windowSize"`      // 窗口大小（毫秒）
	AllowedLateness int64  `json:"allowedLateness"` // 允许乱序度（毫秒）
}

// Fingerprint 配置指纹：任一字段变化指纹即变，用于检查点兼容判定。
func (c RuntimeConfig) Fingerprint() string {
	return fmt.Sprintf("%s/w%d/l%d", c.OperatorName, c.WindowSize, c.AllowedLateness)
}

// String 便于日志与错误信息中展示配置内容。
func (c RuntimeConfig) String() string {
	return fmt.Sprintf("operator=%s window=%d lateness=%d",
		c.OperatorName, c.WindowSize, c.AllowedLateness)
}
