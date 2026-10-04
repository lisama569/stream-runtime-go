package stream

import "errors"

// 可区分的错误类别。
var (
	// ErrCheckpointCorrupt 检查点文件损坏（校验和不匹配或无法解析）。
	ErrCheckpointCorrupt = errors.New("stream: checkpoint corrupt")
	// ErrCheckpointIncompatible 检查点与当前拓扑不兼容。
	ErrCheckpointIncompatible = errors.New("stream: checkpoint incompatible with topology")
	// ErrCheckpointExpired 检查点已过期（超过保留时间）。
	ErrCheckpointExpired = errors.New("stream: checkpoint expired")
	// ErrCheckpointVersionMismatch 检查点来自更早的状态结构（格式版本不一致）。
	ErrCheckpointVersionMismatch = errors.New("stream: checkpoint state format version mismatch")
	// ErrCheckpointConfigMismatch 检查点记录的运行配置与当前实际生效配置不一致。
	ErrCheckpointConfigMismatch = errors.New("stream: checkpoint runtime config mismatch")
	// ErrCheckpointOffsetOutOfRange 检查点记录的消费位置超出当前数据范围。
	ErrCheckpointOffsetOutOfRange = errors.New("stream: checkpoint source offset out of range")
	// ErrSinkRetryExhausted 下游写入重试额度用完，本次运行失败。
	ErrSinkRetryExhausted = errors.New("stream: sink write retries exhausted")
	// ErrOperatorFailed 单个算子执行失败。
	ErrOperatorFailed = errors.New("stream: operator failed")
	// ErrBackpressureTimeout 下游背压超时。
	ErrBackpressureTimeout = errors.New("stream: backpressure timeout")
	// ErrPipelineClosed 流水线已关闭。
	ErrPipelineClosed = errors.New("stream: pipeline closed")
)

// OpError 携带可定位上下文的算子错误。
type OpError struct {
	Op     string // 算子名称
	Offset int64  // 失败时处理的记录位移
	Kind   error  // 错误类别（可用 errors.Is 判定）
	Msg    string
}

func (e *OpError) Error() string {
	return e.Kind.Error() + ": op=" + e.Op + " offset=" + itoa(e.Offset) + ": " + e.Msg
}

func (e *OpError) Unwrap() error { return e.Kind }

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
