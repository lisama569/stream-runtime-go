package stream

import (
	"errors"
	"fmt"
)

// 运行时错误类别（哨兵错误）。调用方可用 errors.Is 区分失败的确定原因；
// 用 errors.As(*OperatorError) 取得可定位上下文（算子、记录、根因）。
var (
	// ErrCheckpointExpired 检查点超过保留 TTL，属于过期快照，拒绝恢复。
	ErrCheckpointExpired = errors.New("checkpoint expired: 检查点已超过保留的 TTL")
	// ErrCheckpointCorrupt 检查点字节损坏、截断或校验和不符，拒绝恢复。
	ErrCheckpointCorrupt = errors.New("checkpoint corrupt: 内容损坏或校验失败")
	// ErrCheckpointIncompat 检查点版本或拓扑指纹与当前运行拓扑不一致，拒绝恢复。
	ErrCheckpointIncompat = errors.New("checkpoint incompatible: 与当前拓扑不兼容")
	// ErrCheckpointMissing 指定路径上不存在任何检查点（首次启动属正常情况）。
	ErrCheckpointMissing = errors.New("checkpoint missing: 指定路径不存在检查点")
	// ErrBackpressure 下游在限定等待时间内未能腾出容量，背压溢出。
	ErrBackpressure = errors.New("backpressure: 下游处理过慢，通道容量耗尽")
	// ErrOperatorFailed 某个算子处理记录时失败（被隔离到单条记录，不拖垮链路）。
	ErrOperatorFailed = errors.New("operator failed: 算子执行失败")
)

// OperatorError 保留可定位上下文：算子名、触发失败的输入记录、根因。
type OperatorError struct {
	Operator string
	Record   *Event
	Inner    error
}

func (e *OperatorError) Error() string {
	if e.Record != nil {
		return fmt.Sprintf("operator %q failed on record(key=%s,eventTime=%d,value=%d): %v",
			e.Operator, e.Record.Key, e.Record.EventTime, e.Record.Value, e.Inner)
	}
	return fmt.Sprintf("operator %q failed: %v", e.Operator, e.Inner)
}

func (e *OperatorError) Unwrap() error { return e.Inner }

// NewOperatorError 构造一个归类为 ErrOperatorFailed 的算子错误。
func NewOperatorError(operator string, rec *Event, inner error) *OperatorError {
	if inner == nil {
		inner = ErrOperatorFailed
	}
	return &OperatorError{Operator: operator, Record: rec, Inner: inner}
}

// SinkError 描述下游处理失败，归类为 ErrOperatorFailed 并携带失败的输出记录。
type SinkError struct {
	Result WindowResult
	Inner  error
}

func (e *SinkError) Error() string {
	return fmt.Sprintf("sink failed on result(key=%s,window=%d): %v",
		e.Result.Key, e.Result.WindowID, e.Inner)
}

func (e *SinkError) Unwrap() error { return e.Inner }
