package stream

import (
	"context"
	"fmt"
)

// LocalSource 是确定性的本地数据源：固定一批事件，按下标顺序回放。
// offset 指向下一条待读记录下标，随检查点持久化；恢复时 SeekOffset 回到
// 检查点位置，因此恢复后既不会重复读、也不会漏读。
type LocalSource struct {
	events []Event
	offset int64
}

// NewLocalSource 基于固定的一批事件构造数据源（拷贝入参，避免外部修改）。
func NewLocalSource(events []Event) *LocalSource {
	cp := make([]Event, len(events))
	copy(cp, events)
	return &LocalSource{events: cp}
}

// Poll 返回下一条待读记录。
// ok=true  且 advance=true：返回记录并把 offset 前移一条（正常读取）。
// ok=false：所有记录已回放完毕。
// 为了让调用方在“读取成功”和“检查点提交”之间保持明确边界，这里读取即前移；
// 检查点保存的是“已读 offset”，恢复后从该 offset 继续（at-least-once 读取 +
// 下游去重共同保证结果 exactly-once）。
func (s *LocalSource) Poll(ctx context.Context) (e Event, ok bool, err error) {
	if err := ctx.Err(); err != nil {
		return Event{}, false, err
	}
	if s.offset >= int64(len(s.events)) {
		return Event{}, false, nil
	}
	e = s.events[s.offset]
	s.offset++
	return e, true, nil
}

// Peek 在不前移偏移量的情况下查看下一条记录（测试用）。
func (s *LocalSource) Peek() (Event, bool) {
	if s.offset >= int64(len(s.events)) {
		return Event{}, false
	}
	return s.events[s.offset], true
}

// Offset 返回下一条待读记录的下标（=已读条数），用于检查点。
func (s *LocalSource) Offset() int64 { return s.offset }

// SeekOffset 恢复到检查点记录的偏移量；越界明确报错，拒绝带着非法中间状态运行。
func (s *LocalSource) SeekOffset(offset int64) error {
	if offset < 0 || offset > int64(len(s.events)) {
		return fmt.Errorf("source offset %d out of range [0,%d]", offset, len(s.events))
	}
	s.offset = offset
	return nil
}

// Len 返回数据源记录总数（测试用）。
func (s *LocalSource) Len() int { return len(s.events) }
