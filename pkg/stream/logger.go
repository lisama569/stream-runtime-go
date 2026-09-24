package stream

import (
	"fmt"
	"io"
	"sync"
	"time"
)

type logCore struct {
	mu    sync.Mutex
	w     io.Writer
	lines []string
}

// TextLogger 将日志写入给定 io.Writer，同时在内存保留副本供测试断言。
// 日志统一带毫秒时间戳，便于测试报告中按顺序复盘水位线、检查点与记录流水。
type TextLogger struct {
	core *logCore
	tag  string
}

// NewTextLogger 创建日志器；w 为 nil 时只保留内存副本。
func NewTextLogger(w io.Writer) *TextLogger {
	return &TextLogger{core: &logCore{w: w}}
}

// WithTag 返回带固定前缀的派生日志器，共享同一份底层输出与缓冲。
func (l *TextLogger) WithTag(tag string) *TextLogger {
	return &TextLogger{core: l.core, tag: tag}
}

// Logf 实现 Logger：线程安全，带时间戳。
func (l *TextLogger) Logf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	ts := time.Now().UTC().Format("15:04:05.000")
	prefix := ""
	if l.tag != "" {
		prefix = "[" + l.tag + "] "
	}
	line := fmt.Sprintf("%s %s%s", ts, prefix, msg)
	l.core.mu.Lock()
	defer l.core.mu.Unlock()
	l.core.lines = append(l.core.lines, line)
	if l.core.w != nil {
		fmt.Fprintln(l.core.w, line)
	}
}

// Lines 返回内存中的日志副本（快照）。
func (l *TextLogger) Lines() []string {
	l.core.mu.Lock()
	defer l.core.mu.Unlock()
	out := make([]string, len(l.core.lines))
	copy(out, l.core.lines)
	return out
}

var _ Logger = (*TextLogger)(nil)
