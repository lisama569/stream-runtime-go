package stream

// Source 是可重放的本地数据源。
type Source interface {
	// Next 返回下一条记录；ok=false 表示数据耗尽。
	Next() (Record, bool)
	// Offset 返回下一条待读记录的位移。
	Offset() int64
	// SeekOffset 将读取位置重置到指定位移（用于从检查点恢复）。
	SeekOffset(offset int64)
}

// RangedSource 是可选接口：能报告当前有效数据位移范围 [min, max] 的数据源。
// 流水线恢复时若源实现了该接口，会校验检查点位移是否落在范围内，
// 落在范围外（数据被裁剪、回放区间变窄）则拒绝恢复。
type RangedSource interface {
	Source
	// OffsetRange 返回当前可消费的位移区间 [min, max]：
	// min 为最早保留的位移，max 为数据末尾（即消费完时的位移）。
	OffsetRange() (min, max int64)
}

// MemSource 基于内存切片的可重放数据源。
type MemSource struct {
	records []Record
	pos     int
	base    int64 // 切片首条记录对应的位移（裁剪后 > 0）
}

// NewMemSource 创建数据源，自动为每条记录分配递增位移。
func NewMemSource(records []Record) *MemSource {
	cp := make([]Record, len(records))
	for i, r := range records {
		r.Offset = int64(i)
		cp[i] = r
	}
	return &MemSource{records: cp}
}

// TrimBefore 裁掉位移小于 offset 的记录（模拟本地数据被裁剪、回放区间变窄）。
func (s *MemSource) TrimBefore(offset int64) {
	n := int(offset - s.base)
	if n < 0 {
		n = 0
	}
	if n > len(s.records) {
		n = len(s.records)
	}
	s.records = s.records[n:]
	s.base += int64(n)
	if s.pos > n {
		s.pos -= n
	} else {
		s.pos = 0
	}
}

// OffsetRange 实现 RangedSource：返回 [base, base+len]。
func (s *MemSource) OffsetRange() (int64, int64) {
	return s.base, s.base + int64(len(s.records))
}

func (s *MemSource) Next() (Record, bool) {
	if s.pos >= len(s.records) {
		return Record{}, false
	}
	r := s.records[s.pos]
	s.pos++
	return r, true
}

func (s *MemSource) Offset() int64 { return s.base + int64(s.pos) }

func (s *MemSource) SeekOffset(offset int64) {
	rel := offset - s.base
	if rel < 0 {
		rel = 0
	}
	if rel > int64(len(s.records)) {
		rel = int64(len(s.records))
	}
	s.pos = int(rel)
}
