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

// MemSource 基于内存切片的可重放数据源。
type MemSource struct {
	records []Record
	pos     int
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

func (s *MemSource) Next() (Record, bool) {
	if s.pos >= len(s.records) {
		return Record{}, false
	}
	r := s.records[s.pos]
	s.pos++
	return r, true
}

func (s *MemSource) Offset() int64 { return int64(s.pos) }

func (s *MemSource) SeekOffset(offset int64) {
	if offset < 0 {
		offset = 0
	}
	if offset > int64(len(s.records)) {
		offset = int64(len(s.records))
	}
	s.pos = int(offset)
}
