package stream

// Source 是可重放的本地数据源。
type Source interface {
	// Next 返回下一条记录；ok=false 表示数据耗尽。
	Next() (Record, bool)
	// Offset 返回下一条待读记录的位移。
	Offset() int64
	// SeekOffset 将读取位置重置到指定位移（用于从检查点恢复）。
	SeekOffset(offset int64)
	// Bounds 返回当前数据的有效位移范围 [min, max]。
	// 检查点记录的位移超出该范围时必须拒绝恢复，不能悄悄截断续读。
	Bounds() (min, max int64)
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

// Bounds 返回有效位移范围 [0, len(records)]；max 为“全部读完”的位置。
func (s *MemSource) Bounds() (min, max int64) { return 0, int64(len(s.records)) }

func (s *MemSource) SeekOffset(offset int64) {
	if offset < 0 {
		offset = 0
	}
	if offset > int64(len(s.records)) {
		offset = int64(len(s.records))
	}
	s.pos = int(offset)
}
