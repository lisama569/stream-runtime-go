package stream

import (
	"time"
)

// CheckpointFormatVersion 检查点格式版本，结构变更时递增。
const CheckpointFormatVersion = 1

// Checkpoint 是一次完整的状态快照。
type Checkpoint struct {
	Version      int64                  `json:"version"`       // 格式版本
	TopologyHash string                 `json:"topologyHash"`  // 拓扑指纹，不兼容则拒绝恢复
	CreatedAt    int64                  `json:"createdAt"`     // 创建时间（Unix 秒）
	SourceOffset int64                  `json:"sourceOffset"`  // 已消费到的源位移
	Watermark    int64                  `json:"watermark"`     // 快照时水位线
	MaxSeen      int64                  `json:"maxSeen"`       // 已见最大事件时间
	Windows      map[string]windowState `json:"windows"`       // 窗口中间状态
	Checksum     uint32                 `json:"checksum"`      // 内容校验和
}

// CheckpointStore 持久化与加载检查点。
type CheckpointStore struct {
	dir      string
	maxAge   time.Duration
	topology string
}

// NewCheckpointStore 创建基于本地目录的检查点存储。
// topologyHash 标识当前拓扑；maxAge 为检查点保留时长，超时视为过期。
func NewCheckpointStore(dir string, topologyHash string, maxAge time.Duration) *CheckpointStore {
	return &CheckpointStore{dir: dir, maxAge: maxAge, topology: topologyHash}
}

// Save 原子写入检查点（先写临时文件再重命名）。
func (s *CheckpointStore) Save(cp *Checkpoint) error {
	return nil // 骨架：待实现
}

// Load 加载并校验检查点。
// 校验失败返回可区分错误：过期 ErrCheckpointExpired、
// 损坏 ErrCheckpointCorrupt、拓扑不兼容 ErrCheckpointIncompatible。
// 无检查点时返回 (nil, nil)。
func (s *CheckpointStore) Load() (*Checkpoint, error) {
	return nil, nil // 骨架：待实现
}
