package stream

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// CheckpointFormatVersion 检查点格式版本，结构变更时递增。
const CheckpointFormatVersion = 1

// Checkpoint 是一次完整的状态快照。
type Checkpoint struct {
	Version      int64                  `json:"version"`      // 格式版本
	TopologyHash string                 `json:"topologyHash"` // 拓扑指纹，不兼容则拒绝恢复
	CreatedAt    int64                  `json:"createdAt"`    // 创建时间（Unix 秒）
	SourceOffset int64                  `json:"sourceOffset"` // 已消费到的源位移
	Watermark    int64                  `json:"watermark"`    // 快照时水位线
	MaxSeen      int64                  `json:"maxSeen"`      // 已见最大事件时间
	Windows      map[string]windowState `json:"windows"`      // 窗口中间状态
	Checksum     uint32                 `json:"checksum"`     // 内容校验和
}

// CheckpointStore 持久化与加载检查点。
type CheckpointStore struct {
	dir      string
	maxAge   time.Duration
	topology string
	now      func() time.Time // 可注入时钟，便于测试过期逻辑
}

// NewCheckpointStore 创建基于本地目录的检查点存储。
// topologyHash 标识当前拓扑；maxAge 为检查点保留时长，超时视为过期。
func NewCheckpointStore(dir string, topologyHash string, maxAge time.Duration) *CheckpointStore {
	return &CheckpointStore{dir: dir, maxAge: maxAge, topology: topologyHash, now: time.Now}
}

func (s *CheckpointStore) path() string { return filepath.Join(s.dir, "checkpoint.json") }

// Save 原子写入检查点（先写临时文件再重命名）。
func (s *CheckpointStore) Save(cp *Checkpoint) error {
	cp.Version = CheckpointFormatVersion
	cp.TopologyHash = s.topology
	cp.CreatedAt = s.now().Unix()
	cp.Checksum = 0
	body, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	cp.Checksum = crc32.ChecksumIEEE(body)
	full, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	tmp := s.path() + ".tmp"
	if err := os.WriteFile(tmp, full, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path())
}

// Load 加载并校验检查点。
// 校验失败返回可区分错误：过期 ErrCheckpointExpired、
// 损坏 ErrCheckpointCorrupt、不兼容 ErrCheckpointIncompatible
// （可进一步区分为旧格式 ErrCheckpointLegacyFormat 与
// 配置不一致 ErrCheckpointConfigMismatch）。
// 无检查点时返回 (nil, nil)。
func (s *CheckpointStore) Load() (*Checkpoint, error) {
	data, err := os.ReadFile(s.path())
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, errors.Join(ErrCheckpointCorrupt, err)
	}
	// 校验和：将 Checksum 置零后重新计算比对。
	got := cp.Checksum
	cp.Checksum = 0
	body, err := json.Marshal(&cp)
	if err != nil {
		return nil, errors.Join(ErrCheckpointCorrupt, err)
	}
	if crc32.ChecksumIEEE(body) != got {
		return nil, errors.Join(ErrCheckpointCorrupt, errors.New("checksum mismatch"))
	}
	cp.Checksum = got
	// 版本兼容性：来自更早（或更新）状态结构的检查点单独归类。
	if cp.Version != CheckpointFormatVersion {
		return nil, errors.Join(ErrCheckpointIncompatible, ErrCheckpointLegacyFormat,
			fmt.Errorf("checkpoint format version %d, runtime expects %d", cp.Version, CheckpointFormatVersion))
	}
	// 运行配置兼容性：拓扑指纹（算子名/窗口大小/允许乱序度）不一致。
	if cp.TopologyHash != s.topology {
		return nil, errors.Join(ErrCheckpointIncompatible, ErrCheckpointConfigMismatch,
			fmt.Errorf("checkpoint config %q, current %q", cp.TopologyHash, s.topology))
	}
	// 过期判定。
	if s.maxAge > 0 && s.now().Unix() > cp.CreatedAt+int64(s.maxAge.Seconds()) {
		return nil, ErrCheckpointExpired
	}
	return &cp, nil
}
