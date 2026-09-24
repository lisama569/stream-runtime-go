package stream

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"
)

// checkpointFormatVersion 是检查点外层格式版本。
const checkpointFormatVersion = 1

// Checkpoint 是一次一致性快照：源偏移、水位线状态、窗口算子状态、下游状态。
// 持久化时带 CRC32 校验和、格式版本与拓扑指纹，恢复时逐项严格校验，
// 任一不符都明确拒绝并返回可区分原因，绝不从半截状态继续运行。
type Checkpoint struct {
	Version      int    `json:"version"`
	TopologyID   string `json:"topologyId"`
	CreatedAtMs  int64  `json:"createdAtMs"`
	SourceOffset int64  `json:"sourceOffset"`
	WMMaxSeen    int64  `json:"wmMaxSeen"`
	WMCurrent    int64  `json:"wmCurrent"`
	WindowState  []byte `json:"windowState"`
	SinkState    []byte `json:"sinkState"`

	// Checksum 覆盖除自身外的所有字段（序列化时临时置空再计算）。
	Checksum uint32 `json:"checksum"`
}

func (c *Checkpoint) computeChecksum() uint32 {
	clone := *c
	clone.Checksum = 0
	var b bytes.Buffer
	// 字段拼接 + JSON 编码嵌套状态，编码确定、无随机成分。
	fmt.Fprintf(&b, "v=%d|topo=%s|ts=%d|off=%d|mx=%d|wm=%d",
		clone.Version, clone.TopologyID, clone.CreatedAtMs,
		clone.SourceOffset, clone.WMMaxSeen, clone.WMCurrent)
	h := crc32.NewIEEE()
	h.Write(b.Bytes())
	h.Write(clone.WindowState)
	h.Write(clone.SinkState)
	return h.Sum32()
}

// Verify 校验检查点本身的完整性（校验和）。
func (c *Checkpoint) Verify() error {
	if c.Version != checkpointFormatVersion {
		return fmt.Errorf("%w: checkpoint format version %d != %d",
			ErrCheckpointIncompat, c.Version, checkpointFormatVersion)
	}
	if c.computeChecksum() != c.Checksum {
		return fmt.Errorf("%w: CRC32 校验和不匹配（文件可能被截断/篡改）", ErrCheckpointCorrupt)
	}
	return nil
}

// CheckpointStore 负责检查点的原子写入与加载（本地单文件）。
type CheckpointStore struct {
	path string
	ttl  time.Duration
	now  func() time.Time
}

// NewCheckpointStore 在 path 上读写检查点；ttl<=0 表示不过期。
func NewCheckpointStore(path string, ttl time.Duration) *CheckpointStore {
	return &CheckpointStore{path: path, ttl: ttl, now: time.Now}
}

// Path 返回检查点文件路径。
func (st *CheckpointStore) Path() string { return st.path }

// Exists 报告检查点文件是否存在。
func (st *CheckpointStore) Exists() (bool, error) {
	_, err := os.Stat(st.path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Save 原子写入：序列化→算校验和→写临时文件→fsync→rename，
// 保证任意时刻读到的要么是上一个完整检查点，要么是新完整检查点，不会读到半成品。
func (st *CheckpointStore) Save(cp *Checkpoint) error {
	if cp.Version == 0 {
		cp.Version = checkpointFormatVersion
	}
	if cp.CreatedAtMs == 0 {
		cp.CreatedAtMs = st.now().UnixMilli()
	}
	cp.Checksum = cp.computeChecksum()
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	dir := filepath.Dir(st.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}
	tmp := st.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open temp checkpoint: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write temp checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("fsync checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close checkpoint: %w", err)
	}
	if err := os.Rename(tmp, st.path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename checkpoint: %w", err)
	}
	return nil
}

// Load 加载并按固定顺序校验：
//
//	不存在 → ErrCheckpointMissing
//	JSON 损坏/截断 → ErrCheckpointCorrupt
//	格式版本不符 → ErrCheckpointIncompat
//	CRC 校验和不符 → ErrCheckpointCorrupt
//	拓扑指纹不符 → ErrCheckpointIncompat
//	超过 TTL → ErrCheckpointExpired
//
// 任何失败都不返回可用状态，调用方不得从中间状态继续运行。
func (st *CheckpointStore) Load(expectedTopology string) (*Checkpoint, error) {
	data, err := os.ReadFile(st.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrCheckpointMissing
		}
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("%w: JSON 解析失败: %v", ErrCheckpointCorrupt, err)
	}
	if cp.Version != checkpointFormatVersion {
		return nil, fmt.Errorf("%w: checkpoint format version %d != %d",
			ErrCheckpointIncompat, cp.Version, checkpointFormatVersion)
	}
	if cp.computeChecksum() != cp.Checksum {
		return nil, fmt.Errorf("%w: CRC32 校验和不匹配（文件被截断/篡改）", ErrCheckpointCorrupt)
	}
	if expectedTopology != "" && cp.TopologyID != expectedTopology {
		return nil, fmt.Errorf("%w: checkpoint topology=%q current topology=%q",
			ErrCheckpointIncompat, cp.TopologyID, expectedTopology)
	}
	if st.ttl > 0 && cp.CreatedAtMs > 0 {
		age := st.now().Sub(time.UnixMilli(cp.CreatedAtMs))
		if age > st.ttl {
			return nil, fmt.Errorf("%w: 年龄 %s 超过 TTL %s", ErrCheckpointExpired, age, st.ttl)
		}
	}
	return &cp, nil
}

// Clear 删除检查点文件（不存在不报错）。
func (st *CheckpointStore) Clear() error {
	if err := os.Remove(st.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
