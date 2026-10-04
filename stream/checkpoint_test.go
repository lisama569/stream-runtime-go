package stream

import (
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testCheckpoint() *Checkpoint {
	return &Checkpoint{
		SourceOffset: 7,
		Watermark:    950,
		MaxSeen:      1000,
		Windows: map[string]windowState{
			"0,100,a":   {Sum: 3, Count: 2},
			"100,200,b": {Sum: 5, Count: 1},
		},
	}
}

func TestCheckpointSaveLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	store := NewCheckpointStore(dir, "topo-1", time.Hour)
	if err := store.Save(testCheckpoint()); err != nil {
		t.Fatal(err)
	}
	cp, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cp.SourceOffset != 7 || cp.Watermark != 950 || cp.MaxSeen != 1000 {
		t.Fatalf("roundtrip mismatch: %+v", cp)
	}
	if cp.Windows["0,100,a"].Sum != 3 || cp.Windows["100,200,b"].Count != 1 {
		t.Fatalf("window state mismatch: %+v", cp.Windows)
	}
	t.Logf("[test] checkpoint roundtrip ok offset=%d watermark=%d windows=%d",
		cp.SourceOffset, cp.Watermark, len(cp.Windows))
}

func TestCheckpointMissingReturnsNil(t *testing.T) {
	store := NewCheckpointStore(t.TempDir(), "topo-1", time.Hour)
	cp, err := store.Load()
	if err != nil || cp != nil {
		t.Fatalf("expected (nil, nil), got (%v, %v)", cp, err)
	}
}

func TestCheckpointCorruptRejected(t *testing.T) {
	dir := t.TempDir()
	store := NewCheckpointStore(dir, "topo-1", time.Hour)
	if err := store.Save(testCheckpoint()); err != nil {
		t.Fatal(err)
	}
	// 篡改文件内容（破坏校验和）。
	p := filepath.Join(dir, "checkpoint.json")
	data, _ := os.ReadFile(p)
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := store.Load()
	if !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("expected ErrCheckpointCorrupt, got %v", err)
	}
	t.Logf("[test] corrupt checkpoint rejected: %v", err)
}

func TestCheckpointGarbageRejected(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), []byte("not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := NewCheckpointStore(dir, "topo-1", time.Hour)
	if _, err := store.Load(); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("expected ErrCheckpointCorrupt, got %v", err)
	}
}

func TestCheckpointIncompatibleTopologyRejected(t *testing.T) {
	dir := t.TempDir()
	// 用拓扑 A 保存，用拓扑 B 加载。
	if err := NewCheckpointStore(dir, "topo-A", time.Hour).Save(testCheckpoint()); err != nil {
		t.Fatal(err)
	}
	_, err := NewCheckpointStore(dir, "topo-B", time.Hour).Load()
	if !errors.Is(err, ErrCheckpointIncompatible) {
		t.Fatalf("expected ErrCheckpointIncompatible, got %v", err)
	}
	t.Logf("[test] incompatible checkpoint rejected: %v", err)
}

func TestCheckpointExpiredRejected(t *testing.T) {
	dir := t.TempDir()
	store := NewCheckpointStore(dir, "topo-1", time.Minute)
	// 注入时钟：保存时为 T0，加载时已超过保留期。
	base := time.Now()
	store.now = func() time.Time { return base }
	if err := store.Save(testCheckpoint()); err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return base.Add(2 * time.Minute) }
	_, err := store.Load()
	if !errors.Is(err, ErrCheckpointExpired) {
		t.Fatalf("expected ErrCheckpointExpired, got %v", err)
	}
	t.Logf("[test] expired checkpoint rejected: %v", err)
}

// 三类非法检查点必须互相可区分。
func TestCheckpointErrorsAreDistinguishable(t *testing.T) {
	dir := t.TempDir()
	if err := NewCheckpointStore(dir, "topo-A", time.Minute).Save(testCheckpoint()); err != nil {
		t.Fatal(err)
	}
	_, errCompat := NewCheckpointStore(dir, "topo-B", time.Minute).Load()
	if errors.Is(errCompat, ErrCheckpointCorrupt) || errors.Is(errCompat, ErrCheckpointExpired) {
		t.Fatalf("incompatible misclassified: %v", errCompat)
	}
	p := filepath.Join(dir, "checkpoint.json")
	data, _ := os.ReadFile(p)
	data[10] ^= 0xFF
	_ = os.WriteFile(p, data, 0o644)
	_, errCorrupt := NewCheckpointStore(dir, "topo-A", time.Minute).Load()
	if !errors.Is(errCorrupt, ErrCheckpointCorrupt) ||
		errors.Is(errCorrupt, ErrCheckpointIncompatible) ||
		errors.Is(errCorrupt, ErrCheckpointExpired) {
		t.Fatalf("corrupt misclassified: %v", errCorrupt)
	}
}

func testRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{OperatorName: "win", WindowSize: 100, AllowedLateness: 50}
}

// 配置变化（窗口大小、允许乱序度）后的检查点必须被拒绝，
// 且与损坏、版本过旧可区分。
func TestCheckpointConfigMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	cfg := testRuntimeConfig()
	if err := NewCheckpointStoreWithConfig(dir, cfg, time.Hour).Save(testCheckpoint()); err != nil {
		t.Fatal(err)
	}
	// 相同配置可以正常加载（往返一致）。
	cp, err := NewCheckpointStoreWithConfig(dir, cfg, time.Hour).Load()
	if err != nil || cp == nil {
		t.Fatalf("same config should load, got cp=%v err=%v", cp, err)
	}
	if cp.Config != cfg {
		t.Fatalf("checkpoint should record effective config, got %+v", cp.Config)
	}
	// 调整窗口大小 -> 拒绝。
	changed := cfg
	changed.WindowSize = 200
	_, err = NewCheckpointStoreWithConfig(dir, changed, time.Hour).Load()
	if !errors.Is(err, ErrCheckpointConfigMismatch) {
		t.Fatalf("expected ErrCheckpointConfigMismatch, got %v", err)
	}
	if errors.Is(err, ErrCheckpointCorrupt) || errors.Is(err, ErrCheckpointVersionMismatch) {
		t.Fatalf("config mismatch misclassified: %v", err)
	}
	// 调整允许乱序度 -> 同样拒绝。
	changed = cfg
	changed.AllowedLateness = 10
	if _, err = NewCheckpointStoreWithConfig(dir, changed, time.Hour).Load(); !errors.Is(err, ErrCheckpointConfigMismatch) {
		t.Fatalf("lateness change expected ErrCheckpointConfigMismatch, got %v", err)
	}
	t.Logf("[test] config mismatch rejected and distinguishable: %v", err)
}

// 来自更早状态结构（格式版本过旧）的检查点必须单独归类拒绝。
func TestCheckpointVersionMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	store := NewCheckpointStoreWithConfig(dir, testRuntimeConfig(), time.Hour)
	if err := store.Save(testCheckpoint()); err != nil {
		t.Fatal(err)
	}
	// 把文件改写成旧版本号并重算校验和（内容本身完好）。
	p := filepath.Join(dir, "checkpoint.json")
	data, _ := os.ReadFile(p)
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		t.Fatal(err)
	}
	cp.Version = CheckpointFormatVersion - 1
	cp.Checksum = 0
	body, _ := json.Marshal(&cp)
	cp.Checksum = crc32.ChecksumIEEE(body)
	full, _ := json.Marshal(&cp)
	if err := os.WriteFile(p, full, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := store.Load()
	if !errors.Is(err, ErrCheckpointVersionMismatch) {
		t.Fatalf("expected ErrCheckpointVersionMismatch, got %v", err)
	}
	if errors.Is(err, ErrCheckpointCorrupt) || errors.Is(err, ErrCheckpointConfigMismatch) {
		t.Fatalf("version mismatch misclassified: %v", err)
	}
	t.Logf("[test] old-format checkpoint rejected: %v", err)
}

// 损坏、版本过旧、配置不匹配三类必须两两可区分。
func TestCheckpointNewErrorsAreDistinguishable(t *testing.T) {
	dir := t.TempDir()
	cfg := testRuntimeConfig()
	if err := NewCheckpointStoreWithConfig(dir, cfg, time.Hour).Save(testCheckpoint()); err != nil {
		t.Fatal(err)
	}
	// 配置不匹配：不是损坏、不是版本过旧。
	changed := cfg
	changed.WindowSize = 500
	_, errCfg := NewCheckpointStoreWithConfig(dir, changed, time.Hour).Load()
	if !errors.Is(errCfg, ErrCheckpointConfigMismatch) ||
		errors.Is(errCfg, ErrCheckpointCorrupt) ||
		errors.Is(errCfg, ErrCheckpointVersionMismatch) {
		t.Fatalf("config mismatch misclassified: %v", errCfg)
	}
	// 损坏：不是配置不匹配、不是版本过旧。
	p := filepath.Join(dir, "checkpoint.json")
	data, _ := os.ReadFile(p)
	data[10] ^= 0xFF
	_ = os.WriteFile(p, data, 0o644)
	_, errCorrupt := NewCheckpointStoreWithConfig(dir, cfg, time.Hour).Load()
	if !errors.Is(errCorrupt, ErrCheckpointCorrupt) ||
		errors.Is(errCorrupt, ErrCheckpointConfigMismatch) ||
		errors.Is(errCorrupt, ErrCheckpointVersionMismatch) {
		t.Fatalf("corrupt misclassified: %v", errCorrupt)
	}
}
