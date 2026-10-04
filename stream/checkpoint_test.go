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

// 来自更早状态结构的检查点（格式版本不一致）单独归类为 ErrCheckpointLegacyFormat，
// 且与损坏、配置不一致可区分。
func TestCheckpointLegacyFormatRejected(t *testing.T) {
	dir := t.TempDir()
	// 手工写入一个版本号为 0、校验和正确的“老格式”检查点。
	cp := Checkpoint{
		Version:      0,
		TopologyHash: "topo-1",
		CreatedAt:    time.Now().Unix(),
		SourceOffset: 3,
		Windows:      map[string]windowState{},
	}
	body, err := json.Marshal(&cp)
	if err != nil {
		t.Fatal(err)
	}
	cp.Checksum = crc32.ChecksumIEEE(body)
	full, err := json.Marshal(&cp)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), full, 0o644); err != nil {
		t.Fatal(err)
	}
	_, lerr := NewCheckpointStore(dir, "topo-1", time.Hour).Load()
	if !errors.Is(lerr, ErrCheckpointLegacyFormat) {
		t.Fatalf("expected ErrCheckpointLegacyFormat, got %v", lerr)
	}
	if !errors.Is(lerr, ErrCheckpointIncompatible) {
		t.Fatalf("legacy format should also be ErrCheckpointIncompatible, got %v", lerr)
	}
	if errors.Is(lerr, ErrCheckpointCorrupt) || errors.Is(lerr, ErrCheckpointConfigMismatch) {
		t.Fatalf("legacy format misclassified: %v", lerr)
	}
	t.Logf("[test] legacy checkpoint rejected: %v", lerr)
}

// 配置不一致（拓扑指纹不同）单独归类为 ErrCheckpointConfigMismatch。
func TestCheckpointConfigMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	// 用窗口 100/乱序 50 的配置保存，用窗口 200 的配置加载。
	if err := NewCheckpointStore(dir, TopologyHash("win", 100, 50), time.Hour).Save(testCheckpoint()); err != nil {
		t.Fatal(err)
	}
	_, err := NewCheckpointStore(dir, TopologyHash("win", 200, 50), time.Hour).Load()
	if !errors.Is(err, ErrCheckpointConfigMismatch) {
		t.Fatalf("expected ErrCheckpointConfigMismatch, got %v", err)
	}
	if !errors.Is(err, ErrCheckpointIncompatible) {
		t.Fatalf("config mismatch should also be ErrCheckpointIncompatible, got %v", err)
	}
	if errors.Is(err, ErrCheckpointCorrupt) || errors.Is(err, ErrCheckpointLegacyFormat) {
		t.Fatalf("config mismatch misclassified: %v", err)
	}
	t.Logf("[test] config mismatch rejected: %v", err)
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
