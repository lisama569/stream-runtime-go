package stream

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeCP(t *testing.T, st *CheckpointStore, cp *Checkpoint) {
	t.Helper()
	if err := st.Save(cp); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}
}

// TestCheckpointRoundTrip 正常保存/加载并校验通过。
func TestCheckpointRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := NewCheckpointStore(filepath.Join(dir, "cp.json"), 0)
	cp := &Checkpoint{
		Version:      checkpointFormatVersion,
		TopologyID:   "topo-x",
		SourceOffset: 7,
		WMMaxSeen:    1000,
		WMCurrent:    500,
		WindowState:  []byte("win"),
		SinkState:    []byte("sink"),
	}
	writeCP(t, st, cp)
	got, err := st.Load("topo-x")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.SourceOffset != 7 || got.WMCurrent != 500 {
		t.Fatalf("恢复内容错误: %+v", got)
	}
	t.Logf("CHECKPOINT 正常加载: offset=%d wm=%d checksum=%d", got.SourceOffset, got.WMCurrent, got.Checksum)
}

// TestCheckpointMissing 文件不存在必须归类为 ErrCheckpointMissing。
func TestCheckpointMissing(t *testing.T) {
	st := NewCheckpointStore(filepath.Join(t.TempDir(), "none.json"), 0)
	_, err := st.Load("t")
	if !errors.Is(err, ErrCheckpointMissing) {
		t.Fatalf("期望 ErrCheckpointMissing，得到 %v", err)
	}
}

// TestCheckpointExpired 超过 TTL 必须归类为 ErrCheckpointExpired。
func TestCheckpointExpired(t *testing.T) {
	dir := t.TempDir()
	st := NewCheckpointStore(filepath.Join(dir, "cp.json"), time.Hour)
	writeCP(t, st, &Checkpoint{TopologyID: "t", CreatedAtMs: time.Now().Add(-2 * time.Hour).UnixMilli()})
	_, err := st.Load("t")
	if !errors.Is(err, ErrCheckpointExpired) {
		t.Fatalf("期望 ErrCheckpointExpired，得到 %v", err)
	}
	t.Logf("CHECKPOINT 过期被拒绝: %v", err)
}

// TestCheckpointCorrupt 字节损坏/截断必须归类为 ErrCheckpointCorrupt。
func TestCheckpointCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.json")
	st := NewCheckpointStore(path, 0)
	writeCP(t, st, &Checkpoint{TopologyID: "t", WindowState: []byte("abc")})

	// 1) 直接写入非法 JSON。
	if err := os.WriteFile(path, []byte("{not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Load("t"); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("非法 JSON 期望 ErrCheckpointCorrupt，得到 %v", err)
	}

	// 2) 合法 JSON 但篡改内容导致校验和不符。
	writeCP(t, st, &Checkpoint{TopologyID: "t", WindowState: []byte("abc")})
	raw, _ := os.ReadFile(path)
	raw[len(raw)-20] = raw[len(raw)-20] + 1 // 扰动（尽量落在数字区域）
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Load("t"); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("校验和不符期望 ErrCheckpointCorrupt，得到 %v", err)
	}
	t.Logf("CHECKPOINT 损坏被拒绝（非法JSON / CRC不符），原因可区分")
}

// TestCheckpointIncompatible 拓扑指纹或格式版本不符必须归类为 ErrCheckpointIncompat。
func TestCheckpointIncompatible(t *testing.T) {
	dir := t.TempDir()
	st := NewCheckpointStore(filepath.Join(dir, "cp.json"), 0)
	writeCP(t, st, &Checkpoint{TopologyID: "topo-A"})
	if _, err := st.Load("topo-B"); !errors.Is(err, ErrCheckpointIncompat) {
		t.Fatalf("拓扑不符期望 ErrCheckpointIncompat，得到 %v", err)
	}
	t.Logf("CHECKPOINT 拓扑不兼容被拒绝: %v", st) // 确保 store 被引用
}

// TestCheckpointAtomicSave 写入过程中目录里不应出现被截断的正式文件。
func TestCheckpointAtomicSave(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.json")
	st := NewCheckpointStore(path, 0)
	for i := 0; i < 20; i++ {
		if err := st.Save(&Checkpoint{TopologyID: "t", SourceOffset: int64(i)}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Load("t"); err != nil {
			t.Fatalf("第 %d 次保存后检查点必须可完整加载: %v", i, err)
		}
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("临时文件必须在 rename 后被清理")
	}
}
