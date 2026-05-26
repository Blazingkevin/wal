package wal_test

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/blazingkevin/wal"
	"github.com/blazingkevin/wal/internal/segment"
)

func openWAL(t *testing.T, dir string, opts ...wal.Option) *wal.WAL {
	t.Helper()
	defaults := []wal.Option{wal.WithSyncPolicy(wal.SyncNone())}
	w, err := wal.Open(dir, append(defaults, opts...)...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func TestOpenFresh(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 segment file, got %d", len(entries))
	}
	if entries[0].Name() != segment.Name(1) {
		t.Errorf("first segment = %q, want %q", entries[0].Name(), segment.Name(1))
	}

	if got := w.LastIndex(); got != 0 {
		t.Errorf("LastIndex on fresh WAL = %d, want 0", got)
	}
}

func TestOpenCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "wal")
	w := openWAL(t, dir)
	_ = w
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("directory not created: %v", err)
	}
}

func TestWriteSingle(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)

	idx, err := w.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if idx != 0 {
		t.Errorf("first Write index = %d, want 0", idx)
	}
	if got := w.LastIndex(); got != 0 {
		t.Errorf("LastIndex = %d, want 0", got)
	}
}

func TestWriteMultiple(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)

	for i := 0; i < 10; i++ {
		idx, err := w.Write([]byte(fmt.Sprintf("record-%d", i)))
		if err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		if idx != uint64(i) {
			t.Errorf("Write %d: index = %d, want %d", i, idx, i)
		}
	}
	if got := w.LastIndex(); got != 9 {
		t.Errorf("LastIndex = %d, want 9", got)
	}
}

func TestRotation(t *testing.T) {
	dir := t.TempDir()

	payload := []byte("data")
	oneRecord := int64(segment.DefaultMaxSize)
	_ = oneRecord
	maxSize := int64(16 + 3*(8+len(payload)))

	w := openWAL(t, dir, wal.WithMaxSegmentSize(maxSize))

	for i := 0; i < 6; i++ {
		if _, err := w.Write(payload); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	// Two segment files should now exist.
	entries, _ := os.ReadDir(dir)
	var segFiles []string
	for _, e := range entries {
		var idx uint64
		if _, err := fmt.Sscanf(e.Name(), "segment-%020d", &idx); err == nil {
			segFiles = append(segFiles, e.Name())
		}
	}
	if len(segFiles) != 2 {
		t.Errorf("expected 2 segment files after rotation, got %d: %v", len(segFiles), segFiles)
	}

	if got := w.LastIndex(); got != 5 {
		t.Errorf("LastIndex after rotation = %d, want 5", got)
	}
}

func TestOpenExisting(t *testing.T) {
	dir := t.TempDir()

	// First session... write 5 records.
	w1 := openWAL(t, dir)
	for i := 0; i < 5; i++ {
		if _, err := w1.Write([]byte(fmt.Sprintf("session1-%d", i))); err != nil {
			t.Fatalf("first session Write %d: %v", i, err)
		}
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close first WAL: %v", err)
	}

	// Second session... reopen and write 3 more records.
	w2 := openWAL(t, dir)
	for i := 0; i < 3; i++ {
		idx, err := w2.Write([]byte(fmt.Sprintf("session2-%d", i)))
		if err != nil {
			t.Fatalf("second session Write %d: %v", i, err)
		}
		want := uint64(5 + i)
		if idx != want {
			t.Errorf("second session Write %d: index = %d, want %d", i, idx, want)
		}
	}
	if got := w2.LastIndex(); got != 7 {
		t.Errorf("LastIndex after second session = %d, want 7", got)
	}
}

func TestClosed(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, wal.WithSyncPolicy(wal.SyncNone()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.Close()

	_, err = w.Write([]byte("after close"))
	if !errors.Is(err, wal.ErrClosed) {
		t.Errorf("Write after Close = %v, want ErrClosed", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, _ := wal.Open(dir, wal.WithSyncPolicy(wal.SyncNone()))
	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)

	const goroutines = 50
	const writesPerGoroutine = 10

	var mu sync.Mutex
	seen := make(map[uint64]bool)
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				idx, err := w.Write([]byte(fmt.Sprintf("g%d-i%d", g, i)))
				if err != nil {
					t.Errorf("goroutine %d write %d: %v", g, i, err)
					return
				}
				mu.Lock()
				if seen[idx] {
					t.Errorf("duplicate index %d from goroutine %d", idx, g)
				}
				seen[idx] = true
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	total := goroutines * writesPerGoroutine
	if len(seen) != total {
		t.Errorf("expected %d unique indices, got %d", total, len(seen))
	}
}

func TestRecoverAfterCrash(t *testing.T) {
	dir := t.TempDir()

	w1, err := wal.Open(dir, wal.WithSyncPolicy(wal.SyncAlways()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := w1.Write([]byte(fmt.Sprintf("pre-crash-%d", i))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	entries, _ := os.ReadDir(dir)
	var segPath string
	for _, e := range entries {
		var idx uint64
		if _, err := fmt.Sscanf(e.Name(), "segment-%020d", &idx); err == nil {
			segPath = filepath.Join(dir, e.Name())
		}
	}
	info, _ := os.Stat(segPath)

	f, _ := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0o600)
	f.Write([]byte{0x00, 0x00, 0x05})
	f.Close()

	info2, _ := os.Stat(segPath)
	if info2.Size() <= info.Size() {
		t.Fatal("setup: torn bytes not appended")
	}

	w2 := openWAL(t, dir)

	if got := w2.LastIndex(); got != 4 {
		t.Errorf("LastIndex after recovery = %d, want 4 (records 0-4)", got)
	}

	idx, err := w2.Write([]byte("post-crash"))
	if err != nil {
		t.Fatalf("Write after recovery: %v", err)
	}
	if idx != 5 {
		t.Errorf("post-recovery Write index = %d, want 5", idx)
	}
}

func TestSyncAlways(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, wal.WithSyncPolicy(wal.SyncAlways()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	for i := 0; i < 5; i++ {
		if _, err := w.Write([]byte("durable")); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
}

func TestReadAfterWrite(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)

	payloads := []string{"alpha", "beta", "gamma"}
	for _, p := range payloads {
		if _, err := w.Write([]byte(p)); err != nil {
			t.Fatalf("Write %q: %v", p, err)
		}
	}
	w.Close()

	entries, _ := os.ReadDir(dir)
	r, err := segment.OpenReader(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	for i, want := range payloads {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("Next() record %d: %v", i, err)
		}
		if string(got) != want {
			t.Errorf("record %d = %q, want %q", i, got, want)
		}
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF after all records, got %v", err)
	}
}
