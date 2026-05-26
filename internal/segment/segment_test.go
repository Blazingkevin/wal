package segment_test

import (
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/blazingkevin/wal/internal/record"
	"github.com/blazingkevin/wal/internal/segment"
)

func TestName(t *testing.T) {
	cases := []struct {
		index uint64
		want  string
	}{
		{0, "segment-00000000000000000000"},
		{1, "segment-00000000000000000001"},
		{42, "segment-00000000000000000042"},
		{1_000_000, "segment-00000000000001000000"},
		{math.MaxUint64, "segment-18446744073709551615"},
	}

	for _, tc := range cases {
		got := segment.Name(tc.index)
		if got != tc.want {
			t.Errorf("Name(%d) = %q, want %q", tc.index, got, tc.want)
		}
	}
}

func TestPath(t *testing.T) {
	got := segment.Path("/data/wal", 3)
	want := filepath.Join("/data/wal", "segment-00000000000000000003")
	if got != want {
		t.Errorf("Path = %q, want %q", got, want)
	}
}

func TestCreate(t *testing.T) {
	dir := t.TempDir()
	const idx = uint64(7)

	s, err := segment.Create(dir, idx, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()

	// File must exist at the expected path.
	path := segment.Path(dir, idx)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("segment file not found: %v", err)
	}

	// Size immediately after Create must be exactly the header size.
	// since No records have been written yet.
	if s.Size() != 16 {
		t.Errorf("Size after Create = %d, want 16", s.Size())
	}

	// Read the raw bytes and inspect the header fields
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(raw) != 16 {
		t.Fatalf("file size = %d bytes, want 16", len(raw))
	}

	// Bytes 0-3 the magic number must be "WAL1".
	if string(raw[0:4]) != "WAL1" {
		t.Errorf("magic = %q, want %q", raw[0:4], "WAL1")
	}
	// Byte 4 which is version should be 1
	if raw[4] != 1 {
		t.Errorf("version = %d, want 1", raw[4])
	}

	// Bytes 5-7 which we reserved, must be zero
	if raw[5] != 0 || raw[6] != 0 || raw[7] != 0 {
		t.Errorf("reserved bytes = %v, want all zero", raw[5:8])
	}

	// Bytes 8-15 should be segment index as big-endian uint64.
	storedIndex := uint64(raw[8])<<56 | uint64(raw[9])<<48 |
		uint64(raw[10])<<40 | uint64(raw[11])<<32 |
		uint64(raw[12])<<24 | uint64(raw[13])<<16 |
		uint64(raw[14])<<8 | uint64(raw[15])
	if storedIndex != idx {
		t.Errorf("header index = %d, want %d", storedIndex, idx)
	}
}

func TestCreateExcl(t *testing.T) {
	dir := t.TempDir()

	s, err := segment.Create(dir, 1, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	s.Close()

	_, err = segment.Create(dir, 1, segment.DefaultMaxSize)
	if err == nil {
		t.Fatal("second Create with same index should have returned an error, got nil")
	}
}

func TestWrite(t *testing.T) {
	dir := t.TempDir()
	s, err := segment.Create(dir, 1, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()

	payload := []byte("hello, wal")
	if err := s.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// size must be header (16) + record header (8) + len(payload).
	wantSize := int64(16 + record.HeaderSize + len(payload))
	if s.Size() != wantSize {
		t.Errorf("Size = %d, want %d", s.Size(), wantSize)
	}

	// Verify file size on disk matches the tracked size.
	info, err := os.Stat(segment.Path(dir, 1))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != wantSize {
		t.Errorf("disk size = %d, want %d", info.Size(), wantSize)
	}
}

func TestWriteMultiple(t *testing.T) {
	dir := t.TempDir()
	s, err := segment.Create(dir, 1, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()

	payloads := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third record is a bit longer"),
		{},
	}

	for _, p := range payloads {
		if err := s.Write(p); err != nil {
			t.Fatalf("Write(%q): %v", p, err)
		}
	}

	raw, err := os.ReadFile(segment.Path(dir, 1))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	buf := raw[16:] // we skip segment header
	for i, want := range payloads {
		got, n, err := record.Decode(buf)
		if err != nil {
			t.Fatalf("record %d Decode: %v", i, err)
		}
		if string(got) != string(want) {
			t.Errorf("record %d = %q, want %q", i, got, want)
		}
		buf = buf[n:]
	}

	// After decoding all records, the buffer should be empty.
	if len(buf) != 0 {
		t.Errorf("unexpected trailing bytes: %d", len(buf))
	}
}

func TestWriteFull(t *testing.T) {
	payload := []byte("exactly one record")
	oneRecordSize := int64(record.HeaderSize + len(payload))

	// maxSize fits the header plus exactly one record.
	maxSize := int64(16) + oneRecordSize

	dir := t.TempDir()
	s, err := segment.Create(dir, 1, maxSize)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()

	// First write must succeed.
	if err := s.Write(payload); err != nil {
		t.Fatalf("first Write: %v", err)
	}

	// second write must return ErrFull.
	err = s.Write([]byte("one more"))
	if err == nil {
		t.Fatal("expected ErrFull, got nil")
	}
	if err.Error() != segment.ErrFull.Error() {
		t.Errorf("Write error = %v, want ErrFull", err)
	}
}

func TestFull(t *testing.T) {
	dir := t.TempDir()

	payload := []byte("hello")
	maxSize := int64(16 + record.HeaderSize + len(payload))

	s, err := segment.Create(dir, 1, maxSize)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()

	if s.Full(len(payload)) {
		t.Error("Full(5) = true before any writes, want false")
	}
	if !s.Full(len(payload) + 1) {
		t.Error("Full(6) = false before any writes, want true")
	}

	if err := s.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !s.Full(0) {
		t.Error("Full(0) = false after filling segment, want true")
	}
}

func TestIndex(t *testing.T) {
	dir := t.TempDir()
	s, err := segment.Create(dir, 42, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()

	if s.Index() != 42 {
		t.Errorf("Index = %d, want 42", s.Index())
	}
}

func TestSyncClose(t *testing.T) {
	dir := t.TempDir()
	s, err := segment.Create(dir, 1, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Write([]byte("data")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := s.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestWriteEmpty(t *testing.T) {
	dir := t.TempDir()
	s, err := segment.Create(dir, 1, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()

	if err := s.Write([]byte{}); err != nil {
		t.Errorf("Write(empty): %v", err)
	}

	wantSize := int64(16 + record.HeaderSize)
	if s.Size() != wantSize {
		t.Errorf("Size = %d, want %d", s.Size(), wantSize)
	}
}

// Reader TYEsts
func TestOpenReader(t *testing.T) {
	dir := t.TempDir()
	s, err := segment.Create(dir, 1, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	payloads := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
	}
	for _, p := range payloads {
		if err := s.Write(p); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	s.Close()

	r, err := segment.OpenReader(segment.Path(dir, 1))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	for i, want := range payloads {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("Next() record %d: %v", i, err)
		}
		if string(got) != string(want) {
			t.Errorf("record %d = %q, want %q", i, got, want)
		}
	}

	_, err = r.Next()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Next() after last record = %v, want io.EOF", err)
	}
}

func TestReaderIndex(t *testing.T) {
	dir := t.TempDir()
	s, err := segment.Create(dir, 5, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	s.Close()

	r, err := segment.OpenReader(segment.Path(dir, 5))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer r.Close()

	if r.Index() != 5 {
		t.Errorf("Index = %d, want 5", r.Index())
	}
}

func makeSegment(t *testing.T, dir string, index uint64, payloads [][]byte) string {
	t.Helper()
	s, err := segment.Create(dir, index, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	for _, p := range payloads {
		if err := s.Write(p); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return segment.Path(dir, index)
}

func TestRecoverClean(t *testing.T) {
	dir := t.TempDir()
	payloads := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	makeSegment(t, dir, 1, payloads)

	info, _ := os.Stat(segment.Path(dir, 1))
	sizeBeforeRecover := info.Size()

	seg, err := segment.Recover(dir, 1, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	defer seg.Close()

	if seg.Size() != sizeBeforeRecover {
		t.Errorf("size after clean recover = %d, want %d", seg.Size(), sizeBeforeRecover)
	}

	if err := seg.Write([]byte("after recovery")); err != nil {
		t.Errorf("Write after recovery: %v", err)
	}
}

func TestRecoverTornWriteMidPayload(t *testing.T) {
	dir := t.TempDir()
	payloads := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	path := makeSegment(t, dir, 1, payloads)

	cleanSize, _ := os.Stat(path)
	wantSize := cleanSize.Size()

	torn := []byte{
		0x00, 0x00, 0x00, 0x14, // length = 20
		0xDE, 0xAD, 0xBE, 0xEF, // checksum (garbage tha won't match)
		0x01, 0x02, 0x03, 0x04, // only 4 of the 20 payload bytes present
	}
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	f.Write(torn)
	f.Close()

	seg, err := segment.Recover(dir, 1, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	defer seg.Close()

	if seg.Size() != wantSize {
		t.Errorf("size after recover = %d, want %d (clean boundary)", seg.Size(), wantSize)
	}

	info, _ := os.Stat(path)
	if info.Size() != wantSize {
		t.Errorf("disk size after recover = %d, want %d", info.Size(), wantSize)
	}

	r, err := segment.OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader after recover: %v", err)
	}
	defer r.Close()

	for i, want := range payloads {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("Next() record %d after recover: %v", i, err)
		}
		if string(got) != string(want) {
			t.Errorf("record %d after recover = %q, want %q", i, got, want)
		}
	}
}

func TestRecoverTornWritePartialHeader(t *testing.T) {
	dir := t.TempDir()
	payloads := [][]byte{[]byte("alpha"), []byte("beta")}
	path := makeSegment(t, dir, 1, payloads)

	info, _ := os.Stat(path)
	cleanSize := info.Size()

	os.Truncate(path, cleanSize+2)

	seg, err := segment.Recover(dir, 1, segment.DefaultMaxSize)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	defer seg.Close()

	if seg.Size() != cleanSize {
		t.Errorf("size after recover = %d, want %d", seg.Size(), cleanSize)
	}
}

func TestRecoverInvalidMagic(t *testing.T) {
	dir := t.TempDir()
	path := segment.Path(dir, 1)

	garbage := make([]byte, 32)
	copy(garbage[0:4], "NOPE")
	os.WriteFile(path, garbage, 0o600)

	_, err := segment.Recover(dir, 1, segment.DefaultMaxSize)
	if !errors.Is(err, segment.ErrInvalidSegment) {
		t.Errorf("Recover with bad magic = %v, want ErrInvalidSegment", err)
	}
}

func TestRecoverIndexMismatch(t *testing.T) {
	dir := t.TempDir()

	makeSegment(t, dir, 1, [][]byte{[]byte("data")})

	src := segment.Path(dir, 1)
	dst := segment.Path(dir, 2)
	os.Rename(src, dst)

	_, err := segment.Recover(dir, 2, segment.DefaultMaxSize)
	if !errors.Is(err, segment.ErrIndexMismatch) {
		t.Errorf("Recover with index mismatch = %v, want ErrIndexMismatch", err)
	}
}
