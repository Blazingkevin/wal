package wal_test

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/blazingkevin/wal"
)

func TestReaderEmpty(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)

	r, err := w.Reader(0)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()

	_, err = r.Next()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Next() on empty WAL = %v, want io.EOF", err)
	}
}

// TestReaderAllRecords writes N records and verifies the Reader returns all of
// them in order with the correct payloads.
func TestReaderAllRecords(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)

	const n = 20
	for i := 0; i < n; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("record-%02d", i))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	r, err := w.Reader(0)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()

	for i := 0; i < n; i++ {
		data, err := r.Next()
		if err != nil {
			t.Fatalf("Next() record %d: %v", i, err)
		}
		want := fmt.Sprintf("record-%02d", i)
		if string(data) != want {
			t.Errorf("record %d = %q, want %q", i, data, want)
		}
	}

	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("Next() after last record = %v, want io.EOF", err)
	}
}

func TestReaderStartIndex(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)

	const n = 10
	for i := 0; i < n; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("record-%d", i))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	const start = 5
	r, err := w.Reader(start)
	if err != nil {
		t.Fatalf("Reader(%d): %v", start, err)
	}
	defer r.Close()

	for i := start; i < n; i++ {
		data, err := r.Next()
		if err != nil {
			t.Fatalf("Next() at position %d: %v", i, err)
		}
		want := fmt.Sprintf("record-%d", i)
		if string(data) != want {
			t.Errorf("position %d = %q, want %q", i, data, want)
		}
	}

	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Errorf("Next() after last record = %v, want io.EOF", err)
	}
}

func TestReaderCrossesSegmentBoundary(t *testing.T) {
	dir := t.TempDir()

	payload := []byte("data")
	maxSize := int64(16 + 3*(8+len(payload)))

	w := openWAL(t, dir, wal.WithMaxSegmentSize(maxSize))

	const total = 9
	for i := 0; i < total; i++ {
		if _, err := w.Write(payload); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	r, err := w.Reader(0)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()

	count := 0
	for {
		data, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next() at record %d: %v", count, err)
		}
		if string(data) != string(payload) {
			t.Errorf("record %d = %q, want %q", count, data, payload)
		}
		count++
	}

	if count != total {
		t.Errorf("read %d records, want %d", count, total)
	}
}

func TestReaderSnapshotSemantics(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)

	for i := 0; i < 5; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("before-%d", i))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	r, err := w.Reader(0)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()

	for i := 0; i < 5; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("after-%d", i))); err != nil {
			t.Fatalf("post-Reader Write %d: %v", i, err)
		}
	}

	count := 0
	for {
		data, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next(): %v", err)
		}
		want := fmt.Sprintf("before-%d", count)
		if string(data) != want {
			t.Errorf("record %d = %q, want %q", count, data, want)
		}
		count++
	}

	if count != 5 {
		t.Errorf("Reader returned %d records, want 5 (snapshot)", count)
	}
}

func TestMultipleReaders(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)

	const n = 30
	for i := 0; i < n; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("r%d", i))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	const numReaders = 5
	readers := make([]*wal.Reader, numReaders)
	for i := range readers {
		r, err := w.Reader(0)
		if err != nil {
			t.Fatalf("Reader %d: %v", i, err)
		}
		readers[i] = r
	}

	var wg sync.WaitGroup
	for i, r := range readers {
		wg.Add(1)
		go func(id int, r *wal.Reader) {
			defer wg.Done()
			defer r.Close()
			count := 0
			for {
				_, err := r.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Errorf("reader %d Next(): %v", id, err)
					return
				}
				count++
			}
			if count != n {
				t.Errorf("reader %d: got %d records, want %d", id, count, n)
			}
		}(i, r)
	}
	wg.Wait()
}

func TestConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)

	const initialWrites = 50
	for i := 0; i < initialWrites; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("init-%d", i))); err != nil {
			t.Fatalf("initial Write %d: %v", i, err)
		}
	}

	r, err := w.Reader(0)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()

	var writerErr error
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := w.Write([]byte(fmt.Sprintf("concurrent-%d", i))); err != nil {
				writerErr = err
				return
			}
		}
	}()

	var readCount int
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			_, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Errorf("concurrent Read: %v", err)
				return
			}
			readCount++
		}
	}()

	wg.Wait()

	if writerErr != nil {
		t.Errorf("concurrent writer: %v", writerErr)
	}
	if readCount != initialWrites {
		t.Errorf("reader got %d records, want %d", readCount, initialWrites)
	}
}

func TestReaderAfterReopen(t *testing.T) {
	dir := t.TempDir()

	w1 := openWAL(t, dir)
	payloads := []string{"alpha", "beta", "gamma", "delta"}
	for _, p := range payloads {
		if _, err := w1.Write([]byte(p)); err != nil {
			t.Fatalf("Write %q: %v", p, err)
		}
	}
	w1.Close()

	w2 := openWAL(t, dir)
	r, err := w2.Reader(0)
	if err != nil {
		t.Fatalf("Reader after reopen: %v", err)
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
