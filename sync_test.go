package wal_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/blazingkevin/wal"
)

func TestSyncPeriodicWritesSucceed(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir, wal.WithSyncPolicy(wal.SyncPeriodic(10*time.Second)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const n = 20
	for i := 0; i < n; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("record-%d", i))); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2 := openWAL(t, dir)
	r, err := w2.Reader(0)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()

	count := 0
	for {
		data, err := r.Next()
		if err != nil {
			break
		}
		want := fmt.Sprintf("record-%d", count)
		if string(data) != want {
			t.Errorf("record %d = %q, want %q", count, data, want)
		}
		count++
	}
	if count != n {
		t.Errorf("read %d records after reopen, want %d", count, n)
	}
}

func TestSyncPeriodicFlushesPeriodically(t *testing.T) {
	dir := t.TempDir()

	const interval = 20 * time.Millisecond
	w, err := wal.Open(dir, wal.WithSyncPolicy(wal.SyncPeriodic(interval)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w.Close()

	if _, err := w.Write([]byte("periodic")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	time.Sleep(3 * interval)

	r, err := w.Reader(0)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()

	data, err := r.Next()
	if err != nil {
		t.Fatalf("Next(): %v", err)
	}
	if string(data) != "periodic" {
		t.Errorf("got %q, want %q", data, "periodic")
	}
}

func TestSyncPeriodicCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, wal.WithSyncPolicy(wal.SyncPeriodic(time.Second)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestSyncPeriodicNoGoroutineLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()

	const n = 5
	for i := 0; i < n; i++ {
		dir := t.TempDir()
		w, err := wal.Open(dir, wal.WithSyncPolicy(wal.SyncPeriodic(time.Second)))
		if err != nil {
			t.Fatalf("Open %d: %v", i, err)
		}
		if _, err := w.Write([]byte("data")); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close %d: %v", i, err)
		}
	}

	time.Sleep(10 * time.Millisecond)
	runtime.Gosched()

	after := runtime.NumGoroutine()
	if after > baseline+2 {
		t.Errorf("goroutine count after %d open+close cycles: before=%d after=%d ... possible leak",
			n, baseline, after)
	}
}
