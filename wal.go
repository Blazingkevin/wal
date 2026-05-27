package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/blazingkevin/wal/internal/segment"
)

var ErrClosed = errors.New("wal: closed")

type WAL struct {
	mu         sync.Mutex
	dir        string
	active     *segment.Segment
	segIndices []uint64
	nextIndex  uint64
	maxSegSize int64
	syncPolicy SyncPolicy
	closed     bool
}

type Option func(*WAL)

func WithMaxSegmentSize(size int64) Option {
	return func(w *WAL) { w.maxSegSize = size }
}

func WithSyncPolicy(p SyncPolicy) Option {
	return func(w *WAL) { w.syncPolicy = p }
}

func Open(dir string, opts ...Option) (*WAL, error) {
	w := &WAL{
		dir:        dir,
		maxSegSize: segment.DefaultMaxSize,
		syncPolicy: SyncAlways(),
	}
	for _, o := range opts {
		o(w)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", dir, err)
	}

	indices, err := listSegments(dir)
	if err != nil {
		return nil, err
	}

	if len(indices) == 0 {
		return w.openFresh()
	}
	return w.openExisting(indices)
}

func (w *WAL) openFresh() (*WAL, error) {
	seg, err := segment.Create(w.dir, 1, w.maxSegSize)
	if err != nil {
		return nil, fmt.Errorf("wal: create first segment: %w", err)
	}
	w.active = seg
	w.segIndices = []uint64{1}
	w.nextIndex = 0
	return w, nil
}

func (w *WAL) openExisting(indices []uint64) (*WAL, error) {
	var totalRecords uint64
	for _, idx := range indices[:len(indices)-1] {
		count, err := countSegmentRecords(segment.Path(w.dir, idx))
		if err != nil {
			return nil, fmt.Errorf("wal: scan segment %d: %w", idx, err)
		}
		totalRecords += count
	}

	tailIdx := indices[len(indices)-1]
	active, err := segment.Recover(w.dir, tailIdx, w.maxSegSize)
	if err != nil {
		return nil, fmt.Errorf("wal: recover tail segment %d: %w", tailIdx, err)
	}

	tailCount, err := countSegmentRecords(segment.Path(w.dir, tailIdx))
	if err != nil {
		active.Close()
		return nil, fmt.Errorf("wal: count tail segment %d records: %w", tailIdx, err)
	}
	totalRecords += tailCount

	w.active = active
	w.segIndices = indices
	w.nextIndex = totalRecords
	return w, nil
}

func (w *WAL) Write(data []byte) (uint64, error) {
	w.mu.Lock()

	if w.closed {
		w.mu.Unlock()
		return 0, ErrClosed
	}

	if w.active.Full(len(data)) {
		if err := w.rotate(); err != nil {
			w.mu.Unlock()
			return 0, err
		}
	}

	if err := w.active.Write(data); err != nil {
		w.mu.Unlock()
		return 0, fmt.Errorf("wal: write record: %w", err)
	}

	idx := w.nextIndex
	w.nextIndex++

	active := w.active
	w.mu.Unlock()

	if err := w.syncPolicy.Sync(active); err != nil {
		return 0, fmt.Errorf("wal: sync: %w", err)
	}

	return idx, nil
}

func (w *WAL) rotate() error {
	if err := w.active.Sync(); err != nil {
		return fmt.Errorf("wal: rotate sync: %w", err)
	}
	if err := w.active.Close(); err != nil {
		return fmt.Errorf("wal: rotate close: %w", err)
	}

	nextSegIdx := w.segIndices[len(w.segIndices)-1] + 1
	seg, err := segment.Create(w.dir, nextSegIdx, w.maxSegSize)
	if err != nil {
		return fmt.Errorf("wal: rotate create segment %d: %w", nextSegIdx, err)
	}

	w.active = seg
	w.segIndices = append(w.segIndices, nextSegIdx)
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if err := w.syncPolicy.Close(); err != nil {
		return fmt.Errorf("wal: close sync policy: %w", err)
	}

	if err := w.active.Sync(); err != nil {
		return fmt.Errorf("wal: final sync: %w", err)
	}
	return w.active.Close()
}

func (w *WAL) LastIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.nextIndex == 0 {
		return 0
	}
	return w.nextIndex - 1
}

func (w *WAL) Reader(startIndex uint64) (*Reader, error) {
	w.mu.Lock()
	indices := make([]uint64, len(w.segIndices))
	copy(indices, w.segIndices)
	endIndex := w.nextIndex
	w.mu.Unlock()

	paths := make([]string, len(indices))
	for i, idx := range indices {
		paths[i] = segment.Path(w.dir, idx)
	}

	return &Reader{
		paths:      paths,
		startIndex: startIndex,
		endIndex:   endIndex,
		curIndex:   0,
		segPos:     0,
	}, nil
}

func listSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: read dir %s: %w", dir, err)
	}

	var indices []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var idx uint64
		if _, err := fmt.Sscanf(e.Name(), "segment-%020d", &idx); err != nil {
			continue
		}
		indices = append(indices, idx)
	}
	return indices, nil
}

func countSegmentRecords(path string) (uint64, error) {
	r, err := segment.OpenReader(path)
	if err != nil {
		return 0, err
	}
	defer r.Close()

	var count uint64
	for {
		_, err := r.Next()
		if errors.Is(err, io.EOF) {
			return count, nil
		}
		if err != nil {
			return count, fmt.Errorf("read record: %w", err)
		}
		count++
	}
}
