package wal

import (
	"sync"
	"time"

	"github.com/blazingkevin/wal/internal/segment"
)

type SyncPolicy interface {
	Sync(seg *segment.Segment) error

	Close() error
}

func SyncAlways() SyncPolicy { return &syncAlways{} }

type syncAlways struct{}

func (s *syncAlways) Sync(seg *segment.Segment) error { return seg.Sync() }
func (s *syncAlways) Close() error                    { return nil }

func SyncPeriodic(interval time.Duration) SyncPolicy {
	s := &syncPeriodic{
		interval: interval,
		done:     make(chan struct{}),
		exited:   make(chan struct{}),
	}

	go s.loop()
	return s
}

type syncPeriodic struct {
	interval time.Duration

	mu  sync.Mutex
	seg *segment.Segment

	closeOnce sync.Once
	done      chan struct{}
	exited    chan struct{}
}

func (s *syncPeriodic) Sync(seg *segment.Segment) error {
	s.mu.Lock()
	s.seg = seg
	s.mu.Unlock()
	return nil
}

func (s *syncPeriodic) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
		<-s.exited
	})
	return nil
}

func (s *syncPeriodic) loop() {
	defer close(s.exited)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.flush()
		case <-s.done:
			s.flush()
			return
		}
	}
}

func (s *syncPeriodic) flush() {
	s.mu.Lock()
	seg := s.seg
	s.mu.Unlock()
	if seg != nil {
		_ = seg.Sync()
	}
}

func SyncNone() SyncPolicy { return &syncNone{} }

type syncNone struct{}

func (s *syncNone) Sync(_ *segment.Segment) error { return nil }
func (s *syncNone) Close() error                  { return nil }
