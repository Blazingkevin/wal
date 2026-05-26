package wal

import "github.com/blazingkevin/wal/internal/segment"

type SyncPolicy interface {
	Sync(seg *segment.Segment) error

	Close() error
}

func SyncAlways() SyncPolicy { return &syncAlways{} }

type syncAlways struct{}

func (s *syncAlways) Sync(seg *segment.Segment) error { return seg.Sync() }
func (s *syncAlways) Close() error                    { return nil }

func SyncNone() SyncPolicy { return &syncNone{} }

type syncNone struct{}

func (s *syncNone) Sync(_ *segment.Segment) error { return nil }
func (s *syncNone) Close() error                  { return nil }
