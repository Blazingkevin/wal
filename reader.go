package wal

import (
	"errors"
	"fmt"
	"io"

	"github.com/blazingkevin/wal/internal/segment"
)

type Reader struct {
	paths      []string
	startIndex uint64
	endIndex   uint64
	curIndex   uint64
	segPos     int
	segReader  *segment.Reader
}

func (r *Reader) Next() ([]byte, error) {
	for {
		if r.curIndex >= r.endIndex {
			return nil, io.EOF
		}

		if r.segReader == nil {
			if r.segPos >= len(r.paths) {
				return nil, io.EOF
			}
			sr, err := segment.OpenReader(r.paths[r.segPos])
			if err != nil {
				return nil, fmt.Errorf("wal reader: open segment %s: %w",
					r.paths[r.segPos], err)
			}
			r.segReader = sr
			r.segPos++
		}

		data, err := r.segReader.Next()
		if errors.Is(err, io.EOF) {
			r.segReader.Close()
			r.segReader = nil
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("wal reader: decode record at index %d: %w",
				r.curIndex, err)
		}

		thisIndex := r.curIndex
		r.curIndex++

		if thisIndex < r.startIndex {
			continue
		}

		return data, nil
	}
}

func (r *Reader) Close() error {
	if r.segReader != nil {
		err := r.segReader.Close()
		r.segReader = nil
		return err
	}
	return nil
}
