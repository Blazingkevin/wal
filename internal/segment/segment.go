package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blazingkevin/wal/internal/record"
)

const (
	headerSize    = 16
	headerMagic   = "WAL1"
	headerVersion = 1
)

// DefaultMaxSize is the maximum size for each segment
const DefaultMaxSize = 64 * 1024 * 1024

var ErrFull = errors.New("segment: full")

type Segment struct {
	f       *os.File
	index   uint64
	size    int64
	maxSize int64
}

func Name(index uint64) string {
	return fmt.Sprintf("segment-%020d", index)
}

func Path(dir string, index uint64) string {
	return filepath.Join(dir, Name(index))
}

func Create(dir string, index uint64, maxSize int64) (*Segment, error) {
	path := Path(dir, index)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("segment: create %s : %w", path, err)
	}

	s := &Segment{
		f:       f,
		index:   index,
		size:    0,
		maxSize: maxSize,
	}

	if err := s.writeHeader(); err != nil {
		// remove partially written file
		f.Close()
		os.Remove(path)
		return nil, err
	}

	return s, nil
}

func (s *Segment) writeHeader() error {
	var h [headerSize]byte

	copy(h[0:4], []byte(headerMagic))

	h[4] = headerVersion

	// bytes 5-7 will be reserversed(default to 0's for future use), we use 8-16 for segment index
	binary.BigEndian.PutUint64(h[8:16], s.index)

	if _, err := s.f.Write(h[:]); err != nil {
		return fmt.Errorf("segment: write header: %w", err)
	}

	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("segment: sync header: %w", err)
	}

	s.size = headerSize
	return nil
}

// Write encode data as WAL record and appends it to the segment
func (s *Segment) Write(data []byte) error {
	encoded := record.Encode(data)

	// check the current space left before writing
	if s.size+int64(len(data)) > s.maxSize {
		return ErrFull
	}

	if _, err := s.f.Write(encoded); err != nil {
		return fmt.Errorf("segment: write record: %w", err)
	}

	// update size
	s.size += int64(len(encoded))
	return nil
}

// Sync flushes buffered data to durable storage
func (s *Segment) Sync() error {
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("segment : sync : %w", err)
	}
	return nil
}

// CLose flushes and releases the file descriptor
func (s *Segment) Close() error {
	if err := s.f.Close(); err != nil {
		return fmt.Errorf("segment : close : %w", err)
	}
	return nil
}

// Size returns the number of bytes currently written to the segment
func (s *Segment) Size() int64 {
	return s.size
}

// Index returns the position of the segment in the log sequence
func (s *Segment) Index() uint64 {
	return s.index
}

func (s *Segment) Full(payloadSize int) bool {
	return s.size+int64(payloadSize+record.HeaderSize) > s.maxSize
}
