package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/blazingkevin/wal/internal/record"
)

var (
	ErrInvalidSegment     = errors.New("segment: invalid magic number")
	ErrUnsupportedVersion = errors.New("segment: unsupported version")
	ErrIndexMismatch      = errors.New("segment: index mismatch")
)

type Reader struct {
	f     *os.File
	index uint64
	buf   []byte
	pos   int
}

func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("segment open %s : %w", path, err)
	}

	// validate segment header
	idx, err := readAndValidateHeader(f)

	if err != nil {
		f.Close()
		return nil, err
	}

	body, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("segment: read body of %s : %w", path, err)
	}

	return &Reader{
		f:     f,
		index: idx,
		buf:   body,
		pos:   0,
	}, nil
}

func (r *Reader) Next() ([]byte, error) {
	if r.pos >= len(r.buf) {
		return nil, io.EOF
	}

	data, n, err := record.Decode(r.buf[r.pos:])
	if err != nil {
		return nil, err
	}

	r.pos += n
	return data, nil
}

func (r *Reader) Index() uint64 {
	return r.index
}

func (r *Reader) Close() error {
	return r.f.Close()
}

func Recover(dir string, index uint64, maxSize int64) (*Segment, error) {
	path := Path(dir, index)

	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("segment: recover open %s: %w", path, err)
	}

	storedIndex, err := readAndValidateHeader(f)
	if err != nil {
		f.Close()
		return nil, err
	}

	if storedIndex != index {
		f.Close()
		return nil, fmt.Errorf("%w: header has %d, filename says %d",
			ErrIndexMismatch, storedIndex, index)
	}

	body, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("segment: recover read body of %s: %w", path, err)
	}

	lastClean := int64(headerSize)
	buf := body
	torn := false

	for len(buf) > 0 {
		_, n, err := record.Decode(buf)
		if errors.Is(err, record.ErrTruncated) || errors.Is(err, record.ErrChecksum) {
			torn = true
			log.Printf("segment: recover %s: torn write detected at offset %d (%v), truncating",
				path, lastClean, err)
			break
		}
		lastClean += int64(n)
		buf = buf[n:]
	}

	if torn {
		if err := f.Truncate(lastClean); err != nil {
			f.Close()
			return nil, fmt.Errorf("segment: recover truncate %s to %d: %w", path, lastClean, err)
		}

		if err := f.Sync(); err != nil {
			f.Close()
			return nil, fmt.Errorf("segment: recover sync %s: %w", path, err)
		}
	}

	if _, err := f.Seek(lastClean, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("segment: recover seek %s: %w", path, err)
	}

	return &Segment{
		f:       f,
		index:   index,
		size:    lastClean,
		maxSize: maxSize,
	}, nil
}

func readAndValidateHeader(f *os.File) (uint64, error) {
	var h [headerSize]byte

	if _, err := io.ReadFull(f, h[:]); err != nil {
		return 0, fmt.Errorf("segment: read header from %s : %w", f.Name(), err)
	}

	if string(h[0:4]) != headerMagic {
		return 0, fmt.Errorf("%w: got %q, want %q", ErrInvalidSegment, h[0:4], headerMagic)
	}

	if h[4] != headerVersion {
		return 0, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedVersion, h[4], headerVersion)
	}

	return binary.BigEndian.Uint64(h[8:16]), nil
}
