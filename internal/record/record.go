package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// HeaderSize is the number of bytes in every record header
//
// # Disk Layout
//
//	bytes 0 - 3: payload length as uint32, big-endian
//	bytes 4 - 7: CRC32c checksum as uint32, big-endian
//	bytes 8+ : payload
const HeaderSize = 8

var (
	//ErrTruncated means the input buffer doesn't contain a complete record
	ErrTruncated = errors.New("record: truncated")

	// ErrChecksum means the CRC32c computed from the record does not match the checksum in the header
	ErrChecksum = errors.New("record: checksum mismatch")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func Encode(data []byte) []byte {
	buf := make([]byte, HeaderSize+len(data))

	// write the payload length into bytes 0 - 3
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(data)))

	// generate checksum of length of data and data itself
	checksum := crc32.Checksum(buf[0:4], crcTable)
	checksum = crc32.Update(checksum, crcTable, data)
	binary.BigEndian.PutUint32(buf[4:8], checksum)

	// copy the payload into bytes 8+
	copy(buf[HeaderSize:], data)
	return buf
}

func Decode(b []byte) (data []byte, n int, err error) {
	// check for header completeness
	if len(b) < HeaderSize {
		return nil, 0, ErrTruncated
	}

	length := binary.BigEndian.Uint32(b[0:4])
	storedChecksum := binary.BigEndian.Uint32(b[4:8])
	total := HeaderSize + int(length)

	if len(b) < total {
		return nil, 0, ErrTruncated
	}

	computedChecksum := crc32.Checksum(b[0:4], crcTable)
	computedChecksum = crc32.Update(computedChecksum, crcTable, b[HeaderSize:total])

	if computedChecksum != storedChecksum {
		return nil, 0, ErrChecksum
	}

	return b[HeaderSize:total], total, nil

}
