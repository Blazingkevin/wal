package record_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/blazingkevin/wal/internal/record"
)

func TestEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty payload", []byte{}},
		{"single zero byte", []byte{0x00}},
		{"single max byte", []byte{0xFF}},

		{"ascii text", []byte("hello, world")},
		{"binary data", []byte{0x00, 0xFF, 0x01, 0xFE, 0x7F, 0x80}},

		{"64 KB payload", bytes.Repeat([]byte{0xAB}, 64*1024)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := record.Encode(tt.data)

			wantLen := record.HeaderSize + len(tt.data)
			if len(encoded) != wantLen {
				t.Fatalf("Encode() len = %d, want %d", len(encoded), wantLen)
			}

			got, n, err := record.Decode(encoded)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}

			if n != len(encoded) {
				t.Errorf("Decode() n = %d, want %d", n, len(encoded))
			}

			if !bytes.Equal(got, tt.data) {
				t.Errorf("Decode() data mismatch\ngot  %x\nwant %x", got, tt.data)
			}
		})
	}
}

func TestDecodeSequential(t *testing.T) {
	payloads := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	var buf []byte
	for _, p := range payloads {
		buf = append(buf, record.Encode(p)...)
	}

	remaining := buf
	for i, want := range payloads {
		got, n, err := record.Decode(remaining)
		if err != nil {
			t.Fatalf("record %d: Decode() error = %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("record %d: got %q, want %q", i, got, want)
		}
		remaining = remaining[n:]
	}

	if len(remaining) != 0 {
		t.Errorf("%d bytes remaining after decoding all records", len(remaining))
	}
}

func TestDecodeTruncated(t *testing.T) {
	encoded := record.Encode([]byte("hello"))

	tests := []struct {
		name string
		buf  []byte
	}{
		{"empty buffer", []byte{}},
		{"one byte", encoded[:1]},
		{"partial header — 3 bytes", encoded[:3]},
		{"full header, no payload", encoded[:record.HeaderSize]},
		{"header + partial payload", encoded[:record.HeaderSize+2]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := record.Decode(tt.buf)
			if !errors.Is(err, record.ErrTruncated) {
				t.Errorf("Decode() error = %v, want ErrTruncated", err)
			}
		})
	}
}

func TestDecodeChecksumMismatch(t *testing.T) {
	encoded := record.Encode([]byte("hello"))

	corrupted := make([]byte, len(encoded))
	copy(corrupted, encoded)
	corrupted[record.HeaderSize] ^= 0xFF

	_, _, err := record.Decode(corrupted)
	if !errors.Is(err, record.ErrChecksum) {
		t.Errorf("Decode() error = %v, want ErrChecksum", err)
	}
}

func TestDecodeCorruptedLengthField(t *testing.T) {
	encoded := record.Encode([]byte("hello"))

	corrupted := make([]byte, len(encoded))
	copy(corrupted, encoded)
	corrupted[0] ^= 0xFF

	_, _, err := record.Decode(corrupted)
	if err == nil {
		t.Error("Decode() with corrupted length returned nil error — corruption undetected")
	}
}

func BenchmarkEncode(b *testing.B) {
	data := bytes.Repeat([]byte{0xAB}, 128)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = record.Encode(data)
	}
}

// BenchmarkDecode to measure the cost of decoding a pre-encoded record.
func BenchmarkDecode(b *testing.B) {
	encoded := record.Encode(bytes.Repeat([]byte{0xAB}, 128))
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, _ = record.Decode(encoded)
	}
}
