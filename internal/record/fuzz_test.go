package record_test

import (
	"bytes"
	"testing"

	"github.com/blazingkevin/wal/internal/record"
)

func FuzzEncodeDecode(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF})
	f.Add([]byte("hello, world"))
	f.Add([]byte{0x00, 0xFF, 0x01, 0xFE, 0x7F})
	f.Add(bytes.Repeat([]byte{0xAB}, 128))

	f.Fuzz(func(t *testing.T, data []byte) {
		encoded := record.Encode(data)

		// First invariant...encoded length is always exactly HeaderSize + len(data).
		if len(encoded) != record.HeaderSize+len(data) {
			t.Fatalf("Encode(%x): len = %d, want %d",
				data, len(encoded), record.HeaderSize+len(data))
		}

		got, n, err := record.Decode(encoded)

		// Second invariant... Decode(Encode(x)) must never return an error.
		if err != nil {
			t.Fatalf("Decode(Encode(%x)) error = %v", data, err)
		}

		// Third invariant ... bytes consumed must equal total encoded length.
		if n != len(encoded) {
			t.Fatalf("Decode(Encode(%x)): n = %d, want %d",
				data, n, len(encoded))
		}

		// Fourth invariant.. decoded data must exactly equal original input.
		if !bytes.Equal(got, data) {
			t.Fatalf("Decode(Encode(%x)) = %x, want original", data, got)
		}
	})
}

func FuzzDecode(f *testing.F) {
	// Seed a combinaation of valid records, truncated records, and raw garbage.
	f.Add(record.Encode([]byte{}))
	f.Add(record.Encode([]byte("hello")))
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00, 0x00, 0x05})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0x00})
	f.Add(make([]byte, 1024))

	f.Fuzz(func(t *testing.T, b []byte) {
		_, _, _ = record.Decode(b)
	})
}
