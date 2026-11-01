package sniff

import (
	"encoding/binary"
	"testing"
)

// The proxy reads every frame a client or broker sends; malformed ones
// must pass through without taking it down.

func FuzzBatchFields(f *testing.F) {
	f.Add(make([]byte, 61))
	f.Add(append(make([]byte, 16), 2))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		_ = batchRecords(b)
		_, _ = batchTime(b)
	})
}

func FuzzSkipTags(f *testing.F) {
	f.Add([]byte{0})
	f.Add(binary.AppendUvarint(binary.AppendUvarint(binary.AppendUvarint(nil, 1), 0), 3))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01})
	f.Fuzz(func(t *testing.T, b []byte) {
		rest, ok := skipTags(b)
		if ok && len(rest) > len(b) {
			t.Fatalf("%d bytes left of %d", len(rest), len(b))
		}
	})
}
