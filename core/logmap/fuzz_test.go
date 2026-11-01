package logmap

import "testing"

// FuzzParseBatches feeds fetch bytes as a broker, or a corrupt log,
// might send them: it must never panic, and every batch it reads must
// lie within the bytes it was given.
func FuzzParseBatches(f *testing.F) {
	f.Add(batch(0, 9, 10, 0, -1, nil))
	f.Add(append(batch(100, 4, 5, 0x10, 7, nil), batch(105, 0, 1, 0x30, 7, marker(true))...))
	f.Add(batch(0, 0, 1, 0x20, 7, marker(false))[:40]) // cut short
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		total := 0
		for _, bt := range ParseBatches(b) {
			if bt.Bytes < 61 {
				t.Fatalf("a %d-byte batch", bt.Bytes)
			}
			total += bt.Bytes
		}
		if total > len(b) {
			t.Fatalf("batches of %d bytes from %d", total, len(b))
		}
	})
}
