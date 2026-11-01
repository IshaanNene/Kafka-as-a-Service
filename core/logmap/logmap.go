// Package logmap maps a partition's log from its record batch headers:
// which offsets hold records, which were compacted away, which belong to
// transactions that committed or aborted, and how many bytes each range
// takes. Kafka doesn't show its segment files over the protocol, so the
// map is by offset; the batches are the log's own.
package logmap

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/core/offsets"
)

// What a batch holds.
const (
	KindRecords   = iota // plain records
	KindCommitted        // records of a transaction that committed
	KindAborted          // records of a transaction that aborted
	KindOpen             // records of a transaction not ended yet
	KindMarker           // a transaction's commit or abort marker
)

// Batch is one record batch's header.
type Batch struct {
	Base, Last int64 // the offsets it spans
	Records    int   // fewer than Last-Base+1 once compaction removed some
	Bytes      int
	MaxTime    time.Time
	Codec      int // 0 none, 1 gzip, 2 snappy, 3 lz4, 4 zstd
	PID        int64
	Txn        bool
	Control    bool
	Commit     bool // a control batch's marker: commit, else abort
	Kind       int
}

// Codecs names compression codecs by their attribute bits.
var Codecs = []string{"none", "gzip", "snappy", "lz4", "zstd"}

// ParseBatches reads the v2 batch headers in b, stopping at a batch cut
// short by the fetch's size limit.
func ParseBatches(b []byte) []Batch {
	var out []Batch
	for len(b) >= 61 {
		size := int(int32(binary.BigEndian.Uint32(b[8:])))
		if size <= 0 || 12+size > len(b) {
			break
		}
		if b[16] != 2 {
			b = b[12+size:] // an old message set: not mapped
			continue
		}
		if 12+size < 61 {
			break // shorter than a v2 header: corrupt, and its fields would run past it
		}
		attrs := binary.BigEndian.Uint16(b[21:])
		base := int64(binary.BigEndian.Uint64(b))
		bt := Batch{
			Base:    base,
			Last:    base + int64(int32(binary.BigEndian.Uint32(b[23:]))),
			Records: int(int32(binary.BigEndian.Uint32(b[57:]))),
			Bytes:   12 + size,
			MaxTime: time.UnixMilli(int64(binary.BigEndian.Uint64(b[35:]))),
			Codec:   int(attrs & 7),
			PID:     int64(binary.BigEndian.Uint64(b[43:])),
			Txn:     attrs&0x10 != 0,
			Control: attrs&0x20 != 0,
		}
		if bt.Control {
			bt.Kind, bt.Commit = KindMarker, markerCommits(b[61:12+size])
		}
		out = append(out, bt)
		b = b[12+size:]
	}
	return out
}

// markerCommits reads a control record's key, which says commit (1) or
// abort (0). A record is its length, one byte of attributes, timestamp
// and offset deltas and its key's length, as varints, then the key: a
// version and a type, two bytes each.
func markerCommits(rec []byte) bool {
	varint := func() bool {
		_, n := binary.Varint(rec)
		if n <= 0 {
			return false
		}
		rec = rec[n:]
		return true
	}
	if !varint() || len(rec) < 1 { // length
		return false
	}
	rec = rec[1:] // attributes
	for range 3 { // timestamp delta, offset delta, key length
		if !varint() {
			return false
		}
	}
	if len(rec) < 4 {
		return false
	}
	return binary.BigEndian.Uint16(rec[2:]) == 1
}

// classify sets each transactional batch's kind from the marker that
// ends its producer's transaction.
func classify(bs []Batch) {
	open := map[int64][]int{}
	for i := range bs {
		b := &bs[i]
		switch {
		case b.Control:
			kind := KindAborted
			if b.Commit {
				kind = KindCommitted
			}
			for _, j := range open[b.PID] {
				bs[j].Kind = kind
			}
			delete(open, b.PID)
		case b.Txn:
			b.Kind = KindOpen
			open[b.PID] = append(open[b.PID], i)
		default:
			b.Kind = KindRecords
		}
	}
}

// Map is what was read of one partition's log.
type Map struct {
	Topic      string
	Partition  int32
	Start, End int64 // the log start and the high watermark
	Batches    []Batch
	Sampled    bool // read in samples, one per cell, not whole
	Read       int64
	Fetches    int
	Took       time.Duration
}

// Progress is a read so far, for a screen to show.
type Progress struct {
	At      int64 // the offset read up to, or the sample taken
	Fetches int
	Read    int64
}

// fetchBytes is how much each fetch asks for: a whole read takes a
// megabyte at a time, a sample one batch's worth.
const (
	fetchBytes  = 1 << 20
	sampleBytes = 64 << 10
)

// Read maps r's partition from start to end: the whole of it when budget
// bytes cover size, else one sample of a batch or so at each of cells
// offsets spread across it.
func Read(ctx context.Context, r *offsets.Reader, topic string, partition int32, start, end, size, budget int64, cells int, step func(Progress)) (Map, error) {
	m := Map{Topic: topic, Partition: partition, Start: start, End: end}
	t0 := time.Now()
	read := func(at int64, n int32) ([]Batch, error) {
		raw, err := r.Batches(ctx, at, n)
		if err != nil {
			return nil, fmt.Errorf("reading %s p%d at %d: %w", topic, partition, at, err)
		}
		m.Fetches++
		m.Read += int64(len(raw))
		return ParseBatches(raw), nil
	}
	if size <= budget || cells <= 0 {
		for at := start; at < end; {
			bs, err := read(at, fetchBytes)
			if err != nil {
				return m, err
			}
			moved := false
			for _, b := range bs {
				if b.Last >= at {
					m.Batches = append(m.Batches, b)
					at, moved = b.Last+1, true
				}
			}
			if !moved {
				break // nothing more before the end
			}
			if step != nil {
				step(Progress{At: at, Fetches: m.Fetches, Read: m.Read})
			}
		}
	} else {
		m.Sampled = true
		seen := map[int64]bool{}
		span := end - start
		cells = int(min(int64(cells), max(span, 1))) // no more samples than offsets
		for i := range cells {
			at := start + span*int64(i)/int64(cells)
			bs, err := read(at, sampleBytes)
			if err != nil {
				return m, err
			}
			next := start + span*int64(i+1)/int64(cells)
			for _, b := range bs {
				if b.Base >= next {
					break
				}
				if !seen[b.Base] {
					seen[b.Base] = true
					m.Batches = append(m.Batches, b)
				}
			}
			if step != nil {
				step(Progress{At: at, Fetches: m.Fetches, Read: m.Read})
			}
		}
		sort.Slice(m.Batches, func(i, j int) bool { return m.Batches[i].Base < m.Batches[j].Base })
	}
	classify(m.Batches)
	m.Took = time.Since(t0)
	return m, nil
}

// Estimate guesses the log's size from its first batches: their bytes an
// offset, times the offsets. For when the broker won't say how big the
// log is on disk.
func Estimate(ctx context.Context, r *offsets.Reader, start, end int64) (int64, error) {
	if end <= start {
		return 0, nil
	}
	raw, err := r.Batches(ctx, start, sampleBytes)
	if err != nil {
		return 0, err
	}
	var bytes, offs int64
	for _, b := range ParseBatches(raw) {
		bytes += int64(b.Bytes)
		offs += b.Last + 1 - max(b.Base, start)
	}
	if offs <= 0 {
		return 0, nil
	}
	return bytes * (end - start) / offs, nil
}
