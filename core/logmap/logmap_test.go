package logmap

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/IshaanNene/Kafka-as-a-Service/core/offsets"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

// batch encodes a v2 batch header (and records, for a marker).
func batch(base int64, lastDelta, records int32, attrs int16, pid int64, body []byte) []byte {
	rb := kmsg.RecordBatch{FirstOffset: base, Magic: 2, Attributes: attrs, LastOffsetDelta: lastDelta,
		ProducerID: pid, NumRecords: records, Records: body, FirstTimestamp: 1, MaxTimestamp: 2}
	b := rb.AppendTo(nil)
	rb.Length = int32(len(b) - 12)
	return rb.AppendTo(nil)
}

// marker is a control record whose key says commit or abort.
func marker(commit bool) []byte {
	typ := byte(0)
	if commit {
		typ = 1
	}
	var r []byte
	r = append(r, 0)               // attributes
	r = binary.AppendVarint(r, 0)  // timestamp delta
	r = binary.AppendVarint(r, 0)  // offset delta
	r = binary.AppendVarint(r, 4)  // key length
	r = append(r, 0, 0, 0, typ)    // version 0, type
	r = binary.AppendVarint(r, -1) // no value
	r = binary.AppendVarint(r, 0)  // no headers
	return append(binary.AppendVarint(nil, int64(len(r))), r...)
}

func TestParseAndCells(t *testing.T) {
	var raw []byte
	raw = append(raw, batch(0, 9, 10, 0, -1, nil)...)             // 0-9 plain
	raw = append(raw, batch(10, 9, 4, 0, -1, nil)...)             // 10-19, compacted to 4
	raw = append(raw, batch(30, 4, 5, 0x10, 7, nil)...)           // 30-34 txn by pid 7; 20-29 a hole
	raw = append(raw, batch(35, 0, 1, 0x30, 7, marker(false))...) // its abort marker
	raw = append(raw, batch(36, 3, 4, 0x13, 8, nil)...)           // 36-39 txn by pid 8, lz4
	raw = append(raw, batch(40, 0, 1, 0x30, 8, marker(true))...)  // its commit
	cut := batch(41, 9, 10, 0, -1, nil)
	raw = append(raw, cut[:30]...) // cut short by the fetch: dropped
	bs := ParseBatches(raw)
	if len(bs) != 6 {
		t.Fatalf("%d batches", len(bs))
	}
	if !bs[3].Control || bs[3].Commit || !bs[5].Control || !bs[5].Commit || bs[4].Codec != 3 {
		t.Fatalf("markers %+v %+v, codec %d", bs[3], bs[5], bs[4].Codec)
	}
	classify(bs)
	if bs[2].Kind != KindAborted || bs[4].Kind != KindCommitted || bs[0].Kind != KindRecords || bs[3].Kind != KindMarker {
		t.Errorf("kinds %d %d %d %d", bs[2].Kind, bs[4].Kind, bs[0].Kind, bs[3].Kind)
	}

	m := Map{Start: 0, End: 41, Batches: bs}
	s := m.Stats()
	if s.Records != 10+4+5+1+4+1 || s.Holes != 1 || s.HoleOffsets != 10 || s.LargestHole != 10 || s.ByKind[KindAborted] != 1 || s.Codecs[3] != 1 {
		t.Errorf("stats %+v", s)
	}
	cells := m.Cells(4) // 0-9, 10-19, 20-29, 30-40
	if len(cells) != 4 || cells[0].Density() != 1 || cells[1].Density() != 0.4 || !cells[2].Hole() || cells[3].Kind != KindAborted {
		t.Errorf("cells %+v", cells)
	}
	if cells[3].From != 30 || cells[3].To != 41 {
		t.Errorf("last cell %d-%d", cells[3].From, cells[3].To)
	}
	// More cells than offsets: one an offset.
	if n := len(m.Cells(1000)); n != 41 {
		t.Errorf("%d cells", n)
	}
	// The log start in the middle of the first batch: half of it counts.
	if s := (Map{Start: 5, End: 10, Batches: bs[:1]}).Stats(); s.Records != 5 || s.Density != 1 || s.Holes != 0 {
		t.Errorf("cut %+v", s)
	}
	// Sampled: holes unknown, density from what was read.
	m.Sampled = true
	if s := m.Stats(); s.Holes != -1 || s.Records != int64(s.Density*41) {
		t.Errorf("sampled %+v", s)
	}
}

func TestRead(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	ctx := context.Background()
	pc, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.TransactionalID("ledger"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	plain, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	rec := func(v string) *kgo.Record { return &kgo.Record{Topic: "orders", Value: []byte(v)} }
	for i := range 20 {
		if err := plain.ProduceSync(ctx, rec("p"), rec("p")).FirstErr(); err != nil {
			t.Fatal(err)
		}
		commit := kgo.TryCommit
		if i%4 == 3 {
			commit = kgo.TryAbort
		}
		if err := pc.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		if err := pc.ProduceSync(ctx, rec("t"), rec("t"), rec("t")).FirstErr(); err != nil {
			t.Fatal(err)
		}
		if err := pc.EndTransaction(ctx, commit); err != nil {
			t.Fatal(err)
		}
	}
	r, err := offsets.NewReader(ctx, cl, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	end := int64(20 * (2 + 3 + 1))
	steps := 0
	m, err := Read(ctx, r, "orders", 0, 0, end, 1<<20, 64<<20, 48, func(Progress) { steps++ })
	if err != nil {
		t.Fatal(err)
	}
	s := m.Stats()
	if m.Sampled || s.Records != end || s.Holes != 0 || s.Density != 1 || steps == 0 {
		t.Errorf("whole: sampled %v, %+v", m.Sampled, s)
	}
	if s.ByKind[KindMarker] != 20 || s.ByKind[KindAborted] != 5 || s.ByKind[KindCommitted] != 15 || s.ByKind[KindRecords] != 20 {
		t.Errorf("kinds %v", s.ByKind)
	}
	// A budget below the size: sampled, one fetch a cell.
	sm, err := Read(ctx, r, "orders", 0, 0, end, 1<<20, 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !sm.Sampled || sm.Fetches != 10 || sm.Stats().Holes != -1 || len(sm.Batches) == 0 {
		t.Errorf("sampled: %v, %d fetches, %d batches", sm.Sampled, sm.Fetches, len(sm.Batches))
	}
	// More samples than offsets: one an offset at most.
	if many, _ := Read(ctx, r, "orders", 0, 0, end, 1<<20, 1, 1000, nil); many.Fetches != int(end) || m.Took <= 0 {
		t.Errorf("%d fetches for %d offsets, took %v", many.Fetches, end, m.Took)
	}
	if est, err := Estimate(ctx, r, 0, end); err != nil || est < m.Read/2 || est > m.Read*2 {
		t.Errorf("estimate %d for %d read: %v", est, m.Read, err)
	}
	for _, c := range sm.Cells(10) {
		if !c.Sampled || c.Hole() {
			t.Errorf("sampled cell %+v", c)
		}
	}
}
