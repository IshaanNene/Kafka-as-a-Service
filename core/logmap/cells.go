package logmap

import "time"

// Cell is a range of offsets on the map.
type Cell struct {
	From, To int64   // [From, To)
	Covered  int64   // offsets inside batches read
	Records  float64 // a batch's records and bytes are shared out by the offsets it spans
	Bytes    float64
	Kind     int  // what most of its offsets hold
	Sampled  bool // read in a sample: Covered is the sample's
}

// Hole is whether no batch read covers any of the cell.
func (c Cell) Hole() bool { return c.Covered == 0 }

// Density is the share of the cell's offsets that still hold a record,
// as far as the batches read say.
func (c Cell) Density() float64 {
	n := c.To - c.From
	if c.Sampled {
		n = c.Covered
	}
	if n <= 0 {
		return 0
	}
	return c.Records / float64(n)
}

// Cells cuts the map into n ranges of offsets, as even as they divide.
func (m Map) Cells(n int) []Cell {
	span := m.End - m.Start
	if n <= 0 || span <= 0 {
		return nil
	}
	n = int(min(int64(n), span))
	cells := make([]Cell, n)
	for i := range cells {
		cells[i] = Cell{From: m.Start + span*int64(i)/int64(n), To: m.Start + span*int64(i+1)/int64(n), Sampled: m.Sampled}
	}
	kinds := make([][KindMarker + 1]int64, n)
	first := 0
	for _, b := range m.Batches {
		lo, hi := max(b.Base, m.Start), min(b.Last+1, m.End)
		if hi <= lo {
			continue
		}
		whole := float64(b.Last + 1 - b.Base)
		for first < n && cells[first].To <= lo {
			first++
		}
		for i := first; i < n && cells[i].From < hi; i++ {
			c := &cells[i]
			ov := min(hi, c.To) - max(lo, c.From)
			if ov <= 0 {
				continue
			}
			c.Covered += ov
			c.Records += float64(b.Records) * float64(ov) / whole
			c.Bytes += float64(b.Bytes) * float64(ov) / whole
			kinds[i][b.Kind] += ov
		}
	}
	for i := range cells {
		for k, v := range kinds[i] {
			if v > kinds[i][cells[i].Kind] {
				cells[i].Kind = k
			}
		}
	}
	return cells
}

// Stats sums up a map.
type Stats struct {
	Offsets     int64
	Records     int64 // counted; estimated from the samples when sampled
	Density     float64
	Bytes       int64 // of the batches read
	Batches     int
	Holes       int   // runs of offsets no batch covers; -1 when sampled
	HoleOffsets int64 // offsets in them
	LargestHole int64
	ByKind      [KindMarker + 1]int // batches of each kind
	Codecs      [5]int              // batches by codec
	Oldest      time.Time           // the earliest and latest batch times
	Newest      time.Time
}

// Stats sums up the map.
func (m Map) Stats() Stats {
	s := Stats{Offsets: m.End - m.Start, Batches: len(m.Batches)}
	var covered int64
	var records float64
	gap := func(from, to int64) { // [from, to)
		if to > from {
			s.Holes++
			s.HoleOffsets += to - from
			s.LargestHole = max(s.LargestHole, to-from)
		}
	}
	at := m.Start
	for _, b := range m.Batches {
		s.Bytes += int64(b.Bytes)
		s.ByKind[b.Kind]++
		if b.Codec < len(s.Codecs) {
			s.Codecs[b.Codec]++
		}
		if !b.MaxTime.IsZero() && b.MaxTime.UnixMilli() > 0 {
			if s.Oldest.IsZero() || b.MaxTime.Before(s.Oldest) {
				s.Oldest = b.MaxTime
			}
			if b.MaxTime.After(s.Newest) {
				s.Newest = b.MaxTime
			}
		}
		// A batch the log start or end cuts counts by the share inside.
		lo, hi := max(b.Base, m.Start), min(b.Last+1, m.End)
		if hi <= lo {
			continue
		}
		covered += hi - lo
		records += float64(b.Records) * float64(hi-lo) / float64(b.Last+1-b.Base)
		if !m.Sampled {
			gap(at, lo)
			at = max(at, hi)
		}
	}
	if m.Sampled {
		s.Holes = -1
		if covered > 0 {
			s.Density = records / float64(covered)
		}
		s.Records = int64(s.Density * float64(s.Offsets))
		return s
	}
	gap(at, m.End)
	s.Records = int64(records + 0.5)
	if s.Offsets > 0 {
		s.Density = records / float64(s.Offsets)
	}
	return s
}
