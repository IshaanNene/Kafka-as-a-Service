package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/logmap"
	"github.com/IshaanNene/Kafka-as-a-Service/core/offsets"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newSegmentsCmd(brokers, themeName *string) *cobra.Command {
	var (
		output, budget string
		part           int32
	)
	cmd := &cobra.Command{
		Use:   "segments <topic>",
		Short: "Map a partition's log: records, compaction, holes, transactions",
		Long: `Map one partition's log from its record batches, as a grid of cells,
each a range of offsets from the log start to the end.

A cell is solid where every offset still holds a record and thins out
where compaction removed some; a dot is a hole, offsets no batch covers.
Colours say what the batches hold: plain records, transactions that
committed or aborted, ones still open, and their markers. The panel adds
the records, the bytes on disk, the holes, the codecs, and roughly how
many segment files that makes.

Kafka doesn't show segment files over its protocol, so the map is by
offset, read from the batch headers the leader serves. A log that fits
in --budget is read whole; a bigger one is sampled, one small fetch a
cell, and then holes can't be counted and records are estimated.

Without -p, kaas maps the partition with the most bytes on disk.
Nothing is consumed or committed.`,
		Example: `  # The biggest partition of orders
  kaas segments orders
  # A compacted topic's partition 3
  kaas segments users -p 3
  # Read up to 1GB whole rather than sampling
  kaas segments orders --budget 1GB
  # For a script
  kaas segments orders -o json | jq '{density, holes}'`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return usageError(cmd, "topic", "", "required: the topic to map")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "text", "json"); err != nil {
				return err
			}
			b, err := parseSize(budget)
			if err != nil {
				return usageError(cmd, "budget", budget, err.Error())
			}
			if _, err := connect(cmd, *brokers, *themeName); err != nil {
				return err
			}
			cl, err := kgo.NewClient(kgo.SeedBrokers(splitSeeds(*brokers)...))
			if err != nil {
				return err
			}
			defer cl.Close()
			s := segmenter{topic: args[0], part: -1, budget: int64(b)}
			if cmd.Flags().Changed("partition") {
				s.part = part
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			err = s.load(ctx, cmd, kadm.NewClient(cl))
			cancel()
			if err != nil {
				return err
			}
			return s.run(cmd, cl, detect(cmd.OutOrStdout(), *themeName), output)
		},
	}
	fl := cmd.Flags()
	fl.Int32VarP(&part, "partition", "p", 0, "the partition to map (default: the most bytes on disk)")
	fl.StringVar(&budget, "budget", "64MB", "read a log this size or smaller whole; sample a bigger one")
	fl.StringVarP(&output, "output", "o", "text", "output format: text|json")
	return cmd
}

// segmenter is one kaas segments: the partition, what the cluster says
// about it, and the map read from its batches.
type segmenter struct {
	topic      string
	part       int32 // -1 until chosen
	budget     int64
	start, end int64
	size       int64 // on the leader's disk; -1 unknown
	estimated  bool  // size is a guess from the first batches
	cleanup    string
	segBytes   int64 // segment.bytes; 0 unknown
	leader     int32
	mw, mh     int  // the grid: a cell of the map each
	sample     bool // too big for the budget: read in samples
	m          logmap.Map
	reads      []screens.HeapLine
}

// load checks the topic, picks the partition, and asks for its offsets,
// size and configs.
func (s *segmenter) load(ctx context.Context, cmd *cobra.Command, adm *kadm.Client) error {
	td, err := adm.ListTopics(ctx)
	if err != nil {
		return err
	}
	t, ok := td[s.topic]
	if !ok {
		describe := func(n string) string { return plural(len(td[n].Partitions), "partition") }
		return notFoundError("topic", s.topic, "the cluster", td.Names(), describe, "kaas topics")
	}
	if s.part >= 0 {
		if _, ok := t.Partitions[s.part]; !ok {
			return usageError(cmd, "partition", strconv.Itoa(int(s.part)), fmt.Sprintf("%s has partitions 0 to %d", s.topic, len(t.Partitions)-1))
		}
	}
	// Sizes on each partition's leader, to pick the biggest and to judge
	// the budget. Log dirs need DESCRIBE on the cluster: without them
	// every size is unknown and the most offsets win.
	sizes := map[int32]int64{}
	if dirs, err := adm.DescribeAllLogDirs(ctx, kadm.TopicsSet{s.topic: {}}); err == nil {
		dirs.Each(func(d kadm.DescribedLogDir) {
			for id, p := range d.Topics[s.topic] {
				if lp, ok := t.Partitions[id]; ok && lp.Leader == d.Broker {
					sizes[id] += p.Size
				}
			}
		})
	}
	starts, err := adm.ListStartOffsets(ctx, s.topic)
	if err != nil {
		return err
	}
	ends, err := adm.ListEndOffsets(ctx, s.topic)
	if err != nil {
		return err
	}
	if s.part < 0 {
		best, bestSpan := int64(-1), int64(-1)
		for _, p := range t.Partitions.Sorted() {
			st, _ := starts.Lookup(s.topic, p.Partition)
			en, _ := ends.Lookup(s.topic, p.Partition)
			sz, span := sizes[p.Partition], en.Offset-st.Offset
			if sz > best || sz == best && span > bestSpan {
				s.part, best, bestSpan = p.Partition, sz, span
			}
		}
	}
	st, _ := starts.Lookup(s.topic, s.part)
	en, _ := ends.Lookup(s.topic, s.part)
	if st.Err != nil || en.Err != nil {
		return fmt.Errorf("%s p%d: %w", s.topic, s.part, errors.Join(st.Err, en.Err))
	}
	s.start, s.end, s.size = st.Offset, en.Offset, -1
	if sz, ok := sizes[s.part]; ok {
		s.size = sz
	}
	if rcs, err := adm.DescribeTopicConfigs(ctx, s.topic); err == nil {
		for _, rc := range rcs {
			for _, c := range rc.Configs {
				if c.Value == nil {
					continue
				}
				switch c.Key {
				case "cleanup.policy":
					s.cleanup = *c.Value
				case "segment.bytes":
					s.segBytes, _ = strconv.ParseInt(*c.Value, 10, 64)
				}
			}
		}
	}
	return nil
}

func (s *segmenter) run(cmd *cobra.Command, cl *kgo.Client, surf surface, output string) error {
	rich := surf.rich && output == "text"
	k := surf.kit
	r, err := offsets.NewReader(cmd.Context(), cl, s.topic, s.part)
	if err != nil {
		return err
	}
	s.leader = r.Leader()
	height := min(max(surf.height-1, 24), 28)
	s.mw, s.mh = 48, 20
	if rich {
		s.mw, s.mh = screens.HeapGrid(surf.width, height)
	}
	var lv *live
	if rich {
		lv = &live{s: surf}
		defer lv.done()
		lv.draw(s.screen(nil).Render(k, surf.width, height))
	}
	size := s.size
	if size < 0 { // the broker didn't say: guess from the first batches
		if size, err = logmap.Estimate(cmd.Context(), r, s.start, s.end); err != nil {
			return err
		}
		s.size, s.estimated = size, true
	}
	s.sample = size > s.budget
	var last logmap.Progress
	step := func(p logmap.Progress) {
		s.note(p, last)
		last = p
		if rich {
			lv.draw(s.screen(&p).Render(k, surf.width, height))
		}
	}
	s.m, err = logmap.Read(cmd.Context(), r, s.topic, s.part, s.start, s.end, size, s.budget, s.mw*s.mh, step)
	if err != nil {
		return err
	}
	if rich {
		lv.draw(s.screen(nil).Render(k, surf.width, height))
		return nil
	}
	return s.print(cmd, output)
}

// readsShown is how many reads the log keeps.
const readsShown = 6

// note adds a read to the log, as the piece's calls.
func (s *segmenter) note(p, last logmap.Progress) {
	verb, role := "fetch", kit.OK
	if s.sample {
		verb, role = "sample", kit.Notice
	}
	line := fmt.Sprintf("%-6s @%-9s %8s", verb, screens.Num(float64(p.At)), screens.Bytes(p.Read-last.Read))
	s.reads = append(s.reads, screens.HeapLine{Text: line, Role: role})
	if len(s.reads) > readsShown {
		s.reads = s.reads[len(s.reads)-readsShown:]
	}
}

// cellKind is the screen's kind for a cell of the map.
func cellKind(c logmap.Cell) int {
	switch c.Kind {
	case logmap.KindCommitted:
		return 2
	case logmap.KindAborted:
		return 3
	case logmap.KindOpen:
		return 4
	case logmap.KindMarker:
		return 5
	}
	if c.Density() < 0.9 {
		return 1
	}
	return 0
}

// txns counts the transactions the map's markers end, and those open.
func txns(m logmap.Map) (commit, abort, open int) {
	seen := map[int64]bool{}
	for _, b := range m.Batches {
		switch {
		case b.Control && b.Commit:
			commit++
		case b.Control:
			abort++
		case b.Kind == logmap.KindOpen && !seen[b.PID]:
			seen[b.PID] = true
			open++
		}
	}
	return commit, abort, open
}

// codecMix names the codecs batches use, the most first: "lz4 92%".
func codecMix(st logmap.Stats) string {
	if st.Batches == 0 {
		return "-"
	}
	best := 0
	for i, n := range st.Codecs {
		if n > st.Codecs[best] {
			best = i
		}
	}
	if st.Codecs[best] == st.Batches {
		return logmap.Codecs[best]
	}
	next := -1
	for i, n := range st.Codecs {
		if i != best && n > 0 && (next < 0 || n > st.Codecs[next]) {
			next = i
		}
	}
	return fmt.Sprintf("%s %d%% · %s", logmap.Codecs[best], st.Codecs[best]*100/st.Batches, logmap.Codecs[next])
}

// segments is roughly how many segment files the log takes, from its size
// and segment.bytes: at least one, the active one.
func (s *segmenter) segments() string {
	if s.size < 0 || s.segBytes <= 0 {
		return "-"
	}
	n := max(1, (s.size+s.segBytes-1)/s.segBytes)
	return fmt.Sprintf("≈%d × %s", n, screens.Bytes(s.segBytes))
}

// screen is the heap-map piece for the map, or for a read in progress.
func (s *segmenter) screen(p *logmap.Progress) screens.HeapMap {
	title := fmt.Sprintf("log · %s p%d · leader %d", s.topic, s.part, s.leader)
	if s.cleanup != "" {
		title += " · " + s.cleanup
	}
	span := s.end - s.start
	h := screens.HeapMap{
		Title:  title,
		Reads:  s.reads,
		Legend: "· hole   █ records   ▓▒░ thinned by compaction   ▌ run end",
	}
	sizeLine := screens.Bytes(s.size)
	if s.estimated {
		sizeLine = "≈" + sizeLine + " (est.)"
	}
	switch {
	case span == 0:
		h.Range = fmt.Sprintf("empty: the log starts and ends at %s", screens.Num(float64(s.end)))
	case p != nil:
		h.Range = fmt.Sprintf("reading %s – %s · at %s", screens.Num(float64(s.start)), screens.Num(float64(s.end)), screens.Num(float64(p.At)))
	default:
		cells := s.m.Cells(s.mw * s.mh)
		per := screens.Num(float64(span) / float64(max(1, len(cells))))
		h.Range = fmt.Sprintf("offsets %s – %s · %s a cell", screens.Num(float64(s.start)), screens.Num(float64(s.end)), per)
	}
	if p != nil || s.m.Fetches == 0 && span > 0 {
		h.Stats = []screens.HeapStat{
			{Label: "offsets", Value: screens.Num(float64(span)), Role: kit.FG},
			{Label: "on disk", Value: sizeLine, Role: kit.FG},
		}
		if p != nil {
			h.Stats = append(h.Stats, screens.HeapStat{Label: "read", Value: screens.Bytes(p.Read) + " · " + plural(p.Fetches, "fetch"), Role: kit.Notice})
		}
		h.Foot = "reading the batch headers from the leader…"
		return h
	}
	st := s.m.Stats()
	for _, c := range s.m.Cells(s.mw * s.mh) {
		h.Cells = append(h.Cells, screens.HeapCell{Hole: c.Hole(), Density: c.Density(), Kind: cellKind(c)})
	}
	records, holes := screens.Num(float64(st.Records)), "unknown: sampled"
	if s.m.Sampled {
		records = "≈" + records
	} else {
		holes = strconv.Itoa(st.Holes)
		if st.Holes > 0 {
			holes += " · max " + screens.Num(float64(st.LargestHole))
		}
	}
	holeRole := kit.FG
	if st.Holes > 0 {
		holeRole = kit.Warn
	}
	commit, abort, open := txns(s.m)
	tx := fmt.Sprintf("%d ok · %d aborted", commit, abort)
	if open > 0 {
		tx += fmt.Sprintf(" · %d open", open)
	}
	txRole := kit.Accent2
	if commit+abort+open == 0 {
		tx, txRole = "none", kit.Muted
	} else if abort > 0 {
		txRole = kit.Err
	}
	gone := 1 - st.Density
	goneRole := kit.FG
	if gone >= 0.005 {
		goneRole = kit.Notice
	}
	h.Stats = []screens.HeapStat{
		{Label: "offsets", Value: screens.Num(float64(st.Offsets)), Role: kit.FG},
		{Label: "records", Value: records, Role: kit.Accent},
		{Label: "on disk", Value: sizeLine, Role: kit.FG},
		{Label: "batches", Value: screens.Num(float64(st.Batches)), Role: kit.FG},
		{Label: "holes", Value: holes, Role: holeRole},
		{Label: "txns", Value: tx, Role: txRole},
		{Label: "codec", Value: codecMix(st), Role: kit.FG},
		{Label: "segments", Value: s.segments(), Role: kit.Muted},
		{Label: "no record", Value: fmt.Sprintf("%.0f%% of offsets", gone*100), Role: goneRole},
	}
	h.Bar = gone
	how := "read whole"
	if s.m.Sampled {
		how = "sampled, one fetch a cell"
	}
	h.Foot = fmt.Sprintf("%s: %s in %s · %s   cells are offset ranges, not segment files",
		how, screens.Bytes(s.m.Read), plural(s.m.Fetches, "fetch"), took(s.m.Took))
	if span == 0 {
		h.Foot = "nothing to map"
	}
	return h
}

type segmentsJSON struct {
	Topic         string         `json:"topic"`
	Partition     int32          `json:"partition"`
	Leader        int32          `json:"leader"`
	Start         int64          `json:"startOffset"`
	End           int64          `json:"endOffset"`
	SizeBytes     *int64         `json:"sizeBytes"` // null: unknown
	SizeEstimated bool           `json:"sizeEstimated,omitempty"`
	CleanupPolicy string         `json:"cleanupPolicy,omitempty"`
	SegmentBytes  int64          `json:"segmentBytes,omitempty"`
	Sampled       bool           `json:"sampled"`
	Records       int64          `json:"records"`
	Density       float64        `json:"density"`
	Batches       int            `json:"batches"`
	Holes         *int           `json:"holes"` // null when sampled
	HoleOffsets   int64          `json:"holeOffsets"`
	LargestHole   int64          `json:"largestHole"`
	Transactions  map[string]int `json:"transactions"`
	Codecs        map[string]int `json:"codecs"`
	Oldest        *time.Time     `json:"oldest,omitempty"`
	Newest        *time.Time     `json:"newest,omitempty"`
	Fetches       int            `json:"fetches"`
	ReadBytes     int64          `json:"readBytes"`
	Cells         []cellJSON     `json:"cells"`
}

type cellJSON struct {
	From    int64   `json:"from"`
	To      int64   `json:"to"`
	Records float64 `json:"records"`
	Bytes   float64 `json:"bytes"`
	Kind    string  `json:"kind"`
}

var kindNames = []string{"records", "committed", "aborted", "open", "marker"}

// print is the map without the screen: a line a stat, or JSON.
func (s *segmenter) print(cmd *cobra.Command, output string) error {
	st := s.m.Stats()
	commit, abort, open := txns(s.m)
	j := segmentsJSON{
		Topic: s.topic, Partition: s.part, Leader: s.leader, Start: s.start, End: s.end,
		CleanupPolicy: s.cleanup, SegmentBytes: s.segBytes, Sampled: s.m.Sampled,
		Records: st.Records, Density: st.Density, Batches: st.Batches,
		HoleOffsets: st.HoleOffsets, LargestHole: st.LargestHole,
		Transactions: map[string]int{"committed": commit, "aborted": abort, "open": open},
		Codecs:       map[string]int{}, Fetches: s.m.Fetches, ReadBytes: s.m.Read, Cells: []cellJSON{},
	}
	if s.size >= 0 {
		j.SizeBytes, j.SizeEstimated = &s.size, s.estimated
	}
	if !s.m.Sampled {
		j.Holes = &st.Holes
	}
	for i, n := range st.Codecs {
		if n > 0 {
			j.Codecs[logmap.Codecs[i]] = n
		}
	}
	if !st.Oldest.IsZero() {
		o, n := st.Oldest.UTC(), st.Newest.UTC()
		j.Oldest, j.Newest = &o, &n
	}
	for _, c := range s.m.Cells(s.mw * s.mh) {
		kind := kindNames[c.Kind]
		if c.Hole() {
			kind = "hole"
		}
		j.Cells = append(j.Cells, cellJSON{From: c.From, To: c.To, Records: c.Records, Bytes: c.Bytes, Kind: kind})
	}
	out := cmd.OutOrStdout()
	if output == "json" {
		return json.NewEncoder(out).Encode(j)
	}
	holes := "unknown (sampled)"
	if j.Holes != nil {
		holes = strconv.Itoa(*j.Holes)
	}
	size := "-"
	if j.SizeBytes != nil {
		size = strconv.FormatInt(*j.SizeBytes, 10)
		if j.SizeEstimated {
			size += " (estimated)"
		}
	}
	var codecs []string
	for _, c := range logmap.Codecs {
		if n := j.Codecs[c]; n > 0 {
			codecs = append(codecs, c+"="+strconv.Itoa(n))
		}
	}
	slices.Sort(codecs)
	for _, kv := range [][2]string{
		{"topic", s.topic}, {"partition", strconv.Itoa(int(s.part))}, {"leader", strconv.Itoa(int(s.leader))},
		{"offsets", fmt.Sprintf("%d-%d", s.start, s.end)}, {"size", size},
		{"sampled", strconv.FormatBool(j.Sampled)}, {"records", strconv.FormatInt(j.Records, 10)},
		{"density", strconv.FormatFloat(j.Density, 'f', 4, 64)}, {"batches", strconv.Itoa(j.Batches)},
		{"holes", holes}, {"largest_hole", strconv.FormatInt(j.LargestHole, 10)},
		{"transactions", fmt.Sprintf("committed=%d aborted=%d open=%d", commit, abort, open)},
		{"codecs", orDash(strings.Join(codecs, " "))},
	} {
		fmt.Fprintf(out, "%-13s %s\n", kv[0], kv[1])
	}
	return nil
}
