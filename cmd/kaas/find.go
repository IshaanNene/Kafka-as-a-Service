package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/consume"
	"github.com/IshaanNene/Kafka-as-a-Service/core/offsets"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newOffsetsFindCmd(brokers, themeName *string) *cobra.Command {
	var (
		output, at, match string
		parts             []int32
	)
	cmd := &cobra.Command{
		Use:   "find <topic> (--time <when> | --match <regex>)",
		Short: "Bisect a topic for the offset at a time, or the first bad record",
		Long: `Find an offset by bisecting each partition: read the record in the middle
of what is left, keep the half the answer is in, and repeat. A million
records take about 20 reads.

--time finds the first record at or after a moment: 2026-10-31T09:00,
2026-10-31 09:00 (local unless it names a zone), or how long ago, like 2h.
kaas then asks the broker's time index the same question. They agree
unless producers stamped records out of order, and then kaas says where.

--match finds the first record whose value matches a regular expression,
for "where did the bad records start". A bisect assumes that once records
match, the ones after keep matching; kaas reads around the answer to
check.

Each read is one Fetch from the partition's leader. Nothing is consumed
or committed.`,
		Example: `  # Where 09:00 starts, on every partition
  kaas offsets find orders --time 2026-10-31T09:00
  # Then read from there
  kaas offsets reset billing -t orders --to 2026-10-31T09:00
  # The first record with the new schema
  kaas offsets find orders --match '"version":2' -p 0
  # For a script
  kaas offsets find orders --time 2h -o json | jq '.[] | {partition, offset}'`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return usageError(cmd, "topic", "", "required: the topic to search")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "text", "json"); err != nil {
				return err
			}
			f := finder{topic: args[0], cmdline: findCmdline(cmd, args[0])}
			switch {
			case at != "" && match != "":
				return usageError(cmd, "match", match, "--time and --match look for different things: use one")
			case at != "":
				t, err := offsets.ParseTo(at, time.Now())
				if err != nil || t.Kind != offsets.Time {
					msg := "want a time like 2026-10-31T09:00 or a duration back like 2h"
					if err != nil {
						msg = err.Error()
					}
					return usageError(cmd, "time", at, msg)
				}
				f.at = t.At
			case match != "":
				re, err := regexp.Compile(match)
				if err != nil {
					return usageError(cmd, "match", match, "not a regular expression: "+strings.TrimPrefix(err.Error(), "error parsing regexp: "))
				}
				f.re = re
			default:
				return usageError(cmd, "time", "", "required: --time to find a moment, or --match to find the first matching record")
			}
			if _, err := connect(cmd, *brokers, *themeName); err != nil {
				return err
			}
			cl, err := kgo.NewClient(kgo.SeedBrokers(splitSeeds(*brokers)...))
			if err != nil {
				return err
			}
			defer cl.Close()
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			err = f.load(ctx, cmd, kadm.NewClient(cl), parts)
			cancel()
			if err != nil {
				return err
			}
			return f.run(cmd, cl, detect(cmd.OutOrStdout(), *themeName), output)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&at, "time", "", "the first record at or after: a time, or a duration back like 2h")
	fl.StringVar(&match, "match", "", "the first record whose value matches this regular expression")
	fl.Int32SliceVarP(&parts, "partition", "p", nil, "only these partitions (repeatable; default all)")
	fl.StringVarP(&output, "output", "o", "text", "output format: text|json")
	return cmd
}

// findCmdline is the command as typed, for the top of the log.
func findCmdline(cmd *cobra.Command, topic string) string {
	parts := []string{"kaas offsets find", topic}
	cmd.Flags().Visit(func(f *pflag.Flag) {
		if f.Name == "brokers" || f.Name == "output" || f.Name == "theme" {
			return
		}
		v := f.Value.String()
		if f.Name == "partition" {
			v = strings.Trim(v, "[]")
		}
		if strings.ContainsAny(v, " \"'") {
			v = "'" + v + "'"
		}
		parts = append(parts, "--"+f.Name, v)
	})
	return strings.Join(parts, " ")
}

// finder is one kaas offsets find: what to look for, where, and what each
// partition's hunt found.
type finder struct {
	topic   string
	cmdline string
	at      time.Time      // --time
	re      *regexp.Regexp // --match
	parts   []int32
	starts  map[int32]int64
	ends    map[int32]int64
	index   map[int32]int64 // the broker's time index, -1 for none
	hunts   []offsets.Hunt
	log     []string
}

// load checks the topic and partitions and lists their offsets.
func (f *finder) load(ctx context.Context, cmd *cobra.Command, adm *kadm.Client, want []int32) error {
	td, err := adm.ListTopics(ctx)
	if err != nil {
		return err
	}
	t, ok := td[f.topic]
	if !ok {
		describe := func(n string) string { return plural(len(td[n].Partitions), "partition") }
		return notFoundError("topic", f.topic, "the cluster", td.Names(), describe, "kaas topics")
	}
	for _, p := range t.Partitions.Sorted() {
		if len(want) == 0 || slices.Contains(want, p.Partition) {
			f.parts = append(f.parts, p.Partition)
		}
	}
	for _, p := range want {
		if _, ok := t.Partitions[p]; !ok {
			return usageError(cmd, "partition", strconv.Itoa(int(p)), fmt.Sprintf("%s has partitions 0 to %d", f.topic, len(t.Partitions)-1))
		}
	}
	starts, err := adm.ListStartOffsets(ctx, f.topic)
	if err != nil {
		return err
	}
	ends, err := adm.ListEndOffsets(ctx, f.topic)
	if err != nil {
		return err
	}
	f.starts, f.ends, f.index = map[int32]int64{}, map[int32]int64{}, map[int32]int64{}
	for _, p := range f.parts {
		s, _ := starts.Lookup(f.topic, p)
		e, _ := ends.Lookup(f.topic, p)
		if s.Err != nil || e.Err != nil {
			return fmt.Errorf("%s p%d: %w", f.topic, p, errors.Join(s.Err, e.Err))
		}
		f.starts[p], f.ends[p] = s.Offset, e.Offset
	}
	if !f.at.IsZero() {
		idx, err := adm.ListOffsetsAfterMilli(ctx, f.at.UnixMilli(), f.topic)
		if err != nil {
			return err
		}
		for _, p := range f.parts {
			f.index[p] = -1
			if o, ok := idx.Lookup(f.topic, p); ok && o.Err == nil && o.Offset >= 0 && o.Offset < f.ends[p] {
				f.index[p] = o.Offset
			}
		}
	}
	return nil
}

// bad is the hunt's predicate.
func (f *finder) bad(r *kgo.Record) bool {
	if f.re != nil {
		return f.re.Match(r.Value)
	}
	return !r.Timestamp.Before(f.at)
}

// what is what the hunt looks for, for titles and log lines.
func (f *finder) what() string {
	if f.re != nil {
		return "first value matching /" + f.re.String() + "/"
	}
	return "first record at or after " + stamp(f.at)
}

// stamp is a time to the millisecond, or the second when that is all it has.
func stamp(t time.Time) string {
	if t.Nanosecond() == 0 {
		return t.Local().Format("2006-01-02 15:04:05")
	}
	return t.Local().Format("2006-01-02 15:04:05.000")
}

func (f *finder) run(cmd *cobra.Command, cl *kgo.Client, surf surface, output string) error {
	rich := surf.rich && output == "text"
	k := surf.kit
	f.log = append(f.log, k.S(kit.OK, "$ ")+k.S(kit.FG, f.cmdline))
	var lv *live
	if rich {
		lv = &live{s: surf}
		defer lv.done()
	}
	height := min(max(surf.height-1, 16), 26)
	for _, p := range f.parts {
		r, err := offsets.NewReader(cmd.Context(), cl, f.topic, p)
		if err != nil {
			return err
		}
		start, end := f.starts[p], f.ends[p]
		step := func(h offsets.Hunt) {
			f.note(k, h)
			if rich {
				lv.draw(f.screen(h).Render(k, surf.width, height))
			}
		}
		h, err := offsets.Bisect(cmd.Context(), r, start, end, f.bad, step)
		if err != nil {
			return err
		}
		if f.re != nil && h.Found != nil {
			if l := f.recheck(cmd.Context(), k, r, h); l != "" {
				f.log = append(f.log, l)
			}
		}
		f.hunts = append(f.hunts, h)
	}
	if !f.at.IsZero() {
		f.log = append(f.log, "", k.S(kit.Muted, "read from there: ")+k.S(kit.Accent, "kaas offsets reset <group> -t "+f.topic+" --to "+f.at.Format(time.RFC3339)))
	}
	if rich {
		if len(f.hunts) > 0 {
			lv.draw(f.screen(f.hunts[len(f.hunts)-1]).Render(k, surf.width, height))
		}
		return nil
	}
	return f.print(cmd, output)
}

// note adds a hunt's latest step to the log, as the piece's bisect log.
func (f *finder) note(k kit.Kit, h offsets.Hunt) {
	pn := "p" + strconv.Itoa(int(h.Partition))
	switch {
	case h.Done:
		f.log = append(f.log, f.verdict(k, h)...)
	case h.Testing >= 0:
		f.log = append(f.log, k.S(kit.Muted, "Bisecting "+pn+": ")+k.S(kit.FG, plural(int(h.Left()), "offset")+" left to read after this"))
	default:
		pr := h.Probes[len(h.Probes)-1]
		if pr.Record == nil {
			f.log = append(f.log, k.S(kit.Warn, fmt.Sprintf("[%s@%d] ", pn, pr.At))+k.S(kit.Muted, "no records from here to the end"))
			return
		}
		rec := pr.Record
		f.log = append(f.log, k.S(kit.Warn, fmt.Sprintf("[%s@%d] ", pn, rec.Offset))+k.S(kit.FG, rec.Timestamp.Local().Format("15:04:05.000"))+k.S(kit.Muted, " · "+took(pr.Took)+"  "+excerpt(rec, 52)))
		word, role := f.words()
		v := word[0]
		r := role[0]
		if pr.Bad {
			v, r = word[1], role[1]
		}
		f.log = append(f.log, "  "+k.S(r, v))
	}
}

// recheckReads is how many records after a match kaas reads to check
// that matches don't come and go.
const recheckReads = 3

// recheck reads the records after a --match find. A bisect assumes they
// match too; one that doesn't means the found record is a match, not
// necessarily the first.
func (f *finder) recheck(ctx context.Context, k kit.Kit, r *offsets.Reader, h offsets.Hunt) string {
	at, read := h.Found.Offset+1, 0
	for range recheckReads {
		rec, err := r.At(ctx, at)
		if err != nil || rec == nil {
			break
		}
		if !f.re.Match(rec.Value) {
			return "    " + k.S(kit.Warn, fmt.Sprintf("p%d@%s doesn't match: matches come and go here, so there may be earlier ones", h.Partition, screens.Num(float64(rec.Offset))))
		}
		at, read = rec.Offset+1, read+1
	}
	if read == 0 {
		return "" // the last record: nothing after it to check
	}
	return "    " + k.S(kit.OK, "✓ the next "+plural(read, "record")+" match too")
}

// words is the log's and the legend's names for good and bad reads.
func (f *finder) words() ([2]string, [2]kit.Role) {
	roles := [2]kit.Role{kit.OK, kit.Err}
	if f.re != nil {
		return [2]string{"no match", "matches"}, roles
	}
	return [2]string{"before " + f.at.Local().Format("15:04:05"), "at or after " + f.at.Local().Format("15:04:05")}, roles
}

// verdict is a finished hunt's lines: what it found, and in time mode
// whether the broker's index agrees.
func (f *finder) verdict(k kit.Kit, h offsets.Hunt) []string {
	pn := "p" + strconv.Itoa(int(h.Partition))
	if h.Found == nil {
		why := "no record matches"
		if f.re == nil {
			why = "no record at or after " + stamp(f.at)
		}
		if h.End == h.Start {
			why = "empty"
		}
		return []string{k.S(kit.Muted, pn+": "+why)}
	}
	rec := h.Found
	out := []string{
		k.S(kit.Err, pn+"@"+screens.Num(float64(rec.Offset))) + k.S(kit.FG, " is the "+f.what()+" · "+plural(len(h.Probes), "read")),
		"    " + k.S(kit.Notice, stamp(rec.Timestamp)+"  "+excerpt(rec, 56)),
	}
	if f.re != nil {
		return out
	}
	switch idx := f.index[h.Partition]; {
	case idx == rec.Offset:
		out = append(out, "    "+k.S(kit.OK, "✓ the broker's time index agrees"))
	case idx < 0:
		out = append(out, "    "+k.S(kit.Warn, "the broker's time index has nothing at or after it: timestamps run out of order"))
	default:
		out = append(out, "    "+k.S(kit.Warn, fmt.Sprintf("the broker's time index says %s: a record before this one is stamped later", screens.Num(float64(idx)))))
	}
	return out
}

// excerpt is a record's key and value on one line.
func excerpt(r *kgo.Record, w int) string {
	v, _ := consume.Text(r.Value)
	v = strings.Join(strings.Fields(v), " ")
	if len(r.Key) > 0 {
		key, _ := consume.Text(r.Key)
		v = key + "  " + v
	}
	return kit.Fit(v, w)
}

// screen is the bisect-hunt piece for one partition's hunt.
func (f *finder) screen(h offsets.Hunt) screens.Bisect {
	word, _ := f.words()
	v := screens.Bisect{
		Title: fmt.Sprintf("find · %s p%d · %s", f.topic, h.Partition, f.what()),
		From:  "offset " + screens.Num(float64(h.Start)), To: "end " + screens.Num(float64(h.End)),
		Start: h.Start, End: h.End, Lo: h.Lo, Hi: h.Hi, Testing: h.Testing, Found: -1, Done: h.Done,
		Log: f.log, Good: word[0], Bad: word[1],
	}
	if f.re == nil {
		v.Good, v.Bad = "before", "at or after"
	}
	for _, p := range h.Probes {
		if p.Record != nil {
			v.Marks = append(v.Marks, screens.BisectMark{Offset: p.Record.Offset, Bad: p.Bad})
		}
	}
	if h.Found != nil {
		v.Found = h.Found.Offset
	}
	return v
}

type findJSON struct {
	Topic       string     `json:"topic"`
	Partition   int32      `json:"partition"`
	Offset      *int64     `json:"offset"` // null: none
	Timestamp   *time.Time `json:"timestamp,omitempty"`
	Reads       int        `json:"reads"`
	IndexOffset *int64     `json:"indexOffset,omitempty"` // --time: the broker's time index; null when it has none
}

// print is the hunts without the screen: a line per partition, or JSON.
func (f *finder) print(cmd *cobra.Command, output string) error {
	out := cmd.OutOrStdout()
	var js []findJSON
	for _, h := range f.hunts {
		j := findJSON{Topic: h.Topic, Partition: h.Partition, Reads: len(h.Probes)}
		if h.Found != nil {
			o, ts := h.Found.Offset, h.Found.Timestamp.UTC()
			j.Offset, j.Timestamp = &o, &ts
		}
		if f.re == nil {
			idx := f.index[h.Partition]
			if idx >= 0 {
				j.IndexOffset = &idx
			}
		}
		js = append(js, j)
	}
	if output == "json" {
		if js == nil {
			js = []findJSON{}
		}
		return json.NewEncoder(out).Encode(js)
	}
	for _, j := range js {
		if j.Offset == nil {
			fmt.Fprintf(out, "%s p%d -\n", j.Topic, j.Partition)
			continue
		}
		fmt.Fprintf(out, "%s p%d %d %s\n", j.Topic, j.Partition, *j.Offset, j.Timestamp.UTC().Format(time.RFC3339Nano))
	}
	return nil
}
