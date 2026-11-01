package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/consume"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newConsumeCmd(brokers, themeName *string) *cobra.Command {
	var (
		output, from string
		parts        []int
		maxRecords   int
		exit         bool
	)
	cmd := &cobra.Command{
		Use:     "consume <topic>",
		Aliases: []string{"tail"},
		Short:   "Read records from a topic, like a chat channel, without joining a group",
		Long: `Read a topic's records as they arrive. On a terminal each record is a
line in a channel: its time, its key in its partition's colour, and its
value, with tombstones and headers called out. On a pipe each value is
one line, as kafka-console-consumer prints them; -o json gives every
field, one object per line.

kaas consume never joins a consumer group and never commits, so reading
never moves anyone's offsets.

--from is where each partition starts: end (the default: only new
records), beginning, -N (the last N records of each partition), a
duration such as 15m (records since then), or an RFC 3339 time.`,
		Example: `  # New records as they arrive
  kaas consume orders
  # The last 20 records of each partition, then keep following
  kaas consume orders --from -20
  # Everything from the last hour on partition 2, then stop
  kaas consume orders --from 1h -p 2 --exit
  # Values as JSON lines, for jq
  kaas consume orders --from beginning --exit -o json | jq -r '.value | fromjson | .amount'`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return usageError(cmd, "topic", "", "required: the topic to read")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "text", "json"); err != nil {
				return err
			}
			start, err := consume.ParseFrom(from, time.Now())
			if err != nil {
				return usageError(cmd, "from", from, err.Error())
			}
			if maxRecords < 0 {
				return usageError(cmd, "max", strconv.Itoa(maxRecords), "must be 0 or more")
			}
			var want []int32
			for _, p := range parts {
				if p < 0 {
					return usageError(cmd, "partition", strconv.Itoa(p), "must be 0 or more")
				}
				want = append(want, int32(p))
			}
			if _, err := connect(cmd, *brokers, *themeName); err != nil {
				return err
			}
			seeds := splitSeeds(*brokers)
			cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
			if err != nil {
				return err
			}
			defer cl.Close()
			topic := args[0]
			pctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			defer cancel()
			s, err := snapshot.Fetch(pctx, cl)
			if err != nil {
				return err
			}
			if err := missingTopic(s, []string{topic}); err != nil {
				return err
			}
			plan, err := consume.Plan(pctx, kadm.NewClient(cl), topic, want, start)
			if err != nil {
				if strings.Contains(err.Error(), "has no partition") {
					return usageError(cmd, "partition", "", err.Error())
				}
				return err
			}
			o := consume.Options{Seeds: seeds, Topic: topic, Plan: plan, Max: maxRecords, Exit: exit}
			surf := detect(cmd.OutOrStdout(), *themeName)
			if surf.rich && output == "text" {
				return newChannelView(surf, o, start, *brokers).run(cmd.Context())
			}
			return consume.Read(cmd.Context(), o, nil, recordPrinter(cmd.OutOrStdout(), output == "json"))
		},
	}
	f := cmd.Flags()
	f.StringVarP(&output, "output", "o", "text", "output format: text|json (one object per line)")
	f.StringVar(&from, "from", "end", "where to start: end, beginning, -N, a duration like 15m, or an RFC 3339 time")
	f.IntSliceVarP(&parts, "partition", "p", nil, "partitions to read (repeatable); all when unset")
	f.IntVarP(&maxRecords, "max", "n", 0, "stop after this many records; 0 for no limit")
	f.BoolVarP(&exit, "exit", "e", false, "stop once every partition reaches the end it had when the read began")
	return cmd
}

type recordJSON struct {
	Partition int32            `json:"partition"`
	Offset    int64            `json:"offset"`
	Timestamp time.Time        `json:"timestamp"`
	Key       *string          `json:"key"`   // null for no key
	Value     *string          `json:"value"` // null for a tombstone
	Encoding  string           `json:"encoding,omitempty"`
	Headers   []consume.Header `json:"headers"`
}

// recordJSONOf keeps text as text; a binary key or value is base64 and the
// record says so in encoding.
func recordJSONOf(r consume.Record) recordJSON {
	j := recordJSON{Partition: r.Partition, Offset: r.Offset, Timestamp: r.Time, Headers: r.Headers}
	if j.Headers == nil {
		j.Headers = []consume.Header{}
	}
	enc := func(b []byte) *string {
		if b == nil {
			return nil
		}
		if s, ok := consume.Text(b); ok {
			return &s
		}
		s := base64.StdEncoding.EncodeToString(b)
		j.Encoding = "base64"
		return &s
	}
	j.Key, j.Value = enc(r.Key), enc(r.Value)
	return j
}

// recordPrinter writes one value per line, raw, as kafka-console-consumer
// does, or one JSON object per line.
func recordPrinter(out io.Writer, asJSON bool) func(consume.Record) error {
	enc := json.NewEncoder(out)
	return func(r consume.Record) error {
		if asJSON {
			return enc.Encode(recordJSONOf(r))
		}
		_, err := out.Write(append(r.Value, '\n'))
		return err
	}
}

// channelView is the live relay-chat-channel screen for one read.
type channelView struct {
	surf    surface
	o       consume.Options
	from    consume.From
	server  string
	lines   []screens.ChanLine
	counts  map[int32]int
	read    int
	rate    float64
	lastN   int
	lastAt  time.Time
	stopped string
}

func newChannelView(surf surface, o consume.Options, from consume.From, server string) *channelView {
	v := &channelView{surf: surf, o: o, from: from, server: server, counts: map[int32]int{}, lastAt: time.Now()}
	now := time.Now()
	for _, p := range o.Plan {
		v.counts[p.Partition] = 0
		text := "reading from offset " + screens.Num(float64(p.Offset))
		if p.Offset >= p.End {
			text += ", at the end: waiting for new records"
		} else {
			text += fmt.Sprintf(", %s behind the end", screens.Num(float64(p.End-p.Offset)))
		}
		v.lines = append(v.lines, screens.ChanLine{Kind: screens.LineJoin, Time: now, Nick: fmt.Sprintf("p%d", p.Partition), Hue: int(p.Partition), Text: text})
	}
	return v
}

// keep bounds the backlog to what a tall terminal could show.
func (v *channelView) keep() {
	if n := len(v.lines); n > 400 {
		v.lines = v.lines[n-400:]
	}
}

func (v *channelView) add(r consume.Record) {
	v.read++
	v.counts[r.Partition]++
	nick := "·" // no key
	if r.Key != nil {
		nick, _ = consume.Text(r.Key)
		nick = strings.Join(strings.Fields(nick), " ")
		if w := len([]rune(nick)); w > 24 {
			nick = string([]rune(nick)[:23]) + "⋯"
		}
	}
	meta := fmt.Sprintf("p%d:%d", r.Partition, r.Offset)
	l := screens.ChanLine{Kind: screens.LineMsg, Time: r.Time, Meta: meta, Nick: nick, Hue: int(r.Partition)}
	if r.Value == nil {
		l.Kind, l.Text = screens.LinePart, "tombstone: deletes this key once compacted"
	} else {
		l.Text, _ = consume.Text(r.Value)
	}
	v.lines = append(v.lines, l)
	if len(r.Headers) > 0 {
		var hs []string
		for _, h := range r.Headers {
			val, _ := consume.Text([]byte(h.Value))
			hs = append(hs, h.Key+"="+val)
		}
		v.lines = append(v.lines, screens.ChanLine{Kind: screens.LineAct, Time: r.Time, Text: "headers: " + strings.Join(hs, " · ")})
	}
	v.keep()
}

func (v *channelView) screen(now time.Time) screens.Channel {
	c := screens.Channel{
		Name:   "#" + v.o.Topic,
		Info:   fmt.Sprintf("[%s]  from %s", plural(len(v.o.Plan), "partition"), v.from),
		Server: v.server,
		Lines:  v.lines,
		Status: []string{now.Format("15:04:05"), v.o.Topic,
			fmt.Sprintf("read %s · %s/s", screens.Num(float64(v.read)), screens.Num(v.rate)),
			plural(len(v.o.Plan), "partition"), "no group, nothing committed"},
		Input: "ctrl-c to stop · -o json for scripts",
	}
	for _, p := range v.o.Plan {
		c.Nicks = append(c.Nicks, screens.Nick{Name: fmt.Sprintf("p%d", p.Partition), Hue: int(p.Partition), Count: screens.Num(float64(v.counts[p.Partition]))})
	}
	if v.stopped != "" {
		c.Input = v.stopped
	}
	return c
}

func (v *channelView) frame(now time.Time) []string {
	return v.screen(now).Render(v.surf.kit, v.surf.width, max(10, v.surf.height-1))
}

// run reads in the background and redraws at most ten times a second, so a
// busy topic does not flood the terminal with frames.
func (v *channelView) run(ctx context.Context) error {
	recs := make(chan consume.Record, 1024)
	done := make(chan error, 1)
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		done <- consume.Read(rctx, v.o, nil, func(r consume.Record) error {
			select {
			case recs <- r:
				return nil
			case <-rctx.Done():
				return consume.ErrDone
			}
		})
		close(recs)
	}()
	lv := &live{s: v.surf}
	defer lv.done()
	lv.draw(v.frame(time.Now()))
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	dirty := false
	for {
		select {
		case r, ok := <-recs:
			if !ok {
				err := <-done
				if v.o.Exit || v.o.Max > 0 {
					v.stopped = fmt.Sprintf("stopped after %s", plural(v.read, "record"))
					if v.o.Exit && (v.o.Max == 0 || v.read < v.o.Max) {
						v.stopped = fmt.Sprintf("read to the end: %s", plural(v.read, "record"))
					}
				}
				lv.draw(v.frame(time.Now()))
				if ctx.Err() != nil {
					return nil // Ctrl-C ends a live read, not a failure
				}
				return err
			}
			v.add(r)
			dirty = true
		case now := <-t.C:
			if dt := now.Sub(v.lastAt).Seconds(); dt >= 1 {
				v.rate = float64(v.read-v.lastN) / dt
				v.lastN, v.lastAt = v.read, now
				dirty = true
			}
			if dirty {
				lv.draw(v.frame(now))
				dirty = false
			}
		}
	}
}
