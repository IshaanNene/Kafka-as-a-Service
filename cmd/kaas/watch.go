package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
)

func newWatchCmd(brokers *string) *cobra.Command {
	var (
		topics  []string
		output  string
		verbose bool
		cfg     engine.Config
	)
	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Stream cluster events: leader moves, ISR changes, topics created and deleted",
		Long: `Stream typed cluster events as the engine sees them.

Topics named with -t are watched: their offsets refresh on the fast interval
and record growth is printed. Every request is paid from a per-cluster
budget; when it runs dry, refreshes are skipped rather than queued.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if output != "text" && output != "json" {
				return fmt.Errorf("unknown output format %q", output)
			}
			cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...))
			if err != nil {
				return err
			}
			defer cl.Close()

			e := engine.New(cl, cfg)
			sub := e.Subscribe()
			defer sub.Close()
			if len(topics) > 0 {
				defer e.Watch(topics...)()
			}
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- e.Run(ctx) }()

			w := &watchPrinter{out: cmd.OutOrStdout(), json: output == "json", topics: topics, last: map[string]int64{}}
			for {
				select {
				case <-ctx.Done():
					// Ctrl-C or a deadline is a clean exit, not a failure.
					if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						return err
					}
					return nil
				case <-sub.Ready():
					u, _ := sub.Next()
					if err := w.print(u); err != nil {
						return err
					}
					if verbose {
						w.stats(e.Stats())
					}
				}
			}
		},
	}
	f := cmd.Flags()
	f.StringSliceVarP(&topics, "topic", "t", nil, "topics to watch closely (repeatable)")
	f.StringVarP(&output, "output", "o", "text", "output format: text|json (one object per line)")
	f.BoolVarP(&verbose, "verbose", "v", false, "print engine stats after each update")
	f.DurationVar(&cfg.MetadataInterval, "metadata-interval", 5*time.Second, "metadata refresh interval")
	f.DurationVar(&cfg.FastInterval, "fast-interval", time.Second, "offset refresh interval for watched topics")
	f.DurationVar(&cfg.SlowInterval, "slow-interval", 30*time.Second, "offset refresh interval for other topics")
	f.Float64Var(&cfg.RequestsPerSecond, "budget", 20, "admin requests per second to this cluster")
	return cmd
}

type watchPrinter struct {
	out     io.Writer
	json    bool
	topics  []string
	last    map[string]int64 // records per watched topic at last print
	started bool
}

type watchLine struct {
	Time       time.Time     `json:"time"`
	Generation uint64        `json:"generation"`
	Event      *engine.Event `json:"event,omitempty"`
	Topic      string        `json:"topic,omitempty"`
	Records    *int64        `json:"records,omitempty"`
	Baseline   *baseline     `json:"baseline,omitempty"`
	Dropped    int           `json:"droppedEvents,omitempty"`
}

type baseline struct {
	Brokers, Topics, Partitions int
}

func (w *watchPrinter) print(u engine.Update) error {
	s := u.Snapshot
	if s == nil {
		return nil
	}
	now := time.Now()
	if !w.started {
		w.started = true
		b := &baseline{Brokers: len(s.Brokers), Topics: len(s.Topics), Partitions: s.PartitionCount()}
		if err := w.emit(watchLine{Time: now, Generation: s.Generation, Baseline: b},
			fmt.Sprintf("baseline · %d brokers · %d topics · %d partitions", b.Brokers, b.Topics, b.Partitions)); err != nil {
			return err
		}
	}
	for i := range u.Events {
		ev := u.Events[i]
		if err := w.emit(watchLine{Time: now, Generation: s.Generation, Event: &ev}, ev.String()); err != nil {
			return err
		}
	}
	if u.DroppedEvents > 0 {
		if err := w.emit(watchLine{Time: now, Generation: s.Generation, Dropped: u.DroppedEvents},
			fmt.Sprintf("fell behind: %d events dropped", u.DroppedEvents)); err != nil {
			return err
		}
	}
	for _, name := range w.topics {
		n, ok := records(s, name)
		if !ok {
			continue
		}
		prev, seen := w.last[name]
		if seen && n == prev {
			continue
		}
		w.last[name] = n
		text := fmt.Sprintf("%s %d records", name, n)
		if seen {
			text = fmt.Sprintf("%s %+d records (%d retained)", name, n-prev, n)
		}
		if err := w.emit(watchLine{Time: now, Generation: s.Generation, Topic: name, Records: &n}, text); err != nil {
			return err
		}
	}
	return nil
}

func (w *watchPrinter) stats(st engine.Stats) {
	line := fmt.Sprintf("  · gen %d · budget %.1f tokens · %d granted · %d denied",
		st.Generation, st.Budget.Tokens, st.Budget.Granted, st.Budget.Denied)
	if st.LastError != nil {
		line += " · last error: " + st.LastError.Error()
	}
	if !w.json {
		fmt.Fprintln(w.out, line)
	}
}

func (w *watchPrinter) emit(l watchLine, text string) error {
	if w.json {
		return json.NewEncoder(w.out).Encode(l)
	}
	_, err := fmt.Fprintf(w.out, "%s gen %-4d %s\n", l.Time.Format("15:04:05.000"), l.Generation, text)
	return err
}

// records is the number of records currently retained in a topic, once its
// offsets have been read at least once.
func records(s *snapshot.Snapshot, topic string) (int64, bool) {
	t, ok := s.Topic(topic)
	if !ok || t.OffsetsAt.IsZero() {
		return 0, false
	}
	var n int64
	for _, p := range t.Partitions {
		if p.StartOffset < 0 || p.EndOffset < 0 {
			return 0, false
		}
		n += p.EndOffset - p.StartOffset
	}
	return n, true
}
