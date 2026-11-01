package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/bench"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newBenchCmd(brokers, themeName *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Measure produce and consume throughput and latency against a cluster",
		Args:  unknownCommand,
	}
	cmd.AddCommand(newBenchProduceCmd(brokers, themeName), newBenchConsumeCmd(brokers, themeName))
	return cmd
}

func newBenchProduceCmd(brokers, themeName *string) *cobra.Command {
	var (
		topic      string
		records    int
		sizes      []int
		acks       []string
		codecs     []string
		throughput int
		output     string
	)
	cmd := &cobra.Command{
		Use:   "produce",
		Short: "Benchmark producing to a topic: a case per size × acks × compression",
		Long: `Produce records to a topic and measure throughput and per-record ack latency,
like kafka-producer-perf-test. Sizes, acks and codecs multiply into a
matrix of cases that run one after another, each with its own producer.`,
		Example: `  # The perf-test run: 5,000 × 100 B at 500 rec/s
  kaas bench produce -t orders --records 5000 --size 100 --throughput 500
  # Compare compression codecs at two record sizes, unthrottled
  kaas bench produce -t orders --size 100,1024 --compression none,lz4,zstd
  # acks=1 against acks=all, results as JSON
  kaas bench produce -t orders --acks 1,all -o json | jq '.[] | {acks: .case.acks, p99}'`,
		Args: unknownCommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if topic == "" {
				return usageError(cmd, "topic", "", "required: the topic to produce to")
			}
			if err := oneOf(cmd, "output", output, "table", "json"); err != nil {
				return err
			}
			cases := bench.Matrix(sizes, acks, codecs, records, throughput)
			for _, c := range cases {
				if field, msg := c.Validate(); field != "" {
					value := map[string]string{
						"size": fmt.Sprint(c.Size), "records": fmt.Sprint(c.Records), "throughput": fmt.Sprint(c.Throughput),
						"acks": c.Acks, "compression": c.Compression,
					}[field]
					return usageError(cmd, field, value, msg)
				}
			}
			pr, err := connect(cmd, *brokers, *themeName)
			if err != nil {
				return err
			}
			seeds := kgo.SeedBrokers(splitSeeds(*brokers)...)
			if err := topicExists(cmd.Context(), seeds, topic); err != nil {
				return err
			}

			r := produceRun(cmd, topic, cases, seeds)
			r.out, r.json = detect(cmd.OutOrStdout(), *themeName), output == "json"
			r.title, r.right = "bench produce · "+topic, strings.Join(pr.Seeds, ",")+" · kafka "+pr.Version
			return r.run()
		},
	}
	f := cmd.Flags()
	f.StringVarP(&topic, "topic", "t", "", "topic to produce to (must exist)")
	f.IntVar(&records, "records", 10_000, "records per case")
	f.IntSliceVar(&sizes, "size", []int{100}, "value size in bytes (comma-separated for several cases)")
	f.StringSliceVar(&acks, "acks", []string{"all"}, "acks: 0, 1 or all (comma-separated)")
	f.StringSliceVar(&codecs, "compression", []string{"none"}, "none, gzip, snappy, lz4 or zstd (comma-separated)")
	f.IntVar(&throughput, "throughput", 0, "records/sec cap per case; 0 is unlimited")
	f.StringVarP(&output, "output", "o", "table", "output format: table|json")
	return cmd
}

// topicExists is a 404, with suggestions, for a topic the cluster lacks.
func topicExists(ctx context.Context, seeds kgo.Opt, topic string) error {
	cl, err := kgo.NewClient(seeds)
	if err != nil {
		return err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	s, err := snapshot.Fetch(ctx, cl)
	if err != nil {
		return err
	}
	return missingTopic(s, []string{topic})
}

// benchRun drives a matrix of cases through the benchmark-table screen,
// whatever each case measures.
type benchRun struct {
	cmd          *cobra.Command
	out          surface
	json         bool
	title, right string
	lv           *live // optional; run makes one on a rich surface

	names   []string
	totals  []int64
	verb    string // the running counter's word
	latency string // the histogram's title: what each sample timed
	foot    func(i int) string
	exec    func(ctx context.Context, i int, o bench.Options) (bench.Stats, error)
	plain   func(i int) string // case i's result as a line
	results func() any         // every result, for -o json
}

func produceRun(cmd *cobra.Command, topic string, cases []bench.Case, seeds kgo.Opt) benchRun {
	var results []bench.Result
	b := benchRun{cmd: cmd, verb: "acked", latency: "latency", results: func() any { return results }}
	for _, c := range cases {
		b.names = append(b.names, c.Name())
		b.totals = append(b.totals, int64(c.Records))
	}
	b.foot = func(i int) string {
		c := cases[i]
		cap := "unthrottled"
		if c.Throughput > 0 {
			cap = fmt.Sprintf("%d rec/s cap", c.Throughput)
		}
		return fmt.Sprintf("running %d/%d · %s records per case · %s", i+1, len(cases), screens.Num(float64(c.Records)), cap)
	}
	b.exec = func(ctx context.Context, i int, o bench.Options) (bench.Stats, error) {
		o.Client = []kgo.Opt{seeds}
		res, err := bench.Produce(ctx, topic, cases[i], o)
		results = append(results, res)
		return res.Stats, err
	}
	b.plain = func(i int) string { return plainResult(results[i]) }
	return b
}

func (b benchRun) run() error {
	ctx := b.cmd.Context()
	view := screens.Bench{Title: b.title, Right: b.right, Buckets: bench.Buckets, RateTitle: "throughput · rec/s", Verb: b.verb}
	for i, name := range b.names {
		view.Rows = append(view.Rows, screens.BenchRow{Name: name, Total: b.totals[i]})
	}
	rich := b.out.rich && !b.json
	lv := b.lv
	if rich && lv == nil {
		lv = &live{s: b.out}
		defer lv.done()
	}
	still := os.Getenv("KAAS_REDUCED_MOTION") == "1"
	frame := 0

	for i, name := range b.names {
		view.Rows[i].State = screens.BenchRunning
		view.HistTitle = b.latency + " · " + name
		view.Hist, view.Rate = nil, nil
		view.Foot = b.foot(i)
		var opts bench.Options
		if rich {
			lv.draw(view.Render(b.out.kit, b.out.width))
			opts.Report = func(p bench.Progress) {
				if !still {
					frame++
				}
				view.Spin = frame
				view.Rows[i].Acked = p.Acked
				view.Hist, view.Rate = p.Hist, p.Rate
				lv.draw(view.Render(b.out.kit, b.out.width))
			}
		}
		res, err := b.exec(ctx, i, opts)

		row := &view.Rows[i]
		row.State, row.RecsPerSec, row.MBPerSec = screens.BenchDone, res.RecsPerSec, res.MBPerSec
		row.P50, row.P99, row.Max, row.Errors, row.Err = res.P50, res.P99, res.Max, res.Errors, res.FirstError
		view.Hist = res.Hist
		if err != nil && !errors.Is(err, context.Canceled) {
			row.State, row.Err = screens.BenchFailed, err.Error()
		}
		switch {
		case rich:
			lv.draw(view.Render(b.out.kit, b.out.width))
		case !b.json:
			fmt.Fprintln(b.out.w, b.plain(i))
		}
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("case %s: %w", name, err)
		}
	}
	view.Foot = fmt.Sprintf("done · %s · %s", plural(len(b.names), "case"), b.right)
	if rich {
		lv.draw(view.Render(b.out.kit, b.out.width))
	}
	if b.json {
		enc := json.NewEncoder(b.cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(b.results())
	}
	return nil
}

// plainResult is one case as a line, in the same terms as
// kafka-producer-perf-test's summary.
func plainResult(r bench.Result) string {
	s := fmt.Sprintf("%s: %d records sent, %.1f records/sec (%.2f MB/sec), %s avg latency, %s max latency, %s 50th, %s 95th, %s 99th, %s 99.9th.",
		r.Case.Name(), r.Records, r.RecsPerSec, r.MBPerSec,
		screens.Dur(r.Avg), screens.Dur(r.Max), screens.Dur(r.P50), screens.Dur(r.P95), screens.Dur(r.P99), screens.Dur(r.P999))
	if r.Errors > 0 {
		s += fmt.Sprintf(" %d errors (first: %s).", r.Errors, r.FirstError)
	}
	return s
}

func newBenchConsumeCmd(brokers, themeName *string) *cobra.Command {
	var (
		topic   string
		records int
		sizes   []string
		waits   []time.Duration
		output  string
	)
	cmd := &cobra.Command{
		Use:   "consume",
		Short: "Benchmark reading a topic: a case per fetch size × max wait",
		Long: `Read a topic from the beginning and measure throughput and how long each
poll waited for its batch, like kafka-consumer-perf-test. Fetch sizes and
waits multiply into a matrix of cases, each with its own client, so no
case reads from another's buffer.

It never joins a group and never commits. A case reads --records, or the
whole topic when it holds fewer; kaas bench produce fills one first.`,
		Example: `  # The perf-test run: 10,000 records with default fetches
  kaas bench consume -t orders
  # Small fetches against large ones
  kaas bench consume -t orders --records 100000 --fetch-size 64KB,1MB,8MB
  # Fill a topic, then read it back, as JSON
  kaas bench produce -t orders --records 50000 && kaas bench consume -t orders -o json | jq '.[] | {case: .case, records_per_sec}'`,
		Args: unknownCommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if topic == "" {
				return usageError(cmd, "topic", "", "required: the topic to read")
			}
			if err := oneOf(cmd, "output", output, "table", "json"); err != nil {
				return err
			}
			var bytes []int
			for _, s := range sizes {
				n, err := parseSize(s)
				if err != nil {
					return usageError(cmd, "fetch-size", s, err.Error())
				}
				bytes = append(bytes, n)
			}
			cases := bench.FetchMatrix(bytes, waits, records)
			for _, c := range cases {
				if field, msg := c.Validate(); field != "" {
					value := map[string]string{"fetch-size": bench.Bytes(int64(c.FetchSize)),
						"max-wait": c.MaxWait.String(), "records": fmt.Sprint(c.Records)}[field]
					return usageError(cmd, field, value, msg)
				}
			}
			pr, err := connect(cmd, *brokers, *themeName)
			if err != nil {
				return err
			}
			seeds := kgo.SeedBrokers(splitSeeds(*brokers)...)
			if err := topicExists(cmd.Context(), seeds, topic); err != nil {
				return err
			}
			avail, err := topicRecords(cmd.Context(), seeds, topic)
			if err != nil {
				return err
			}
			if avail == 0 {
				return usageError(cmd, "topic", topic, "has no records to read yet; kaas bench produce -t "+topic+" fills it")
			}
			r := consumeRun(cmd, topic, cases, seeds, avail)
			r.out, r.json = detect(cmd.OutOrStdout(), *themeName), output == "json"
			r.title, r.right = "bench consume · "+topic, strings.Join(pr.Seeds, ",")+" · kafka "+pr.Version
			return r.run()
		},
	}
	f := cmd.Flags()
	f.StringVarP(&topic, "topic", "t", "", "topic to read (must hold records)")
	f.IntVar(&records, "records", 10_000, "records per case; the whole topic when it holds fewer")
	f.StringSliceVar(&sizes, "fetch-size", []string{"1MB"}, "max bytes per partition per fetch, like 64KB or 1MB (comma-separated)")
	f.DurationSliceVar(&waits, "max-wait", []time.Duration{500 * time.Millisecond}, "how long a broker may hold a fetch for data (comma-separated)")
	f.StringVarP(&output, "output", "o", "table", "output format: table|json")
	return cmd
}

// topicRecords is how many records topic holds now.
func topicRecords(ctx context.Context, seeds kgo.Opt, topic string) (int64, error) {
	cl, err := kgo.NewClient(seeds)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	n, _, err := bench.Available(ctx, kadm.NewClient(cl), topic)
	return n, err
}

func consumeRun(cmd *cobra.Command, topic string, cases []bench.FetchCase, seeds kgo.Opt, avail int64) benchRun {
	var results []bench.ConsumeResult
	b := benchRun{cmd: cmd, verb: "read", latency: "poll wait", results: func() any { return results }}
	for _, c := range cases {
		b.names = append(b.names, c.Name())
		b.totals = append(b.totals, min(int64(c.Records), avail))
	}
	b.foot = func(i int) string {
		return fmt.Sprintf("running %d/%d · %s records per case from the beginning · no group, nothing committed",
			i+1, len(cases), screens.Num(float64(b.totals[i])))
	}
	b.exec = func(ctx context.Context, i int, o bench.Options) (bench.Stats, error) {
		o.Client = []kgo.Opt{seeds}
		res, err := bench.Consume(ctx, topic, cases[i], o)
		results = append(results, res)
		return res.Stats, err
	}
	b.plain = func(i int) string {
		r := results[i]
		s := fmt.Sprintf("%s: %d records read, %.1f records/sec (%.2f MB/sec), %d polls, %s avg poll wait, %s max, %s 50th, %s 95th, %s 99th.",
			r.Case.Name(), r.Records, r.RecsPerSec, r.MBPerSec, r.Polls,
			screens.Dur(r.Avg), screens.Dur(r.Max), screens.Dur(r.P50), screens.Dur(r.P95), screens.Dur(r.P99))
		if r.Errors > 0 {
			s += fmt.Sprintf(" %d errors (first: %s).", r.Errors, r.FirstError)
		}
		return s
	}
	return b
}

// parseSize reads a byte count: 1048576, 64KB, 1MB (powers of 1024).
func parseSize(s string) (int, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	mult := 1
	for _, u := range []struct {
		suffix string
		mult   int
	}{{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"KB", 1 << 10}, {"MB", 1 << 20}, {"K", 1 << 10}, {"M", 1 << 20}, {"B", 1}} {
		if strings.HasSuffix(t, u.suffix) {
			t, mult = strings.TrimSpace(strings.TrimSuffix(t, u.suffix)), u.mult
			break
		}
	}
	n, err := strconv.Atoi(t)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("want a size like 64KB or 1MB")
	}
	if n > (1<<31-1)/mult {
		return 0, fmt.Errorf("must be under 2GB")
	}
	return n * mult, nil
}
