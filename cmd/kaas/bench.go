package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/bench"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newBenchCmd(brokers, themeName *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Measure produce throughput and latency against a cluster",
		Args:  unknownCommand,
	}
	cmd.AddCommand(newBenchProduceCmd(brokers, themeName))
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

			r := benchRun{
				cmd: cmd, topic: topic, cases: cases, seeds: seeds,
				out: detect(cmd.OutOrStdout(), *themeName), json: output == "json",
				title: "bench produce · " + topic,
				right: strings.Join(pr.Seeds, ",") + " · kafka " + pr.Version,
			}
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

type benchRun struct {
	cmd          *cobra.Command
	topic        string
	cases        []bench.Case
	seeds        kgo.Opt
	out          surface
	json         bool
	title, right string
}

func (b benchRun) run() error {
	ctx := b.cmd.Context()
	view := screens.Bench{Title: b.title, Right: b.right, Buckets: bench.Buckets, RateTitle: "throughput · rec/s"}
	for _, c := range b.cases {
		view.Rows = append(view.Rows, screens.BenchRow{Name: c.Name(), Total: int64(c.Records)})
	}
	rich := b.out.rich && !b.json
	var lv *live
	if rich {
		lv = &live{s: b.out}
		defer lv.done()
	}
	still := os.Getenv("KAAS_REDUCED_MOTION") == "1"
	frame := 0
	foot := func(i int) string {
		c := b.cases[i]
		cap := "unthrottled"
		if c.Throughput > 0 {
			cap = fmt.Sprintf("%d rec/s cap", c.Throughput)
		}
		return fmt.Sprintf("running %d/%d · %s records per case · %s", i+1, len(b.cases), screens.Num(float64(c.Records)), cap)
	}

	var results []bench.Result
	for i, c := range b.cases {
		view.Rows[i].State = screens.BenchRunning
		view.HistTitle = "latency · " + c.Name()
		view.Hist, view.Rate = nil, nil
		view.Foot = foot(i)
		opts := bench.Options{Client: []kgo.Opt{b.seeds}}
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
		res, err := bench.Produce(ctx, b.topic, c, opts)
		results = append(results, res)

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
			fmt.Fprintln(b.out.w, plainResult(res))
		}
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("case %s: %w", c.Name(), err)
		}
	}
	view.Foot = fmt.Sprintf("done · %d cases · %s", len(b.cases), b.right)
	if rich {
		lv.draw(view.Render(b.out.kit, b.out.width))
	}
	if b.json {
		enc := json.NewEncoder(b.cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(results)
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
