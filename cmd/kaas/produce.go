package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/produce"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// produceBatch is how many piped lines are sent together.
const produceBatch = 1000

func newProduceCmd(brokers, themeName *string) *cobra.Command {
	var (
		output, key, value, keySep string
		headers                    []string
		partition                  int
		valueSet                   bool
	)
	cmd := &cobra.Command{
		Use:   "produce <topic>",
		Short: "Write records to a topic: compose one in an editor, or pipe them in",
		Long: `Write records to a topic.

On a terminal with nothing piped in, kaas produce opens an editor: the
value, key and headers each have a tab, JSON is highlighted and checked as
you type, and ctrl-s sends. The buffers stay after a send, so you can
change a field and send again.

Otherwise each line of stdin is one record, as kafka-console-producer
reads them: the whole line is the value, or with --key-separator, the part
before it is the key. --value sends one record without reading stdin.
-H adds headers to every record; -p picks the partition.`,
		Example: `  # Compose a record in the editor
  kaas produce orders
  # One record from flags
  kaas produce orders -k user-17 --value '{"order":1043}' -H source=cli
  # Keyed records from a file, one per line
  kaas produce orders --key-separator ':' < orders.txt
  # Where each record landed, for a script
  printf 'a\nb\n' | kaas produce orders -o json | jq .offset`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return usageError(cmd, "topic", "", "required: the topic to write to")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "text", "json"); err != nil {
				return err
			}
			var hs []produce.Header
			for _, h := range headers {
				p, err := produce.ParseHeader(h)
				if err != nil {
					return usageError(cmd, "header", h, "want key=value")
				}
				hs = append(hs, p)
			}
			if partition < -1 {
				return usageError(cmd, "partition", strconv.Itoa(partition), "must be 0 or more")
			}
			valueSet = cmd.Flags().Changed("value")
			if _, err := connect(cmd, *brokers, *themeName); err != nil {
				return err
			}
			opts := []kgo.Opt{kgo.SeedBrokers(splitSeeds(*brokers)...)}
			if partition >= 0 {
				opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
			}
			cl, err := kgo.NewClient(opts...)
			if err != nil {
				return err
			}
			defer cl.Close()
			topic := args[0]
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			s, err := snapshot.Fetch(ctx, cl)
			cancel()
			if err != nil {
				return err
			}
			if err := missingTopic(s, []string{topic}); err != nil {
				return err
			}
			if n := partitionsOf(s, topic); partition >= n {
				return usageError(cmd, "partition", strconv.Itoa(partition), fmt.Sprintf("topic %s has %s", topic, plural(n, "partition")))
			}

			p := producer{cl: cl, topic: topic, partition: int32(partition), headers: hs, json: output == "json", out: cmd.OutOrStdout()}
			if valueSet {
				r := produce.Record{Value: []byte(value), Headers: hs, Partition: int32(partition)}
				if cmd.Flags().Changed("key") {
					r.Key = []byte(key)
				}
				return p.send(cmd.Context(), []produce.Record{r}, detect(cmd.OutOrStdout(), *themeName))
			}
			surf := detect(cmd.OutOrStdout(), *themeName)
			if in, ok := cmd.InOrStdin().(*os.File); ok && term.IsTerminal(in.Fd()) && surf.rich && output == "text" {
				return p.edit(cmd, *themeName)
			}
			return p.lines(cmd.Context(), cmd.InOrStdin(), keySep)
		},
	}
	f := cmd.Flags()
	f.StringVarP(&output, "output", "o", "text", "output format: text|json (where each record landed, one object per line)")
	f.StringVarP(&key, "key", "k", "", "the record key, with --value")
	f.StringVar(&value, "value", "", "send one record with this value instead of reading stdin")
	f.StringArrayVarP(&headers, "header", "H", nil, "a key=value header on every record (repeatable)")
	f.IntVarP(&partition, "partition", "p", -1, "the partition to write to; -1 lets the key decide")
	f.StringVar(&keySep, "key-separator", "", "split each stdin line into key and value at the first occurrence")
	return cmd
}

func partitionsOf(s *snapshot.Snapshot, topic string) int {
	for _, t := range s.Topics {
		if t.Name == topic {
			return len(t.Partitions)
		}
	}
	return 0
}

// producer writes records for one run of kaas produce.
type producer struct {
	cl        *kgo.Client
	topic     string
	partition int32
	headers   []produce.Header
	json      bool
	out       io.Writer
}

// resultJSON is one record's landing in -o json.
type resultJSON struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

// send writes recs and reports them: JSON lines, a confirmation on a
// terminal, nothing on a pipe, like kafka-console-producer.
func (p producer) send(ctx context.Context, recs []produce.Record, surf surface) error {
	res := produce.Send(ctx, p.cl, p.topic, recs)
	enc := json.NewEncoder(p.out)
	failed, first := 0, error(nil)
	for _, r := range res {
		if r.Err != nil {
			failed++
			first = cmpErr(first, r.Err)
			continue
		}
		switch {
		case p.json:
			if err := enc.Encode(resultJSON{Topic: p.topic, Partition: r.Partition, Offset: r.Offset, Timestamp: r.Timestamp}); err != nil {
				return err
			}
		case surf.rich:
			surf.print([]string{surf.kit.S(kit.OK, "✓ ") + surf.kit.S(kit.FG, fmt.Sprintf("sent to %s/%d", p.topic, r.Partition)) +
				surf.kit.S(kit.Muted, " @ "+screens.Num(float64(r.Offset)))})
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %s failed: %w", failed, plural(len(recs), "record"), first)
	}
	return nil
}

func cmpErr(a, b error) error {
	if a != nil {
		return a
	}
	return b
}

// lines sends each stdin line as a record, in batches.
func (p producer) lines(ctx context.Context, in io.Reader, keySep string) error {
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	var batch []produce.Record
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := p.send(ctx, batch, surface{})
		batch = batch[:0]
		return err
	}
	n := 0
	for sc.Scan() {
		n++
		r, err := produce.ParseLine(sc.Text(), keySep)
		if err != nil {
			return fmt.Errorf("line %d: %w", n, err)
		}
		r.Headers, r.Partition = p.headers, p.partition
		batch = append(batch, r)
		if len(batch) == produceBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return flush()
}

// edit runs the editor until the user quits, then prints where each sent
// record landed.
func (p producer) edit(cmd *cobra.Command, themeName string) error {
	th, err := theme.Get(themeName)
	if err != nil {
		th, _ = theme.Get(theme.Default)
	}
	m := newEditor(p, kit.New(th, os.Getenv("NO_COLOR") == ""))
	m.bg = th.BG
	final, err := tea.NewProgram(m, tea.WithContext(cmd.Context())).Run()
	if err != nil {
		return err
	}
	for _, line := range final.(editorModel).sent {
		fmt.Fprintln(cmd.OutOrStdout(), line)
	}
	return nil
}
