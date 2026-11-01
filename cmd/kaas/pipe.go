package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	_ "github.com/IshaanNene/Kafka-as-a-Service/connectors/httpio" // http, https and listen endpoints
	"github.com/IshaanNene/Kafka-as-a-Service/core/pipe"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

// pipeFile is a pipeline written down, for kaas pipe -f.
type pipeFile struct {
	Name    string   `yaml:"name"`
	Brokers string   `yaml:"brokers"`
	From    []string `yaml:"from"`
	To      []string `yaml:"to"`
	Where   string   `yaml:"where"`
	Max     int64    `yaml:"max"`
	For     string   `yaml:"for"`
	Plugins []string `yaml:"plugins"` // directories to find plugins in
}

func readPipeFile(path string) (pipeFile, error) {
	var f pipeFile
	b, err := os.ReadFile(path)
	if err != nil {
		return f, err
	}
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true) // a misspelt key is an error, not a silent no-op
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return f, fmt.Errorf("%s: %w", path, err)
	}
	return f, nil
}

func newPipeCmd(brokers, themeName *string) *cobra.Command {
	var (
		from, to, pluginDirs []string
		where, file, output  string
		limit                int64
		forDur               time.Duration
	)
	cmd := &cobra.Command{
		Use:   "pipe",
		Short: "Move records between Kafka, files, HTTP and plugins, filtered on the way",
		Long: `Read records from one or more sources and write each to every sink. A batch
is acknowledged to its source only after every sink wrote it, so a Kafka
consumer group commits only what landed and an HTTP sender is answered
only once its records are stored: delivery is at least once.

Endpoints are URLs:
  kafka:orders?group=etl        a topic, in a consumer group (or from=start, stop=end)
  file:orders.jsonl             JSON lines, CSV (.csv) or text
  stdin, stdout                 the same formats on the process's streams
  https://hooks.example.com/x   a webhook sink, retried on 429 and 5xx
  listen://:8080/ingest         an HTTP source; POST JSON lines or an array
  gen:orders?rate=500           made-up orders, clicks, payments or iot data
  anything:else                 a plugin: kaas-source-anything or kaas-sink-anything

--where keeps records a CEL expression passes; it sees key, value (parsed
JSON), raw, headers, topic, partition, offset and time.`,
		Example: `  # Copy a topic to a JSON-lines file and stop at its end
  kaas pipe --from 'kafka:orders?stop=end' --to file:orders.jsonl
  # Big EU orders to a webhook and an audit topic, as a consumer group
  kaas pipe --from 'kafka:orders?group=eu-big' \
    --where 'value.amount > 100 && headers["region"] == "eu"' \
    --to https://hooks.example.com/orders --to kafka:audit
  # Replay a sample taken with kaas consume -o json into staging
  kaas pipe --from file:sample.jsonl --to 'kafka:orders?brokers=staging:9092'
  # Load test: 2,000 made-up payments a second for a minute
  kaas pipe --from 'gen:payments?rate=2000' --to kafka:payments --for 1m
  # A pipeline kept in a file
  kaas pipe -f pipelines/orders-to-lake.yaml`,
		Args: unknownCommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := oneOf(cmd, "output", output, "screen", "json"); err != nil {
				return err
			}
			name := "pipe"
			seeds := *brokers
			if file != "" {
				f, err := readPipeFile(file)
				if err != nil {
					return usageError(cmd, "file", file, err.Error())
				}
				from, to = append(f.From, from...), append(f.To, to...)
				pluginDirs = append(pluginDirs, f.Plugins...)
				if where == "" {
					where = f.Where
				}
				if limit == 0 {
					limit = f.Max
				}
				if forDur == 0 && f.For != "" {
					d, err := time.ParseDuration(f.For)
					if err != nil {
						return usageError(cmd, "file", file, "for: "+err.Error())
					}
					forDur = d
				}
				if f.Brokers != "" && !cmd.Flags().Changed("brokers") {
					seeds = f.Brokers
				}
				if f.Name != "" {
					name = f.Name
				}
			}
			if len(from) == 0 {
				return usageError(cmd, "from", "", "required: where records come from, as kafka:orders, file:x.jsonl or stdin")
			}
			if len(to) == 0 {
				return usageError(cmd, "to", "", "required: where records go, as kafka:copy, file:x.jsonl or stdout")
			}
			toStdout := false
			for _, s := range to {
				toStdout = toStdout || s == "-" || strings.HasPrefix(s, "stdout")
			}
			if output == "json" && toStdout {
				return usageError(cmd, "output", output, "stdout is a sink, so the stats can't go there too")
			}
			var o pipe.Options
			if where != "" {
				f, err := pipe.NewFilter(where)
				if err != nil {
					return usageError(cmd, "where", where, strings.TrimPrefix(err.Error(), "--where: "))
				}
				o.Where = f
			}
			if limit < 0 {
				return usageError(cmd, "max", fmt.Sprint(limit), "must be 0 or more")
			}
			o.Max = limit

			env := pipe.Env{
				Brokers: splitSeeds(seeds), Stdin: cmd.InOrStdin(), Stdout: cmd.OutOrStdout(),
				PluginPath: pluginDirs, Stderr: cmd.ErrOrStderr(),
			}
			if usesCluster(append(append([]string(nil), from...), to...)) {
				if _, err := connect(cmd, seeds, *themeName); err != nil {
					return err
				}
			}
			var srcs, sinks []pipe.Endpoint
			closeAll := func() {
				for _, e := range srcs {
					_ = e.Source.Close()
				}
				for _, e := range sinks {
					_ = e.Sink.Close()
				}
			}
			for _, s := range from {
				e, err := pipe.OpenSource(s, env)
				if err != nil {
					closeAll()
					return usageError(cmd, "from", s, err.Error())
				}
				srcs = append(srcs, e)
			}
			for _, s := range to {
				e, err := pipe.OpenSink(s, env)
				if err != nil {
					closeAll()
					return usageError(cmd, "to", s, err.Error())
				}
				sinks = append(sinks, e)
			}

			ctx := cmd.Context()
			if forDur > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, forDur)
				defer cancel()
			}
			p := pipe.New(srcs, sinks, o)
			// The screen goes to stderr, so stdout stays free for records;
			// not when records go to the same terminal, which would tear it.
			surf := detect(cmd.ErrOrStderr(), *themeName)
			if toStdout && isTerminal(cmd.OutOrStdout()) {
				surf.rich = false
			}
			v := &pipeView{name: name, p: p, where: where, max: limit, sinks: len(sinks)}
			err := v.run(ctx, surf)
			st := p.Stats()
			if output == "json" {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				if jerr := enc.Encode(pipeJSON(name, st)); jerr != nil {
					return jerr
				}
			} else if !surf.rich {
				fmt.Fprintln(cmd.ErrOrStderr(), pipeSummary(st))
			}
			return err
		},
	}
	f := cmd.Flags()
	f.StringArrayVar(&from, "from", nil, "a source; repeat to read several at once")
	f.StringArrayVar(&to, "to", nil, "a sink; repeat to write every record to each")
	f.StringVar(&where, "where", "", "keep records this CEL expression passes, as 'value.amount > 100'")
	f.Int64Var(&limit, "max", 0, "stop after writing this many records; 0 for no limit")
	f.DurationVar(&forDur, "for", 0, "stop after this long, as 30s or 1h; 0 to run until the sources end or Ctrl-C")
	f.StringVarP(&file, "file", "f", "", "a pipeline file (YAML: name, brokers, from, to, where, max, for, plugins)")
	f.StringArrayVar(&pluginDirs, "plugin-dir", nil, "a directory to find kaas-source-* and kaas-sink-* plugins in, before the defaults")
	f.StringVarP(&output, "output", "o", "screen", "output: screen (a live data-flow on a terminal, a summary line elsewhere) or json (the final stats)")
	cmd.AddCommand(newPipePluginsCmd())
	return cmd
}

func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

// usesCluster is whether a spec reads or writes the --brokers cluster, so
// it's worth probing before the pipe starts.
func usesCluster(specs []string) bool {
	for _, s := range specs {
		if strings.HasPrefix(s, "kafka:") && !strings.Contains(s, "brokers=") {
			return true
		}
	}
	return false
}

// pipeView draws a run as the data-flow piece, or nothing until the end.
type pipeView struct {
	name  string
	p     *pipe.Pipe
	where string
	max   int64
	sinks int

	frame   int
	last    pipe.Stats
	written []int64 // per sink, the frame before
	rate    float64
	at      time.Time
}

func (v *pipeView) run(ctx context.Context, surf surface) error {
	if !surf.rich {
		return v.p.Run(ctx)
	}
	done := make(chan error, 1)
	go func() { done <- v.p.Run(ctx) }()
	lv := &live{s: surf}
	defer lv.done()
	t := time.NewTicker(120 * time.Millisecond)
	defer t.Stop()
	still := os.Getenv("KAAS_REDUCED_MOTION") == "1"
	for {
		select {
		case err := <-done:
			lv.draw(v.screen(v.p.Stats(), time.Now()).Render(surf.kit, surf.width))
			return err
		case now := <-t.C:
			if !still {
				v.frame++
			}
			lv.draw(v.screen(v.p.Stats(), now).Render(surf.kit, surf.width))
		}
	}
}

// screen is st as the data-flow piece.
func (v *pipeView) screen(st pipe.Stats, now time.Time) screens.DataFlow {
	if !v.at.IsZero() {
		if dt := now.Sub(v.at).Seconds(); dt > 0 {
			inst := float64(st.Written-v.last.Written) / dt
			if v.rate == 0 {
				v.rate = inst
			} else {
				v.rate = 0.7*v.rate + 0.3*inst // smoothed, so the number can be read
			}
		}
	}
	d := screens.DataFlow{InFlight: st.InFlight, Batch: 500, Frame: v.frame}
	for _, s := range st.Sources {
		e := screens.FlowEnd{Name: specName(s.Spec), Count: screens.Num(float64(s.Read)) + " read", State: screens.FlowIdle}
		switch {
		case s.Err != "":
			e.State, e.Detail = screens.FlowFailed, s.Err
		case s.Done:
			e.State = screens.FlowDone
		case !st.Done && !s.LastRead.IsZero() && now.Sub(s.LastRead) < 2*time.Second:
			e.State = screens.FlowLive
		}
		d.Sources = append(d.Sources, e)
	}
	if len(v.written) != len(st.Sinks) {
		v.written = make([]int64, len(st.Sinks))
	}
	for i, s := range st.Sinks {
		e := screens.FlowEnd{Name: specName(s.Spec), Count: screens.Num(float64(s.Written)) + " written", Sub: screens.Bytes(s.Bytes) + " out", State: screens.FlowIdle}
		switch {
		case s.Err != "":
			e.State, e.Detail = screens.FlowFailed, s.Err
		case st.Done:
			e.State = screens.FlowDone
		case s.Written > v.written[i]:
			e.State = screens.FlowLive
		}
		v.written[i] = s.Written
		if v.max > 0 {
			e.Fill = float64(s.Written) / float64(v.max)
		} else {
			e.Fill = math.Log10(float64(s.Written)+1) / 7 // a million records fills most of it
		}
		d.Sinks = append(d.Sinks, e)
	}
	keep := "keep all"
	if v.where != "" {
		keep = "where " + v.where
	}
	write := fmt.Sprintf("write ×%d", v.sinks)
	if v.max > 0 {
		write += " ≤ " + screens.Num(float64(v.max))
	}
	d.Steps = []screens.FlowStep{{Text: "read"}, {Text: keep}, {Text: write}, {Text: "ack"}}
	if st.InFlight > 0 || v.rate > 0.5 {
		d.Steps[v.frame/3%len(d.Steps)].Active = true
	}
	d.Stats = []screens.FlowStat{
		{Label: "written", Value: screens.Num(float64(st.Written)), Role: kit.OK},
		{Label: "filtered", Value: screens.Num(float64(st.Filtered)), Role: kit.FG},
		{Label: "rejected", Value: screens.Num(float64(st.Rejected)), Role: kit.Err},
		{Label: "rate", Value: rateText(v.rate), Role: kit.FG},
	}
	d.Job = v.name + "  " + clock(now.Sub(st.Started))
	if st.Started.IsZero() {
		d.Job = v.name
	}
	v.last, v.at = st, now
	return d
}

// specName is an endpoint without its parameters, which a box has no
// room for; a credential in a query string stays off the screen too.
func specName(spec string) string {
	base, _, _ := strings.Cut(spec, "?")
	return base
}

func rateText(r float64) string {
	switch {
	case r >= 1e6:
		return fmt.Sprintf("%.1fM/s", r/1e6)
	case r >= 1e4:
		return fmt.Sprintf("%.0fk/s", r/1e3)
	case r >= 1e3:
		return fmt.Sprintf("%.1fk/s", r/1e3)
	}
	return fmt.Sprintf("%.0f/s", r)
}

// clock is d as mm:ss, or h:mm:ss past an hour.
func clock(d time.Duration) string {
	s := int(d.Seconds())
	if s >= 3600 {
		return fmt.Sprintf("%d:%02d:%02d", s/3600, s/60%60, s%60)
	}
	return fmt.Sprintf("%02d:%02d", s/60, s%60)
}

// pipeSummary is a finished run as one line, for logs.
func pipeSummary(st pipe.Stats) string {
	d := time.Duration(0)
	if !st.Started.IsZero() {
		d = time.Since(st.Started)
	}
	line := fmt.Sprintf("pipe: read %s · wrote %s · filtered %s · rejected %s · %s",
		screens.Num(float64(st.Read)), screens.Num(float64(st.Written)),
		screens.Num(float64(st.Filtered)), screens.Num(float64(st.Rejected)), took(d))
	if st.Err != "" {
		line += " · failed"
	}
	return line
}

type pipeEndJSON struct {
	Spec    string `json:"spec"`
	Records int64  `json:"records"`
	Bytes   int64  `json:"bytes,omitempty"`
	Done    bool   `json:"done,omitempty"`
	Error   string `json:"error,omitempty"`
}

type pipeStatsJSON struct {
	Name     string        `json:"name"`
	Seconds  float64       `json:"seconds"`
	Read     int64         `json:"read"`
	Written  int64         `json:"written"`
	Filtered int64         `json:"filtered"`
	Rejected int64         `json:"rejected"`
	Acked    int64         `json:"acked"`
	Sources  []pipeEndJSON `json:"sources"`
	Sinks    []pipeEndJSON `json:"sinks"`
	Error    string        `json:"error,omitempty"`
}

func pipeJSON(name string, st pipe.Stats) pipeStatsJSON {
	out := pipeStatsJSON{Name: name, Read: st.Read, Written: st.Written, Filtered: st.Filtered,
		Rejected: st.Rejected, Acked: st.Acked, Error: st.Err}
	if !st.Started.IsZero() {
		out.Seconds = math.Round(time.Since(st.Started).Seconds()*1000) / 1000
	}
	for _, s := range st.Sources {
		out.Sources = append(out.Sources, pipeEndJSON{Spec: s.Spec, Records: s.Read, Done: s.Done, Error: s.Err})
	}
	for _, s := range st.Sinks {
		out.Sinks = append(out.Sinks, pipeEndJSON{Spec: s.Spec, Records: s.Written, Bytes: s.Bytes, Error: s.Err})
	}
	return out
}

func newPipePluginsCmd() *cobra.Command {
	var output string
	var dirs []string
	cmd := &cobra.Command{
		Use:   "plugins",
		Short: "List the endpoint kinds a pipe can use: built in, and plugins found",
		Long: `List the built-in endpoint schemes and every kaas-source-* and kaas-sink-*
executable on --plugin-dir, $KAAS_PLUGIN_PATH, ~/.config/kaas/plugins and
PATH. A plugin is any program that speaks JSON lines on stdin and stdout;
see docs/USAGE.md#plugins for the protocol.`,
		Example: `  kaas pipe plugins
  kaas pipe plugins -o json | jq '.[] | select(.builtin | not)'`,
		Args: unknownCommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := oneOf(cmd, "output", output, "table", "json"); err != nil {
				return err
			}
			type row struct {
				Scheme  string `json:"scheme"`
				Source  bool   `json:"source"`
				Sink    bool   `json:"sink"`
				Builtin bool   `json:"builtin"`
				Summary string `json:"summary"`
				Path    string `json:"path,omitempty"`
			}
			var rows []row
			for _, k := range pipe.Kinds() {
				rows = append(rows, row{Scheme: k.Scheme, Source: k.Source != nil, Sink: k.Sink != nil, Builtin: true, Summary: k.Summary})
			}
			for _, p := range pipe.Discover(pipe.Env{PluginPath: dirs}) {
				rows = append(rows, row{Scheme: p.Scheme, Source: p.Role == "source", Sink: p.Role == "sink", Summary: "plugin", Path: p.Path})
			}
			if output == "json" {
				enc := json.NewEncoder(cmd.OutOrStdout())
				enc.SetIndent("", "  ")
				return enc.Encode(rows)
			}
			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "%-8s %-6s %-6s %s\n", "SCHEME", "SOURCE", "SINK", "WHAT")
			for _, r := range rows {
				what := r.Summary
				if !r.Builtin {
					what = r.Path
				}
				fmt.Fprintf(w, "%-8s %-6s %-6s %s\n", r.Scheme, yes(r.Source), yes(r.Sink), what)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "table", "output format: table|json")
	cmd.Flags().StringArrayVar(&dirs, "plugin-dir", nil, "a directory to look in first")
	return cmd
}

func yes(b bool) string {
	if b {
		return "yes"
	}
	return "-"
}
