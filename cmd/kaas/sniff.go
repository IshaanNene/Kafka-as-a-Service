package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/IshaanNene/Kafka-as-a-Service/core/sniff"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newSniffCmd(brokers, themeName *string) *cobra.Command {
	var (
		output, listen, clientID string
		apis                     []string
		maxReqs                  int
		trace, profile           bool
		window                   time.Duration
	)
	cmd := &cobra.Command{
		Use:   "sniff",
		Short: "Proxy the cluster and watch every request clients send it",
		Long: `Run a proxy in front of the cluster and show every Kafka request that
passes through it, coloured by API: produce, fetch, metadata, groups,
transactions and the requests that change the cluster.

Point a client at the --listen address instead of the cluster. The proxy
gives each broker a listener of its own (the ports after --listen's) and
rewrites the broker addresses in the answers that carry them, so the
client's later connections come through it too. Everything else is
forwarded as it came. Nothing is changed for clients that don't use the
proxy.

On a terminal the capture list is live: ↑↓ selects a request to show
its path and first bytes, enter follows the newest again. On a pipe each
request is a line; -o json gives every field, one object per line.

--trace times requests instead, on a waterfall: from the first request
through the proxy, for --for, each request's stages as the proxy sees
them: connect (dialing the broker, for a connection's first request),
send (its bytes in and on to the broker), queue (behind earlier requests
on the connection, which a broker answers in order), wait (the broker
working on it) and ack (its answer's bytes back). Lines mark when a
group was joined and the first produce was acknowledged.

--profile adds those stages up instead, as a flame graph: all, then each
API, broker and topic, then the stages, each as wide as its share of the
time. A fetch's wait can be polling rather than serving: all of it when
it came back empty ("idle poll"), and the part before its first record
was written when it waited for new ones ("long poll", by the records'
timestamps). Polls are hidden until i shows them.
It records until q, or for --for when given. On a pipe it prints folded
stacks when it ends, as flamegraph.pl and speedscope read them, or a
tree with -o json.`,
		Example: `  # Proxy on localhost:19092 and watch
  kaas sniff
  # ...then, in another terminal, send something through it
  kaas produce orders --brokers localhost:19092
  # Only produce and fetch requests
  kaas sniff --api produce --api fetch
  # The first 100 requests as JSON, for jq
  kaas sniff -n 100 -o json | jq -r '.api'
  # A waterfall of one service's first 10 seconds
  kaas sniff --trace --client-id billing-svc
  # Where produce and fetch time goes, until q
  kaas sniff --profile --api produce --api fetch
  # A minute's profile as folded stacks, for speedscope
  kaas sniff --profile --for 1m > kafka.folded`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := oneOf(cmd, "output", output, "text", "json"); err != nil {
				return err
			}
			if window <= 0 && cmd.Flags().Changed("for") {
				return usageError(cmd, "for", window.String(), "must be more than 0")
			}
			if cmd.Flags().Changed("for") && !trace && !profile {
				return usageError(cmd, "for", window.String(), "times a --trace or --profile: add one")
			}
			if trace && profile {
				return usageError(cmd, "profile", "true", "--trace and --profile are two views: pick one")
			}
			if profile && !cmd.Flags().Changed("for") {
				window = 0 // until q, or Ctrl-C
			}
			if maxReqs < 0 {
				return usageError(cmd, "max", strconv.Itoa(maxReqs), "must be 0 or more")
			}
			only, err := apiFilter(apis)
			if err != nil {
				return usageError(cmd, "api", strings.Join(apis, ","), err.Error())
			}
			if _, err := connect(cmd, *brokers, *themeName); err != nil {
				return err
			}
			cl, err := kgo.NewClient(kgo.SeedBrokers(splitSeeds(*brokers)...))
			if err != nil {
				return err
			}
			defer cl.Close()

			c := newCapture(only)
			c.clientID = clientID
			o := sniff.Options{Listen: listen, Out: c.add}
			if trace {
				o.Sent, o.Deep = c.addSent, true
			}
			if profile {
				o.Deep = true // to tell an idle poll from a fetch that brought records
			}
			sctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			p, err := sniff.StartWith(sctx, cl, o)
			cancel()
			if err != nil {
				if strings.Contains(err.Error(), "address already in use") || strings.Contains(err.Error(), "listen address") {
					return usageError(cmd, "listen", listen, err.Error())
				}
				return err
			}
			defer p.Close()

			surf := detect(cmd.OutOrStdout(), *themeName)
			if profile {
				return runProfile(cmd, surf, p, c, traceOf(apis, clientID), window, maxReqs, output == "json")
			}
			if trace {
				return runTrace(cmd, surf, p, c, traceOf(apis, clientID), window, maxReqs, output == "json")
			}
			if surf.rich && output == "text" && answerable(cmd.InOrStdin()) {
				m := newSniffModel(surf.kit, p, c, filterText(apis), maxReqs)
				m.w, m.h = surf.width, surf.height
				_, err := tea.NewProgram(m, tea.WithContext(cmd.Context())).Run()
				if cmd.Context().Err() != nil {
					return nil // Ctrl-C ends a capture, not a failure
				}
				return err
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "kaas sniff: proxy on %s for %s; point clients there\n", p.Addr(), routesText(p.Routes()))
			return c.stream(cmd.Context(), cmd.OutOrStdout(), output == "json", maxReqs)
		},
	}
	f := cmd.Flags()
	f.StringVar(&listen, "listen", "localhost:19092", "where clients connect; the other brokers get the ports after it")
	f.StringSliceVar(&apis, "api", nil, "only these requests, by API name such as produce or fetch (repeatable)")
	f.StringVar(&clientID, "client-id", "", "only requests from clients with this client ID")
	f.BoolVar(&trace, "trace", false, "time requests on a waterfall: connect, send, queue, wait, ack")
	f.BoolVar(&profile, "profile", false, "add up where request time goes, on a flame graph")
	f.DurationVar(&window, "for", 10*time.Second, "how long --trace (or --profile, when given) records, from the first request")
	f.IntVarP(&maxReqs, "max", "n", 0, "stop after this many requests; 0 for no limit")
	f.StringVarP(&output, "output", "o", "text", "output format: text|json (one object per line, on a pipe)")
	return cmd
}

// apiFilter is the API keys names asks for, nil for all; names match
// without case, so produce is Produce.
func apiFilter(names []string) (map[int16]bool, error) {
	if len(names) == 0 {
		return nil, nil
	}
	known := map[string]int16{}
	for k := int16(0); k < 100; k++ {
		if kmsg.RequestForKey(k) != nil {
			known[strings.ToLower(kmsg.NameForKey(k))] = k
		}
	}
	only := map[int16]bool{}
	for _, n := range names {
		k, ok := known[strings.ToLower(strings.TrimSpace(n))]
		if !ok {
			return nil, fmt.Errorf("no Kafka API is called %q: try produce, fetch, metadata, joingroup or offsetcommit", n)
		}
		only[k] = true
	}
	return only, nil
}

func filterText(names []string) string {
	if len(names) == 0 {
		return "all requests"
	}
	return strings.Join(names, ", ")
}

func routesText(rs []sniff.Route) string {
	var parts []string
	for _, r := range rs {
		parts = append(parts, fmt.Sprintf("broker %d %s → %s", r.Broker, r.Listen, r.Upstream))
	}
	return strings.Join(parts, ", ")
}

// capture takes requests from the proxy's goroutines. It never blocks
// them: when the screen falls behind, requests are counted as dropped
// rather than slowing the clients down.
type capture struct {
	only     map[int16]bool
	clientID string
	ch       chan sniff.Request
	sent     chan sniff.Request // with --trace: requests on their way, before their answers
	dropped  atomic.Int64
	start    time.Time // the capture's clock starts here
}

func newCapture(only map[int16]bool) *capture {
	return &capture{only: only, ch: make(chan sniff.Request, 4096), sent: make(chan sniff.Request, 4096), start: time.Now()}
}

func (c *capture) wants(r sniff.Request) bool {
	return (c.only == nil || c.only[r.Key]) && (c.clientID == "" || r.ClientID == c.clientID)
}

func (c *capture) add(r sniff.Request) {
	if !c.wants(r) {
		return
	}
	select {
	case c.ch <- r:
	default:
		c.dropped.Add(1)
	}
}

// addSent never counts a drop: the answer, when it comes, is what counts.
func (c *capture) addSent(r sniff.Request) {
	if !c.wants(r) || !r.Answered {
		return
	}
	select {
	case c.sent <- r:
	default:
	}
}

type requestJSON struct {
	N         int64     `json:"n"`
	Time      time.Time `json:"time"`
	Client    string    `json:"client"`
	ClientID  string    `json:"client_id"`
	Broker    int32     `json:"broker"`
	API       string    `json:"api"`
	Key       int16     `json:"key"`
	Version   int16     `json:"version"`
	Bytes     int       `json:"bytes"`
	RespBytes int       `json:"response_bytes"`
	TookMs    float64   `json:"took_ms"`
	Answered  bool      `json:"answered"`
	Info      string    `json:"info"`
}

// stream prints each request as a line until ctx ends or max are printed.
func (c *capture) stream(ctx context.Context, out io.Writer, asJSON bool, maxReqs int) error {
	enc := json.NewEncoder(out)
	n := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case r := <-c.ch:
			var err error
			if asJSON {
				err = enc.Encode(requestJSON{r.N, r.At.UTC(), r.Client, r.ClientID, r.Broker, r.Name(), r.Key, r.Version,
					r.Size, r.RespSize, float64(r.Took.Microseconds()) / 1000, r.Answered, r.Info})
			} else {
				answer := "no answer due"
				if r.Answered {
					answer = fmt.Sprintf("→ %d B in %s", r.RespSize, screens.Dur(r.Took))
				}
				_, err = fmt.Fprintf(out, "%s %s broker %d %s v%d %d B %s %s\n",
					r.At.Format("15:04:05.000000"), r.Client, r.Broker, r.Name(), r.Version, r.Size, answer, r.Info)
			}
			if err != nil {
				return err
			}
			if n++; maxReqs > 0 && n >= maxReqs {
				return nil
			}
		}
	}
}

// keepReqs is how many requests the screen keeps to scroll back through.
const keepReqs = 2000

// mixOf is how many recent requests the API mix counts.
const mixOf = 200

// sniffModel is the live packet-sniffer screen.
type sniffModel struct {
	k       kit.Kit
	w, h    int
	p       *sniff.Proxy
	c       *capture
	filter  string
	max     int
	reqs    []sniff.Request
	total   int64
	sel     int // index into reqs; -1 follows the newest
	bytes   int64
	lastB   int64
	lastAt  time.Time
	rate    float64
	stopped bool
}

type sniffTick time.Time

func newSniffModel(k kit.Kit, p *sniff.Proxy, c *capture, filter string, maxReqs int) sniffModel {
	now := time.Now()
	return sniffModel{k: k, p: p, c: c, filter: filter, max: maxReqs, sel: -1, lastAt: now}
}

func sniffEvery() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(t time.Time) tea.Msg { return sniffTick(t) })
}

func (m sniffModel) Init() tea.Cmd { return sniffEvery() }

// take adds a request, keeping the selection on the same one as old
// requests fall off the front.
func (m *sniffModel) take(r sniff.Request) {
	m.reqs = append(m.reqs, r)
	m.total++
	m.bytes += int64(r.Size + r.RespSize)
	if n := len(m.reqs); n > keepReqs {
		m.reqs = m.reqs[n-keepReqs:]
		if m.sel >= 0 {
			m.sel = max(0, m.sel-(n-keepReqs))
		}
	}
}

func (m sniffModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
	case sniffTick:
	drain:
		for {
			select {
			case r := <-m.c.ch:
				m.take(r)
				if m.max > 0 && m.total >= int64(m.max) {
					m.stopped = true
					return m, tea.Quit
				}
			default:
				break drain
			}
		}
		now := time.Time(msg)
		if dt := now.Sub(m.lastAt).Seconds(); dt >= 1 {
			m.rate = float64(m.bytes-m.lastB) / dt
			m.lastB, m.lastAt = m.bytes, now
		}
		return m, sniffEvery()
	case tea.KeyPressMsg:
		switch msg.String() {
		case "ctrl+c", "esc", "q":
			m.stopped = true
			return m, tea.Quit
		case "up", "k":
			if len(m.reqs) > 0 {
				if m.sel < 0 {
					m.sel = len(m.reqs) - 1
				}
				m.sel = max(0, m.sel-1)
			}
		case "down", "j":
			if m.sel >= 0 {
				if m.sel++; m.sel >= len(m.reqs)-1 {
					m.sel = -1 // back at the newest: follow it
				}
			}
		case "enter", "end", "G":
			m.sel = -1
		case "home", "g":
			if len(m.reqs) > 0 {
				m.sel = 0
			}
		}
	}
	return m, nil
}

func (m sniffModel) screen() screens.Sniffer {
	routes := m.p.Routes()
	capture := "proxy on " + m.p.Addr()
	if len(routes) > 0 {
		capture += " → " + routes[0].Upstream
	}
	if len(routes) > 1 {
		capture += fmt.Sprintf(" +%d brokers", len(routes)-1)
	}
	s := screens.Sniffer{
		Capture: capture, Filter: m.filter, Total: m.total, Dropped: m.c.dropped.Load(),
		Sel: m.sel, MixOf: mixOf, Status: "capturing",
		Rate:    screens.Bytes(int64(m.rate)) + "/s",
		Keys:    "q quit · ↑↓ select · enter follow",
		Waiting: "nothing yet: point a client at " + m.p.Addr() + ", such as kaas consume <topic> --brokers " + m.p.Addr(),
	}
	if m.stopped {
		s.Status = ""
	}
	for _, r := range m.reqs {
		s.Rows = append(s.Rows, screens.SniffRow{
			N: r.N, Time: r.At.Sub(m.c.start).Seconds(), Client: r.Client,
			Broker: fmt.Sprintf("broker %d", r.Broker), API: r.Name(), Key: r.Key, Len: r.Size, Info: r.Info,
		})
	}
	sel := m.sel
	if sel < 0 || sel >= len(m.reqs) {
		sel = len(m.reqs) - 1
	}
	if sel >= 0 {
		r := m.reqs[sel]
		s.Detail = fmt.Sprintf("Request %d v%d: %s", r.N, r.Version, screens.Bytes(int64(r.Size)))
		switch {
		case !r.Answered:
			s.Detail += ", acks 0: no answer"
		default:
			s.Detail += fmt.Sprintf(" → %s in %s", screens.Bytes(int64(r.RespSize)), screens.Dur(r.Took))
		}
		if r.Rewrote {
			s.Detail += ", addresses rewritten"
		}
		who := r.Client
		if r.ClientID != "" {
			who += " (" + r.ClientID + ")"
		}
		s.Path = fmt.Sprintf("%s > broker %d > ", who, r.Broker)
		s.Head = r.Head
	}
	counts := map[int16]int{}
	for _, r := range m.reqs[max(0, len(m.reqs)-mixOf):] {
		counts[r.Key]++
	}
	for key, n := range counts {
		s.Mix = append(s.Mix, screens.SniffMix{API: kmsg.NameForKey(key), Key: key, Count: n})
	}
	sort.Slice(s.Mix, func(i, j int) bool {
		if s.Mix[i].Count != s.Mix[j].Count {
			return s.Mix[i].Count > s.Mix[j].Count
		}
		return s.Mix[i].Key < s.Mix[j].Key
	})
	return s
}

func (m sniffModel) View() tea.View {
	w, h := m.w, m.h
	if w == 0 {
		w = 100
	}
	if h == 0 {
		h = 30
	}
	return tea.NewView(kit.Join(m.screen().Render(m.k, w, max(20, h-1))))
}
