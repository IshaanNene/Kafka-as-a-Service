package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/clients"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

var clientSorts = []string{"rate", "parts", "name"}

// clientTabs are the screen's sort tabs, in clientSorts order.
var clientTabs = []string{"Rate", "Parts", "Name"}

// idleAfter hides producers that have not written for this long: Kafka
// keeps a producer's state for producer.id.expiration.ms, a day by
// default, long after it disconnects.
const idleAfter = 10 * time.Minute

func newClientsCmd(brokers, themeName *string) *cobra.Command {
	var (
		output, sortBy string
		interval       time.Duration
		once, all      bool
	)
	cmd := &cobra.Command{
		Use:   "clients [pattern]",
		Short: "Consumers and producers the cluster can see, by records per second, live",
		Long: `List the clients the Kafka protocol can see. Consumers are consumer group
members, with their client.id, host, group, partitions and lag. Producers
are idempotent and transactional producers, from DescribeProducers
(KIP-664), by producer ID with the topics they write.

Kafka has no API that lists connections or request rates, so the rate
here is records per second: a member's commits, a producer's sequence
numbers, between two refreshes. A single read (--once, a pipe, or -o
json) has no rate. Producers with idempotence turned off, and readers
without a group such as kaas consume, don't show.

Kafka remembers producers for a day after their last write, so producers
idle for more than 10m are hidden unless you pass --all. The pattern is a
glob over client.id, group and topic (billing-*).`,
		Example: `  # Every client, busiest first, refreshing every 2s
  kaas clients
  # Members of the billing groups
  kaas clients 'billing*'
  # Hosts running consumers, for a script
  kaas clients -o json | jq -r '.[] | select(.kind == "consumer") | .host' | sort -u`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "table", "json"); err != nil {
				return err
			}
			if err := oneOf(cmd, "sort", sortBy, clientSorts...); err != nil {
				return err
			}
			if interval < 100*time.Millisecond {
				return usageError(cmd, "interval", interval.String(), "must be at least 100ms")
			}
			pattern := ""
			if len(args) == 1 {
				pattern = args[0]
				if _, err := path.Match(pattern, ""); err != nil {
					return usageError(cmd, "pattern", pattern, "not a valid glob: "+err.Error())
				}
			}
			if _, err := connect(cmd, *brokers, *themeName); err != nil {
				return err
			}
			cl, err := kgo.NewClient(kgo.SeedBrokers(splitSeeds(*brokers)...))
			if err != nil {
				return err
			}
			defer cl.Close()
			cv := &clientView{cl: cl, adm: kadm.NewClient(cl), pattern: pattern, sortBy: sortBy, interval: interval, all: all}
			surf := detect(cmd.OutOrStdout(), *themeName)
			if surf.rich && output == "table" && !once {
				return refreshLoop(cmd.Context(), surf, interval,
					func(ctx context.Context) error { cv.err = cv.refresh(ctx); return cv.err },
					func() []string { return cv.screen(true).Render(surf.kit, surf.width) })
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			defer cancel()
			if err := cv.refresh(ctx); err != nil {
				return err
			}
			return cv.print(cmd, output, surf)
		},
	}
	f := cmd.Flags()
	f.StringVarP(&output, "output", "o", "table", "output format: table|json")
	f.StringVar(&sortBy, "sort", "rate", "sort by: "+strings.Join(clientSorts, "|"))
	f.DurationVar(&interval, "interval", 2*time.Second, "refresh interval on a terminal")
	f.BoolVar(&once, "once", false, "read once and exit instead of refreshing")
	f.BoolVar(&all, "all", false, "include producers idle for more than "+screens.Span(idleAfter.Seconds()))
	return cmd
}

// clientView is kaas clients' state across refreshes: the last two
// samples for rates, and total records/s over time for the chart.
type clientView struct {
	cl       *kgo.Client
	adm      *kadm.Client
	pattern  string
	sortBy   string
	interval time.Duration
	all      bool

	prev, cur clients.Sample
	rows      []clientRow
	idle      int // producers hidden for being idle
	partial   error
	history   []float64
	err       error
}

// clientRow is one shown client with its rate, -1 until known.
type clientRow struct {
	clients.Client
	Rate float64
}

func (cv *clientView) refresh(ctx context.Context) error {
	s, err := snapshot.Fetch(ctx, cv.cl)
	if err != nil {
		return err
	}
	var topics []string
	for _, t := range s.Topics {
		if !t.Internal {
			topics = append(topics, t.Name)
		}
	}
	sample, err := clients.Fetch(ctx, cv.adm, topics)
	if len(sample.Clients) == 0 && err != nil {
		return err
	}
	cv.partial = err
	cv.apply(sample)
	return nil
}

// apply makes sample the current one and rebuilds the shown rows.
func (cv *clientView) apply(sample clients.Sample) {
	cv.prev, cv.cur = cv.cur, sample
	cv.rows, cv.idle = nil, 0
	total := -1.0
	for _, c := range sample.Clients {
		if !cv.match(c) {
			continue
		}
		if c.Kind == clients.Producer && !cv.all && !c.Txn && sample.At.Sub(c.Last) > idleAfter {
			cv.idle++
			continue
		}
		r := clientRow{Client: c, Rate: -1}
		if !cv.prev.At.IsZero() {
			r.Rate = clients.Rate(cv.prev, cv.cur, c)
		}
		if r.Rate >= 0 {
			total = max(total, 0) + r.Rate
		}
		cv.rows = append(cv.rows, r)
	}
	if total >= 0 {
		cv.history = append(cv.history, total)
		if n := len(cv.history); n > historyLen {
			cv.history = cv.history[n-historyLen:]
		}
	}
	cv.sort()
}

// match is the pattern against a client's id, group, transactional id or
// any of its topics.
func (cv *clientView) match(c clients.Client) bool {
	if cv.pattern == "" {
		return true
	}
	for _, s := range append([]string{c.ID, c.Group, c.TxnID}, c.Topics...) {
		if ok, _ := path.Match(cv.pattern, s); ok && s != "" {
			return true
		}
	}
	return false
}

func (cv *clientView) sort() {
	slices.SortStableFunc(cv.rows, func(a, b clientRow) int {
		byName := cmp.Or(cmp.Compare(a.name(), b.name()), cmp.Compare(a.Group, b.Group), cmp.Compare(a.Host, b.Host))
		switch cv.sortBy {
		case "parts":
			return cmp.Or(cmp.Compare(b.Parts, a.Parts), byName)
		case "name":
			return byName
		}
		return cmp.Or(cmp.Compare(b.Rate, a.Rate), byName)
	})
}

func (r clientRow) name() string {
	if r.Kind == clients.Producer {
		return cmp.Or(r.TxnID, screens.ProducerName(r.ID))
	}
	return cmp.Or(r.ID, "(no client.id)")
}

func (cv *clientView) screen(live bool) screens.Clients {
	v := screens.Clients{
		Tabs: clientTabs, Sorted: slices.Index(clientSorts, cv.sortBy),
		Badge:   plural(len(cv.rows), "client"),
		History: cv.history,
		Empty:   "No clients the cluster can see yet. Consumers show once they join a group; producers once they write with idempotence on, the default in current clients.",
	}
	if cv.pattern != "" {
		v.Empty = fmt.Sprintf("No clients match %s.", cv.pattern)
	}
	now := cv.cur.At
	for _, r := range cv.rows {
		row := screens.ClientRow{Name: r.name(), Producer: r.Kind == clients.Producer, Host: r.Host,
			Parts: r.Parts, Rate: r.Rate, Lag: r.Lag, Txn: r.Txn}
		if row.Producer {
			row.Where = topicsText(r.Topics)
			row.Idle = now.Sub(r.Last)
		} else {
			row.Where = r.Group
		}
		v.Rows = append(v.Rows, row)
	}
	hint := []string{"sorted by " + cv.sortBy}
	if live {
		v.Badge = "live · every " + cv.interval.String()
		hint = append(hint, "ctrl-c to stop")
		if cv.prev.At.IsZero() {
			hint = append(hint, "rates after the next refresh")
		}
	} else {
		hint = append(hint, "no rates in a single read")
	}
	if cv.idle > 0 {
		hint = append(hint, fmt.Sprintf("%s idle over %s hidden, --all shows them", plural(cv.idle, "producer"), screens.Span(idleAfter.Seconds())))
	}
	if cv.partial != nil {
		hint = append(hint, "partial: "+cv.partial.Error())
	}
	hint = append(hint, "producers without idempotence and group-less readers don't show")
	v.Hint = strings.Join(hint, " · ")
	if cv.err != nil {
		v.Hint = "refresh failed, showing the last read: " + cv.err.Error()
	}
	return v
}

// topicsText is "orders" or "orders +2".
func topicsText(ts []string) string {
	switch len(ts) {
	case 0:
		return ""
	case 1:
		return ts[0]
	}
	return fmt.Sprintf("%s +%d", ts[0], len(ts)-1)
}

type clientJSON struct {
	Kind       string     `json:"kind"`
	ClientID   string     `json:"client_id,omitempty"`
	MemberID   string     `json:"member_id,omitempty"`
	Group      string     `json:"group,omitempty"`
	Host       string     `json:"host,omitempty"`
	ProducerID string     `json:"producer_id,omitempty"`
	TxnID      string     `json:"transactional_id,omitempty"`
	Epoch      *int16     `json:"epoch,omitempty"`
	Topics     []string   `json:"topics"`
	Partitions int        `json:"partitions"`
	Lag        *int64     `json:"lag,omitempty"` // consumers; omitted when unknown
	LastWrite  *time.Time `json:"last_write,omitempty"`
	InTxn      bool       `json:"in_transaction,omitempty"`
}

func (cv *clientView) json() []clientJSON {
	out := []clientJSON{}
	for _, r := range cv.rows {
		j := clientJSON{Kind: string(r.Kind), Topics: r.Topics, Partitions: r.Parts}
		if j.Topics == nil {
			j.Topics = []string{}
		}
		if r.Kind == clients.Producer {
			last, epoch := r.Last, r.Epoch
			j.ProducerID, j.TxnID, j.Epoch, j.InTxn = r.ID, r.TxnID, &epoch, r.Txn
			if !last.IsZero() {
				j.LastWrite = &last
			}
		} else {
			j.ClientID, j.MemberID, j.Group, j.Host, j.Lag = r.ID, r.Member, r.Group, r.Host, known(r.Lag)
		}
		out = append(out, j)
	}
	return out
}

func (cv *clientView) print(cmd *cobra.Command, output string, surf surface) error {
	out := cmd.OutOrStdout()
	if output == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(cv.json())
	}
	if surf.rich {
		surf.print(cv.screen(false).Render(surf.kit, surf.width))
		return nil
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "KIND\tCLIENT\tGROUP\tHOST\tTOPICS\tPARTITIONS\tLAG")
	for _, r := range cv.rows {
		lag := "-"
		if r.Lag >= 0 {
			lag = fmt.Sprint(r.Lag)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%s\n", r.Kind, r.name(), cmp.Or(r.Group, "-"), cmp.Or(r.Host, "-"),
			cmp.Or(strings.Join(r.Topics, ","), "-"), r.Parts, lag)
	}
	return tw.Flush()
}
