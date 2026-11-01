package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/core/topics"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

var topicSorts = []string{"status", "name", "partitions", "records", "disk"}

func newTopicsCmd(brokers, themeName *string) *cobra.Command {
	var (
		output, sortBy, sep string
		internal, tree      bool
	)
	cmd := &cobra.Command{
		Use:   "topics [pattern]",
		Short: "List topics with retention, cleanup policy and disk use",
		Long: `List topics with their partitions, replication, records retained, disk used
across all replicas, retention and cleanup policy. Values a topic sets itself,
rather than inheriting from the broker, are marked.

The pattern is a glob (orders.*, *-dlq); a plain name that matches no topic
suggests the closest ones. --tree groups topics by name prefix.`,
		Example: `  # Every topic, problems first
  kaas topics
  # The payments family, largest on disk first
  kaas topics 'payments.*' --sort disk
  # Topic names as a tree of prefixes
  kaas topics --tree
  # Compacted topics, for a script
  kaas topics -o json | jq -r '.[] | select(.cleanupPolicy | contains("compact")) | .name'`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "table", "json"); err != nil {
				return err
			}
			if err := oneOf(cmd, "sort", sortBy, topicSorts...); err != nil {
				return err
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
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			defer cancel()
			l, err := listTopics(ctx, cl, pattern, internal, sortBy)
			if err != nil {
				return err
			}
			l.tree, l.sep = tree, sep
			return l.print(cmd, output, *themeName)
		},
	}
	cmd.AddCommand(newTopicsDescribeCmd(brokers, themeName))
	cmd.AddCommand(newTopicsAlterCmd(brokers, themeName))
	cmd.AddCommand(newTopicsCreateCmd(brokers, themeName))
	f := cmd.Flags()
	f.StringVarP(&output, "output", "o", "table", "output format: table|json")
	f.StringVar(&sortBy, "sort", "status", "sort by: "+strings.Join(topicSorts, "|"))
	f.BoolVar(&internal, "internal", false, "include internal topics such as __consumer_offsets")
	f.BoolVar(&tree, "tree", false, "group topics into a tree by name prefix")
	f.StringVar(&sep, "sep", ".", "separator between name prefixes for --tree")
	return cmd
}

// topicList is everything kaas topics shows: the snapshot's rows joined
// with each topic's retention, cleanup and disk use.
type topicList struct {
	snap      *snapshot.Snapshot
	rows      []topicRow
	details   map[string]topics.Details
	detailErr error // details are best effort
	pattern   string
	hidden    int
	sortBy    string
	tree      bool
	sep       string
}

func listTopics(ctx context.Context, cl *kgo.Client, pattern string, internal bool, sortBy string) (topicList, error) {
	s, err := snapshot.Fetch(ctx, cl)
	if err != nil {
		return topicList{}, err
	}
	// A plain name is a lookup: missing means 404 with suggestions. A glob
	// that matches nothing is just an empty list.
	if pattern != "" && !strings.ContainsAny(pattern, "*?[") {
		if err := missingTopic(s, []string{pattern}); err != nil {
			return topicList{}, err
		}
	}
	keep := func(t snapshot.Topic) bool {
		if t.Internal && !internal && !strings.HasPrefix(pattern, "_") {
			return false
		}
		ok, _ := path.Match(pattern, t.Name)
		return pattern == "" || ok
	}
	var names []string
	for _, t := range s.Topics {
		if keep(t) {
			names = append(names, t.Name)
		}
	}
	adm := kadm.NewClient(cl)
	if len(names) > 0 {
		if starts, err := adm.ListStartOffsets(ctx, names...); err == nil {
			if ends, err := adm.ListEndOffsets(ctx, names...); err == nil {
				s = snapshot.WithOffsets(s, names, starts, ends, time.Now())
			}
		}
	}
	l := topicList{snap: s, pattern: pattern, sortBy: sortBy}
	l.details, l.detailErr = topics.Describe(ctx, adm, names)
	l.rows, l.hidden = topicRows(s, cmp.Or(map[string]string{"disk": "name"}[sortBy], sortBy), keep)
	if sortBy == "disk" {
		slices.SortStableFunc(l.rows, func(a, b topicRow) int {
			return cmp.Compare(l.details[b.name].Disk, l.details[a.name].Disk)
		})
	}
	return l, nil
}

func (l topicList) print(cmd *cobra.Command, output, themeName string) error {
	out := cmd.OutOrStdout()
	if output == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(l.json())
	}
	if surf := detect(out, themeName); surf.rich {
		if l.tree {
			surf.print(l.treeScreen().Render(surf.kit, surf.width))
		} else {
			surf.print(l.tableScreen().Render(surf.kit, surf.width))
		}
		return nil
	}
	if l.detailErr != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "kaas: warning: some topic details are unavailable: %v\n", l.detailErr)
	}
	if l.tree {
		var walk func(nodes []screens.TreeNode, depth int)
		walk = func(nodes []screens.TreeNode, depth int) {
			for _, n := range nodes {
				if len(n.Children) > 0 {
					fmt.Fprintf(out, "%s%s/\n", strings.Repeat("  ", depth), n.Name)
					walk(n.Children, depth+1)
					continue
				}
				fmt.Fprintf(out, "%s%s\n", strings.Repeat("  ", depth), n.Name)
			}
		}
		walk(l.treeScreen().Nodes, 0)
		return nil
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	// Raw numbers on a pipe, so awk and sort -n work: no separators or units.
	fmt.Fprintln(w, "TOPIC\tPARTITIONS\tRF\tRECORDS\tDISK_BYTES\tRETENTION\tCLEANUP\tSTATUS")
	raw := func(n int64) string {
		if n < 0 {
			return "-"
		}
		return strconv.FormatInt(n, 10)
	}
	for _, r := range l.rows {
		d := l.details[r.name]
		fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\n", r.name, r.partitions, r.rf, raw(r.records),
			raw(d.Disk), retentionText(d.Retention), cmp.Or(d.Cleanup, "-"), r.statusText())
	}
	return w.Flush()
}

// topicJSON is one topic in -o json: stable names for scripts.
type topicJSON struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replicationFactor"`
	UnderReplicated   int    `json:"underReplicated"`
	Offline           int    `json:"offline"`
	Records           *int64 `json:"records"`
	DiskBytes         *int64 `json:"diskBytes"`
	RetentionMs       *int64 `json:"retentionMs"`
	RetentionBytes    int64  `json:"retentionBytes"`
	CleanupPolicy     string `json:"cleanupPolicy"`
	Overrides         int    `json:"overrides"`
	Status            string `json:"status"`
}

func (l topicList) json() []topicJSON {
	out := make([]topicJSON, 0, len(l.rows))
	for _, r := range l.rows {
		d := l.details[r.name]
		j := topicJSON{
			Name: r.name, Partitions: r.partitions, ReplicationFactor: r.rf, UnderReplicated: r.urp, Offline: r.off,
			RetentionBytes: d.RetentionBytes, CleanupPolicy: d.Cleanup, Overrides: d.Overrides, Status: r.statusText(),
		}
		if r.records >= 0 {
			j.Records = &r.records
		}
		if d.Disk >= 0 {
			j.DiskBytes = &d.Disk
		}
		if d.Retention != 0 {
			ms := int64(-1)
			if d.Retention != topics.Forever {
				ms = d.Retention.Milliseconds()
			}
			j.RetentionMs = &ms
		}
		out = append(out, j)
	}
	return out
}

func (l topicList) badge() string {
	return fmt.Sprintf("cluster %s · %s", short(l.snap.ClusterID), plural(len(l.rows), "topic"))
}

// tableScreen is kaas topics as the data-table piece. Retention and cleanup
// a topic sets itself are purple, the colour the pieces give configuration;
// inherited broker defaults stay grey.
func (l topicList) tableScreen() screens.Table {
	title := "Topics"
	if l.pattern != "" {
		title += " · " + l.pattern
	}
	t := screens.Table{
		Title: title,
		Badge: l.badge(),
		Columns: []screens.Column{
			{Name: "Topic"},
			{Name: "Parts", Right: true},
			{Name: "RF", Right: true, Drop: 5},
			{Name: "Records", Right: true, Drop: 1},
			{Name: "Disk", Right: true, Drop: 2},
			{Name: "Retention", Right: true, Drop: 3},
			{Name: "Cleanup", Drop: 4},
			{Name: "Status"},
		},
		Sorted: map[string]int{"name": 0, "partitions": 1, "records": 3, "disk": 4, "status": 7}[l.sortBy],
		Asc:    l.sortBy == "name",
		Empty:  "No topics match " + strconv.Quote(l.pattern) + ".",
		Foot:   "purple: set on the topic · grey: broker default",
	}
	if l.pattern == "" {
		t.Empty = "No topics yet."
	}
	if l.hidden > 0 && l.pattern == "" {
		t.FootRight = plural(l.hidden, "internal topic") + " hidden · --internal"
	}
	if l.detailErr != nil {
		t.FootRight = "some details unavailable"
	}
	var parts, compacted, unhealthy int
	var records, disk int64
	for _, r := range l.rows {
		d := l.details[r.name]
		parts += r.partitions
		records += max(r.records, 0)
		disk += max(d.Disk, 0)
		if strings.Contains(d.Cleanup, "compact") {
			compacted++
		}
		if r.status != kit.Good {
			unhealthy++
		}
		set := func(text string, override bool) screens.Cell {
			if override {
				return screens.Cell{Text: text, Role: kit.Accent2}
			}
			return screens.Cell{Text: text}
		}
		t.Rows = append(t.Rows, []screens.Cell{
			{Text: r.name},
			{Text: strconv.Itoa(r.partitions)},
			{Text: strconv.Itoa(r.rf)},
			{Text: recordsText(r.records)},
			{Text: screens.Bytes(d.Disk)},
			set(retentionText(d.Retention), d.RetentionOverride),
			set(cmp.Or(d.Cleanup, "-"), d.CleanupOverride),
			{Text: "● " + r.statusText(), Role: statusRole(r.status)},
		})
	}
	bad := kit.FG
	if unhealthy > 0 {
		bad = kit.Warn
	}
	t.Tiles = []screens.Tile{
		{Label: "Topics", Value: strconv.Itoa(len(l.rows))},
		{Label: "Partitions", Value: strconv.Itoa(parts)},
		{Label: "Records", Value: screens.Num(float64(records))},
		{Label: "Disk", Value: screens.Bytes(disk)},
		{Label: "Compacted", Value: strconv.Itoa(compacted), Role: kit.Accent2},
		{Label: "Unhealthy", Value: strconv.Itoa(unhealthy), Role: bad},
	}
	return t
}

// treeScreen is kaas topics --tree as the file-tree piece: prefixes are
// folders, topics are leaves with a status bullet, their size, and a flag
// where the piece marks modified files: ! offline, U under-replicated,
// C compacted.
func (l topicList) treeScreen() screens.Tree {
	byName := make(map[string]topicRow, len(l.rows))
	var names []string
	var unhealthy int
	biggest := topicRow{records: -1}
	for _, r := range l.rows {
		byName[r.name] = r
		names = append(names, r.name)
		if r.status != kit.Good {
			unhealthy++
		}
		if r.records > biggest.records {
			biggest = r
		}
	}
	nodes := screens.Nest(names, l.sep, func(full string) screens.TreeNode {
		r := byName[full]
		d := l.details[full]
		n := screens.TreeNode{Dot: statusRole(r.status), Info: fmt.Sprintf("%dp · %s", r.partitions, recordsText(r.records))}
		switch {
		case r.status == kit.Down:
			n.Mark, n.MarkRole, n.Changed = "!", kit.Err, true
		case r.status == kit.Degraded:
			n.Mark, n.MarkRole, n.Changed = "U", kit.Warn, true
		case strings.Contains(d.Cleanup, "compact"):
			n.Mark, n.MarkRole = "C", kit.Accent2
		}
		return n
	})
	folders := 0
	var count func([]screens.TreeNode)
	count = func(ns []screens.TreeNode) {
		for _, n := range ns {
			if len(n.Children) > 0 {
				folders++
				count(n.Children)
			}
		}
	}
	count(nodes)
	t := screens.Tree{
		Title: "TOPICS", Badge: l.badge(), Root: short(l.snap.ClusterID),
		Head:  fmt.Sprintf("%s in %s", plural(len(names), "topic"), plural(folders, "prefix")),
		Nodes: nodes, Empty: "No topics match.",
		FootRight: "! offline · U under-replicated · C compacted",
	}
	if unhealthy > 0 {
		t.HeadRight = plural(unhealthy, "unhealthy topic")
	}
	if biggest.records >= 0 && biggest.name != "" {
		t.Foot = fmt.Sprintf("• %s  %s records", biggest.name, recordsText(biggest.records))
	}
	return t
}

func statusRole(s kit.Status) kit.Role {
	return map[kit.Status]kit.Role{kit.Good: kit.OK, kit.Degraded: kit.Warn, kit.Down: kit.Err}[s]
}

func recordsText(n int64) string {
	if n < 0 {
		return "-"
	}
	return screens.Num(float64(n))
}

// retentionText prints retention.ms in the largest whole unit: 7d, 12h.
func retentionText(d time.Duration) string {
	switch {
	case d == topics.Forever:
		return "forever"
	case d <= 0:
		return "-"
	case d%(24*time.Hour) == 0:
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	}
	return d.String()
}
