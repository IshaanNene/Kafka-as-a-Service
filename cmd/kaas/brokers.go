package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/brokers"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newBrokersCmd(brokersFlag, themeName *string) *cobra.Command {
	var (
		output   string
		top      int
		internal bool
	)
	cmd := &cobra.Command{
		Use:   "brokers",
		Short: "Brokers as a tree: what each one leads, holds, and keeps on disk",
		Long: `Show every broker under the cluster, with the partitions it leads, the
replicas it holds and how many of them are behind, its disk use, and the
topics it leads most. A broker the partitions still name but metadata no
longer lists is shown as offline. Skew is the busiest broker's leaders
over the even share: 1.00x is balanced.`,
		Example: `  # The cluster, its brokers and their busiest topics
  kaas brokers
  # Brokers only
  kaas brokers --top 0
  # Brokers with replicas out of sync, for a script
  kaas brokers -o json | jq '.[] | select(.behind > 0) | .id'`,
		Args: unknownCommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := oneOf(cmd, "output", output, "table", "json"); err != nil {
				return err
			}
			if top < 0 {
				return usageError(cmd, "top", strconv.Itoa(top), "must be 0 or more")
			}
			if _, err := connect(cmd, *brokersFlag, *themeName); err != nil {
				return err
			}
			cl, err := kgo.NewClient(kgo.SeedBrokers(splitSeeds(*brokersFlag)...))
			if err != nil {
				return err
			}
			defer cl.Close()
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			defer cancel()
			s, err := snapshot.Fetch(ctx, cl)
			if err != nil {
				return err
			}
			disk, diskErr := brokers.Disk(ctx, kadm.NewClient(cl))
			bl := brokerList{snap: s, brokers: brokers.Summarize(s, disk), diskErr: diskErr, top: top, internal: internal}
			return bl.print(cmd, output, *themeName)
		},
	}
	f := cmd.Flags()
	f.StringVarP(&output, "output", "o", "table", "output format: table|json")
	f.IntVar(&top, "top", 5, "topics listed under each broker, most led first (0 hides them)")
	f.BoolVar(&internal, "internal", false, "list internal topics such as __consumer_offsets by name")
	return cmd
}

// brokerList is everything kaas brokers shows.
type brokerList struct {
	snap     *snapshot.Snapshot
	brokers  []brokers.Broker
	diskErr  error // disk is best effort
	top      int
	internal bool
}

type brokerTopicJSON struct {
	Name     string  `json:"name"`
	Internal bool    `json:"internal"`
	Leads    []int32 `json:"leads"`
	Follows  int     `json:"follows"`
	Behind   int     `json:"behind"`
}

type brokerJSON struct {
	ID         int32             `json:"id"`
	Host       string            `json:"host"`
	Port       int32             `json:"port"`
	Rack       string            `json:"rack"`
	Controller bool              `json:"controller"`
	Online     bool              `json:"online"`
	Leaders    int               `json:"leaders"`
	Replicas   int               `json:"replicas"`
	Behind     int               `json:"behind"`
	DiskBytes  *int64            `json:"diskBytes"` // null when unknown
	Topics     []brokerTopicJSON `json:"topics"`
}

func (bl brokerList) json() []brokerJSON {
	out := []brokerJSON{}
	for _, b := range bl.brokers {
		j := brokerJSON{
			ID: b.ID, Host: b.Host, Port: b.Port, Rack: b.Rack, Controller: b.Controller, Online: b.Online,
			Leaders: b.Leaders, Replicas: b.Replicas, Behind: b.Behind, DiskBytes: known(b.Disk),
			Topics: []brokerTopicJSON{},
		}
		for _, t := range b.Topics {
			j.Topics = append(j.Topics, brokerTopicJSON{Name: t.Name, Internal: t.Internal, Leads: orEmpty(t.Leads), Follows: t.Follows, Behind: t.Behind})
		}
		out = append(out, j)
	}
	return out
}

func (bl brokerList) print(cmd *cobra.Command, output, themeName string) error {
	out := cmd.OutOrStdout()
	if output == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(bl.json())
	}
	if surf := detect(out, themeName); surf.rich {
		surf.print(bl.screen().Render(surf.kit, surf.width))
		return nil
	}
	if bl.diskErr != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "kaas: warning: disk use is unavailable: %v\n", bl.diskErr)
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tHOST\tRACK\tSTATUS\tLEADERS\tREPLICAS\tBEHIND\tDISK_BYTES")
	for _, b := range bl.brokers {
		host := "-"
		if b.Online {
			host = b.Host + ":" + strconv.Itoa(int(b.Port))
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%d\t%d\t%d\t%s\n", b.ID, host, orDash(b.Rack), brokerStatus(b),
			b.Leaders, b.Replicas, b.Behind, offsetText(b.Disk))
	}
	return tw.Flush()
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// brokerStatus is the one word a script greps for.
func brokerStatus(b brokers.Broker) string {
	switch {
	case !b.Online:
		return "offline"
	case b.Controller:
		return "controller"
	}
	return "online"
}

// ranges prints partition IDs compactly: p0-2 p5.
func ranges(ids []int32) string {
	var parts []string
	for i := 0; i < len(ids); {
		j := i
		for j+1 < len(ids) && ids[j+1] == ids[j]+1 {
			j++
		}
		if j > i {
			parts = append(parts, fmt.Sprintf("p%d-%d", ids[i], ids[j]))
		} else {
			parts = append(parts, fmt.Sprintf("p%d", ids[i]))
		}
		i = j + 1
	}
	return strings.Join(parts, " ")
}

// screen is the process-tree piece: the cluster at the root, its brokers,
// and under each the topics it leads most. Internal topics fold into one
// row unless --internal; topics past --top fold into "+ N more".
func (bl brokerList) screen() screens.ProcTree {
	s := bl.snap
	online, leaders := 0, 0
	for _, b := range bl.brokers {
		if b.Online {
			online++
			leaders += b.Leaders
		}
	}
	even := 0.0
	if online > 0 {
		even = float64(leaders) / float64(online)
	}
	root := screens.ProcNode{Name: "cluster", NameRole: kit.Accent2,
		Args: fmt.Sprintf("%s · controller %d", short(s.ClusterID), s.Controller)}
	offline := 0
	for _, b := range bl.brokers {
		n := screens.ProcNode{ID: strconv.Itoa(int(b.ID)), Name: fmt.Sprintf("broker %d", b.ID), NameRole: kit.Accent}
		leadRole := kit.FG
		if even > 0 && float64(b.Leaders) > 1.25*even {
			leadRole = kit.Warn
		}
		n.Cells = []screens.Cell{
			{Text: strconv.Itoa(b.Leaders), Role: leadRole},
			{Text: strconv.Itoa(b.Replicas), Role: kit.Muted},
			{Text: screens.Bytes(b.Disk), Role: kit.Muted},
		}
		args := []string{fmt.Sprintf("%s:%d", b.Host, b.Port)}
		if b.Rack != "" {
			args = append(args, "rack "+b.Rack)
		}
		switch {
		case !b.Online:
			offline++
			n.NameRole, n.Cells[0].Role = kit.Err, kit.Err
			args = []string{"named by partitions, missing from metadata"}
			n.Tags = append(n.Tags, screens.Cell{Text: "offline", Role: kit.Err})
		case b.Controller:
			n.Tags = append(n.Tags, screens.Cell{Text: "controller", Role: kit.Notice})
		}
		if b.Behind > 0 {
			n.Tags = append(n.Tags, screens.Cell{Text: fmt.Sprintf("%d behind", b.Behind), Role: kit.Warn})
		}
		n.Args = strings.Join(args, " · ")
		n.Children = bl.topicNodes(b)
		root.Children = append(root.Children, n)
	}

	partitions := s.PartitionCount()
	view := screens.ProcTree{
		Title:  "kaas brokers · cluster " + short(s.ClusterID),
		IDName: "ID", TreeName: "NODE",
		Columns:   []screens.Column{{Name: "LEADS", Right: true}, {Name: "REPLICAS", Right: true, Drop: 2}, {Name: "DISK", Right: true, Drop: 1}},
		Count:     plural(len(bl.brokers), "broker"),
		Root:      root,
		Legend:    []screens.Legend{{Label: "controller", Role: kit.Notice}, {Label: "behind", Role: kit.Warn}, {Label: "offline", Role: kit.Err}},
		FootRight: fmt.Sprintf("%s · %s", plural(len(s.Topics), "topic"), plural(partitions, "partition")),
	}
	if offline > 0 {
		view.Count += fmt.Sprintf(" · %d offline", offline)
	}
	if skew := brokers.Skew(bl.brokers); skew > 0 {
		view.Load, view.LoadValue, view.LoadRole = "skew", fmt.Sprintf("%.2fx", skew), kit.OK
		switch {
		case skew > 1.5:
			view.LoadRole = kit.Warn
		case skew > 1.2:
			view.LoadRole = kit.Notice
		}
	}
	if bl.diskErr != nil {
		view.FootRight = "disk unavailable: " + bl.diskErr.Error()
	}
	return view
}

func (bl brokerList) topicNodes(b brokers.Broker) []screens.ProcNode {
	if bl.top == 0 {
		return nil
	}
	var nodes []screens.ProcNode
	var internalNames []string
	internalLeads, moreLeads, more := 0, 0, 0
	for _, t := range b.Topics {
		if t.Internal && !bl.internal {
			internalNames = append(internalNames, t.Name)
			internalLeads += len(t.Leads)
			continue
		}
		if len(nodes) >= bl.top {
			more++
			moreLeads += len(t.Leads)
			continue
		}
		n := screens.ProcNode{Name: t.Name, NameRole: kit.Accent,
			Cells: []screens.Cell{{Text: "", Role: kit.Rule}}}
		var args []string
		if len(t.Leads) > 0 {
			n.NameRole = kit.Info
			n.Cells[0].Text = strconv.Itoa(len(t.Leads))
			args = append(args, ranges(t.Leads))
		}
		if t.Follows > 0 {
			args = append(args, fmt.Sprintf("follows %d", t.Follows))
		}
		n.Args = strings.Join(args, " · ")
		if t.Behind > 0 {
			n.Tags = []screens.Cell{{Text: fmt.Sprintf("%d behind", t.Behind), Role: kit.Warn}}
		}
		nodes = append(nodes, n)
	}
	if more > 0 {
		nodes = append(nodes, screens.ProcNode{Name: fmt.Sprintf("+ %d more", more), NameRole: kit.Rule,
			Cells: []screens.Cell{{Text: leadsText(moreLeads), Role: kit.Rule}}, Args: "--top to list them"})
	}
	if len(internalNames) > 0 {
		args := internalNames[0]
		if len(internalNames) > 1 {
			args += fmt.Sprintf(" +%d", len(internalNames)-1)
		}
		nodes = append(nodes, screens.ProcNode{Name: "internal", NameRole: kit.Muted,
			Cells: []screens.Cell{{Text: leadsText(internalLeads), Role: kit.Rule}}, Args: args + " · --internal to list"})
	}
	return nodes
}

func leadsText(n int) string {
	if n == 0 {
		return ""
	}
	return strconv.Itoa(n)
}
