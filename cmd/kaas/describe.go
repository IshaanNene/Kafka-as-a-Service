package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/core/topics"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newTopicsDescribeCmd(brokers, themeName *string) *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "describe <topic>",
		Short: "Show a topic's partitions, where its replicas live, and its settings",
		Long: `Show one topic in full: every partition's leader, replicas, in-sync replicas,
offsets, records and size; how its replicas spread across brokers, sized
by bytes on disk; and the configs the topic sets itself.`,
		Example: `  # One topic, laid out per broker
  kaas topics describe orders
  # The same in kafka-topics --describe form, for grep
  kaas topics describe orders | grep -v 'Isr: 1,2,3'
  # Partitions behind on replication, for a script
  kaas topics describe orders -o json | jq '.partitions[] | select(.isr | length < 3) | .id'`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return usageError(cmd, "topic", "", "required: the topic to describe")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "table", "json"); err != nil {
				return err
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
			td, err := describeTopic(ctx, cl, args[0])
			if err != nil {
				return err
			}
			return td.print(cmd, output, *themeName)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "table", "output format: table|json")
	return cmd
}

// topicDescription is one topic as kaas topics describe shows it.
type topicDescription struct {
	snap      *snapshot.Snapshot
	topic     snapshot.Topic
	details   topics.Details
	detailErr error
}

func describeTopic(ctx context.Context, cl *kgo.Client, name string) (topicDescription, error) {
	s, err := snapshot.Fetch(ctx, cl)
	if err != nil {
		return topicDescription{}, err
	}
	if err := missingTopic(s, []string{name}); err != nil {
		return topicDescription{}, err
	}
	adm := kadm.NewClient(cl)
	if starts, err := adm.ListStartOffsets(ctx, name); err == nil {
		if ends, err := adm.ListEndOffsets(ctx, name); err == nil {
			s = snapshot.WithOffsets(s, []string{name}, starts, ends, time.Now())
		}
	}
	t, _ := s.Topic(name)
	all, derr := topics.Describe(ctx, adm, []string{name})
	return topicDescription{snap: s, topic: t, details: all[name], detailErr: derr}, nil
}

func (td topicDescription) records(p snapshot.Partition) int64 {
	if p.StartOffset == snapshot.UnknownOffset || p.EndOffset == snapshot.UnknownOffset {
		return -1
	}
	return p.EndOffset - p.StartOffset
}

// leaderBytes is a partition's size on its leader, or -1.
func (td topicDescription) leaderBytes(p snapshot.Partition) int64 {
	if b, ok := td.details.Replicas[p.Leader][p.ID]; ok {
		return b
	}
	return -1
}

type partitionJSON struct {
	ID          int32   `json:"id"`
	Leader      int32   `json:"leader"`
	Replicas    []int32 `json:"replicas"`
	ISR         []int32 `json:"isr"`
	Offline     []int32 `json:"offline"`
	StartOffset *int64  `json:"startOffset"`
	EndOffset   *int64  `json:"endOffset"`
	Records     *int64  `json:"records"`
	Bytes       *int64  `json:"bytes"`
}

type describeJSON struct {
	Name              string            `json:"name"`
	Internal          bool              `json:"internal"`
	ReplicationFactor int               `json:"replicationFactor"`
	CleanupPolicy     string            `json:"cleanupPolicy"`
	RetentionMs       *int64            `json:"retentionMs"`
	Configs           map[string]string `json:"configs"` // set on the topic itself
	DiskBytes         *int64            `json:"diskBytes"`
	Partitions        []partitionJSON   `json:"partitions"`
}

func known(n int64) *int64 {
	if n < 0 {
		return nil
	}
	return &n
}

// orEmpty keeps JSON arrays as [] rather than null.
func orEmpty(xs []int32) []int32 {
	if xs == nil {
		return []int32{}
	}
	return xs
}

func (td topicDescription) json() describeJSON {
	d := td.details
	out := describeJSON{
		Name: td.topic.Name, Internal: td.topic.Internal, CleanupPolicy: d.Cleanup,
		Configs: map[string]string{}, DiskBytes: known(d.Disk),
		Partitions: []partitionJSON{},
	}
	maps.Copy(out.Configs, d.Set)
	if d.Retention != 0 {
		ms := int64(-1)
		if d.Retention != topics.Forever {
			ms = d.Retention.Milliseconds()
		}
		out.RetentionMs = &ms
	}
	for _, p := range td.topic.Partitions {
		out.ReplicationFactor = max(out.ReplicationFactor, len(p.Replicas))
		out.Partitions = append(out.Partitions, partitionJSON{
			ID: p.ID, Leader: p.Leader, Replicas: orEmpty(p.Replicas), ISR: orEmpty(p.ISR),
			Offline: orEmpty(p.Offline), StartOffset: known(p.StartOffset), EndOffset: known(p.EndOffset),
			Records: known(td.records(p)), Bytes: known(td.leaderBytes(p)),
		})
	}
	return out
}

func (td topicDescription) print(cmd *cobra.Command, output, themeName string) error {
	out := cmd.OutOrStdout()
	if output == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(td.json())
	}
	if surf := detect(out, themeName); surf.rich {
		surf.print(td.screen().Render(surf.kit, surf.width))
		return nil
	}
	if td.detailErr != nil {
		fmt.Fprintf(cmd.ErrOrStderr(), "kaas: warning: some topic details are unavailable: %v\n", td.detailErr)
	}
	// The kafka-topics --describe layout, so existing grep habits carry over.
	j := td.json()
	var configs []string
	for _, k := range slices.Sorted(maps.Keys(j.Configs)) {
		configs = append(configs, k+"="+j.Configs[k])
	}
	fmt.Fprintf(out, "Topic: %s\tPartitionCount: %d\tReplicationFactor: %d\tConfigs: %s\n",
		j.Name, len(j.Partitions), j.ReplicationFactor, strings.Join(configs, ","))
	for _, p := range td.topic.Partitions {
		fmt.Fprintf(out, "\tTopic: %s\tPartition: %d\tLeader: %s\tReplicas: %s\tIsr: %s\tStart: %s\tEnd: %s\n",
			j.Name, p.ID, leaderText(p.Leader), joinIDs(p.Replicas), joinIDs(p.ISR),
			offsetText(p.StartOffset), offsetText(p.EndOffset))
	}
	return nil
}

func leaderText(l int32) string {
	if l < 0 {
		return "none"
	}
	return strconv.Itoa(int(l))
}

func offsetText(o int64) string {
	if o < 0 {
		return "-"
	}
	return strconv.FormatInt(o, 10)
}

func joinIDs(xs []int32) string {
	s := make([]string, len(xs))
	for i, x := range xs {
		s[i] = strconv.Itoa(int(x))
	}
	return strings.Join(s, ",")
}

// screen is the disk-partitioner piece filled with the topic. The marked
// partition is the one that needs attention: offline first, then
// under-replicated, else the largest.
func (td topicDescription) screen() screens.Partitions {
	t, d := td.topic, td.details
	var rf int
	var records int64
	var offline, urp []string
	focus, focusRank := -1, int64(-1)
	view := screens.Partitions{Topic: t.Name, Focus: -1}
	for i, p := range t.Partitions {
		rf = max(rf, len(p.Replicas))
		n := td.records(p)
		records += max(n, 0)
		rank := max(n, 0)
		switch {
		case p.Leader < 0 || len(p.Offline) > 0:
			offline = append(offline, strconv.Itoa(int(p.ID)))
			rank = 1 << 62
		case p.UnderReplicated():
			urp = append(urp, strconv.Itoa(int(p.ID)))
			rank = 1 << 61
		}
		if rank > focusRank {
			focus, focusRank = i, rank
		}
		view.Parts = append(view.Parts, screens.Part{
			ID: p.ID, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR,
			Start: p.StartOffset, End: p.EndOffset, Bytes: td.leaderBytes(p),
		})
	}
	view.Focus = focus
	for _, b := range td.snap.Brokers {
		view.Brokers = append(view.Brokers, b.ID)
	}
	view.ReplicaBytes = d.Replicas

	view.Info = []string{
		fmt.Sprintf("%s · RF %d · %s records · %s on disk", plural(len(t.Partitions), "partition"), rf,
			screens.Num(float64(records)), screens.Bytes(d.Disk)),
		fmt.Sprintf("cleanup %s · retention %s · cluster %s", cmp.Or(d.Cleanup, "-"), retentionText(d.Retention), short(td.snap.ClusterID)),
	}

	leaders := map[int32]int{}
	for _, p := range t.Partitions {
		if p.Leader >= 0 {
			leaders[p.Leader]++
		}
	}
	var spread []string
	for _, b := range view.Brokers {
		spread = append(spread, fmt.Sprintf("broker %d: %d", b, leaders[b]))
	}
	inSync := len(t.Partitions) - len(urp) - len(offline)
	syncRole := kit.OK
	if inSync < len(t.Partitions) {
		syncRole = kit.Warn
	}
	config, configRole := "broker defaults", kit.FG
	if len(d.Set) > 0 {
		var kv []string
		for _, k := range slices.Sorted(maps.Keys(d.Set)) {
			v := d.Set[k]
			if k == "retention.ms" {
				v = retentionText(d.Retention)
			}
			kv = append(kv, k+"="+v)
		}
		config, configRole = strings.Join(kv, " · "), kit.Accent2
	}
	view.Details = []screens.Detail{
		{Label: "Leaders", Value: strings.Join(spread, " · "), Role: kit.Info},
		{Label: "In sync", Value: fmt.Sprintf("%d of %d partitions", inSync, len(t.Partitions)), Role: syncRole},
		{Label: "Set on topic", Value: config, Role: configRole},
	}
	view.Actions = []string{"kaas watch -t " + t.Name, "kaas topics", "kaas bench produce -t " + t.Name}

	switch {
	case len(offline) > 0:
		view.Status, view.StatusRole = fmt.Sprintf("Partition %s has no leader: its producers and consumers are stuck.", strings.Join(offline, ", ")), kit.Err
		if len(offline) > 1 {
			view.Status = fmt.Sprintf("Partitions %s have no leader: their producers and consumers are stuck.", strings.Join(offline, ", "))
		}
	case len(urp) > 0:
		view.Status, view.StatusRole = fmt.Sprintf("Under-replicated: partition %s. A replica is behind or its broker is down.", strings.Join(urp, ", ")), kit.Warn
	default:
		view.Status, view.StatusRole = fmt.Sprintf("All %s in sync.", plural(len(t.Partitions), "partition")), kit.OK
	}
	view.Hint = "-o json for scripts · kaas topics for every topic"
	if td.detailErr != nil {
		view.Hint = "sizes or configs unavailable: " + td.detailErr.Error()
	}
	return view
}
