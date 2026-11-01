package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/groups"
	"github.com/IshaanNene/Kafka-as-a-Service/core/topics"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newTopicsAlterCmd(brokers, themeName *string) *cobra.Command {
	var (
		output      string
		partitions  int
		dryRun, yes bool
	)
	cmd := &cobra.Command{
		Use:   "alter <topic> --partitions <n>",
		Short: "Add partitions to a topic, after a preview and a typed yes",
		Long: `Raise a topic's partition count. The controller places the new partitions'
replicas; kaas shows the plan first, asks you to type yes, then shows the
topic as written.

Kafka can add partitions but never remove them. Keys hash over the new
count from then on, so a key's new records can land on a different
partition from its old ones and per-key order breaks across the change.

--dry-run asks the controller to check the change without making it.
Without a terminal to answer the prompt, --yes is required.`,
		Example: `  # Preview, confirm, write
  kaas topics alter orders --partitions 12
  # Check the change with the controller, write nothing
  kaas topics alter orders --partitions 12 --dry-run
  # In a script
  kaas topics alter orders --partitions 12 --yes -o json | jq .added`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return usageError(cmd, "topic", "", "required: the topic to alter")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "text", "json"); err != nil {
				return err
			}
			if !cmd.Flags().Changed("partitions") {
				return usageError(cmd, "partitions", "", "required: the topic's new partition count")
			}
			if partitions < 1 {
				return usageError(cmd, "partitions", strconv.Itoa(partitions), "must be 1 or more")
			}
			if !dryRun && !yes && !answerable(cmd.InOrStdin()) {
				return usageError(cmd, "yes", "", "required without a terminal to confirm: added partitions can't be removed")
			}
			if _, err := connect(cmd, *brokers, *themeName); err != nil {
				return err
			}
			cl, err := kgo.NewClient(kgo.SeedBrokers(splitSeeds(*brokers)...))
			if err != nil {
				return err
			}
			defer cl.Close()
			topic := args[0]
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			td, err := describeTopic(ctx, cl, topic)
			cancel()
			if err != nil {
				return err
			}
			a := alteration{td: td, to: partitions, dry: dryRun}
			from := len(td.topic.Partitions)
			switch {
			case partitions < from:
				return usageError(cmd, "partitions", strconv.Itoa(partitions),
					fmt.Sprintf("%s has %s and Kafka can't remove partitions; copy into a new topic to have fewer", topic, plural(from, "partition")))
			case partitions == from:
				return a.print(cmd, output, *themeName, "")
			}
			return a.run(cmd, cl, output, *themeName, yes)
		},
	}
	f := cmd.Flags()
	f.IntVar(&partitions, "partitions", 0, "the topic's new partition count; Kafka only adds")
	f.BoolVar(&dryRun, "dry-run", false, "have the controller check the change without making it")
	f.BoolVarP(&yes, "yes", "y", false, "skip the prompt")
	f.StringVarP(&output, "output", "o", "text", "output format: text|json")
	return cmd
}

// answerable is whether in is a terminal someone can type yes into; a
// variable so tests can answer the prompt.
var answerable = func(in io.Reader) bool {
	f, ok := in.(*os.File)
	return ok && term.IsTerminal(f.Fd())
}

// topicReaders is the groups with commits on topic, best effort: they
// rebalance to pick up new partitions.
func topicReaders(ctx context.Context, adm *kadm.Client, topic string) []string {
	s, _ := groups.Fetch(ctx, adm)
	var out []string
	for _, g := range s.Groups {
		for _, t := range g.Topics {
			if t == topic {
				out = append(out, g.Name)
				break
			}
		}
	}
	return out
}

// alteration is one run of kaas topics alter: the topic before, the count
// asked for, and the topic after once written.
type alteration struct {
	td      topicDescription
	to      int
	dry     bool
	readers []string
	after   *topicDescription
}

func (a alteration) from() int { return len(a.td.topic.Partitions) }

// added is the new partition IDs.
func (a alteration) added() []int32 {
	var out []int32
	for id := a.from(); id < a.to; id++ {
		out = append(out, int32(id))
	}
	return out
}

func (a alteration) run(cmd *cobra.Command, cl *kgo.Client, output, themeName string, yes bool) error {
	surf := detect(cmd.OutOrStdout(), themeName)
	topic := a.td.topic.Name
	adm := kadm.NewClient(cl)
	gctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
	a.readers = topicReaders(gctx, adm, topic)
	cancel()
	shown := 0
	if a.dry || !yes {
		// The plan, before anything is written.
		if surf.rich && output == "text" {
			lines := a.screen().Render(surf.kit, surf.width)
			surf.print(lines)
			shown = len(lines)
		}
	}
	if a.dry {
		ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
		defer cancel()
		if err := topics.Grow(ctx, adm, topic, a.to, true); err != nil {
			return fmt.Errorf("the controller refused %s for %s: %w", plural(a.to, "partition"), topic, err)
		}
		return a.print(cmd, output, themeName, "checked")
	}
	if !yes {
		w, k := cmd.ErrOrStderr(), kit.Kit{}
		if surf.rich {
			w, k = surf.w, surf.kit
		}
		q := fmt.Sprintf("Are you sure you want to write %s to %s? ", plural(a.to-a.from(), "new partition"), topic)
		if !confirm(cmd.InOrStdin(), w, k, q, "yes") {
			return cancelled("Not confirmed: nothing was written.")
		}
		shown++
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
	defer cancel()
	if err := topics.Grow(ctx, adm, topic, a.to, false); err != nil {
		return fmt.Errorf("adding partitions to %s: %w", topic, err)
	}
	// Metadata catches up with the controller a moment later; wait for
	// every new partition to have a leader so the screen shows where they
	// went.
	for {
		td, err := describeTopic(ctx, cl, topic)
		if err == nil && len(td.topic.Partitions) >= a.to && allLed(td) {
			a.after = &td
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("added partitions to %s, but metadata didn't show them in time: run kaas topics describe %s", topic, topic)
		case <-time.After(200 * time.Millisecond):
		}
	}
	if surf.rich && output == "text" && shown > 0 && shown < surf.height {
		surf.erase(shown) // the written table replaces the plan, as in the piece
	}
	return a.print(cmd, output, themeName, "written")
}

func allLed(td topicDescription) bool {
	for _, p := range td.topic.Partitions {
		if p.Leader < 0 {
			return false
		}
	}
	return true
}

// confirm asks q and reads one line: only want, typed exactly, goes ahead.
func confirm(in io.Reader, w io.Writer, k kit.Kit, q, want string) bool {
	_, _ = io.WriteString(w, "    "+k.S(kit.FG, q)+k.S(kit.Muted, "(type ")+k.S(kit.Warn, want)+k.S(kit.Muted, ") "))
	line, _ := bufio.NewReader(in).ReadString('\n')
	return strings.TrimSpace(line) == want
}

type alterJSON struct {
	Topic  string  `json:"topic"`
	From   int     `json:"from"`
	To     int     `json:"to"`
	Added  []int32 `json:"added"`
	DryRun bool    `json:"dryRun"`
}

// print reports the outcome: state is "" for no change, "checked" for a
// dry run and "written".
func (a alteration) print(cmd *cobra.Command, output, themeName, state string) error {
	out := cmd.OutOrStdout()
	topic := a.td.topic.Name
	if output == "json" {
		j := alterJSON{Topic: topic, From: a.from(), To: max(a.to, a.from()), Added: a.added(), DryRun: a.dry}
		if j.Added == nil {
			j.Added = []int32{}
		}
		return json.NewEncoder(out).Encode(j)
	}
	surf := detect(out, themeName)
	switch {
	case state == "":
		msg := fmt.Sprintf("%s already has %s; nothing to do.", topic, plural(a.to, "partition"))
		if surf.rich {
			surf.print([]string{surf.kit.S(kit.OK, "✓ ") + surf.kit.S(kit.FG, msg)})
			return nil
		}
		fmt.Fprintln(out, msg)
	case !surf.rich && state == "checked":
		fmt.Fprintf(out, "%s: %d → %d partitions (dry run, nothing written)\n", topic, a.from(), a.to)
	case !surf.rich:
		fmt.Fprintf(out, "%s: %d → %d partitions, added %s\n", topic, a.from(), a.to, joinIDs(a.added()))
	case state == "checked":
		surf.print([]string{"", "    " + surf.kit.S(kit.OK, fmt.Sprintf("✓ The controller accepts %s for %s. Nothing was written.", plural(a.to, "partition"), topic))})
	default:
		surf.print(a.screen().Render(surf.kit, surf.width))
	}
	return nil
}

// screen is the disk-partitioner piece mid-edit: the plan with the new
// partitions in green before writing, the topic as written after.
func (a alteration) screen() screens.Partitions {
	topic, n := a.td.topic.Name, a.to-a.from()
	readers := "no group has committed offsets here"
	if len(a.readers) > 0 {
		readers = strings.Join(a.readers, ", ") + " rebalance to pick them up; a group that resets to latest skips what lands before it does"
	}
	ids := make([]string, n)
	for i, id := range a.added() {
		ids[i] = "p" + strconv.Itoa(int(id))
	}

	if a.after != nil {
		v := a.after.screen()
		var leaders []string
		for i := range v.Parts {
			if v.Parts[i].ID >= int32(a.from()) {
				v.Parts[i].New = true
				leaders = append(leaders, leaderText(v.Parts[i].Leader))
			}
		}
		v.Focus = a.from()
		v.Details = append([]screens.Detail{
			{Label: "Added", Value: strings.Join(ids, ", ") + " · leaders " + strings.Join(leaders, ", "), Role: kit.OK},
			{Label: "Producers", Value: "see them at their next metadata refresh: metadata.max.age.ms, 5m by default", Role: kit.Info},
			{Label: "Readers", Value: readers, Role: kit.Warn},
		}, v.Details[:2]...)
		v.Status, v.StatusRole = fmt.Sprintf("Written: %s now has %s.", topic, plural(a.to, "partition")), kit.OK
		v.Actions = []string{"kaas topics describe " + topic, "kaas groups", "kaas top"}
		v.Hint = "kaas topics describe " + topic + " to look again"
		return v
	}

	v := a.td.screen()
	rf := 0
	for _, p := range a.td.topic.Partitions {
		rf = max(rf, len(p.Replicas))
	}
	for _, id := range a.added() {
		v.Parts = append(v.Parts, screens.Part{ID: id, Leader: -1, Start: 0, End: 0, Bytes: -1, New: true})
	}
	v.Focus = a.from()
	v.Details = []screens.Detail{
		{Label: "Change", Value: fmt.Sprintf("%d → %d partitions · adds %s · RF %d, placed by the controller", a.from(), a.to, strings.Join(ids, ", "), rf), Role: kit.OK},
		{Label: "Keys", Value: fmt.Sprintf("hash over %d partitions from now on, so a key's new records can land on a different partition from its old ones", a.to), Role: kit.Warn},
		{Label: "Readers", Value: readers, Role: kit.Info},
	}
	v.Actions = nil
	v.Status, v.StatusRole = "Partitions can't be removed once added.", kit.Warn
	v.Hint = "--dry-run checks with the controller · --yes skips the prompt"
	if a.dry {
		v.Hint = "dry run: nothing is written"
	}
	return v
}
