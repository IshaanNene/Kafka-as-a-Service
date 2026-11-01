package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/offsets"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newOffsetsCmd(brokers, themeName *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "offsets",
		Short: "Move consumer group offsets",
		Args:  unknownCommand,
		RunE:  func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}
	cmd.AddCommand(newOffsetsResetCmd(brokers, themeName))
	return cmd
}

func newOffsetsResetCmd(brokers, themeName *string) *cobra.Command {
	var (
		output, to  string
		topicNames  []string
		all         bool
		dryRun, yes bool
	)
	cmd := &cobra.Command{
		Use:   "reset [group] -t <topic>... --to <where>",
		Short: "Move a group's committed offsets, after a plan and a typed yes",
		Long: `Move where a consumer group reads from: back to read records again, or
forward to skip them. kaas shows the plan first, every partition's
committed offset and where it goes, then asks you to type yes.

--to takes:
  earliest, latest     the start or end of each partition's log
  1043                 an offset
  +100, -100           a shift from the committed offset
  2026-10-31T09:00     the first record at or after a time (local unless it
                       has a zone); also 2026-10-31 09:00 and 2026-10-31
  2h, 90m, 7d          the first record at or after that long ago

A target outside a partition's log is moved to its start or end, as the
broker requires. A partition the group has never committed gets its first
commit; a shift skips it, since there is nothing to shift from.

Kafka only takes commits from a group with no members, so stop its
consumers first. On a terminal, kaas asks for whatever the flags leave
out: the group, its topics, then where to. Without a terminal, --yes is
required.`,
		Example: `  # Ask for each piece, then show the plan
  kaas offsets reset
  # Read the last two hours of orders again
  kaas offsets reset billing -t orders --to 2h
  # Skip to the end of everything billing reads
  kaas offsets reset billing --all-topics --to latest
  # Plan only
  kaas offsets reset billing -t orders --to -500 --dry-run
  # In a script
  kaas offsets reset billing -t orders --to 2026-10-31T09:00Z --yes -o json`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "text", "json"); err != nil {
				return err
			}
			r := resetAsk{topics: slices.Compact(slices.Sorted(slices.Values(topicNames))), all: all, toText: to}
			if len(args) == 1 {
				r.group = args[0]
			}
			if all && len(topicNames) > 0 {
				return usageError(cmd, "all-topics", "true", "-t names the topics; --all-topics takes every topic the group has committed. Use one")
			}
			if to != "" {
				t, err := offsets.ParseTo(to, time.Now())
				if err != nil {
					return usageError(cmd, "to", to, err.Error())
				}
				r.to = &t
			}
			surf := detect(cmd.OutOrStdout(), *themeName)
			typing := answerable(cmd.InOrStdin())
			wizard := !r.complete() && typing && surf.rich && output == "text"
			if !r.complete() && !wizard {
				switch {
				case r.group == "":
					return usageError(cmd, "group", "", "required: the group whose offsets move")
				case len(r.topics) == 0 && !r.all:
					return usageError(cmd, "topic", "", "required: -t for each topic, or --all-topics")
				default:
					return usageError(cmd, "to", "", "required: earliest, latest, an offset, +N or -N, a time, or a duration back like 2h")
				}
			}
			if !dryRun && !yes && !typing {
				return usageError(cmd, "yes", "", "required without a terminal to confirm: moved offsets change what the group reads next")
			}
			if _, err := connect(cmd, *brokers, *themeName); err != nil {
				return err
			}
			cl, err := kgo.NewClient(kgo.SeedBrokers(splitSeeds(*brokers)...))
			if err != nil {
				return err
			}
			defer cl.Close()
			adm := kadm.NewClient(cl)
			if wizard {
				done, err := r.wizard(cmd, adm, surf.kit)
				if err != nil || !done {
					return err
				}
				yes = false // the wizard's answers still go through the plan
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 20*time.Second)
			defer cancel()
			if err := r.resolve(ctx, cmd, adm); err != nil {
				return err
			}
			p, err := offsets.PlanReset(ctx, adm, r.group, r.topics, *r.to)
			if errors.Is(err, offsets.ErrActive) {
				return activeGroup(ctx, adm, p)
			}
			if err != nil {
				return err
			}
			rs := reset{plan: p, cmdline: r.cmdline(), dry: dryRun}
			return rs.run(cmd, adm, surf, output, yes)
		},
	}
	f := cmd.Flags()
	f.StringArrayVarP(&topicNames, "topic", "t", nil, "a topic to move the group on (repeatable)")
	f.BoolVar(&all, "all-topics", false, "every topic the group has committed offsets on")
	f.StringVar(&to, "to", "", "earliest|latest|<offset>|+N|-N|<time>|<duration ago>")
	f.BoolVar(&dryRun, "dry-run", false, "show the plan, commit nothing")
	f.BoolVarP(&yes, "yes", "y", false, "skip the prompt")
	f.StringVarP(&output, "output", "o", "text", "output format: text|json")
	return cmd
}

// resetAsk is what was asked for, from flags or the wizard.
type resetAsk struct {
	group  string
	topics []string
	all    bool
	toText string
	to     *offsets.Target
}

func (r resetAsk) complete() bool {
	return r.group != "" && (len(r.topics) > 0 || r.all) && r.to != nil
}

// cmdline is the command that asks for this reset with flags, for the top
// of the plan.
func (r resetAsk) cmdline() string {
	parts := []string{"kaas offsets reset", r.group}
	if r.all {
		parts = append(parts, "--all-topics")
	}
	for _, t := range r.topics {
		if !r.all {
			parts = append(parts, "-t", t)
		}
	}
	return strings.Join(append(parts, "--to", r.toText), " ")
}

// resolve turns --all-topics into the group's committed topics and checks
// -t names against the cluster.
func (r *resetAsk) resolve(ctx context.Context, cmd *cobra.Command, adm *kadm.Client) error {
	if r.all {
		ts, err := offsets.Committed(ctx, adm, r.group)
		if err != nil {
			return err
		}
		if len(ts) == 0 {
			lg, _ := adm.ListGroups(ctx)
			if !slices.Contains(lg.Groups(), r.group) {
				return notFoundError("group", r.group, "the cluster", lg.Groups(), nil, "kaas groups")
			}
			return usageError(cmd, "all-topics", "true", r.group+" has no committed offsets yet: name its topics with -t")
		}
		r.topics = ts
		return nil
	}
	td, err := adm.ListTopics(ctx)
	if err != nil {
		return err
	}
	for _, t := range r.topics {
		if !td.Has(t) {
			describe := func(n string) string { return plural(len(td[n].Partitions), "partition") }
			return notFoundError("topic", t, "the cluster", td.Names(), describe, "kaas topics")
		}
	}
	return nil
}

// activeGroup is the error for a group with members: who they are, and
// what to do.
func activeGroup(ctx context.Context, adm *kadm.Client, p offsets.Plan) error {
	var who []string
	if dg, err := adm.DescribeGroups(ctx, p.Group); err == nil {
		for _, m := range dg[p.Group].Members {
			who = append(who, m.ClientID+"@"+strings.TrimPrefix(m.ClientHost, "/"))
		}
	}
	slices.Sort(who)
	who = slices.Compact(who)
	msg := fmt.Sprintf("%s has %s: Kafka only takes offset commits from a group with none", p.Group, plural(p.Members, "active member"))
	return &cliError{
		Code: "group_active", Exit: exitFailure, Message: msg,
		Hints:  []string{"stop " + p.Group + "'s consumers, then run this again"},
		Detail: map[string]any{"group": p.Group, "state": p.State, "members": who},
		screen: func(k kit.Kit, w int) []string {
			facts := [][2]string{{"group", p.Group + " · " + p.State}, {"members", orDash(strings.Join(who, ", "))}}
			return screens.Failure{
				Code: "409", Title: p.Group + " is in use",
				Subtitle: "Kafka only takes offset commits from a group with no members. Stop its consumers, then run this again.",
				Facts:    facts, Actions: []string{"kaas groups " + p.Group, "kaas clients"},
			}.Render(k, w)
		},
	}
}

// reset is one planned reset and, once applied, what each commit did.
type reset struct {
	plan    offsets.Plan
	cmdline string
	dry     bool
	results map[string]map[int32]error
	took    time.Duration
	applied bool
}

func (rs *reset) run(cmd *cobra.Command, adm *kadm.Client, surf surface, output string, yes bool) error {
	add, change, _ := rs.plan.Counts()
	rich := surf.rich && output == "text"
	if add+change == 0 {
		return rs.print(cmd, surf, output)
	}
	drawn := 0
	if rich && (rs.dry || !yes) {
		phase, role := "awaiting approval", kit.Accent
		if rs.dry {
			phase, role = "dry run: nothing committed", kit.Muted
		}
		lines := rs.screen(phase, role).Render(surf.kit, surf.width)
		if rs.dry {
			surf.print(lines)
			return nil
		}
		drawn = rs.ask(surf, lines)
	}
	if rs.dry {
		return rs.print(cmd, surf, output)
	}
	if !yes {
		ok := false
		if drawn > 0 {
			line, _ := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
			ok = strings.TrimSpace(line) == "yes"
			_, _ = fmt.Fprint(surf.raw, "\x1b[2B\r\n") // below the footer
		} else {
			w := cmd.ErrOrStderr()
			ok = confirm(cmd.InOrStdin(), w, kit.Kit{}, fmt.Sprintf("Move %s for %s? ", plural(add+change, "offset"), rs.plan.Group), "yes")
		}
		if !ok {
			return cancelled("Not confirmed: no offsets were committed.")
		}
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
	defer cancel()
	start := time.Now()
	res, err := offsets.Apply(ctx, adm, rs.plan)
	rs.took = time.Since(start)
	if err != nil {
		return fmt.Errorf("committing %s's offsets: %w", rs.plan.Group, err)
	}
	rs.results, rs.applied = res, true
	if rich && drawn > 0 && drawn < surf.height {
		surf.erase(drawn) // the plan again, with the answer and the apply under it
		drawn = 0
	}
	if rich {
		v := rs.screen("applied", kit.OK)
		if rs.failed() > 0 {
			v.Phase, v.PhaseOf = "applied with errors", kit.Err
		}
		if drawn == 0 {
			surf.print(v.Render(surf.kit, surf.width))
		} else {
			// The plan is already above and too tall to redraw: just the rest.
			v.Blocks, v.Lead, v.Command, v.Note = nil, "", rs.cmdline, ""
			surf.print(v.Render(surf.kit, surf.width))
		}
		if err := rs.outcome(); err != nil {
			return shown("reset_failed", err) // the screen says which and why
		}
		return nil
	}
	if err := rs.print(cmd, surf, output); err != nil {
		return err
	}
	return rs.outcome()
}

// ask prints the plan with the prompt above the footer, as the piece has
// it, and leaves the cursor after the prompt. It returns the lines shown.
func (rs *reset) ask(surf surface, lines []string) int {
	k := surf.kit
	q := " " + k.S(kit.FG, "Do you want to perform these actions? ") + k.S(kit.Muted, "Enter a value: ")
	foot := lines[len(lines)-2:]
	all := append(append(lines[:len(lines)-2:len(lines)-2], "", q, ""), foot...)
	surf.print(all)
	_, _ = fmt.Fprintf(surf.raw, "\x1b[4A\r\x1b[%dC", kit.Width(q))
	return len(all)
}

// failed is how many commits the coordinator refused.
func (rs *reset) failed() int {
	n := 0
	for _, ps := range rs.results {
		for _, err := range ps {
			if err != nil {
				n++
			}
		}
	}
	return n
}

// outcome is the error to exit with: the first refused commit, and how many.
func (rs *reset) outcome() error {
	for _, m := range rs.plan.Moves {
		if err := rs.results[m.Topic][m.Partition]; err != nil {
			if n := rs.failed(); n > 1 {
				return fmt.Errorf("%d commits refused, first %s p%d: %w", n, m.Topic, m.Partition, err)
			}
			return fmt.Errorf("commit refused for %s p%d: %w", m.Topic, m.Partition, err)
		}
	}
	return nil
}

// screen is the infra-plan-diff piece for the plan, and once applied, the
// commit lines under it.
func (rs *reset) screen(phase string, role kit.Role) screens.Plan {
	p := rs.plan
	add, change, _ := p.Counts()
	v := screens.Plan{
		Command: rs.cmdline, Lead: "kaas will perform the following actions:",
		Add: add, Change: change, Note: rs.note(),
		Footer: "group: " + p.Group + "  ·  state: " + orDash(p.State),
		Phase:  phase, PhaseOf: role, Lock: "members: " + strconv.Itoa(p.Members),
	}
	for _, t := range rs.topics() {
		b := screens.PlanBlock{Change: screens.PlanAdd, Comment: p.Group + " · " + t, Resource: fmt.Sprintf("offsets %q %q", p.Group, t)}
		same, skipped := 0, 0
		for _, m := range p.Moves {
			if m.Topic != t {
				continue
			}
			switch {
			case m.Skip != "":
				skipped++
				continue
			case !m.Changed():
				same++
				continue
			}
			a := screens.PlanAttr{Key: "p" + strconv.Itoa(int(m.Partition)), To: screens.Num(float64(m.To)), Note: moveNote(m)}
			if m.From >= 0 {
				a.From, b.Change = screens.Num(float64(m.From)), screens.PlanChange
			}
			b.Attrs = append(b.Attrs, a)
		}
		if len(b.Attrs) == 0 {
			continue
		}
		var hidden []string
		if same > 0 {
			hidden = append(hidden, plural(same, "unchanged partition"))
		}
		if skipped > 0 {
			hidden = append(hidden, plural(skipped, "partition")+" with nothing to shift from")
		}
		if len(hidden) > 0 {
			b.Hidden = "(" + strings.Join(hidden, ", ") + " hidden)"
		}
		v.Blocks = append(v.Blocks, b)
	}
	if !rs.applied {
		return v
	}
	v.Ask, v.Answer = "Do you want to perform these actions?", "yes"
	for _, b := range v.Blocks {
		t := strings.TrimPrefix(b.Comment, p.Group+" · ")
		status := planMarkRole(b.Change)
		v.Apply = append(v.Apply, screens.ApplyLine{Name: t, Status: "Committing...", Role: status, Detail: "[" + plural(len(b.Attrs), "partition") + "]"})
	}
	for _, b := range v.Blocks {
		t := strings.TrimPrefix(b.Comment, p.Group+" · ")
		var bad []string
		var first error
		for _, a := range b.Attrs {
			id, _ := strconv.Atoi(strings.TrimPrefix(a.Key, "p"))
			if err := rs.results[t][int32(id)]; err != nil {
				bad = append(bad, a.Key)
				if first == nil {
					first = err
				}
			}
		}
		if len(bad) > 0 {
			v.Apply = append(v.Apply, screens.ApplyLine{Name: t, Status: "Commit refused for " + strings.Join(bad, ", "), Role: kit.Err, Detail: first.Error()})
			continue
		}
		v.Apply = append(v.Apply, screens.ApplyLine{Name: t, Status: "Commit complete after " + took(rs.took), Role: planMarkRole(b.Change)})
	}
	v.Done = fmt.Sprintf("Reset complete! Offsets: %d added, %d changed.", add, change)
	if n := rs.failed(); n > 0 {
		v.Done, v.Failed = fmt.Sprintf("Reset failed for %s. The rest were committed.", plural(n, "partition")), true
	}
	return v
}

func planMarkRole(change int) kit.Role {
	if change == screens.PlanAdd {
		return kit.OK
	}
	return kit.Notice
}

// took is a commit's time, in the unit that reads best.
func took(d time.Duration) string {
	if d < time.Second {
		return strconv.Itoa(max(1, int(d.Milliseconds()))) + "ms"
	}
	return strconv.Itoa(int(d.Round(time.Second).Seconds())) + "s"
}

// moveNote is what a move means for the group's next read.
func moveNote(m offsets.Move) string {
	where := ""
	switch {
	case m.Start == m.End:
		where = "empty log"
	case m.To == m.Start:
		where = "start of the log"
	case m.To == m.End:
		where = "end of the log"
	}
	var parts []string
	switch {
	case m.From < 0:
	case m.To < m.From:
		parts = append(parts, "re-reads "+screens.Num(float64(m.From-m.To)))
	default:
		parts = append(parts, "skips "+screens.Num(float64(m.To-m.From)))
	}
	if where != "" {
		parts = append(parts, where)
	}
	if m.Clamped {
		parts = append(parts, "clamped")
	}
	return strings.Join(parts, ", ")
}

// note is the plan's effect in records, and what --to resolved to.
func (rs *reset) note() string {
	p := rs.plan
	reread, skipped := p.Records()
	var parts []string
	if reread > 0 {
		parts = append(parts, fmt.Sprintf("%s reads %s again", p.Group, plural(int(reread), "record")))
	}
	if skipped > 0 {
		parts = append(parts, fmt.Sprintf("%s skips %s", p.Group, plural(int(skipped), "record")))
	}
	if add, _, _ := p.Counts(); add > 0 {
		parts = append(parts, plural(add, "first commit"))
	}
	if p.Target.Kind == offsets.Time {
		parts = append(parts, "the first record at or after "+p.Target.At.Local().Format("2006-01-02 15:04:05"))
	}
	if p.State == "" || p.State == "Dead" {
		parts = append(parts, p.Group+" has no commits yet: this creates it")
	}
	return strings.Join(parts, " · ")
}

// topics is the plan's topics in order.
func (rs *reset) topics() []string {
	var out []string
	for _, m := range rs.plan.Moves {
		if len(out) == 0 || out[len(out)-1] != m.Topic {
			out = append(out, m.Topic)
		}
	}
	return out
}

type moveJSON struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	From      *int64 `json:"from"` // null: no commit before
	To        int64  `json:"to"`
	Change    string `json:"change"` // add, change, same, skip
	Clamped   bool   `json:"clamped,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Error     string `json:"error,omitempty"`
}

type resetJSON struct {
	Group   string     `json:"group"`
	State   string     `json:"state"`
	To      string     `json:"to"`
	DryRun  bool       `json:"dryRun"`
	Applied bool       `json:"applied"`
	Moves   []moveJSON `json:"moves"`
}

// print reports the plan or what was committed without the screen: JSON,
// or a line per moved partition.
func (rs *reset) print(cmd *cobra.Command, surf surface, output string) error {
	out := cmd.OutOrStdout()
	p := rs.plan
	if output == "json" {
		j := resetJSON{Group: p.Group, State: p.State, To: p.Target.String(), DryRun: rs.dry, Applied: rs.applied, Moves: []moveJSON{}}
		for _, m := range p.Moves {
			mj := moveJSON{Topic: m.Topic, Partition: m.Partition, To: m.To, Clamped: m.Clamped, Reason: m.Skip}
			if m.From >= 0 {
				from := m.From
				mj.From = &from
			}
			switch {
			case m.Skip != "":
				mj.Change, mj.To = "skip", m.From
			case !m.Changed():
				mj.Change = "same"
			case m.From < 0:
				mj.Change = "add"
			default:
				mj.Change = "change"
			}
			if err := rs.results[m.Topic][m.Partition]; err != nil {
				mj.Error = err.Error()
			}
			j.Moves = append(j.Moves, mj)
		}
		return json.NewEncoder(out).Encode(j)
	}
	add, change, _ := p.Counts()
	if add+change == 0 {
		msg := fmt.Sprintf("%s is already there on %s: nothing to commit.", p.Group, andList(rs.topics()))
		if surf.rich {
			surf.print([]string{surf.kit.S(kit.OK, "✓ ") + surf.kit.S(kit.FG, msg)})
			return nil
		}
		_, err := fmt.Fprintln(out, msg)
		return err
	}
	for _, m := range p.Moves {
		if !m.Changed() {
			continue
		}
		from := "-"
		if m.From >= 0 {
			from = strconv.FormatInt(m.From, 10)
		}
		state := ""
		switch {
		case rs.dry:
			state = " (dry run)"
		case rs.results[m.Topic][m.Partition] != nil:
			state = " refused: " + rs.results[m.Topic][m.Partition].Error()
		}
		fmt.Fprintf(out, "%s p%d: %s -> %d%s\n", m.Topic, m.Partition, from, m.To, state)
	}
	return nil
}
