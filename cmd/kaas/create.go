package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/core/topics"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

func newTopicsCreateCmd(brokers, themeName *string) *cobra.Command {
	var (
		output              string
		partitions, rf      int
		configs             []string
		dryRun, ifNotExists bool
	)
	cmd := &cobra.Command{
		Use:   "create [topic]",
		Short: "Create a topic: step by step on a terminal, or from flags",
		Long: `Create a topic.

On a terminal without --partitions, kaas walks you through it: the name,
checked against Kafka's rules and the topics you have; the partitions and
replication factor, starting from the cluster's defaults; the configs
that differ from the broker's; and a check with the controller before
anything is created.

With --partitions, or without a terminal, the flags say it all. A
partition count or replication factor of 0 takes the broker's default.
--dry-run has the controller check the topic without creating it.`,
		Example: `  # Step by step
  kaas topics create
  # From flags
  kaas topics create payments -p 12 -r 3 -c retention.ms=2592000000 -c cleanup.policy=compact
  # Idempotent, for a script
  kaas topics create payments -p 12 --if-not-exists -o json | jq .created`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "text", "json"); err != nil {
				return err
			}
			switch {
			case partitions < 0:
				return usageError(cmd, "partitions", strconv.Itoa(partitions), "must be 0 (the broker's default) or more")
			case rf < 0 || rf > 32767:
				return usageError(cmd, "replication-factor", strconv.Itoa(rf), "must be 0 (the broker's default) or more")
			}
			set := map[string]string{}
			for _, c := range configs {
				k, v, ok := strings.Cut(c, "=")
				if !ok || strings.TrimSpace(k) == "" {
					return usageError(cmd, "config", c, "want key=value")
				}
				set[strings.TrimSpace(k)] = v
			}
			name := ""
			if len(args) == 1 {
				name = args[0]
			}
			surf := detect(cmd.OutOrStdout(), *themeName)
			in, ok := cmd.InOrStdin().(*os.File)
			wizard := ok && term.IsTerminal(in.Fd()) && surf.rich && output == "text" && !cmd.Flags().Changed("partitions")
			if !wizard {
				if name == "" {
					return usageError(cmd, "topic", "", "required without a terminal: the topic to create")
				}
				if err := topics.ValidName(name); err != nil {
					return usageError(cmd, "topic", name, err.Error())
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
			s, err := snapshot.Fetch(ctx, cl)
			cancel()
			if err != nil {
				return err
			}
			c := creation{cl: cl, adm: kadm.NewClient(cl), snap: s, dry: dryRun,
				spec: topics.Spec{Name: name, Partitions: int32(partitions), ReplicationFactor: int16(rf), Configs: set}}
			if wizard {
				return c.wizard(cmd, *themeName)
			}
			if _, ok := s.Topic(name); ok {
				if ifNotExists {
					return c.print(cmd, output, surf, false)
				}
				return &cliError{Code: "exists", Exit: exitFailure, Message: "topic " + name + " already exists",
					Hints: []string{"--if-not-exists makes this a no-op", "kaas topics describe " + name}}
			}
			if other := topics.Collides(name, c.names()); other != "" {
				return usageError(cmd, "topic", name, collision(other))
			}
			ctx, cancel = context.WithTimeout(cmd.Context(), 30*time.Second)
			defer cancel()
			if err := c.create(ctx); err != nil {
				return err
			}
			return c.print(cmd, output, surf, true)
		},
	}
	f := cmd.Flags()
	f.IntVarP(&partitions, "partitions", "p", 0, "partitions; 0 takes the broker's num.partitions")
	f.IntVarP(&rf, "replication-factor", "r", 0, "replicas per partition; 0 takes the broker's default.replication.factor")
	f.StringArrayVarP(&configs, "config", "c", nil, "a key=value topic config (repeatable)")
	f.BoolVar(&ifNotExists, "if-not-exists", false, "succeed without changes when the topic exists")
	f.BoolVar(&dryRun, "dry-run", false, "have the controller check the topic without creating it")
	f.StringVarP(&output, "output", "o", "text", "output format: text|json")
	return cmd
}

func collision(other string) string {
	return fmt.Sprintf("collides with %s: Kafka's metrics turn '.' and '_' into the same name", other)
}

// creation is one run of kaas topics create: what to create and, once
// the controller answers, what it made.
type creation struct {
	cl   *kgo.Client
	adm  *kadm.Client
	snap *snapshot.Snapshot
	spec topics.Spec
	dry  bool
	made topics.Created
}

// names is the cluster's topics.
func (c creation) names() []string {
	var out []string
	for _, t := range c.snap.Topics {
		out = append(out, t.Name)
	}
	return out
}

// create asks the controller, then for a real create waits until every
// partition has a leader, so the topic is usable when kaas returns.
func (c *creation) create(ctx context.Context) error {
	made, err := topics.Create(ctx, c.adm, c.spec, c.dry)
	if err != nil {
		return fmt.Errorf("creating %s: %w", c.spec.Name, err)
	}
	c.made = made
	if c.dry {
		return nil
	}
	for {
		if led, total := c.leaders(ctx); total > 0 && led == total {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("created %s, but its partitions had no leaders in time: run kaas topics describe %s", c.spec.Name, c.spec.Name)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// leaders is how many of the topic's partitions have a leader.
func (c creation) leaders(ctx context.Context) (led, total int) {
	s, err := snapshot.Fetch(ctx, c.cl)
	if err != nil {
		return 0, 0
	}
	t, ok := s.Topic(c.spec.Name)
	if !ok {
		return 0, 0
	}
	for _, p := range t.Partitions {
		if p.Leader >= 0 {
			led++
		}
	}
	return led, len(t.Partitions)
}

type createJSON struct {
	Topic             string            `json:"topic"`
	ID                string            `json:"id,omitempty"`
	Partitions        int32             `json:"partitions,omitempty"`
	ReplicationFactor int16             `json:"replicationFactor,omitempty"`
	Configs           map[string]string `json:"configs"`
	Created           bool              `json:"created"`
	DryRun            bool              `json:"dryRun"`
}

// print reports a flag-driven create; created is false when the topic was
// already there.
func (c creation) print(cmd *cobra.Command, output string, surf surface, created bool) error {
	out := cmd.OutOrStdout()
	name := c.spec.Name
	if output == "json" {
		j := createJSON{Topic: name, ID: c.made.ID, Partitions: c.made.Partitions, ReplicationFactor: c.made.ReplicationFactor,
			Configs: c.spec.Configs, Created: created && !c.dry, DryRun: c.dry}
		if !created {
			j.Configs = map[string]string{}
		}
		return json.NewEncoder(out).Encode(j)
	}
	what := fmt.Sprintf("%s · RF %d", plural(int(c.made.Partitions), "partition"), c.made.ReplicationFactor)
	if n := len(c.spec.Configs); n > 0 {
		what += " · " + strings.Join(configPairs(c.spec.Configs), " · ")
	}
	var msg string
	switch {
	case !created:
		msg = name + " already exists; nothing to do."
	case c.dry:
		msg = fmt.Sprintf("The controller accepts %s: %s. Nothing was created.", name, what)
	default:
		msg = fmt.Sprintf("Created %s: %s.", name, what)
	}
	if !surf.rich {
		fmt.Fprintln(out, msg)
		return nil
	}
	surf.print([]string{surf.kit.S(kit.OK, "✓ ") + surf.kit.S(kit.FG, msg)})
	return nil
}

func configPairs(m map[string]string) []string {
	var out []string
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, k+"="+m[k])
	}
	return out
}

// wizard runs the stepper until the topic is created or the user quits.
func (c creation) wizard(cmd *cobra.Command, themeName string) error {
	th, err := theme.Get(themeName)
	if err != nil {
		th, _ = theme.Get(theme.Default)
	}
	m := newWizard(c, kit.New(th, os.Getenv("NO_COLOR") == ""))
	final, err := tea.NewProgram(m, tea.WithContext(cmd.Context())).Run()
	if err != nil {
		return err
	}
	if w := final.(wizardModel); !w.finished {
		return fmt.Errorf("cancelled: nothing was created")
	}
	return nil
}
