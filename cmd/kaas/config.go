package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/configs"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newConfigCmd(brokers, themeName *string) *cobra.Command {
	var (
		output  string
		broker  int32
		setOnly bool
	)
	cmd := &cobra.Command{
		Use:   "config (<topic> | --broker <id>)",
		Short: "Show a topic's or a broker's configs, with what isn't a default flagged",
		Long: `Show every config of a topic, or of a broker with --broker, in sections:
retention, cleanup, segments, messages and replication for a topic.

A value that isn't Kafka's default is drawn bright and tagged with where
it is set: on the topic, on a broker, as a cluster-wide default, or in a
broker's server.properties. Switches are on or off; durations and sizes
sit on a slider from a second to a year and a kilobyte to a terabyte, so
the odd ones out stand out. The foot shows the selected config's value,
what it would be if unset, and the broker's documentation for it.

↑↓ move, s shows only what is set, q quits. On a pipe it prints a line
a config, and -o json gives each with its source and default. Sensitive
values are never shown.`,
		Example: `  # A topic's configs
  kaas config orders
  # Only what differs from the defaults
  kaas config orders --set
  # A broker's
  kaas config --broker 1
  # For a script
  kaas config orders -o json | jq '.[] | select(.source == "topic")'`,
		Args: func(cmd *cobra.Command, args []string) error {
			byBroker := cmd.Flags().Changed("broker")
			switch {
			case len(args) > 1:
				return usageError(cmd, "topic", args[1], "one topic at a time")
			case len(args) == 0 && !byBroker:
				return usageError(cmd, "topic", "", "required: a topic, or --broker <id> for a broker's configs")
			case len(args) == 1 && byBroker:
				return usageError(cmd, "broker", strconv.Itoa(int(broker)), "a topic or a broker, not both")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "text", "json"); err != nil {
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
			v := configView{setOnly: setOnly}
			if len(args) == 1 {
				v.topic = args[0]
			} else {
				v.broker = &broker
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			err = v.load(ctx, cmd, cl)
			cancel()
			if err != nil {
				return err
			}
			surf := detect(cmd.OutOrStdout(), *themeName)
			if output == "json" || !surf.rich {
				return v.print(cmd, output)
			}
			if !answerable(cmd.InOrStdin()) {
				// Nowhere to take keys from: the whole list, once.
				s := v.screen(surf.width)
				_, err := fmt.Fprintln(surf.w, kit.Join(s.Render(surf.kit, surf.width, s.Height())))
				return err
			}
			m := configModel{v: v, k: surf.kit, w: surf.width, h: surf.height}
			m.v.sel = m.v.first()
			_, err = tea.NewProgram(m, tea.WithContext(cmd.Context())).Run()
			if cmd.Context().Err() != nil {
				return nil
			}
			return err
		},
	}
	fl := cmd.Flags()
	fl.Int32Var(&broker, "broker", 0, "a broker's configs, by node ID, instead of a topic's")
	fl.BoolVar(&setOnly, "set", false, "only configs that aren't Kafka's defaults")
	fl.StringVarP(&output, "output", "o", "text", "output format: text|json")
	return cmd
}

// configView is one kaas config: whose configs, and what they are.
type configView struct {
	topic   string
	broker  *int32
	rf      int // the topic's replication factor, for min.insync.replicas
	entries []configs.Entry
	setOnly bool
	sel     int // the selected row of rows()
}

// load checks the topic or broker and reads its configs.
func (v *configView) load(ctx context.Context, cmd *cobra.Command, cl *kgo.Client) error {
	adm := kadm.NewClient(cl)
	if v.broker != nil {
		bs, err := adm.ListBrokers(ctx)
		if err != nil {
			return err
		}
		ids := bs.NodeIDs()
		if !slices.Contains(ids, *v.broker) {
			names := make([]string, len(ids))
			for i, id := range ids {
				names[i] = strconv.Itoa(int(id))
			}
			return usageError(cmd, "broker", strconv.Itoa(int(*v.broker)), "the brokers are "+strings.Join(names, ", "))
		}
	} else {
		td, err := adm.ListTopics(ctx)
		if err != nil {
			return err
		}
		t, ok := td[v.topic]
		if !ok {
			describe := func(n string) string { return plural(len(td[n].Partitions), "partition") }
			return notFoundError("topic", v.topic, "the cluster", td.Names(), describe, "kaas topics")
		}
		for _, p := range t.Partitions {
			v.rf = max(v.rf, len(p.Replicas))
		}
	}
	es, err := configs.Describe(ctx, cl, v.topic, v.broker)
	if err != nil {
		return err
	}
	v.entries = es
	return nil
}

// levelTag names where a set value comes from, for a row's tag.
func (v *configView) levelTag(l int) (string, kit.Role) {
	switch l {
	case configs.LevelTopic:
		return "topic", kit.OK
	case configs.LevelBroker:
		if v.broker != nil {
			return "dynamic", kit.OK
		}
		return "broker", kit.Accent2
	case configs.LevelCluster:
		return "cluster", kit.Accent2
	case configs.LevelStatic:
		return "file", kit.Info
	}
	return "", kit.Muted
}

// levelName is where a value comes from, in words.
func levelName(l int) string {
	return [...]string{"default", "static", "cluster", "broker", "topic"}[l]
}

// levelPhrase is where a value comes from, for the foot.
func (v *configView) levelPhrase(l int) string {
	switch l {
	case configs.LevelTopic:
		return "set on the topic"
	case configs.LevelBroker:
		if v.broker != nil {
			return "set on this broker"
		}
		return "from a broker override"
	case configs.LevelCluster:
		return "a cluster-wide default"
	case configs.LevelStatic:
		return "in server.properties"
	}
	return "Kafka's default"
}

// visible is the entries the view shows.
func (v *configView) visible() []configs.Entry {
	if !v.setOnly {
		return v.entries
	}
	var out []configs.Entry
	for _, e := range v.entries {
		if e.Set() {
			out = append(out, e)
		}
	}
	return out
}

// rows is the settings rows under their headings, and the entry each
// row shows (nil for a heading).
func (v *configView) rows() ([]screens.SettingRow, []*configs.Entry) {
	var rows []screens.SettingRow
	var es []*configs.Entry
	for _, sec := range configs.Sections(v.visible(), v.broker != nil) {
		rows = append(rows, screens.SettingRow{Heading: sec.Name})
		es = append(es, nil)
		for i := range sec.Entries {
			e := &sec.Entries[i]
			rows = append(rows, v.row(*e))
			es = append(es, e)
		}
	}
	return rows, es
}

// row is one config as a settings row.
func (v *configView) row(e configs.Entry) screens.SettingRow {
	r := screens.SettingRow{Key: e.Key, Detail: configs.Summary(e), Set: e.Set(), Value: v.valueText(e)}
	r.Tag, r.TagRole = v.levelTag(e.Level)
	if e.Null {
		return r
	}
	switch configs.KindOf(e) {
	case configs.KindBool:
		r.Control, r.On = screens.SettingToggle, e.Value == "true"
	case configs.KindDuration:
		if d, forever, ok := configs.Duration(e); ok {
			r.Control, r.Pos = screens.SettingSlider, logPos(float64(d/time.Millisecond), 1e3, 365*24*3600e3)
			if forever {
				r.Pos = 1
			}
		}
	case configs.KindBytes:
		if n, err := strconv.ParseInt(e.Value, 10, 64); err == nil {
			r.Control, r.Pos = screens.SettingSlider, logPos(float64(n), 1<<10, 1<<40)
			if n < 0 {
				r.Pos = 1
			}
		}
	case configs.KindRatio:
		if f, err := strconv.ParseFloat(e.Value, 64); err == nil && f >= 0 && f <= 1 {
			r.Control, r.Pos = screens.SettingSlider, f
		}
	case configs.KindCount:
		if n, err := strconv.Atoi(e.Value); err == nil && e.Key == "min.insync.replicas" && v.rf > 1 {
			r.Control, r.Pos = screens.SettingSlider, float64(n-1)/float64(v.rf-1)
		}
	}
	return r
}

// logPos is where v sits between lo and hi on a log scale, 0 to 1.
func logPos(v, lo, hi float64) float64 {
	if v <= lo {
		return 0
	}
	return min(1, math.Log(v/lo)/math.Log(hi/lo))
}

// valueText is a config's value as a person reads it: 7d, 1.0 GiB,
// unlimited, 2 of 3.
func (v *configView) valueText(e configs.Entry) string {
	switch {
	case e.Sensitive:
		return "hidden"
	case e.Null:
		return "-"
	case e.Value == "":
		return `""`
	}
	return humanValue(e, v.rf)
}

// humanValue reads a set value for a person.
func humanValue(e configs.Entry, rf int) string {
	switch configs.KindOf(e) {
	case configs.KindDuration:
		d, forever, ok := configs.Duration(e)
		switch {
		case !ok:
		case forever:
			return "forever"
		case d == 0:
			return "0"
		case d < time.Second:
			return strconv.Itoa(int(d/time.Millisecond)) + "ms"
		default:
			return retentionText(d)
		}
	case configs.KindBytes:
		if n, err := strconv.ParseInt(e.Value, 10, 64); err == nil {
			if n < 0 {
				return "unlimited"
			}
			return screens.Bytes(n)
		}
	case configs.KindCount:
		if e.Key == "min.insync.replicas" && rf > 0 {
			return e.Value + " of " + strconv.Itoa(rf)
		}
	}
	return e.Value
}

// counts is how many values are set at each level.
func (v *configView) counts() [configs.LevelTopic + 1]int {
	var n [configs.LevelTopic + 1]int
	for _, e := range v.entries {
		n[e.Level]++
	}
	return n
}

// badge is the frame's summary of what is set.
func (v *configView) badge() string {
	n := v.counts()
	var parts []string
	if v.broker != nil {
		parts = append(parts, fmt.Sprintf("%d set dynamically", n[configs.LevelBroker]))
		if n[configs.LevelCluster] > 0 {
			parts = append(parts, fmt.Sprintf("%d cluster-wide", n[configs.LevelCluster]))
		}
		if n[configs.LevelStatic] > 0 {
			parts = append(parts, fmt.Sprintf("%d in the file", n[configs.LevelStatic]))
		}
	} else {
		parts = append(parts, fmt.Sprintf("%d set on the topic", n[configs.LevelTopic]))
		if b := n[configs.LevelBroker] + n[configs.LevelCluster] + n[configs.LevelStatic]; b > 0 {
			parts = append(parts, fmt.Sprintf("%d from brokers", b))
		}
	}
	return "✓ " + strings.Join(parts, " · ")
}

// first is the first selectable row.
func (v *configView) first() int {
	rows, _ := v.rows()
	for i, r := range rows {
		if r.Heading == "" {
			return i
		}
	}
	return 0
}

// screen is the settings-toggles piece for the view, its foot wrapped to
// width.
func (v *configView) screen(width int) screens.Settings {
	rows, es := v.rows()
	s := screens.Settings{Badge: v.badge(), Rows: rows, Sel: v.sel, Keys: "↑↓ move · s set only · q quit"}
	if v.broker != nil {
		s.Title = fmt.Sprintf("Config · broker %d", *v.broker)
	} else {
		s.Title = "Config · topic " + v.topic
	}
	if v.setOnly {
		s.Keys = "↑↓ move · s show all · q quit"
		s.Empty = "nothing is set: every config is Kafka's default"
	}
	if v.sel >= 0 && v.sel < len(es) && es[v.sel] != nil {
		s.Foot = v.foot(*es[v.sel], max(20, width-6))
	} else if len(rows) > 0 {
		s.Foot = []string{"", "", ""}
	}
	return s
}

// foot is three lines about e: its value and where it comes from, then
// its documentation.
func (v *configView) foot(e configs.Entry, w int) []string {
	line := e.Key + " = "
	switch {
	case e.Sensitive:
		line += "(sensitive: hidden)"
	case e.Null:
		line += "(unset)"
	default:
		line += e.Value
		if h := humanValue(e, v.rf); h != e.Value {
			line += " (" + h + ")"
		}
	}
	line += " · " + v.levelPhrase(e.Level)
	if e.HasDef {
		line += " · without it " + humanValue(configs.Entry{Key: e.Key, Value: e.Default, Type: e.Type}, v.rf)
	}
	if e.ReadOnly {
		line += " · read-only"
	}
	doc := strings.Join(strings.Fields(e.Doc), " ")
	if doc == "" {
		doc = configs.Summary(e)
	}
	out := []string{line}
	wrapped := kit.Wrap(doc, w)
	if len(wrapped) > 2 {
		wrapped = wrapped[:2]
		wrapped[1] = kit.Fit(wrapped[1], w-1) + "…"
	}
	out = append(out, wrapped...)
	for len(out) < 3 {
		out = append(out, "")
	}
	return out
}

type configJSON struct {
	Key       string  `json:"key"`
	Value     *string `json:"value"` // null: unset, or sensitive
	Source    string  `json:"source"`
	Default   *string `json:"default,omitempty"`
	Section   string  `json:"section"`
	Type      string  `json:"type,omitempty"`
	ReadOnly  bool    `json:"readOnly,omitempty"`
	Sensitive bool    `json:"sensitive,omitempty"`
}

// print is the configs without the screen: a line each, or JSON.
func (v *configView) print(cmd *cobra.Command, output string) error {
	out := cmd.OutOrStdout()
	var js []configJSON
	for _, sec := range configs.Sections(v.visible(), v.broker != nil) {
		for _, e := range sec.Entries {
			j := configJSON{Key: e.Key, Source: levelName(e.Level), Section: strings.ToLower(sec.Name), ReadOnly: e.ReadOnly, Sensitive: e.Sensitive}
			if !e.Null {
				val := e.Value
				j.Value = &val
			}
			if e.HasDef {
				d := e.Default
				j.Default = &d
			}
			if e.Type != 0 {
				j.Type = strings.ToLower(e.Type.String())
			}
			js = append(js, j)
		}
	}
	if output == "json" {
		if js == nil {
			js = []configJSON{}
		}
		return json.NewEncoder(out).Encode(js)
	}
	w := 0
	for _, j := range js {
		w = max(w, len(j.Key))
	}
	for _, j := range js {
		val := "-"
		switch {
		case j.Sensitive:
			val = "(hidden)"
		case j.Value != nil:
			val = *j.Value
		}
		fmt.Fprintf(out, "%-*s  %-8s %s\n", w, j.Key, j.Source, val)
	}
	return nil
}

// configModel scrolls the settings panel.
type configModel struct {
	v    configView
	k    kit.Kit
	w, h int
}

func (m configModel) Init() tea.Cmd { return nil }

// move selects the next setting n rows away, past headings.
func (m *configModel) move(n int) {
	rows, _ := m.v.rows()
	if len(rows) == 0 {
		return
	}
	step := 1
	if n < 0 {
		step, n = -1, -n
	}
	at := m.v.sel
	for ; n > 0; n-- {
		next := at + step
		for next >= 0 && next < len(rows) && rows[next].Heading != "" {
			next += step
		}
		if next < 0 || next >= len(rows) {
			break
		}
		at = next
	}
	m.v.sel = at
}

func (m configModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
	case tea.KeyPressMsg:
		page := max(1, m.h-10)
		switch msg.String() {
		case "ctrl+c", "esc", "q":
			return m, tea.Quit
		case "up", "k":
			m.move(-1)
		case "down", "j":
			m.move(1)
		case "pgup", "ctrl+u":
			m.move(-page)
		case "pgdown", "ctrl+d", "space":
			m.move(page)
		case "home", "g":
			m.v.sel = m.v.first()
		case "end", "G":
			m.move(math.MaxInt32)
		case "s":
			// Keep the same config selected when it is still shown.
			_, es := m.v.rows()
			var key string
			if m.v.sel < len(es) && es[m.v.sel] != nil {
				key = es[m.v.sel].Key
			}
			m.v.setOnly = !m.v.setOnly
			m.v.sel = m.v.first()
			if _, es := m.v.rows(); key != "" {
				for i, e := range es {
					if e != nil && e.Key == key {
						m.v.sel = i
					}
				}
			}
		}
	}
	return m, nil
}

func (m configModel) View() tea.View {
	w, h := m.w, m.h
	if w == 0 {
		w = 100
	}
	if h == 0 {
		h = 30
	}
	return tea.NewView(kit.Join(m.v.screen(w).Render(m.k, w, max(12, h-1))))
}
