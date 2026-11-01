package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/core/topics"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// undoWindow is how long a confirmed delete waits before it is sent:
// Kafka can't bring a topic back, so undo has to come first.
const undoWindow = 5 * time.Second

func newTopicsDeleteCmd(brokers, themeName *string) *cobra.Command {
	var (
		output string
		yes    bool
	)
	cmd := &cobra.Command{
		Use:   "delete <topic>...",
		Short: "Delete topics, after typing the name and a few seconds to undo",
		Long: `Delete one or more topics and every record in them.

On a terminal, a dialog shows what goes with the topic (its records, size
and the groups still reading it) and asks you to type its name. After
that there are 5 seconds to undo before kaas sends the delete, because
Kafka can't bring a topic back. kaas then waits until the cluster no
longer lists it.

Without a terminal to type into, --yes is required and the delete is
sent at once. Internal topics such as __consumer_offsets are refused.`,
		Example: `  # Confirm in a dialog, with a moment to undo
  kaas topics delete orders.v1
  # Several at once
  kaas topics delete orders.v1 orders.v2
  # In a script
  kaas topics delete orders.v1 --yes -o json`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return usageError(cmd, "topic", "", "required: the topics to delete")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := oneOf(cmd, "output", output, "text", "json"); err != nil {
				return err
			}
			surf := detect(cmd.OutOrStdout(), *themeName)
			typing := answerable(cmd.InOrStdin())
			if !yes && !typing {
				return usageError(cmd, "yes", "", "required without a terminal to confirm: deleted topics can't be brought back")
			}
			if _, err := connect(cmd, *brokers, *themeName); err != nil {
				return err
			}
			cl, err := kgo.NewClient(kgo.SeedBrokers(splitSeeds(*brokers)...))
			if err != nil {
				return err
			}
			defer cl.Close()
			names := slices.Compact(slices.Sorted(slices.Values(args)))
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			d, err := gatherDeletion(ctx, cl, names)
			cancel()
			if err != nil {
				return err
			}
			for _, t := range d.topics {
				if t.topic.Internal || strings.HasPrefix(t.topic.Name, "__") {
					return usageError(cmd, "topic", t.topic.Name, "an internal topic: Kafka needs it, so kaas won't delete it")
				}
			}
			switch {
			case yes:
			case surf.rich && output == "text":
				return d.dialog(cmd, *themeName)
			default:
				w, k := cmd.ErrOrStderr(), kit.Kit{}
				if !confirm(cmd.InOrStdin(), w, k, fmt.Sprintf("Delete %s? This can't be undone. ", andList(names)), d.confirmText()) {
					return cancelled("Not confirmed: nothing was deleted.")
				}
			}
			ctx, cancel = context.WithTimeout(cmd.Context(), 45*time.Second)
			defer cancel()
			d.send(ctx)
			return d.print(cmd, output, surf)
		},
	}
	f := cmd.Flags()
	f.BoolVarP(&yes, "yes", "y", false, "skip the dialog and the undo window")
	f.StringVarP(&output, "output", "o", "text", "output format: text|json")
	return cmd
}

// deletion is one run of kaas topics delete: what is about to go, and
// how each delete went.
type deletion struct {
	cl      *kgo.Client
	adm     *kadm.Client
	names   []string
	topics  []topicDescription
	readers []string // groups with commits on any of them
	results map[string]error
	err     error // the request itself failed
}

func gatherDeletion(ctx context.Context, cl *kgo.Client, names []string) (deletion, error) {
	s, err := snapshot.Fetch(ctx, cl)
	if err != nil {
		return deletion{}, err
	}
	if err := missingTopic(s, names); err != nil {
		return deletion{}, err
	}
	d := deletion{cl: cl, adm: kadm.NewClient(cl), names: names}
	for _, n := range names {
		td, err := describeTopic(ctx, cl, n)
		if err != nil {
			return deletion{}, err
		}
		d.topics = append(d.topics, td)
		for _, g := range topicReaders(ctx, d.adm, n) {
			if !slices.Contains(d.readers, g) {
				d.readers = append(d.readers, g)
			}
		}
	}
	slices.Sort(d.readers)
	return d, nil
}

// confirmText is what to type: the name, or "delete N topics".
func (d deletion) confirmText() string {
	if len(d.names) == 1 {
		return d.names[0]
	}
	return fmt.Sprintf("delete %d topics", len(d.names))
}

// size is the records, bytes and partitions going, -1 where unknown.
func (d deletion) size() (records, bytes int64, parts int) {
	for _, td := range d.topics {
		for _, p := range td.topic.Partitions {
			if n := td.records(p); n >= 0 {
				records += n
			}
		}
		bytes += max(td.details.Disk, 0)
		parts += len(td.topic.Partitions)
	}
	return records, bytes, parts
}

// sizeText is "251,038 records (609 MiB) in 4 partitions".
func (d deletion) sizeText() string {
	records, bytes, parts := d.size()
	return fmt.Sprintf("%s records (%s) in %s", screens.Num(float64(records)), screens.Bytes(bytes), plural(parts, "partition"))
}

// send deletes the topics, then waits until metadata no longer lists
// them: Kafka finishes deleting in the background.
func (d *deletion) send(ctx context.Context) {
	d.results, d.err = topics.Delete(ctx, d.adm, d.names)
	if d.err != nil {
		return
	}
	for {
		s, err := snapshot.Fetch(ctx, d.cl)
		if err == nil && !slices.ContainsFunc(d.names, func(n string) bool { _, ok := s.Topic(n); return ok && d.results[n] == nil }) {
			return
		}
		select {
		case <-ctx.Done():
			return // deleted; the cluster just hasn't caught up yet
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// failed is the topics that were not deleted.
func (d deletion) failed() []string {
	var out []string
	for _, n := range d.names {
		if d.err != nil || d.results[n] != nil {
			out = append(out, n)
		}
	}
	return out
}

// outcome is the error to exit with, nil when every topic went.
func (d deletion) outcome() error {
	if d.err != nil {
		return fmt.Errorf("deleting %s: %w", andList(d.names), d.err)
	}
	if f := d.failed(); len(f) > 0 {
		return fmt.Errorf("%d of %s not deleted: %s: %w", len(f), plural(len(d.names), "topic"), f[0], d.results[f[0]])
	}
	return nil
}

type deleteJSON struct {
	Topic   string `json:"topic"`
	Deleted bool   `json:"deleted"`
	Error   string `json:"error,omitempty"`
}

func (d deletion) print(cmd *cobra.Command, output string, surf surface) error {
	out := cmd.OutOrStdout()
	switch {
	case output == "json":
		js := []deleteJSON{}
		for _, n := range d.names {
			j := deleteJSON{Topic: n, Deleted: d.err == nil && d.results[n] == nil}
			if d.err != nil {
				j.Error = d.err.Error()
			} else if e := d.results[n]; e != nil {
				j.Error = e.Error()
			}
			js = append(js, j)
		}
		if err := json.NewEncoder(out).Encode(js); err != nil {
			return err
		}
	case surf.rich:
		surf.print(d.card(0).Render(surf.kit, surf.width))
	default:
		for _, n := range d.names {
			if !slices.Contains(d.failed(), n) {
				fmt.Fprintln(out, "deleted "+n)
			}
		}
	}
	return d.outcome()
}

// card is the trash lid: open while undo is possible (left > 0), shut
// once the delete is sent and answered.
func (d deletion) card(left time.Duration) screens.Trash {
	what := d.names[0]
	if len(d.names) > 1 {
		what = plural(len(d.names), "topic")
	}
	records, _, _ := d.size()
	switch {
	case left > 0:
		secs := int((left + time.Second - 1) / time.Second)
		return screens.Trash{Title: fmt.Sprintf("Deleting %s in %ds", what, secs),
			Detail: screens.Num(float64(records)) + " records · nothing sent yet",
			Undo:   float64(left) / float64(undoWindow), Secs: secs, Hint: "u or esc to undo · enter to delete now"}
	case d.results == nil && d.err == nil:
		return screens.Trash{Title: "Deleting " + what + "…", Detail: "sent; waiting for the cluster to let go", Hint: "too late to undo"}
	}
	if f := d.failed(); len(f) > 0 {
		gone := len(d.names) - len(f)
		t := screens.Trash{Shut: gone > 0, Title: fmt.Sprintf("%d of %s deleted", gone, plural(len(d.names), "topic")), Detail: d.outcome().Error()}
		if len(d.names) == 1 {
			t.Title = "Not deleted"
		}
		return t
	}
	title, detail := "Topic deleted", fmt.Sprintf("%q and its %s records are gone.", what, screens.Num(float64(records)))
	if len(d.names) > 1 {
		title, detail = plural(len(d.names), "topic")+" deleted", fmt.Sprintf("%s and their %s records are gone.", andList(d.names), screens.Num(float64(records)))
	}
	return screens.Trash{Shut: true, Title: title, Detail: detail}
}

// andList is "a", "a and b", "a, b and c".
func andList(xs []string) string {
	switch len(xs) {
	case 0:
		return ""
	case 1:
		return xs[0]
	}
	return strings.Join(xs[:len(xs)-1], ", ") + " and " + xs[len(xs)-1]
}

// dialog runs the modal and the undo window, then leaves the closed bin
// in the scrollback.
func (d deletion) dialog(cmd *cobra.Command, themeName string) error {
	th, err := theme.Get(themeName)
	if err != nil {
		th, _ = theme.Get(theme.Default)
	}
	k := kit.New(th, os.Getenv("NO_COLOR") == "")
	m := deleteModel{d: d, k: k, bg: th.BG, now: time.Now}
	final, err := tea.NewProgram(m, tea.WithContext(cmd.Context())).Run()
	if err != nil {
		return err
	}
	fm := final.(deleteModel)
	if fm.phase != phaseDone {
		if fm.phase == phaseGrace {
			return cancelled("Undone: nothing was deleted.")
		}
		return cancelled("Cancelled: nothing was deleted.")
	}
	surf := detect(cmd.OutOrStdout(), themeName)
	surf.print(fm.d.card(0).Render(surf.kit, surf.width))
	return fm.d.outcome()
}

// Dialog phases.
const (
	phaseConfirm = iota
	phaseGrace
	phaseSending
	phaseDone
)

// deleteModel is the modal over the dimmed topic, then the trash lid.
type deleteModel struct {
	d      deletion
	k      kit.Kit
	bg     string
	w, h   int
	phase  int
	typed  string
	focus  int
	frame  int
	armed  time.Time // when the undo window started
	now    func() time.Time
	closed bool // esc or ctrl-c before the delete was sent
}

type (
	deleteTick struct{}
	deletedMsg struct{ d deletion }
)

func tick() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(time.Time) tea.Msg { return deleteTick{} })
}

func (m deleteModel) Init() tea.Cmd { return nil }

func (m deleteModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
	case tea.PasteMsg:
		if m.phase == phaseConfirm && m.focus == screens.FocusInput {
			m.typed += strings.TrimSpace(msg.Content)
		}
	case deleteTick:
		m.frame++
		switch {
		case m.phase == phaseGrace && m.now().Sub(m.armed) >= undoWindow:
			return m.sendNow()
		case m.phase == phaseGrace || m.phase == phaseSending:
			return m, tick()
		}
	case deletedMsg:
		m.d, m.phase = msg.d, phaseDone
		return m, tea.Quit
	case tea.KeyPressMsg:
		return m.key(msg)
	}
	return m, nil
}

func (m deleteModel) key(k tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	key := k.String()
	switch m.phase {
	case phaseSending, phaseDone:
		return m, nil // too late: the delete is out
	case phaseGrace:
		switch key {
		case "u", "esc", "ctrl+c":
			m.closed = true
			return m, tea.Quit
		case "enter":
			return m.sendNow()
		}
		return m, nil
	}
	switch key {
	case "ctrl+c", "esc":
		m.closed = true
		return m, tea.Quit
	case "tab":
		m.focus = (m.focus + 1) % 3
	case "shift+tab":
		m.focus = (m.focus + 2) % 3
	case "enter":
		switch {
		case m.focus == screens.FocusCancel:
			m.closed = true
			return m, tea.Quit
		case m.typed == m.d.confirmText():
			m.phase, m.armed = phaseGrace, m.now()
			return m, tick()
		default:
			m.focus = screens.FocusInput
		}
	case "backspace":
		if m.focus == screens.FocusInput && m.typed != "" {
			r := []rune(m.typed)
			m.typed = string(r[:len(r)-1])
		}
	default:
		if m.focus == screens.FocusInput {
			m.typed += k.Text
		}
	}
	return m, nil
}

// sendNow ends the undo window and sends the delete.
func (m deleteModel) sendNow() (tea.Model, tea.Cmd) {
	m.phase = phaseSending
	d := m.d
	return m, tea.Batch(tick(), func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		d.send(ctx)
		return deletedMsg{d}
	})
}

// modal is the dialog, filled with what goes with the topics.
func (m deleteModel) modal() screens.Modal {
	k := m.k
	verb, their := "deletes ", "its "
	if len(m.d.names) > 1 {
		their = "their "
	}
	body := []string{
		k.S(kit.Muted, "This permanently "+verb) + k.S(kit.FG, andList(m.d.names)),
		k.S(kit.Muted, "with "+their+m.d.sizeText()+"."),
	}
	if n := len(m.d.readers); n > 0 {
		body = append(body, k.S(kit.Warn, andList(m.d.readers)+" still "+map[bool]string{true: "reads", false: "read"}[n == 1]+" it."))
	}
	body = append(body, k.S(kit.Err, "This cannot be undone once sent."))
	title := "Delete topic?"
	if len(m.d.names) > 1 {
		title = fmt.Sprintf("Delete %d topics?", len(m.d.names))
	}
	return screens.Modal{Title: title, Body: body, Confirm: m.d.confirmText(), Typed: m.typed, Focus: m.focus,
		Action: map[bool]string{true: "Delete topic", false: "Delete topics"}[len(m.d.names) == 1], Frame: m.frame}
}

func (m deleteModel) View() tea.View {
	w, h := m.w, m.h
	if w == 0 {
		w, h = 100, 30
	}
	var lines []string
	switch m.phase {
	case phaseConfirm:
		page := screens.Dim(m.k, m.d.topics[0].screen().Render(m.k, min(w, 90)))
		lines = screens.Overlay(page, m.modal().Render(m.k, min(w-4, 62)), min(w, 90), max(h-1, len(page)))
	default:
		left := time.Duration(0)
		if m.phase == phaseGrace {
			left = max(0, undoWindow-m.now().Sub(m.armed))
		}
		card := m.d.card(left)
		if m.phase == phaseSending {
			card.Title = screens.Spin(m.frame) + " " + card.Title
		}
		lines = kit.Indent(max(0, (w-56)/2), card.Render(m.k, w))
		lines = append(make([]string, max(0, (h-len(lines))/2)), lines...)
	}
	v := tea.NewView(kit.Join(kit.Clip(w, lines)))
	if m.closed {
		v = tea.NewView("")
	}
	v.AltScreen = true
	if m.bg != "" {
		v.BackgroundColor = theme.C(m.bg)
	}
	v.WindowTitle = "kaas topics delete"
	return v
}
