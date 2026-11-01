package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/configs"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

func newConfigEditCmd(brokers, themeName *string) *cobra.Command {
	var (
		output      string
		broker      int32
		sets, unset []string
		dryRun      bool
	)
	cmd := &cobra.Command{
		Use:   "edit (<topic> | --broker <id>)",
		Short: "Edit a topic's or a broker's configs in a vi-style buffer, with a diff before applying",
		Long: `Open a topic's configs, or a broker's with --broker, as a file to edit:
one config a line, "key = value", under the same sections kaas config
shows. A line without # is set on the topic (or dynamically on the
broker); comment a line out or delete it to go back to the default.
Durations take 3d, 12h or 500ms and sizes 1GiB, as well as Kafka's raw
numbers.

The editor is modal, as vi: i, a, o and O insert, Esc goes back to
normal mode, where h j k l, w b, 0 $, gg G move, x, dd and D delete, cw
changes a word, and u undoes. :w checks every change with the cluster
first and shows them; y applies, n keeps editing. :wq applies and
quits, :q! leaves without applying, :e! starts over from the cluster,
and :%s/old/new/g replaces text.

For a script, --set key=value and --unset key change configs without
the editor, and --dry-run only checks them.`,
		Example: `  # Edit a topic's configs
  kaas config edit orders
  # A broker's dynamic configs
  kaas config edit --broker 1
  # In a script: check, then apply
  kaas config edit orders --set retention.ms=3d --unset cleanup.policy --dry-run
  kaas config edit orders --set retention.ms=3d -o json`,
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
			want := map[string]*string{} // nil: unset
			for _, s := range sets {
				k, v, ok := strings.Cut(s, "=")
				if !ok || strings.TrimSpace(k) == "" {
					return usageError(cmd, "set", s, "want key=value")
				}
				v = strings.TrimSpace(v)
				want[strings.TrimSpace(k)] = &v
			}
			for _, k := range unset {
				if _, both := want[k]; both {
					return usageError(cmd, "unset", k, "also given to --set")
				}
				want[k] = nil
			}
			scripted := len(want) > 0
			surf := detect(cmd.OutOrStdout(), *themeName)
			if !scripted && (!surf.rich || !answerable(cmd.InOrStdin())) {
				return usageError(cmd, "set", "", "the editor needs a terminal: use --set key=value or --unset key")
			}
			if dryRun && !scripted {
				return usageError(cmd, "dry-run", "", "for --set and --unset; in the editor, :w checks before anything is applied")
			}
			if _, err := connect(cmd, *brokers, *themeName); err != nil {
				return err
			}
			cl, err := kgo.NewClient(kgo.SeedBrokers(splitSeeds(*brokers)...))
			if err != nil {
				return err
			}
			defer cl.Close()
			v := configView{}
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
			e := newConfigEditor(v, cl, surf.kit)
			if scripted {
				return e.script(cmd, want, dryRun, output)
			}
			e.w, e.h = surf.width, surf.height
			_, err = tea.NewProgram(e, tea.WithContext(cmd.Context())).Run()
			if cmd.Context().Err() != nil {
				return nil
			}
			return err
		},
	}
	fl := cmd.Flags()
	fl.Int32Var(&broker, "broker", 0, "a broker's dynamic configs, by node ID, instead of a topic's")
	fl.StringArrayVar(&sets, "set", nil, "set a config without the editor: key=value (repeatable)")
	fl.StringArrayVar(&unset, "unset", nil, "unset a config without the editor, back to its default (repeatable)")
	fl.BoolVar(&dryRun, "dry-run", false, "with --set or --unset: have the cluster check the change, apply nothing")
	fl.StringVarP(&output, "output", "o", "text", "output format: text|json")
	return cmd
}

// configEditor is kaas config edit: a config file in a modal buffer.
type configEditor struct {
	v    configView
	adm  *kadm.Client
	cl   *kgo.Client
	k    kit.Kit
	w, h int

	b       buffer
	orig    string // the file as the cluster has it
	mode    string
	command string
	msg     string
	msgRole kit.Role
	pending string // a normal-mode key waiting for its second: d, c, g
	last    string
	undo    []editSnap
	hl      string
	cursor  bool

	changes   []configs.Change // checked by the cluster, waiting for y
	busy      bool
	quitAfter bool
	quit      bool
}

type editSnap struct {
	lines     []string
	line, col int
}

type (
	editBlink   struct{}
	editChecked struct {
		cs  []configs.Change
		err error
	}
	editApplied struct {
		n   int
		es  []configs.Entry
		err error
	}
)

// level is the level the file sets: the topic's own, or the broker's.
func (v *configView) level() int {
	if v.broker != nil {
		return configs.LevelBroker
	}
	return configs.LevelTopic
}

// name is what the file is of, for messages: "orders", "broker 1".
func (v *configView) name() string {
	if v.broker != nil {
		return fmt.Sprintf("broker %d", *v.broker)
	}
	return v.topic
}

func (v *configView) file() string {
	title := "kaas config edit · topic " + v.topic
	if v.broker != nil {
		title = fmt.Sprintf("kaas config edit · broker %d", *v.broker)
	}
	return configs.File(title, v.entries, v.broker != nil, v.level(), v.rf)
}

func newConfigEditor(v configView, cl *kgo.Client, k kit.Kit) configEditor {
	text := strings.TrimSuffix(v.file(), "\n")
	e := configEditor{v: v, cl: cl, adm: kadm.NewClient(cl), k: k, b: newBuffer(text), orig: text, mode: screens.ModeNormal, cursor: true}
	// Start on the first set line, or the first config.
	for i, l := range e.b.lines {
		if t := strings.TrimSpace(l); t != "" && !strings.HasPrefix(t, "#") && !strings.HasPrefix(t, "[") {
			e.b.line = i
			break
		}
	}
	e.msg, e.msgRole = fmt.Sprintf("%q %dL, %dB", e.fileName(), len(e.b.lines), len(text)), kit.Muted
	return e
}

func (e configEditor) fileName() string {
	if e.v.broker != nil {
		return fmt.Sprintf("broker-%d.properties", *e.v.broker)
	}
	return e.v.topic + ".properties"
}

func blinkEvery() tea.Cmd {
	return tea.Tick(500*time.Millisecond, func(time.Time) tea.Msg { return editBlink{} })
}

func (e configEditor) Init() tea.Cmd { return blinkEvery() }

func (e configEditor) dirty() bool { return e.b.text() != e.orig }

// save keeps the buffer for u.
func (e *configEditor) save() {
	e.undo = append(e.undo, editSnap{lines: append([]string(nil), e.b.lines...), line: e.b.line, col: e.b.col})
	if len(e.undo) > 200 {
		e.undo = e.undo[1:]
	}
}

// clampNormal keeps the cursor on a character, as normal mode does.
func (e *configEditor) clampNormal() {
	n := len([]rune(e.b.lines[e.b.line]))
	e.b.col = min(e.b.col, max(0, n-1))
}

func (e configEditor) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		e.w, e.h = msg.Width, msg.Height
	case editBlink:
		e.cursor = !e.cursor
		return e, blinkEvery()
	case editChecked:
		e.busy = false
		switch {
		case msg.err != nil:
			e.msg, e.msgRole = "the cluster said no: "+msg.err.Error(), kit.Err
			e.quitAfter = false
		default:
			e.changes = msg.cs
			e.msg = ""
		}
	case editApplied:
		e.busy = false
		if msg.err != nil {
			e.msg, e.msgRole = "not applied: "+msg.err.Error(), kit.Err
			e.quitAfter = false
			return e, nil
		}
		e.v.entries = msg.es
		line := e.b.line
		text := strings.TrimSuffix(e.v.file(), "\n")
		e.b, e.orig, e.undo = newBuffer(text), text, nil // undo stops at what the cluster has
		e.b.line = min(line, len(e.b.lines)-1)
		e.clampNormal()
		e.msg, e.msgRole = fmt.Sprintf("%s applied to %s", plural(msg.n, "change"), e.v.name()), kit.OK
		if e.quitAfter {
			e.quit = true
			return e, tea.Quit
		}
	case tea.KeyPressMsg:
		e.cursor = true
		if msg.String() == "ctrl+c" {
			e.quit = true
			return e, tea.Quit
		}
		if e.busy {
			return e, nil
		}
		if e.changes != nil {
			return e.confirmKey(msg)
		}
		switch e.mode {
		case screens.ModeInsert:
			e.insertKey(msg)
		case screens.ModeCommand:
			return e.commandKey(msg)
		default:
			return e.normalKey(msg)
		}
	}
	return e, nil
}

// confirmKey answers the change panel.
func (e configEditor) confirmKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "y", "Y", "enter":
		cs := e.changes
		e.changes, e.busy = nil, true
		e.msg, e.msgRole = "applying "+plural(len(cs), "change")+"…", kit.Notice
		v, adm, cl := e.v, e.adm, e.cl
		return e, func() tea.Msg {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := configs.Apply(ctx, adm, v.topic, v.broker, cs, false); err != nil {
				return editApplied{err: err}
			}
			es, err := configs.Describe(ctx, cl, v.topic, v.broker)
			return editApplied{n: len(cs), es: es, err: err}
		}
	case "n", "N", "esc", "q":
		e.changes, e.quitAfter = nil, false
		e.msg, e.msgRole = "not applied: still editing", kit.Muted
	}
	return e, nil
}

func (e *configEditor) insertKey(msg tea.KeyPressMsg) {
	switch msg.String() {
	case "esc", "ctrl+[":
		e.mode = screens.ModeNormal
		e.b.col = max(0, e.b.col-1)
		e.clampNormal()
	case "enter":
		e.b.split("")
	case "backspace":
		e.b.backspace()
	case "delete":
		e.b.del()
	case "left":
		e.b.col = max(0, e.b.col-1)
	case "right":
		e.b.col = min(e.b.col+1, len([]rune(e.b.lines[e.b.line])))
	case "up":
		e.b.move(-1, 0)
	case "down":
		e.b.move(1, 0)
	case "tab":
		e.b.insert("  ")
	default:
		if msg.Text != "" {
			e.b.insert(msg.Text)
		}
	}
}

// wordStart is the column of the next (dir 1) or previous (dir -1) word
// on the line, as w and b move.
func wordStart(line string, col, dir int) int {
	rs := []rune(line)
	class := func(i int) int {
		switch r := rs[i]; {
		case unicode.IsSpace(r):
			return 0
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_':
			return 1
		}
		return 2
	}
	if len(rs) == 0 {
		return 0
	}
	i := min(col, len(rs)-1)
	if dir > 0 {
		c := class(i)
		for i < len(rs) && class(i) == c && c != 0 {
			i++
		}
		for i < len(rs) && class(i) == 0 {
			i++
		}
		return min(i, len(rs)-1)
	}
	for i > 0 && class(i-1) == 0 {
		i--
	}
	if i == 0 {
		return 0
	}
	c := class(i - 1)
	for i > 0 && class(i-1) == c {
		i--
	}
	return i
}

// wordEnd is where the word at col ends, exclusive, as cw changes.
func wordEnd(line string, col int) int {
	rs := []rune(line)
	if col >= len(rs) {
		return col
	}
	word := func(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' }
	i := col
	if word(rs[i]) {
		for i < len(rs) && word(rs[i]) {
			i++
		}
		return i
	}
	if unicode.IsSpace(rs[i]) {
		for i < len(rs) && unicode.IsSpace(rs[i]) {
			i++
		}
		return i
	}
	for i < len(rs) && !word(rs[i]) && !unicode.IsSpace(rs[i]) {
		i++
	}
	return i
}

func (e configEditor) normalKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	key := msg.String()
	if msg.Text != "" && len(msg.Text) == 1 {
		key = msg.Text
	}
	b := &e.b
	line := func() []rune { return []rune(b.lines[b.line]) }
	e.msg = ""
	if e.pending != "" {
		p := e.pending
		e.pending = ""
		e.last = p + key
		switch p + key {
		case "dd":
			e.save()
			if len(b.lines) == 1 {
				b.lines[0] = ""
			} else {
				b.lines = append(b.lines[:b.line], b.lines[b.line+1:]...)
				b.line = min(b.line, len(b.lines)-1)
			}
			b.col = 0
		case "cw", "ce":
			e.save()
			rs := line()
			end := wordEnd(string(rs), b.col)
			b.lines[b.line] = string(rs[:b.col]) + string(rs[end:])
			e.mode = screens.ModeInsert
		case "cc":
			e.save()
			b.lines[b.line], b.col = "", 0
			e.mode = screens.ModeInsert
		case "gg":
			b.line, b.col = 0, 0
		default:
			e.last = ""
		}
		e.clampNormal()
		return e, nil
	}
	e.last = ""
	switch key {
	case "h", "left":
		b.col = max(0, b.col-1)
	case "l", "right":
		b.col++
	case "j", "down":
		b.move(1, 0)
	case "k", "up":
		b.move(-1, 0)
	case "w":
		b.col = wordStart(b.lines[b.line], b.col, 1)
	case "b":
		b.col = wordStart(b.lines[b.line], b.col, -1)
	case "0", "home":
		b.col = 0
	case "^":
		b.col = len(line()) - len([]rune(strings.TrimLeft(b.lines[b.line], " ")))
	case "$", "end":
		b.col = len(line())
	case "G":
		b.line = len(b.lines) - 1
	case "d", "c", "g":
		e.pending = key
		e.last = key
		return e, nil
	case "x", "delete":
		if rs := line(); len(rs) > 0 {
			e.save()
			b.lines[b.line] = string(rs[:b.col]) + string(rs[min(len(rs), b.col+1):])
		}
	case "D":
		e.save()
		b.lines[b.line] = string(line()[:b.col])
	case "i":
		e.save()
		e.mode = screens.ModeInsert
	case "a":
		e.save()
		e.mode = screens.ModeInsert
		b.col = min(b.col+1, len(line()))
	case "I":
		e.save()
		e.mode = screens.ModeInsert
		b.col = 0
	case "A":
		e.save()
		e.mode = screens.ModeInsert
		b.col = len(line())
	case "o":
		e.save()
		b.lines = append(b.lines[:b.line+1], append([]string{""}, b.lines[b.line+1:]...)...)
		b.line, b.col = b.line+1, 0
		e.mode = screens.ModeInsert
	case "O":
		e.save()
		b.lines = append(b.lines[:b.line], append([]string{""}, b.lines[b.line:]...)...)
		b.col = 0
		e.mode = screens.ModeInsert
	case "u":
		if n := len(e.undo); n > 0 {
			s := e.undo[n-1]
			e.undo = e.undo[:n-1]
			b.lines, b.line, b.col = s.lines, s.line, s.col
			e.msg, e.msgRole = "1 change undone", kit.Muted
		} else {
			e.msg, e.msgRole = "already at the oldest change", kit.Muted
		}
	case ":":
		e.mode, e.command = screens.ModeCommand, ":"
		return e, nil
	case "esc":
		e.hl = ""
	}
	if e.mode == screens.ModeNormal {
		e.clampNormal()
	}
	if key != "esc" && len(key) <= 2 {
		e.last = key
	}
	return e, nil
}

func (e configEditor) commandKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "ctrl+[":
		e.mode, e.command = screens.ModeNormal, ""
		return e, nil
	case "backspace":
		if len(e.command) <= 1 {
			e.mode, e.command = screens.ModeNormal, ""
			return e, nil
		}
		rs := []rune(e.command)
		e.command = string(rs[:len(rs)-1])
		return e, nil
	case "enter":
		c := strings.TrimSpace(strings.TrimPrefix(e.command, ":"))
		e.mode, e.command = screens.ModeNormal, ""
		return e.run(c)
	}
	if msg.Text != "" {
		e.command += msg.Text
	}
	return e, nil
}

// run does a : command.
func (e configEditor) run(c string) (tea.Model, tea.Cmd) {
	switch {
	case c == "":
	case c == "w":
		return e.check(false)
	case c == "wq" || c == "x":
		return e.check(true)
	case c == "q":
		if e.dirty() {
			e.msg, e.msgRole = "changes not applied: :w applies them, :q! leaves without", kit.Err
			return e, nil
		}
		e.quit = true
		return e, tea.Quit
	case c == "q!":
		e.quit = true
		return e, tea.Quit
	case c == "e!":
		text := e.orig
		e.save()
		e.b = newBuffer(text)
		e.msg, e.msgRole = "back to what "+e.v.name()+" has", kit.Muted
	case strings.HasPrefix(c, "%s/") || strings.HasPrefix(c, "s/"):
		e.substitute(c)
	default:
		if n, err := strconv.Atoi(c); err == nil {
			e.b.line = min(max(0, n-1), len(e.b.lines)-1)
			e.b.col = 0
			return e, nil
		}
		e.msg, e.msgRole = "not a command: "+c, kit.Err
	}
	e.clampNormal()
	return e, nil
}

// substitute does :s/old/new/ on the line or :%s on every line, every
// match with g, the first a line without. old is text, not a pattern.
func (e *configEditor) substitute(c string) {
	all := strings.HasPrefix(c, "%")
	parts := strings.Split(strings.TrimPrefix(strings.TrimPrefix(c, "%"), "s/"), "/")
	if len(parts) < 2 || parts[0] == "" {
		e.msg, e.msgRole = "want :%s/old/new/g", kit.Err
		return
	}
	old, repl := parts[0], parts[1]
	global := len(parts) > 2 && strings.Contains(parts[2], "g")
	e.save()
	subs, lines := 0, 0
	for i := range e.b.lines {
		if !all && i != e.b.line {
			continue
		}
		n := strings.Count(e.b.lines[i], old)
		if n == 0 {
			continue
		}
		if !global {
			n = 1
		}
		e.b.lines[i] = strings.Replace(e.b.lines[i], old, repl, n)
		subs, lines = subs+n, lines+1
	}
	if subs == 0 {
		e.undo = e.undo[:len(e.undo)-1]
		e.msg, e.msgRole = "pattern not found: "+old, kit.Err
		return
	}
	e.hl = repl
	e.msg, e.msgRole = fmt.Sprintf("%s on %s", plural(subs, "substitution"), plural(lines, "line")), kit.Muted
}

// check reads the buffer, diffs it with the cluster's configs, and has
// the cluster check the change; the panel then asks to apply it.
func (e configEditor) check(quitAfter bool) (tea.Model, tea.Cmd) {
	want, err := configs.ParseFile(e.b.text())
	if err != nil {
		var le configs.LineError
		if errors.As(err, &le) {
			e.b.line, e.b.col = le.Line-1, 0
		}
		e.msg, e.msgRole = err.Error(), kit.Err
		return e, nil
	}
	cs, err := configs.Diff(e.v.entries, e.v.level(), want)
	if err != nil {
		e.msg, e.msgRole = err.Error(), kit.Err
		for i, l := range e.b.lines { // put the cursor on the line named
			if k, _, ok := strings.Cut(strings.TrimSpace(l), "="); ok && strings.Contains(err.Error(), strings.TrimSpace(k)+" ") && !strings.HasPrefix(l, "#") {
				e.b.line, e.b.col = i, 0
				break
			}
		}
		return e, nil
	}
	if len(cs) == 0 {
		e.msg, e.msgRole = "nothing to change: "+e.v.name()+" already has this", kit.Muted
		if quitAfter {
			e.quit = true
			return e, tea.Quit
		}
		return e, nil
	}
	e.busy, e.quitAfter = true, quitAfter
	e.msg, e.msgRole = "checking "+plural(len(cs), "change")+" with the cluster…", kit.Notice
	v, adm := e.v, e.adm
	return e, func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return editChecked{cs: cs, err: configs.Apply(ctx, adm, v.topic, v.broker, cs, true)}
	}
}

// diffLines are changes for the panel, readable.
func (v *configView) diffLines(cs []configs.Change) []screens.ModalDiff {
	read := func(key, val string) string {
		if val == "" {
			return `""`
		}
		return configs.Readable(configs.Entry{Key: key, Value: val}, v.rf)
	}
	out := make([]screens.ModalDiff, len(cs))
	for i, c := range cs {
		d := screens.ModalDiff{Sign: "~", Key: c.Key, From: read(c.Key, c.From), To: read(c.Key, c.To)}
		switch {
		case c.Op == configs.OpUnset:
			d.Sign = "-"
			if c.To == "" {
				d.To = "default"
			}
		case c.From == "":
			d.Sign = "+"
		}
		out[i] = d
	}
	return out
}

func (e configEditor) screen() screens.ModalEditor {
	m := screens.ModalEditor{
		File: e.fileName(), Lines: e.b.lines, Row: e.b.line, Col: e.b.col, Mode: e.mode,
		Command: e.command, Msg: e.msg, MsgRole: e.msgRole, Dirty: e.dirty(), Cursor: e.cursor,
		Highlight: e.hl, Last: e.last,
	}
	if e.changes != nil {
		m.Diff = e.v.diffLines(e.changes)
		m.DiffTitle = fmt.Sprintf("%s to %s, checked by the cluster", plural(len(e.changes), "change"), e.v.name())
		m.DiffAsk = "apply them? y applies · n keeps editing"
	}
	return m
}

func (e configEditor) View() tea.View {
	w, h := e.w, e.h
	if w == 0 {
		w = 100
	}
	if h == 0 {
		h = 30
	}
	if e.quit {
		// Leave the last message, not the whole buffer, on the terminal.
		if e.msg == "" {
			return tea.NewView("")
		}
		return tea.NewView(e.k.S(e.msgRole, e.msg))
	}
	return tea.NewView(kit.Join(e.screen().Render(e.k, w, max(8, h-1))))
}

type configChangeJSON struct {
	Key  string  `json:"key"`
	Op   string  `json:"op"` // set or unset
	From *string `json:"from"`
	To   *string `json:"to"` // for an unset, the value it falls back to, when known
}

// script makes --set and --unset changes without the editor.
func (e configEditor) script(cmd *cobra.Command, want map[string]*string, dryRun bool, output string) error {
	// Start from what is set now, then apply the flags over it.
	file := map[string]string{}
	for _, en := range e.v.entries {
		if en.Level == e.v.level() && !en.Sensitive {
			file[en.Key] = en.Value
		}
	}
	for k, v := range want {
		if v == nil {
			delete(file, k)
		} else {
			file[k] = *v
		}
	}
	cs, err := configs.Diff(e.v.entries, e.v.level(), file)
	if err != nil {
		return usageError(cmd, "set", "", err.Error())
	}
	ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
	defer cancel()
	if err := configs.Apply(ctx, e.adm, e.v.topic, e.v.broker, cs, true); err != nil {
		return fmt.Errorf("the cluster said no: %w", err)
	}
	if !dryRun {
		if err := configs.Apply(ctx, e.adm, e.v.topic, e.v.broker, cs, false); err != nil {
			return err
		}
	}
	out := cmd.OutOrStdout()
	if output == "json" {
		js := struct {
			Applied bool               `json:"applied"`
			Changes []configChangeJSON `json:"changes"`
		}{Applied: !dryRun && len(cs) > 0, Changes: []configChangeJSON{}}
		for _, c := range cs {
			j := configChangeJSON{Key: c.Key, Op: "set"}
			if c.Op == configs.OpUnset {
				j.Op = "unset"
			}
			if c.From != "" {
				f := c.From
				j.From = &f
			}
			if c.To != "" {
				t := c.To
				j.To = &t
			}
			js.Changes = append(js.Changes, j)
		}
		return json.NewEncoder(out).Encode(js)
	}
	k := e.k
	for _, d := range e.v.diffLines(cs) {
		role := map[string]kit.Role{"+": kit.OK, "~": kit.Notice, "-": kit.Err}[d.Sign]
		line := k.S(role, d.Sign) + " " + k.S(kit.Accent, d.Key) + " "
		switch d.Sign {
		case "+":
			line += k.S(kit.OK, d.To)
		case "-":
			line += k.S(kit.Muted, d.From) + k.S(kit.Err, " → "+d.To+" (unset)")
		default:
			line += k.S(kit.Muted, d.From) + k.S(kit.Notice, " → "+d.To)
		}
		fmt.Fprintln(out, line)
	}
	switch {
	case len(cs) == 0:
		fmt.Fprintln(out, k.S(kit.Muted, "nothing to change: "+e.v.name()+" already has this"))
	case dryRun:
		fmt.Fprintln(out, k.S(kit.OK, "✓ the cluster accepts "+plural(len(cs), "change")+"; nothing applied (--dry-run)"))
	default:
		fmt.Fprintln(out, k.S(kit.OK, "✓ "+plural(len(cs), "change")+" applied to "+e.v.name()))
	}
	return nil
}
