package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/IshaanNene/Kafka-as-a-Service/core/fuzzy"
	"github.com/IshaanNene/Kafka-as-a-Service/core/probe"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

// Exit codes are part of the CLI contract: scripts branch on them.
const (
	exitOK          = 0
	exitFailure     = 1 // anything unclassified
	exitUsage       = 2 // bad flags or arguments
	exitNotFound    = 3 // a named command, topic or group does not exist
	exitUnavailable = 4 // the cluster could not be reached
	exitInterrupted = 130
)

// cliError is an error kaas knows how to explain. Every one has a stable
// code and exit status, a plain message with hints for pipes, the same as
// JSON for -o json, and a full screen for terminals.
type cliError struct {
	Code    string   `json:"code"`
	Exit    int      `json:"exit"`
	Message string   `json:"message"`
	Hints   []string `json:"hints,omitempty"`
	Detail  any      `json:"detail,omitempty"`

	screen func(k kit.Kit, width int) []string
}

func (e *cliError) Error() string { return e.Message }

// report prints err for whoever is reading stderr and returns the exit code.
func report(stderr surface, err error, jsonOut bool) int {
	if err == nil {
		return exitOK
	}
	if errors.Is(err, context.Canceled) {
		return exitInterrupted
	}
	var ce *cliError
	if !errors.As(err, &ce) {
		ce = internalError(err)
	}
	switch {
	case jsonOut:
		enc := json.NewEncoder(stderr.w)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(map[string]*cliError{"error": ce})
	case stderr.rich && ce.screen != nil:
		stderr.print(ce.screen(stderr.kit, stderr.width))
	default:
		fmt.Fprintf(stderr.w, "kaas: %s\n", ce.Message)
		for _, h := range ce.Hints {
			fmt.Fprintf(stderr.w, "  hint: %s\n", h)
		}
	}
	return ce.Exit
}

func internalError(err error) *cliError {
	return &cliError{
		Code: "internal", Exit: exitFailure, Message: err.Error(),
		screen: func(k kit.Kit, w int) []string {
			return screens.Failure{
				Code: "500", Title: "Something went wrong",
				Subtitle: "kaas hit an error it does not have a better explanation for.",
				Facts:    [][2]string{{"error", err.Error()}},
				Actions:  []string{"kaas --help"},
			}.Render(k, w)
		},
	}
}

// unavailableError explains a failed probe, stage by stage.
func unavailableError(r probe.Result) *cliError {
	type step struct {
		Seed  string `json:"seed"`
		Stage string `json:"stage"`
		OK    bool   `json:"ok"`
		Error string `json:"error,omitempty"`
		Took  string `json:"took"`
	}
	var steps []step
	for _, s := range r.Steps {
		st := step{Seed: s.Seed, Stage: s.Stage.String(), OK: s.OK(), Took: s.Took.String()}
		if s.Err != nil {
			st.Error = s.Err.Error()
		}
		steps = append(steps, st)
	}
	return &cliError{
		Code: "unavailable", Exit: exitUnavailable,
		Message: fmt.Sprintf("cannot reach cluster at %s: %v", strings.Join(r.Seeds, ","), r.Err()),
		Hints: []string{
			"pass the right seeds with --brokers host:port or KAAS_BROKERS",
			"for the local dev cluster: task dev && task dev:forward",
		},
		Detail: map[string]any{"brokers": r.Seeds, "steps": steps},
		screen: func(k kit.Kit, w int) []string { return screens.Unreachable(r).Render(k, w) },
	}
}

// notFoundError is a name that does not exist, with the nearest real ones.
func notFoundError(kind, name, where string, candidates []string, describe func(string) string, actions ...string) *cliError {
	sugs := fuzzy.Suggest(name, candidates, 3)
	e := &cliError{
		Code: "not_found", Exit: exitNotFound,
		Message: fmt.Sprintf("unknown %s %q", kind, name),
		Detail:  map[string]any{"kind": kind, "name": name, "suggestions": sugs},
	}
	if len(sugs) > 0 {
		e.Hints = append(e.Hints, "did you mean "+strings.Join(sugs, ", ")+"?")
	}
	items := make([]screens.Item, len(sugs))
	for i, s := range sugs {
		items[i] = screens.Item{Name: s}
		if describe != nil {
			items[i].Desc = describe(s)
		}
	}
	e.screen = func(k kit.Kit, w int) []string {
		return screens.NotFound{Kind: kind, Name: name, Where: where, Suggestions: items, Actions: actions}.Render(k, w)
	}
	return e
}

// unknownCommand rejects stray arguments to a command that takes none, as a
// 404 against its subcommands.
func unknownCommand(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}
	var names []string
	for _, c := range cmd.Commands() {
		if c.IsAvailableCommand() {
			names = append(names, c.Name())
		}
	}
	describe := func(n string) string {
		c, _, _ := cmd.Find([]string{n})
		return c.Short
	}
	e := notFoundError("command", args[0], cmd.CommandPath(), names, describe, cmd.CommandPath()+" --help")
	if len(names) == 0 {
		e.Message = fmt.Sprintf("%s takes no arguments, got %q", cmd.CommandPath(), args[0])
	}
	return e
}

// usageError is a bad flag value or argument. It is drawn as a form: every
// flag the user set, the bad one marked, and why.
func usageError(cmd *cobra.Command, flag, value, msg string) *cliError {
	e := &cliError{
		Code: "usage", Exit: exitUsage,
		Message: fmt.Sprintf("--%s %q: %s", flag, value, msg),
		Hints:   []string{"see " + cmd.CommandPath() + " --help"},
		Detail:  map[string]string{"flag": flag, "value": value, "reason": msg},
	}
	if value == "" {
		e.Message = fmt.Sprintf("--%s: %s", flag, msg)
	}
	e.screen = func(k kit.Kit, w int) []string {
		var fields []screens.Field
		seen := false
		cmd.Flags().VisitAll(func(f *pflag.Flag) {
			switch {
			case f.Name == flag:
				seen = true
				fields = append(fields, screens.Field{Label: "--" + f.Name, Value: value, State: kit.Degraded, Focus: true, Msg: msg})
			case f.Changed:
				fields = append(fields, screens.Field{Label: "--" + f.Name, Value: f.Value.String(), State: kit.Good})
			}
		})
		if !seen {
			fields = append(fields, screens.Field{Label: "--" + flag, Value: value, State: kit.Degraded, Focus: true, Msg: msg})
		}
		return screens.Invalid{Title: cmd.CommandPath(), Fields: fields, Action: cmd.CommandPath() + " --help"}.Render(k, w)
	}
	return e
}

// flagError turns pflag's parse errors into usage errors.
func flagError(cmd *cobra.Command, err error) error {
	var (
		notExist *pflag.NotExistError
		invalid  *pflag.InvalidValueError
		required *pflag.ValueRequiredError
	)
	switch {
	case errors.As(err, &notExist):
		name := strings.TrimLeft(notExist.GetSpecifiedName(), "-")
		if name == "" {
			name = notExist.GetSpecifiedShortnames()
		}
		var names []string
		cmd.Flags().VisitAll(func(f *pflag.Flag) {
			if !f.Hidden {
				names = append(names, f.Name)
			}
		})
		msg := "no such flag"
		if s := fuzzy.Suggest(name, names, 1); len(s) > 0 {
			msg += "; did you mean --" + s[0] + "?"
		}
		return usageError(cmd, name, "", msg)
	case errors.As(err, &invalid):
		msg := invalid.Error()
		if u := errors.Unwrap(invalid); u != nil {
			msg = "not a valid " + invalid.GetFlag().Value.Type() + ": " + cause(u)
		}
		return usageError(cmd, invalid.GetFlag().Name, invalid.GetValue(), msg)
	case errors.As(err, &required):
		return usageError(cmd, required.GetFlag().Name, "", "needs a value")
	}
	return &cliError{Code: "usage", Exit: exitUsage, Message: err.Error(), Hints: []string{"see " + cmd.CommandPath() + " --help"}}
}

// cause trims strconv's "strconv.ParseFloat: parsing "x": " preamble.
func cause(err error) string {
	s := err.Error()
	if i := strings.LastIndex(s, ": "); i >= 0 {
		return s[i+2:]
	}
	return s
}

// oneOf checks an enum flag.
func oneOf(cmd *cobra.Command, flag, value string, allowed ...string) error {
	for _, a := range allowed {
		if value == a {
			return nil
		}
	}
	return usageError(cmd, flag, value, "must be one of "+strings.Join(allowed, ", "))
}

// wantsJSON reports whether the command that ran asked for JSON output, so
// its errors come back as JSON too.
func wantsJSON(cmd *cobra.Command) bool {
	if cmd == nil {
		return false
	}
	f := cmd.Flags().Lookup("output")
	return f != nil && f.Value.String() == "json"
}
