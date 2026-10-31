package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/IshaanNene/Kafka-as-a-Service/internal/version"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

// installHelp draws help as the command palette with an examples session on
// terminals, and leaves cobra's plain help for everything else.
func installHelp(root *cobra.Command, themeName *string) {
	plain := root.HelpFunc()
	root.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		s := detect(cmd.OutOrStdout(), *themeName)
		if !s.rich {
			plain(cmd, args)
			return
		}
		s.print(helpFor(cmd).Render(s.kit, s.width))
	})
}

// helpFor reads a command's usage, flags and examples out of cobra.
func helpFor(cmd *cobra.Command) screens.Help {
	h := screens.Help{
		Path: cmd.CommandPath(), Version: version.Version, Short: cmd.Short, Long: cmd.Long,
		Usage: cmd.UseLine(), Examples: examples(cmd.Example), Prompt: prompt(),
	}
	for _, c := range cmd.Commands() {
		if c.IsAvailableCommand() {
			h.Commands = append(h.Commands, screens.Item{Name: c.Name(), Desc: c.Short, Hint: c.Annotations["hint"]})
		}
	}
	h.Flags = flagItems(cmd.LocalFlags())
	h.Global = flagItems(cmd.InheritedFlags())
	return h
}

func flagItems(fs *pflag.FlagSet) []screens.Item {
	var out []screens.Item
	fs.VisitAll(func(f *pflag.Flag) {
		if f.Hidden || f.Name == "help" {
			return
		}
		name := "    --" + f.Name
		if f.Shorthand != "" {
			name = "-" + f.Shorthand + ", --" + f.Name
		}
		varname, usage := pflag.UnquoteUsage(f)
		if varname != "" {
			name += " " + varname
		}
		hint := f.DefValue
		switch hint {
		case "", "[]", "false", "0", "0s":
			hint = ""
		}
		out = append(out, screens.Item{Name: name, Desc: usage, Hint: hint})
	})
	return out
}

// examples parses cobra's Example text: "# comment" lines annotate the
// command that follows them.
func examples(text string) []screens.Example {
	var out []screens.Example
	comment := ""
	for _, l := range strings.Split(text, "\n") {
		l = strings.TrimSpace(l)
		switch {
		case l == "":
		case strings.HasPrefix(l, "#"):
			comment = strings.TrimSpace(strings.TrimPrefix(l, "#"))
		default:
			out = append(out, screens.Example{Comment: comment, Cmd: strings.TrimPrefix(l, "$ ")})
			comment = ""
		}
	}
	return out
}

// prompt is the examples window title: who and where you are.
func prompt() string {
	user := os.Getenv("USER")
	host, _ := os.Hostname()
	host, _, _ = strings.Cut(host, ".")
	dir, _ := os.Getwd()
	if home, err := os.UserHomeDir(); err == nil {
		if rel, err := filepath.Rel(home, dir); err == nil && !strings.HasPrefix(rel, "..") {
			dir = filepath.Join("~", rel)
		}
	}
	return user + "@" + host + ": " + dir
}
