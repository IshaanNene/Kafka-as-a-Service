package main

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/spf13/cobra"

	"github.com/IshaanNene/Kafka-as-a-Service/ui/artview"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/asciipack"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

func newArtCmd(themeName *string) *cobra.Command {
	var (
		tint    bool
		frame   bool
		seconds float64
	)
	cmd := &cobra.Command{
		Use:   "art [piece]",
		Short: "Play an ascii-magic piece from the KaaS design system",
		Long: `Play one of the ascii-magic pieces baked into kaas. With no piece, list them.

Pieces are MIT licensed by ASCII Magic (ascii-magic.com). Press t to recolour
a piece in the current theme; any other key quits.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			out := cmd.OutOrStdout()
			if len(args) == 0 {
				w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
				for _, s := range asciipack.List() {
					p, err := asciipack.Load(s)
					if err != nil {
						return err
					}
					fmt.Fprintf(w, "%s\t%s\n", s, p.Note)
				}
				return w.Flush()
			}
			p, err := asciipack.Load(args[0])
			if err != nil {
				return err
			}
			th, err := theme.Get(*themeName)
			if err != nil {
				return err
			}
			if frame {
				o := asciipack.RenderOptions{}
				if tint {
					o.Recolor = th.Tint
				}
				// Downsample truecolor to what this terminal supports,
				// and strip it entirely for pipes and NO_COLOR.
				w := colorprofile.NewWriter(out, os.Environ())
				_, err := fmt.Fprintln(w, p.Render(p.FrameAt(time.Duration(seconds*float64(time.Second))), o))
				return err
			}
			m := artview.New(p, th, artview.Options{
				Tint:          tint,
				Limit:         time.Duration(seconds * float64(time.Second)),
				ReducedMotion: os.Getenv("KAAS_REDUCED_MOTION") == "1",
			})
			_, err = tea.NewProgram(m, tea.WithContext(cmd.Context()), tea.WithOutput(out)).Run()
			return err
		},
	}
	f := cmd.Flags()
	f.BoolVar(&tint, "tint", false, "recolour the piece in the theme's hue")
	f.BoolVar(&frame, "frame", false, "print one still frame and exit (at --seconds into the loop)")
	f.Float64Var(&seconds, "seconds", 0, "stop after this many seconds; with --frame, which moment to print")
	return cmd
}
