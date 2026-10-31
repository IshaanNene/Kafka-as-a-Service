package main

import (
	"strings"

	"github.com/spf13/cobra"

	"github.com/IshaanNene/Kafka-as-a-Service/core/probe"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

// connect probes the seeds before a command opens a session. On a terminal
// the probe draws as a boot log on stderr, erased once the cluster answers;
// elsewhere it is silent. A failed probe becomes an unavailable error that
// says which layer broke.
func connect(cmd *cobra.Command, brokers, themeName string) (probe.Result, error) {
	seeds := splitSeeds(brokers)
	s := detect(cmd.ErrOrStderr(), themeName)
	drawn := 0
	draw := func(b []string) {
		b = kit.Clip(s.width, b) // a wrapped line would throw the erase count off
		s.print(b)
		drawn += len(b)
	}
	var o probe.Options
	if s.rich {
		draw(screens.BootStart(s.kit, seeds))
		o.Report = func(st probe.Step) { draw(screens.BootStep(s.kit, st)) }
	}
	r := probe.Run(cmd.Context(), seeds, o)
	if err := cmd.Context().Err(); err != nil {
		return r, err
	}
	if r.OK() {
		if s.rich {
			s.erase(drawn)
		}
		return r, nil
	}
	if s.rich {
		draw(screens.BootDone(s.kit, r))
	}
	return r, unavailableError(r)
}

func splitSeeds(brokers string) []string {
	var out []string
	for _, b := range strings.Split(brokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
