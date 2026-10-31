package main

import (
	"context"
	"os"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/tui"
)

// runTUI opens the terminal UI against the cluster at brokers.
func runTUI(cmd *cobra.Command, brokers, themeName string) error {
	th, err := theme.Get(themeName)
	if err != nil {
		return err
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		return err
	}
	defer cl.Close()

	e := engine.New(cl, engine.Config{})
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()
	go func() { _ = e.Run(ctx) }()

	m := tui.New(e, th, tui.Options{ReducedMotion: os.Getenv("KAAS_REDUCED_MOTION") == "1"})
	_, err = tea.NewProgram(m, tea.WithContext(ctx)).Run()
	return err
}

func newTUICmd(brokers, themeName *string) *cobra.Command {
	return &cobra.Command{
		Use:   "tui",
		Short: "Open the terminal UI (also what plain `kaas` does)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTUI(cmd, *brokers, *themeName)
		},
	}
}
