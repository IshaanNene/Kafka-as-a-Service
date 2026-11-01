// Command kaas is the Kafka As A Service CLI.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/internal/version"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	root := newRoot()
	cmd, err := root.ExecuteContextC(ctx)
	stop()
	theme, _ := root.PersistentFlags().GetString("theme")
	os.Exit(report(detect(os.Stderr, theme), err, wantsJSON(cmd)))
}

func newRoot() *cobra.Command {
	var brokers, themeName string
	root := &cobra.Command{
		Use:   "kaas",
		Short: "Kafka As A Service: CLI and TUI for Apache Kafka",
		Long:  "Kafka As A Service: one binary to watch, operate and eventually host Apache Kafka. Run with no command to open the terminal UI.",
		Example: `  # Open the terminal UI against the local dev cluster
  kaas
  # Point at another cluster
  kaas -b kafka-0.prod:9092,kafka-1.prod:9092
  # One-shot snapshot for scripts
  kaas snapshot -o json | jq '.Topics[].Name'`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          unknownCommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTUI(cmd, brokers, themeName)
		},
	}
	root.PersistentFlags().StringVarP(&brokers, "brokers", "b", envOr("KAAS_BROKERS", "localhost:9092"), "comma-separated seed brokers")
	root.PersistentFlags().StringVar(&themeName, "theme", envOr("KAAS_THEME", theme.Default), "colour theme: "+strings.Join(theme.Names(), ", "))
	root.SetFlagErrorFunc(flagError)
	root.CompletionOptions.HiddenDefaultCmd = true
	installHelp(root, &themeName)

	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  unknownCommand,
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "kaas %s (%s, %s)\n", version.Version, version.Commit, version.Date)
		},
	})

	var output, sortBy string
	snap := &cobra.Command{
		Use:   "snapshot",
		Short: "Print a one-shot cluster snapshot",
		Long:  "Fetch metadata and log offsets once and print the cluster: brokers, topics, partitions and under-replication.",
		Example: `  # Summary table, problems first
  kaas snapshot
  # Biggest topics first
  kaas snapshot --sort records
  # Everything, as JSON, for scripts
  kaas snapshot -o json | jq '.Topics[] | {Name, n: (.Partitions | length)}'`,
		Args: unknownCommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := oneOf(cmd, "output", output, "table", "json"); err != nil {
				return err
			}
			if err := oneOf(cmd, "sort", sortBy, snapshotSorts...); err != nil {
				return err
			}
			if _, err := connect(cmd, brokers, themeName); err != nil {
				return err
			}
			cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
			if err != nil {
				return err
			}
			defer cl.Close()
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			s, err := snapshot.NewTaker(cl).Take(ctx)
			if err != nil {
				return err
			}
			// Offsets are best effort: a topic whose leader is away shows
			// "-" records rather than failing the whole snapshot.
			var topics []string
			for _, t := range s.Topics {
				topics = append(topics, t.Name)
			}
			adm := kadm.NewClient(cl)
			if starts, err := adm.ListStartOffsets(ctx, topics...); err == nil {
				if ends, err := adm.ListEndOffsets(ctx, topics...); err == nil {
					s = snapshot.WithOffsets(s, topics, starts, ends, time.Now())
				}
			}
			return printSnapshot(cmd, s, output, sortBy, themeName)
		},
	}
	snap.Flags().StringVarP(&output, "output", "o", "table", "output format: table|json")
	snap.Flags().StringVar(&sortBy, "sort", "status", "sort topics by: "+strings.Join(snapshotSorts, "|"))
	root.AddCommand(snap)
	root.AddCommand(newTopicsCmd(&brokers, &themeName))
	root.AddCommand(newGroupsCmd(&brokers, &themeName))
	root.AddCommand(newBrokersCmd(&brokers, &themeName))
	root.AddCommand(newLagCmd(&brokers, &themeName))
	root.AddCommand(newConsumeCmd(&brokers, &themeName))
	root.AddCommand(newTopCmd(&brokers, &themeName))
	root.AddCommand(newWatchCmd(&brokers, &themeName))
	root.AddCommand(newBenchCmd(&brokers, &themeName))
	root.AddCommand(newTUICmd(&brokers, &themeName))
	return root
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
