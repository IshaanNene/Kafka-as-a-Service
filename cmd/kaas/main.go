// Command kaas is the Kafka As A Service CLI.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
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

	var output string
	snap := &cobra.Command{
		Use:   "snapshot",
		Short: "Print a one-shot cluster snapshot",
		Long:  "Fetch metadata and log offsets once and print the cluster: brokers, topics, partitions and under-replication.",
		Example: `  # Summary table
  kaas snapshot
  # Everything, as JSON, for scripts
  kaas snapshot -o json | jq '.Topics[] | {Name, n: (.Partitions | length)}'`,
		Args: unknownCommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := oneOf(cmd, "output", output, "table", "json"); err != nil {
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
			return printSnapshot(cmd, s, output)
		},
	}
	snap.Flags().StringVarP(&output, "output", "o", "table", "output format: table|json")
	root.AddCommand(snap)
	root.AddCommand(newWatchCmd(&brokers, &themeName))
	root.AddCommand(newBenchCmd(&brokers, &themeName))
	root.AddCommand(newTUICmd(&brokers, &themeName))
	return root
}

func printSnapshot(cmd *cobra.Command, s *snapshot.Snapshot, output string) error {
	out := cmd.OutOrStdout()
	switch output {
	case "json":
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(s)
	case "table":
		fmt.Fprintf(out, "cluster %s · controller %d · %d brokers · %d topics · %d partitions\n\n",
			s.ClusterID, s.Controller, len(s.Brokers), len(s.Topics), s.PartitionCount())
		w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "TOPIC\tPARTITIONS\tUNDER-REPLICATED")
		for _, t := range s.Topics {
			if t.Internal {
				continue
			}
			urp := 0
			for _, p := range t.Partitions {
				if p.UnderReplicated() {
					urp++
				}
			}
			fmt.Fprintf(w, "%s\t%d\t%d\n", t.Name, len(t.Partitions), urp)
		}
		return w.Flush()
	default:
		return fmt.Errorf("unknown output format %q", output)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
