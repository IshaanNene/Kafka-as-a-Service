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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/internal/version"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := newRoot().ExecuteContext(ctx)
	stop()
	if err != nil {
		os.Exit(1)
	}
}

func newRoot() *cobra.Command {
	var brokers string
	root := &cobra.Command{
		Use:           "kaas",
		Short:         "Kafka As A Service: CLI and TUI for Apache Kafka",
		SilenceUsage:  true,
		SilenceErrors: false,
	}
	root.PersistentFlags().StringVarP(&brokers, "brokers", "b", envOr("KAAS_BROKERS", "localhost:9092"), "comma-separated seed brokers")

	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "kaas %s (%s, %s)\n", version.Version, version.Commit, version.Date)
		},
	})

	var output string
	snap := &cobra.Command{
		Use:   "snapshot",
		Short: "Print a one-shot cluster snapshot",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
			if err != nil {
				return err
			}
			defer cl.Close()
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			s, err := snapshot.NewTaker(kadm.NewClient(cl)).Take(ctx)
			if err != nil {
				return err
			}
			return printSnapshot(cmd, s, output)
		},
	}
	snap.Flags().StringVarP(&output, "output", "o", "table", "output format: table|json")
	root.AddCommand(snap)
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
