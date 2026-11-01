package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/IshaanNene/Kafka-as-a-Service/controlplane/console"
	"github.com/IshaanNene/Kafka-as-a-Service/internal/version"
)

func newConsoleCmd(brokers *string) *cobra.Command {
	var (
		fleetPath, listen, desired, name string
		webDir                           string
		allowApply, demo                 bool
	)
	cmd := &cobra.Command{
		Use:   "console",
		Short: "Serve the web console for a fleet of clusters",
		Long: `Serve the KaaS web console: every cluster in a fleet with its health,
throughput, topics, consumer lag, ACLs, quotas, schemas and GitOps drift,
and a live feed of what changes.

The fleet comes from a YAML file (--fleet), or is the one cluster -b points
at. The console is read-only unless --allow-apply is given; the usual way to
change a cluster is a reviewed desired-state file applied from CI with
kaas apply. It has no login yet, so it listens on localhost by default.`,
		Example: `  # Try it without a cluster: three in-memory clusters with live traffic
  kaas console --demo
  # A fleet
  kaas console --fleet fleet.yaml
  # The cluster -b points at, with its desired state
  kaas console -b kafka-0:9092 --desired gitops/prod.yaml`,
		Args: unknownCommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if demo && fleetPath != "" {
				return usageError(cmd, "demo", "", "can't be used with --fleet")
			}
			if allowApply && !loopback(listen) {
				return usageError(cmd, "listen", listen, "--allow-apply needs a localhost address: the console has no login yet, so anyone who could reach it could change your clusters")
			}
			ctx := cmd.Context()
			var cfg console.Config
			switch {
			case demo:
				d, err := console.StartDemo(ctx)
				if err != nil {
					return err
				}
				defer d.Close()
				cfg = d.Config
			case fleetPath != "":
				var err error
				if cfg, err = console.LoadConfig(fleetPath); err != nil {
					return usageError(cmd, "fleet", fleetPath, err.Error())
				}
			default:
				cfg.Clusters = []console.ClusterConfig{{Name: name, Brokers: splitSeeds(*brokers), Desired: desired}}
			}
			f, err := console.NewFleet(cfg, console.Intervals{})
			if err != nil {
				return usageError(cmd, "fleet", fleetPath, err.Error())
			}
			srv, err := console.NewServer(f, console.Options{AllowApply: allowApply, Version: version.Version, Demo: demo, WebDir: webDir})
			if err != nil {
				return err
			}
			ln, err := net.Listen("tcp", listen)
			if err != nil {
				return usageError(cmd, "listen", listen, err.Error())
			}
			hs := &http.Server{Handler: srv, ReadHeaderTimeout: 10 * time.Second}
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			go f.Run(runCtx)
			go func() {
				<-ctx.Done()
				shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = hs.Shutdown(shut)
			}()
			mode := "read-only"
			if allowApply {
				mode = "plans can be applied"
			}
			fmt.Fprintf(cmd.OutOrStdout(), "kaas console  http://%s  (%s, %s)\n", displayAddr(ln.Addr()), plural(len(cfg.Clusters), "cluster"), mode)
			if !loopback(listen) {
				fmt.Fprintln(cmd.ErrOrStderr(), "warning: the console has no login yet and is listening beyond localhost; put it behind an authenticating proxy")
			}
			if err := hs.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return ctx.Err()
		},
	}
	f := cmd.Flags()
	f.StringVar(&fleetPath, "fleet", "", "fleet file listing the clusters")
	f.StringVar(&listen, "listen", envOr("KAAS_CONSOLE_LISTEN", "127.0.0.1:7070"), "address to serve on")
	f.BoolVar(&allowApply, "allow-apply", false, "let the console apply GitOps plans (localhost only)")
	f.BoolVar(&demo, "demo", false, "serve a built-in demo fleet instead of real clusters")
	f.StringVar(&desired, "desired", "", "desired-state file for the -b cluster")
	f.StringVar(&name, "name", "local", "name for the -b cluster")
	f.StringVar(&webDir, "web-dir", "", "serve the web app from this directory (for working on it)")
	_ = f.MarkHidden("web-dir")
	cmd.Annotations = map[string]string{"hint": "web UI"}
	return cmd
}

// loopback is whether addr only listens on this machine.
func loopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func displayAddr(a net.Addr) string {
	s := a.String()
	return strings.Replace(s, "[::1]", "localhost", 1)
}
