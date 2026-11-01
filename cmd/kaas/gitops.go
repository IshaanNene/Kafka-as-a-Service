package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/plan"
)

// exitDrift is plan --exit-code's answer when the cluster doesn't match the
// file, so CI can tell "changes to make" from "failed".
const exitDrift = 5

func newPlanCmd(brokers *string) *cobra.Command {
	var (
		file, output string
		exitCode     bool
	)
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show what it takes to make a cluster match its desired-state file",
		Long: `Compare a desired-state file (topics, configs, ACLs and quotas) with the
cluster, check each change with the controller and the file's CEL policies,
and print the plan. Nothing is changed.

Each change is marked + (adds), ~ (changes) or - (removes), with its risk.
The plan's ID covers its changes: kaas apply --plan ID refuses to run if
the cluster or the file changed since the plan was reviewed.`,
		Example: `  # In a pull request: what would merging this change?
  kaas plan -f gitops/prod-eu.yaml -b kafka-0.prod-eu:9092
  # Fail CI when the cluster has drifted from git
  kaas plan -f gitops/prod-eu.yaml --exit-code
  # For a bot to comment on the pull request
  kaas plan -f gitops/prod-eu.yaml -o json`,
		Args: unknownCommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := oneOf(cmd, "output", output, "text", "json"); err != nil {
				return err
			}
			p, cl, err := makePlan(cmd, *brokers, file)
			if err != nil {
				return err
			}
			cl.Close()
			if output == "json" {
				return writePlanJSON(cmd.OutOrStdout(), p, nil)
			}
			printPlan(cmd.OutOrStdout(), p, file)
			if why := p.Blocked(); why != "" {
				return &cliError{Code: "blocked", Exit: exitFailure, Message: "the plan can't be applied: " + why}
			}
			if exitCode && len(p.Ops) > 0 {
				return &cliError{Code: "drift", Exit: exitDrift, Message: fmt.Sprintf("%s differs from %s by %s", p.Cluster, file, plural(len(p.Ops), "change"))}
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVarP(&file, "file", "f", "", "desired-state file (required)")
	f.StringVarP(&output, "output", "o", "text", "output: text or json")
	f.BoolVar(&exitCode, "exit-code", false, "exit 5 when there are changes to make")
	cmd.Annotations = map[string]string{"hint": "gitops"}
	return cmd
}

func newApplyCmd(brokers *string) *cobra.Command {
	var (
		file, output, reviewed string
		yes                    bool
	)
	cmd := &cobra.Command{
		Use:   "apply",
		Short: "Make a cluster match its desired-state file",
		Long: `Plan as kaas plan does, then make the changes in order: additions first,
removals last, stopping at the first failure. A plan with a policy
violation, an error, or a change the controller refused is not applied.

On a terminal you confirm by typing the cluster's name. Without one,
--yes is required; pass --plan with the ID from a reviewed plan so a
cluster that changed since the review isn't touched.`,
		Example: `  # After the pull request merges, in CI
  kaas apply -f gitops/prod-eu.yaml --plan 3f9c2a1b7d04e6aa --yes
  # By hand, confirming at the prompt
  kaas apply -f gitops/staging.yaml`,
		Args: unknownCommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := oneOf(cmd, "output", output, "text", "json"); err != nil {
				return err
			}
			in := cmd.InOrStdin()
			f, isFile := in.(*os.File)
			typing := isFile && term.IsTerminal(f.Fd()) && output == "text"
			if !yes && !typing {
				return usageError(cmd, "yes", "", "required without a terminal to confirm")
			}
			p, cl, err := makePlan(cmd, *brokers, file)
			if err != nil {
				return err
			}
			defer cl.Close()
			out := cmd.OutOrStdout()
			if output == "text" {
				printPlan(out, p, file)
			}
			if reviewed != "" && reviewed != p.ID {
				return &cliError{Code: "plan-changed", Exit: exitFailure,
					Message: fmt.Sprintf("the plan is now %s, not the reviewed %s: the cluster or the file changed", p.ID, reviewed),
					Hints:   []string{"review the new plan with kaas plan -f " + file}}
			}
			if why := p.Blocked(); why != "" {
				return &cliError{Code: "blocked", Exit: exitFailure, Message: "the plan can't be applied: " + why}
			}
			if len(p.Ops) == 0 {
				if output == "json" {
					return writePlanJSON(out, p, []plan.Result{})
				}
				return nil
			}
			if !yes {
				fmt.Fprintf(out, "\nType %s to apply %s: ", p.Cluster, plural(len(p.Ops), "change"))
				line, _ := bufio.NewReader(in).ReadString('\n')
				if strings.TrimSpace(line) != p.Cluster {
					return &cliError{Code: "cancelled", Exit: exitFailure, Message: "not applied"}
				}
			}
			res, err := plan.Apply(cmd.Context(), cl, p)
			if output == "json" {
				if werr := writePlanJSON(out, p, res); werr != nil {
					return werr
				}
			} else {
				fmt.Fprintln(out)
				for _, r := range res {
					mark := "✓"
					switch {
					case r.Err != "":
						mark = "✗"
					case !r.Done:
						mark = "·"
					}
					fmt.Fprintf(out, "  %s %-15s %s%s\n", mark, r.Op.Kind, r.Op.Target, keySuffix(r.Op))
					if r.Err != "" {
						fmt.Fprintf(out, "      %s\n", r.Err)
					}
				}
			}
			if err != nil {
				return &cliError{Code: "apply-failed", Exit: exitFailure, Message: "apply stopped: " + err.Error(),
					Hints: []string{"changes before it were made; run kaas plan to see what's left"}}
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVarP(&file, "file", "f", "", "desired-state file (required)")
	f.StringVarP(&output, "output", "o", "text", "output: text or json")
	f.StringVar(&reviewed, "plan", "", "apply only if the plan's ID is this one")
	f.BoolVarP(&yes, "yes", "y", false, "don't ask to confirm")
	cmd.Annotations = map[string]string{"hint": "gitops"}
	return cmd
}

// makePlan loads the file, connects, and plans and checks. The caller
// closes the client.
func makePlan(cmd *cobra.Command, brokers, file string) (*plan.Plan, *kgo.Client, error) {
	if file == "" {
		return nil, nil, usageError(cmd, "file", "", "required: the desired-state file to plan from")
	}
	s, err := plan.Load(file)
	if err != nil {
		var pe *os.PathError
		if errors.As(err, &pe) {
			return nil, nil, usageError(cmd, "file", file, "can't read it: "+cause(err))
		}
		return nil, nil, usageError(cmd, "file", file, err.Error())
	}
	if _, err := connect(cmd, brokers, ""); err != nil {
		return nil, nil, err
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(splitSeeds(brokers)...))
	if err != nil {
		return nil, nil, err
	}
	live, err := plan.Read(cmd.Context(), cl, s)
	if err != nil {
		cl.Close()
		return nil, nil, err
	}
	p := plan.Diff(s, live)
	if p.Cluster == "" {
		p.Cluster = brokers
	}
	plan.Check(cmd.Context(), cl, p)
	return p, cl, nil
}

func keySuffix(o plan.Op) string {
	if o.Key == "" {
		return ""
	}
	return " " + o.Key
}

func printPlan(w io.Writer, p *plan.Plan, file string) {
	n := p.Counts()
	if len(p.Ops) == 0 {
		fmt.Fprintf(w, "%s matches %s: nothing to do.\n", p.Cluster, file)
	} else {
		fmt.Fprintf(w, "Plan %s for %s: %s (%d safe, %d caution, %d destructive)\n\n", p.ID, p.Cluster,
			plural(len(p.Ops), "change"), n[plan.Safe], n[plan.Caution], n[plan.Destructive])
	}
	for _, o := range p.Ops {
		sign := "~"
		switch o.Kind {
		case plan.CreateTopic, plan.CreateACL:
			sign = "+"
		case plan.DeleteACL, plan.DeleteTopic, plan.RemoveQuota, plan.UnsetConfig:
			sign = "-"
		case plan.SetQuota, plan.SetConfig:
			if o.From == "" {
				sign = "+"
			}
		}
		target := o.Target + keySuffix(o)
		change := ""
		switch {
		case o.From != "" && o.To != "":
			change = o.From + " → " + o.To
		case o.To != "":
			change = o.To
		case o.From != "":
			change = "was " + o.From
		}
		fmt.Fprintf(w, "  %s %-14s %-13s %s", sign, o.Kind, "["+string(o.Risk)+"]", target)
		if change != "" {
			fmt.Fprintf(w, "  %s", change)
		}
		fmt.Fprintln(w)
		fmt.Fprintf(w, "      %s\n", o.Detail)
		if o.Problem != "" {
			fmt.Fprintf(w, "      refused by the controller: %s\n", o.Problem)
		}
	}
	section := func(title string, lines []string) {
		if len(lines) == 0 {
			return
		}
		fmt.Fprintf(w, "\n%s\n", title)
		for _, l := range lines {
			fmt.Fprintf(w, "  %s\n", l)
		}
	}
	var vs []string
	for _, v := range p.Violations {
		vs = append(vs, fmt.Sprintf("✗ %s: topic %s: %s", v.Policy, v.Topic, v.Message))
	}
	section("Policy violations", vs)
	section("Errors", p.Errors)
	section("Warnings", p.Warnings)
	if u := p.Unmanaged; u.Topics+u.ACLs+u.Quotas > 0 {
		fmt.Fprintf(w, "\nNot in the file, left alone: %d topics, %d ACLs, %d quotas\n", u.Topics, u.ACLs, u.Quotas)
	}
}

func writePlanJSON(w io.Writer, p *plan.Plan, res []plan.Result) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(struct {
		*plan.Plan
		Blocked string        `json:"blocked,omitempty"`
		Results []plan.Result `json:"results,omitempty"`
	}{p, p.Blocked(), res})
}
