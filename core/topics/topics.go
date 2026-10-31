// Package topics adds what a topic list needs beyond the cluster snapshot:
// how long each topic keeps data, how it is cleaned up, which of those it
// sets itself rather than inheriting from the broker, and how much disk it
// takes across all replicas.
package topics

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Forever is the Retention of a topic that never deletes by age.
const Forever time.Duration = -1

// Details is one topic's retention, cleanup and disk use. Zero values mean
// unknown: the broker did not answer for that part.
type Details struct {
	Retention         time.Duration // retention.ms; Forever for -1
	RetentionBytes    int64         // retention.bytes per partition; -1 unlimited
	Cleanup           string        // cleanup.policy: delete, compact or compact,delete
	RetentionOverride bool          // retention.ms is set on the topic itself
	CleanupOverride   bool          // cleanup.policy is set on the topic itself
	Overrides         int           // configs set on the topic itself
	Disk              int64         // bytes on disk across every replica; -1 unknown
}

// Describe fetches details for names in two requests: DescribeConfigs for
// the topics, then DescribeLogDirs across the cluster. Either may fail on
// its own (log dirs need DESCRIBE on the cluster), so it returns what it
// could get alongside the error.
func Describe(ctx context.Context, adm *kadm.Client, names []string) (map[string]Details, error) {
	out := make(map[string]Details, len(names))
	for _, n := range names {
		out[n] = Details{Disk: -1}
	}
	if len(names) == 0 {
		return out, nil
	}
	var errs []error

	rcs, err := adm.DescribeTopicConfigs(ctx, names...)
	if err != nil {
		errs = append(errs, fmt.Errorf("topic configs: %w", err))
	}
	for _, rc := range rcs {
		d, ok := out[rc.Name]
		if !ok || rc.Err != nil {
			continue
		}
		for _, c := range rc.Configs {
			set := c.Source == kmsg.ConfigSourceDynamicTopicConfig
			if set {
				d.Overrides++
			}
			if c.Value == nil {
				continue
			}
			switch c.Key {
			case "retention.ms":
				d.Retention, d.RetentionOverride = parseMillis(*c.Value), set
			case "retention.bytes":
				d.RetentionBytes, _ = strconv.ParseInt(*c.Value, 10, 64)
			case "cleanup.policy":
				d.Cleanup, d.CleanupOverride = *c.Value, set
			}
		}
		out[rc.Name] = d
	}

	dirs, err := adm.DescribeAllLogDirs(ctx, nil)
	if err != nil {
		errs = append(errs, fmt.Errorf("log dirs: %w", err))
		return out, errors.Join(errs...)
	}
	sizes := make(map[string]int64)
	dirs.Each(func(dir kadm.DescribedLogDir) {
		if dir.Err != nil {
			return
		}
		for topic, parts := range dir.Topics {
			for _, p := range parts {
				sizes[topic] += p.Size
			}
		}
	})
	for n, d := range out {
		d.Disk = sizes[n]
		out[n] = d
	}
	return out, errors.Join(errs...)
}

func parseMillis(s string) time.Duration {
	ms, err := strconv.ParseInt(s, 10, 64)
	switch {
	case err != nil:
		return 0
	case ms < 0:
		return Forever
	}
	return time.Duration(ms) * time.Millisecond
}
