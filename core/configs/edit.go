package configs

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/IshaanNene/Kafka-as-a-Service/core/fuzzy"
)

// A config file is one config a line, "key = value", under a [section].
// A line is set at the level being edited; a commented-out one is what
// the config is without it. An inline note starts at two spaces and #.

// note is what a line says after its value.
func note(e Entry, here int, rf int) string {
	var parts []string
	if h := Readable(e, rf); h != e.Value {
		parts = append(parts, h)
	}
	switch {
	case e.Level == here && e.HasDef:
		parts = append(parts, "default "+Readable(Entry{Key: e.Key, Value: e.Default, Type: e.Type}, rf))
	case e.Level != LevelDefault && e.Level != here:
		parts = append(parts, "from "+[...]string{"default", "server.properties", "the cluster default", "a broker override", "the topic"}[e.Level])
	}
	if e.ReadOnly {
		parts = append(parts, "read-only")
	}
	return strings.Join(parts, " · ")
}

// File writes entries as a config file to edit at level here: LevelTopic
// for a topic, LevelBroker for a broker. Sensitive configs are left out.
func File(title string, es []Entry, broker bool, here, rf int) string {
	var sb strings.Builder
	where := "on the topic"
	if broker {
		where = "on this broker"
	}
	fmt.Fprintf(&sb, "# %s\n", title)
	fmt.Fprintf(&sb, "# A line without # is set %s; comment it out or delete it to\n", where)
	sb.WriteString("# go back to the default. Durations take 3d or 12h, sizes 1GiB.\n")
	sb.WriteString("# :w checks the change with the cluster and shows it, y applies.\n")
	w := 0
	for _, e := range es {
		w = max(w, len(e.Key))
	}
	for _, sec := range Sections(es, broker) {
		fmt.Fprintf(&sb, "\n[%s]\n", strings.ToLower(sec.Name))
		for _, e := range sec.Entries {
			if e.Sensitive {
				continue
			}
			val := e.Value
			if e.Null {
				val = ""
			}
			line := fmt.Sprintf("%-*s = %s", w, e.Key, val)
			if e.Level != here {
				line = "# " + line
			}
			if n := note(e, here, rf); n != "" {
				line += "  # " + n
			}
			sb.WriteString(line + "\n")
		}
	}
	return sb.String()
}

// LineError is a line of a config file that doesn't read.
type LineError struct {
	Line int // from 1
	Msg  string
}

func (e LineError) Error() string { return fmt.Sprintf("line %d: %s", e.Line, e.Msg) }

// ParseFile reads a config file's set lines: key to value, as written.
func ParseFile(text string) (map[string]string, error) {
	out := map[string]string{}
	at := map[string]int{}
	for i, raw := range strings.Split(text, "\n") {
		line := raw
		if j := strings.Index(line, "  #"); j >= 0 {
			line = line[:j]
		}
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		key, val = strings.TrimSpace(key), strings.TrimSpace(val)
		switch {
		case !ok || key == "":
			return nil, LineError{i + 1, "want key = value"}
		case strings.ContainsAny(key, " \t"):
			return nil, LineError{i + 1, fmt.Sprintf("%q isn't a config name", key)}
		}
		if prev, dup := at[key]; dup {
			return nil, LineError{i + 1, fmt.Sprintf("%s is already set on line %d", key, prev)}
		}
		out[key], at[key] = val, i+1
	}
	return out, nil
}

// Normalize turns a readable value into what Kafka takes: 3d into
// milliseconds for a .ms key, 1GiB into bytes for a .bytes key, forever
// and unlimited into -1. Anything else is kept as written.
func Normalize(key, val string) (string, error) {
	e := Entry{Key: key, Value: val}
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return val, nil
	}
	switch KindOf(e) {
	case KindDuration:
		if val == "forever" || val == "unlimited" {
			return "-1", nil
		}
		d, err := parseDuration(val)
		if err != nil {
			return "", fmt.Errorf("%s takes a duration like 3d, 12h or 500ms: %w", key, err)
		}
		unit := time.Millisecond
		switch {
		case strings.HasSuffix(key, ".seconds"):
			unit = time.Second
		case strings.HasSuffix(key, ".minutes"):
			unit = time.Minute
		case strings.HasSuffix(key, ".hours"):
			unit = time.Hour
		}
		if d%unit != 0 {
			return "", fmt.Errorf("%s counts whole %s", key, strings.TrimPrefix(key[strings.LastIndex(key, "."):], "."))
		}
		return strconv.FormatInt(int64(d/unit), 10), nil
	case KindBytes:
		if val == "unlimited" {
			return "-1", nil
		}
		n, err := parseBytes(val)
		if err != nil {
			return "", fmt.Errorf("%s takes a size like 1GiB or 512KiB: %w", key, err)
		}
		return strconv.FormatInt(n, 10), nil
	}
	return val, nil
}

// parseDuration reads 3d, 2w, 12h, 90s, 500ms, and Go's 1h30m.
func parseDuration(s string) (time.Duration, error) {
	for _, u := range []struct {
		suffix string
		d      time.Duration
	}{{"w", 7 * 24 * time.Hour}, {"d", 24 * time.Hour}} {
		if n, ok := strings.CutSuffix(s, u.suffix); ok {
			f, err := strconv.ParseFloat(n, 64)
			if err != nil || f < 0 {
				return 0, errors.New("not a number of " + u.suffix)
			}
			return time.Duration(f * float64(u.d)), nil
		}
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, errors.New("not a duration")
	}
	return d, nil
}

// parseBytes reads 1GiB, 512KiB, 10MB (powers of 1024 either way), 100B.
func parseBytes(s string) (int64, error) {
	t := strings.ToUpper(strings.ReplaceAll(s, " ", ""))
	for _, u := range []struct {
		suffix string
		mult   float64
	}{{"TIB", 1 << 40}, {"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10}, {"TB", 1 << 40}, {"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10}, {"T", 1 << 40}, {"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10}, {"B", 1}} {
		if n, ok := strings.CutSuffix(t, u.suffix); ok {
			f, err := strconv.ParseFloat(n, 64)
			if err != nil || f < 0 || f*u.mult > math.MaxInt64 {
				return 0, errors.New("not a size")
			}
			return int64(f * u.mult), nil
		}
	}
	return 0, errors.New("not a size")
}

// Readable is a value as a person reads it: 7d, 1.0 GiB, forever,
// unlimited, 2 of 3. The value itself when nothing reads better.
func Readable(e Entry, rf int) string {
	switch KindOf(e) {
	case KindDuration:
		d, forever, ok := Duration(e)
		switch {
		case !ok:
		case forever:
			return "forever"
		case d == 0:
			return "0"
		case d < time.Second:
			return strconv.Itoa(int(d/time.Millisecond)) + "ms"
		default:
			return wholeUnit(d)
		}
	case KindBytes:
		if n, err := strconv.ParseInt(e.Value, 10, 64); err == nil {
			if n < 0 {
				return "unlimited"
			}
			return sizeText(n)
		}
	case KindCount:
		if e.Key == "min.insync.replicas" && rf > 0 {
			return e.Value + " of " + strconv.Itoa(rf)
		}
	}
	return e.Value
}

// wholeUnit prints d in the largest unit it is a whole number of.
func wholeUnit(d time.Duration) string {
	switch {
	case d%(24*time.Hour) == 0:
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	}
	return d.String()
}

// sizeText prints a size in binary units: 1 GiB, 1.5 MiB, 100 B.
func sizeText(n int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	v, i := float64(n), 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	if i == 0 || v == math.Trunc(v) {
		return fmt.Sprintf("%.0f %s", v, units[i])
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

// What a change does.
const (
	OpSet   = iota // set a new value, or change one
	OpUnset        // go back to what it is without it
)

// Change is one config to set or unset.
type Change struct {
	Key      string
	Op       int
	From, To string // the value now and after; To is the fallback for an unset
}

func keys(es []Entry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.Key
	}
	return out
}

// Diff is the changes from es, as set at level here, to want, in key
// order. want's values are normalized first; one that won't read, a key
// the cluster didn't list, or a read-only one is an error naming it.
func Diff(es []Entry, here int, want map[string]string) ([]Change, error) {
	by := map[string]Entry{}
	for _, e := range es {
		by[e.Key] = e
	}
	var out []Change
	for key, raw := range want {
		val, err := Normalize(key, raw)
		if err != nil {
			return nil, err
		}
		e, known := by[key]
		if !known {
			msg := fmt.Sprintf("%s isn't a config the cluster lists", key)
			if sug := fuzzy.Suggest(key, keys(es), 1); len(sug) > 0 {
				msg += "; did you mean " + sug[0] + "?"
			}
			return nil, errors.New(msg)
		}
		if e.Level == here && e.Value == val {
			continue
		}
		if e.ReadOnly {
			return nil, fmt.Errorf("%s is read-only: it can only be set in server.properties", key)
		}
		c := Change{Key: key, Op: OpSet, To: val}
		if !e.Null {
			c.From = e.Value
		}
		out = append(out, c)
	}
	for _, e := range es {
		if _, kept := want[e.Key]; kept || e.Level != here || e.Sensitive {
			continue
		}
		c := Change{Key: e.Key, Op: OpUnset, From: e.Value}
		if e.HasDef {
			c.To = e.Default
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// Apply makes the changes to a topic, or a broker when broker is set,
// with IncrementalAlterConfigs; with validate the cluster checks them
// and changes nothing.
func Apply(ctx context.Context, adm *kadm.Client, topic string, broker *int32, cs []Change, validate bool) error {
	if len(cs) == 0 {
		return nil
	}
	alters := make([]kadm.AlterConfig, len(cs))
	for i, c := range cs {
		alters[i] = kadm.AlterConfig{Op: kadm.SetConfig, Name: c.Key, Value: &c.To}
		if c.Op == OpUnset {
			alters[i] = kadm.AlterConfig{Op: kadm.DeleteConfig, Name: c.Key}
		}
	}
	var (
		rs  kadm.AlterConfigsResponses
		err error
	)
	switch {
	case broker != nil && validate:
		rs, err = adm.ValidateAlterBrokerConfigs(ctx, alters, *broker)
	case broker != nil:
		rs, err = adm.AlterBrokerConfigs(ctx, alters, *broker)
	case validate:
		rs, err = adm.ValidateAlterTopicConfigs(ctx, alters, topic)
	default:
		rs, err = adm.AlterTopicConfigs(ctx, alters, topic)
	}
	if err != nil {
		return err
	}
	for _, r := range rs {
		if r.Err != nil {
			if r.ErrMessage != "" {
				return fmt.Errorf("%w: %s", r.Err, r.ErrMessage)
			}
			return r.Err
		}
	}
	return nil
}
