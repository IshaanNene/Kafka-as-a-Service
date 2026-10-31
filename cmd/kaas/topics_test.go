package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

func TestTopicsCommand(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.NumBrokers(3), kfake.SeedTopics(3, "orders", "payments.eu.charges", "payments.us.charges"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	ctx := context.Background()
	compact, day := "compact", "86400000"
	if _, err := kadm.NewClient(cl).CreateTopic(ctx, 2, 3, map[string]*string{"cleanup.policy": &compact, "retention.ms": &day}, "users"); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("an order")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Plain: raw numbers, so scripts can sort and sum.
	plain := runKaas(t, "topics", "-b", brokers, "--sort", "records")
	lines := strings.Split(strings.TrimSpace(plain), "\n")
	if !strings.HasPrefix(lines[0], "TOPIC") || !strings.HasPrefix(lines[1], "orders") || !strings.Contains(lines[1], " 20 ") {
		t.Errorf("plain output, sorted by records:\n%s", plain)
	}

	var rows []topicJSON
	if err := json.Unmarshal([]byte(runKaas(t, "topics", "-b", brokers, "-o", "json")), &rows); err != nil {
		t.Fatal(err)
	}
	byName := map[string]topicJSON{}
	for _, r := range rows {
		byName[r.Name] = r
	}
	users := byName["users"]
	if users.CleanupPolicy != "compact" || users.RetentionMs == nil || *users.RetentionMs != int64(24*time.Hour/time.Millisecond) || users.ReplicationFactor != 3 {
		t.Errorf("users = %+v", users)
	}
	if o := byName["orders"]; o.Records == nil || *o.Records != 20 || o.DiskBytes == nil || *o.DiskBytes <= 0 {
		t.Errorf("orders = %+v, want 20 records on disk", o)
	}

	// A glob filters; the tree nests by prefix.
	glob := runKaas(t, "topics", "-b", brokers, "payments.*")
	if strings.Contains(glob, "orders") || strings.Count(glob, "payments.") != 2 {
		t.Errorf("glob output:\n%s", glob)
	}
	tree := runKaas(t, "topics", "-b", brokers, "--tree")
	for _, want := range []string{"payments/\n  eu/\n    charges\n  us/\n    charges\n", "orders\n", "users\n"} {
		if !strings.Contains(tree, want) {
			t.Errorf("tree missing %q:\n%s", want, tree)
		}
	}

	// A plain name that isn't a topic is a 404 with a suggestion.
	root := newRoot()
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"topics", "-b", brokers, "ordrs"})
	_, err := root.ExecuteC()
	var ce *cliError
	if !errors.As(err, &ce) || ce.Exit != exitNotFound || !strings.Contains(strings.Join(ce.Hints, " "), "orders") {
		t.Errorf("kaas topics ordrs: %v, want a not-found suggesting orders", err)
	}
}

func TestSizesAndRetention(t *testing.T) {
	for in, want := range map[int64]string{-1: "-", 0: "0 B", 1023: "1023 B", 1536: "1.5 KiB", 6084061: "5.8 MiB", 300 << 30: "300 GiB"} {
		if got := bytesText(in); got != want {
			t.Errorf("bytesText(%d) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[time.Duration]string{-1: "forever", 0: "-", 7 * 24 * time.Hour: "7d", 12 * time.Hour: "12h", 90 * time.Minute: "90m"} {
		if got := retentionText(in); got != want {
			t.Errorf("retentionText(%v) = %q, want %q", in, got, want)
		}
	}
}
