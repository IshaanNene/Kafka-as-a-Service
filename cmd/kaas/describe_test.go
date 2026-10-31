package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/core/topics"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
)

func TestTopicsDescribeCommand(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.NumBrokers(3))
	brokers := strings.Join(c.ListenAddrs(), ",")
	ctx := context.Background()
	compact := "compact"
	if _, err := kadm.NewClient(cl).CreateTopic(ctx, 4, 3, map[string]*string{"cleanup.policy": &compact}, "users"); err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "users", Key: []byte{byte(i)}, Value: []byte("a user")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Plain: the kafka-topics --describe layout.
	plain := runKaas(t, "topics", "describe", "-b", brokers, "users")
	lines := strings.Split(strings.TrimSpace(plain), "\n")
	if len(lines) != 5 || !strings.HasPrefix(lines[0], "Topic: users\tPartitionCount: 4\tReplicationFactor: 3\tConfigs: cleanup.policy=compact") {
		t.Errorf("plain describe:\n%s", plain)
	}

	var j describeJSON
	if err := json.Unmarshal([]byte(runKaas(t, "topics", "describe", "-b", brokers, "users", "-o", "json")), &j); err != nil {
		t.Fatal(err)
	}
	var records, bytes int64
	for _, p := range j.Partitions {
		if len(p.Replicas) != 3 || len(p.ISR) != 3 || p.Records == nil || p.Bytes == nil {
			t.Fatalf("partition %+v, want 3 replicas in sync with records and bytes", p)
		}
		records += *p.Records
		bytes += *p.Bytes
	}
	if records != 40 || bytes <= 0 || j.Configs["cleanup.policy"] != "compact" || j.ReplicationFactor != 3 {
		t.Errorf("json = %+v (records %d, bytes %d)", j, records, bytes)
	}

	for _, tc := range []struct {
		args []string
		exit int
	}{
		{[]string{"topics", "describe", "-b", brokers, "usrs"}, exitNotFound},
		{[]string{"topics", "describe", "-b", brokers}, exitUsage},
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(tc.args)
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != tc.exit {
			t.Errorf("kaas %v: %v, want exit %d", tc.args, err, tc.exit)
		}
	}
}

func TestDescribeFocusesWhatNeedsAttention(t *testing.T) {
	all := []int32{1, 2, 3}
	part := func(id, leader int32, isr []int32, end int64) snapshot.Partition {
		return snapshot.Partition{ID: id, Leader: leader, Replicas: all, ISR: isr, StartOffset: 0, EndOffset: end}
	}
	snap := &snapshot.Snapshot{Brokers: []snapshot.Broker{{ID: 1}, {ID: 2}, {ID: 3}}}
	describe := func(parts ...snapshot.Partition) topicDescription {
		return topicDescription{snap: snap, topic: snapshot.Topic{Name: "t", Partitions: parts}, details: topics.Details{Disk: -1}}
	}

	healthy := describe(part(0, 1, all, 10), part(1, 2, all, 99), part(2, 3, all, 5)).screen()
	if healthy.Focus != 1 || healthy.StatusRole != kit.OK {
		t.Errorf("healthy: focus %d status %q, want the largest partition and OK", healthy.Focus, healthy.Status)
	}
	urp := describe(part(0, 1, all, 10), part(1, 2, all, 99), part(2, 3, []int32{3}, 5)).screen()
	if urp.Focus != 2 || urp.StatusRole != kit.Warn || !strings.Contains(urp.Status, "partition 2") {
		t.Errorf("urp: focus %d status %q", urp.Focus, urp.Status)
	}
	down := describe(part(0, -1, nil, 10), part(1, 2, all, 99), part(2, 3, []int32{3}, 5)).screen()
	if down.Focus != 0 || down.StatusRole != kit.Err || !strings.Contains(down.Status, "no leader") {
		t.Errorf("offline: focus %d status %q", down.Focus, down.Status)
	}
}
