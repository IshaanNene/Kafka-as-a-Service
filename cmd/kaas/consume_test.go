package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/consume"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

func TestConsumeCommand(t *testing.T) {
	c, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	ctx := context.Background()
	for i := range 5 {
		r := &kgo.Record{Topic: "orders", Key: []byte(fmt.Sprint("user-", i)), Value: []byte(fmt.Sprint(`{"order":`, i, `}`))}
		if i == 3 {
			r.Value = []byte{0xff, 0x00}
			r.Headers = []kgo.RecordHeader{{Key: "trace", Value: []byte("abc")}}
		}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Plain: one raw value per line, like kafka-console-consumer.
	plain := runKaas(t, "consume", "orders", "-b", brokers, "--from", "-2", "--exit")
	if plain != "\xff\x00\n{\"order\":4}\n" {
		t.Errorf("plain --from -2: %q", plain)
	}

	var rows []recordJSON
	for _, l := range strings.Split(strings.TrimSpace(runKaas(t, "tail", "orders", "-b", brokers, "--from", "beginning", "-n", "4", "-o", "json")), "\n") {
		var r recordJSON
		if err := json.Unmarshal([]byte(l), &r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	if len(rows) != 4 || *rows[0].Key != "user-0" || *rows[0].Value != `{"order":0}` || rows[0].Encoding != "" {
		t.Errorf("json, first record: %+v", rows)
	}
	if b := rows[3]; b.Encoding != "base64" || *b.Value != "/wA=" || len(b.Headers) != 1 || b.Headers[0].Value != "abc" {
		t.Errorf("binary record: %+v", b)
	}

	for args, wantExit := range map[string]int{
		"consume ordrs":              exitNotFound,
		"consume orders --from soon": exitUsage,
		"consume orders -p 4 --exit": exitUsage,
		"consume":                    exitUsage,
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(append(strings.Fields(args), "-b", brokers))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != wantExit {
			t.Errorf("kaas %s: %v, want exit %d", args, err, wantExit)
		}
	}
}

func TestChannelView(t *testing.T) {
	o := consume.Options{Topic: "orders", Plan: []consume.Start{{Partition: 0, Offset: 10, End: 12}, {Partition: 1, Offset: 5, End: 5}}}
	from, _ := consume.ParseFrom("-2", time.Now())
	th, _ := theme.Get(theme.Default)
	v := newChannelView(surface{width: 100, height: 20, kit: kit.New(th, false)}, o, from, "localhost:9092")
	v.add(consume.Record{Partition: 0, Offset: 10, Time: time.Now(), Key: []byte("user-1"), Value: []byte("hello")})
	v.add(consume.Record{Partition: 0, Offset: 11, Time: time.Now(), Key: []byte("user-2"),
		Headers: []consume.Header{{Key: "trace", Value: "t1"}}})
	s := v.screen(time.Now())
	if len(s.Lines) != 5 || s.Lines[0].Kind != screens.LineJoin || s.Lines[3].Kind != screens.LinePart || s.Lines[4].Kind != screens.LineAct {
		t.Errorf("lines: two joins, a message, a tombstone and its headers; got %+v", s.Lines)
	}
	if !strings.Contains(s.Lines[1].Text, "at the end") || !strings.Contains(s.Lines[0].Text, "2 behind the end") {
		t.Errorf("join lines: %q, %q", s.Lines[0].Text, s.Lines[1].Text)
	}
	if s.Nicks[0].Count != "2" || s.Nicks[1].Count != "0" {
		t.Errorf("partition counts: %+v", s.Nicks)
	}
	out := kit.Join(v.frame(time.Now()))
	for _, want := range []string{"#orders", "<user-1> hello", "user-2 tombstone", "headers: trace=t1", "read 2"} {
		if !strings.Contains(out, want) {
			t.Errorf("frame missing %q:\n%s", want, out)
		}
	}
}
