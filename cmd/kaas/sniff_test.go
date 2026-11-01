package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IshaanNene/Kafka-as-a-Service/core/sniff"
	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/theme"
)

// freePort is a port nothing listens on, for a moment.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// syncBuffer is a buffer a test can read while a command writes it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestSniffCommand(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.NumBrokers(2), kfake.SeedTopics(2, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	listen := freePort(t)

	var out syncBuffer
	root := newRoot()
	root.SetOut(&out)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"sniff", "-b", brokers, "--listen", listen, "--api", "produce", "-n", "2", "-o", "json"})
	done := make(chan error, 1)
	go func() { done <- root.Execute() }()

	// Wait for the proxy, then produce through it.
	deadline := time.Now().Add(15 * time.Second)
	for {
		conn, err := net.Dial("tcp", listen)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the proxy never listened on %s", listen)
		}
		time.Sleep(50 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pc, err := kgo.NewClient(kgo.SeedBrokers(listen), kgo.ClientID("billing-svc"))
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	for i := range 2 {
		if err := pc.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte{byte('a' + i)}}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatalf("sniff never stopped after 2 requests; printed:\n%s", out.String())
	}

	sc := bufio.NewScanner(strings.NewReader(out.String()))
	lines := 0
	for sc.Scan() {
		var j requestJSON
		if err := json.Unmarshal(sc.Bytes(), &j); err != nil {
			t.Fatalf("%v: %s", err, sc.Text())
		}
		lines++
		// --api produce lets nothing else through.
		if j.API != "Produce" || j.ClientID != "billing-svc" || !j.Answered || j.Bytes == 0 || !strings.HasPrefix(j.Info, "orders p") {
			t.Errorf("line %d: %+v", lines, j)
		}
	}
	if lines != 2 {
		t.Errorf("%d lines, want 2:\n%s", lines, out.String())
	}

	for _, args := range [][]string{
		{"--api", "shout"},
		{"--listen", "nowhere"},
		{"-n", "-1"},
		{"-o", "yaml"},
	} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(append([]string{"sniff", "-b", brokers}, args...))
		_, err := root.ExecuteC()
		var ce *cliError
		if !errors.As(err, &ce) || ce.Exit != exitUsage {
			t.Errorf("kaas sniff %v: %v, want a usage error", args, err)
		}
	}
}

func TestSniffModel(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	p, err := sniff.Start(context.Background(), cl, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	th, _ := theme.Get(theme.Default)
	c := newCapture(nil)
	m := newSniffModel(kit.New(th, false), p, c, "all requests", 0)
	m.w, m.h = 100, 30
	view := func(m sniffModel) string { return ansi.Strip(m.View().Content) }
	if v := view(m); !strings.Contains(v, "nothing yet: point a client at "+p.Addr()) {
		t.Fatalf("empty:\n%s", v)
	}
	keys := []int16{18, 3, 0, 0, 1}
	for i, k := range keys {
		c.add(sniff.Request{N: int64(i + 1), At: time.Now(), Client: "127.0.0.1:5000", Broker: 0, Key: k, Size: 40, RespSize: 20, Answered: true, Info: "req" + string(rune('a'+i))})
	}
	next, _ := m.Update(sniffTick(time.Now()))
	m = next.(sniffModel)
	v := view(m)
	if m.total != 5 || !strings.Contains(v, "▶    5") || !strings.Contains(v, "Request 5 v0") {
		t.Fatalf("after 5:\n%s", v)
	}
	if !strings.Contains(v, "Produce") || !strings.Contains(v, "by API (last 200)") {
		t.Errorf("no mix:\n%s", v)
	}
	press := func(m sniffModel, code rune) sniffModel {
		next, _ := m.Update(tea.KeyPressMsg{Code: code})
		return next.(sniffModel)
	}
	m = press(m, tea.KeyUp)
	m = press(m, tea.KeyUp)
	if m.sel != 2 || !strings.Contains(view(m), "Request 3 v0") {
		t.Errorf("up twice: sel %d\n%s", m.sel, view(m))
	}
	m = press(m, tea.KeyDown)
	m = press(m, tea.KeyDown)
	if m.sel != -1 {
		t.Errorf("down to the newest should follow it: sel %d", m.sel)
	}

	// A full channel drops, never blocks the proxy.
	f := &capture{ch: make(chan sniff.Request, 1)}
	f.add(sniff.Request{})
	f.add(sniff.Request{})
	if f.dropped.Load() != 1 {
		t.Errorf("dropped %d", f.dropped.Load())
	}
	// -n quits once that many are in.
	m = newSniffModel(kit.New(th, false), p, c, "", 1)
	c.add(sniff.Request{Key: 3})
	if _, cmd := m.Update(sniffTick(time.Now())); cmd == nil {
		t.Error("-n 1 did not quit")
	} else if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Error("-n 1 did not quit")
	}
}
