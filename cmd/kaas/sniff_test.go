package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
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

func TestSniffTrace(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	listen := freePort(t)
	var out syncBuffer
	root := newRoot()
	root.SetOut(&out)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"sniff", "-b", brokers, "--listen", listen, "--trace", "--for", "3s", "--client-id", "tracer", "-o", "json"})
	done := make(chan error, 1)
	go func() { done <- root.Execute() }()
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if conn, err := net.Dial("tcp", listen); err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the proxy never listened")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Someone else's traffic doesn't join the trace.
	other, err := kgo.NewClient(kgo.SeedBrokers(listen), kgo.ClientID("other"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(listen), kgo.ClientID("tracer"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("x")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("the trace never ended")
	}
	sc := bufio.NewScanner(strings.NewReader(out.String()))
	apis := map[string]bool{}
	for sc.Scan() {
		var j stagesJSON
		if err := json.Unmarshal(sc.Bytes(), &j); err != nil {
			t.Fatalf("%v: %s", err, sc.Text())
		}
		apis[j.API] = true
		sum := j.Connect + j.Send + j.Queue + j.Wait + j.Ack
		if j.ClientID != "tracer" || j.StartMs < 0 || j.StartMs > 3000 || sum-j.Total > 0.01 || j.Total-sum > 0.01 || j.Total <= 0 {
			t.Errorf("%+v", j)
		}
	}
	if !apis["Produce"] || !apis["Metadata"] {
		t.Errorf("traced %v", apis)
	}
	root = newRoot()
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"sniff", "-b", brokers, "--for", "3s"})
	var ce *cliError
	if _, err := root.ExecuteC(); !errors.As(err, &ce) || ce.Exit != exitUsage {
		t.Errorf("--for without --trace: %v", err)
	}
}

func TestTraceModel(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	p, err := sniff.Start(context.Background(), cl, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	th, _ := theme.Get(theme.Default)
	c := newCapture(nil)
	m := newTraceModel(kit.New(th, false), p, c, "every client", time.Second, 0)
	m.w, m.h = 104, 30
	view := func(m traceModel) string { return ansi.Strip(m.View().Content) }
	tickAt := func(m traceModel, at time.Time) traceModel {
		next, _ := m.Update(sniffTick(at))
		return next.(traceModel)
	}
	if v := view(m); !strings.Contains(v, "waiting for every client") {
		t.Fatalf("before:\n%s", v)
	}
	t0 := time.Now()
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	req := func(n int64, key int16, start time.Duration, info string, st sniff.Stages) sniff.Request {
		return sniff.Request{N: n, Key: key, Start: t0.Add(start), Answered: true, Info: info, Stages: st, RespSize: 100}
	}
	sync := req(2, 14, ms(10), "billing · generation 1", sniff.Stages{Send: ms(1), Wait: ms(30), Ack: ms(1)})
	c.addSent(req(1, 18, 0, "kgo", sniff.Stages{Connect: ms(2), Send: ms(1)}))
	c.addSent(sync)
	c.add(sync)
	produce := req(3, 0, ms(50), "orders p0 · 1 record · acks all", sniff.Stages{Send: ms(1), Wait: ms(5), Ack: ms(1)})
	produce.Err = "NOT_LEADER_FOR_PARTITION"
	c.add(produce)
	m = tickAt(m, t0.Add(ms(100)))
	v := view(m)
	for _, want := range []string{"● tracing", "pending", "joined billing", "error", "NOT_LEADER_FOR_PARTITION", "3 requests"} {
		if !strings.Contains(v, want) {
			t.Errorf("no %q:\n%s", want, v)
		}
	}
	if strings.Contains(v, "first produce acked") {
		t.Error("a failed produce marked as acked")
	}
	// Groups only.
	next, _ := m.Update(tea.KeyPressMsg{Code: tea.KeyRight})
	m = next.(traceModel)
	next, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyRight})
	m = next.(traceModel)
	next, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyRight})
	m = next.(traceModel)
	next, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyRight})
	m = next.(traceModel)
	if rows := m.shown(); m.tab != 4 || len(rows) != 1 || m.reqs[rows[0]].Key != 14 {
		t.Errorf("groups tab %d: %v", m.tab, rows)
	}
	// After the window nothing new joins, and an answer still lands.
	m = tickAt(m, t0.Add(2*time.Second))
	c.add(req(9, 3, ms(1500), "late", sniff.Stages{Wait: ms(1)}))
	c.add(req(1, 18, 0, "kgo", sniff.Stages{Connect: ms(2), Send: ms(1), Wait: ms(1)}))
	m = tickAt(m, t0.Add(2*time.Second+ms(100)))
	if !m.over || len(m.reqs) != 3 || !m.answered[0] || !strings.Contains(view(m), "■ traced") {
		t.Errorf("after: over %v, %d reqs\n%s", m.over, len(m.reqs), view(m))
	}
}

func TestSniffProfile(t *testing.T) {
	c, _ := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	brokers := strings.Join(c.ListenAddrs(), ",")
	listen := freePort(t)
	var out syncBuffer
	root := newRoot()
	root.SetOut(&out)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"sniff", "-b", brokers, "--listen", listen, "--profile", "--api", "produce", "-n", "3"})
	done := make(chan error, 1)
	go func() { done <- root.Execute() }()
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		if conn, err := net.Dial("tcp", listen); err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the proxy never listened")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(listen))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for range 3 {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("x")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("the profile never ended")
	}
	// Folded stacks: Produce;broker N;orders;stage micros, and nothing else.
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	stages := map[string]bool{}
	for _, l := range lines {
		i := strings.LastIndex(l, " ") // frame names have spaces; the count is last
		stack, n, ok := l[:max(0, i)], l[i+1:], i > 0
		parts := strings.Split(stack, ";")
		if !ok || len(parts) != 4 || parts[0] != "Produce" || !strings.HasPrefix(parts[1], "broker ") || parts[2] != "orders" {
			t.Errorf("line %q", l)
			continue
		}
		if _, err := strconv.Atoi(n); err != nil {
			t.Errorf("line %q: %v", l, err)
		}
		stages[parts[3]] = true
	}
	if !stages["wait"] || !stages["send"] {
		t.Errorf("stages %v in\n%s", stages, out.String())
	}
	for _, args := range [][]string{{"--trace", "--profile"}, {"--for", "0s", "--profile"}} {
		root := newRoot()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs(append([]string{"sniff", "-b", brokers}, args...))
		var ce *cliError
		if _, err := root.ExecuteC(); !errors.As(err, &ce) || ce.Exit != exitUsage {
			t.Errorf("%v: %v", args, err)
		}
	}
}

func TestProfileModel(t *testing.T) {
	_, cl := kafkatest.Fake(t, kfake.SeedTopics(1, "orders"))
	p, err := sniff.Start(context.Background(), cl, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	th, _ := theme.Get(theme.Default)
	c := newCapture(nil)
	m := newProfileModel(kit.New(th, false), p, c, "every client", 0, 0)
	m.w, m.h = 100, 26
	view := func(m profileModel) string { return ansi.Strip(m.View().Content) }
	if v := view(m); !strings.Contains(v, "nothing yet: point a client at") {
		t.Fatalf("empty:\n%s", v)
	}
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	now := time.Now()
	for i := range 4 {
		c.add(sniff.Request{Key: 0, Broker: 1, Topics: []string{"orders"}, Records: -1, Answered: true, Start: now,
			Stages: sniff.Stages{Send: ms(1), Wait: ms(5 + i), Ack: ms(1)}})
	}
	c.add(sniff.Request{Key: 1, Broker: 1, Topics: []string{"orders"}, Records: 0, Answered: true, Start: now, Stages: sniff.Stages{Wait: ms(500)}})
	c.add(sniff.Request{Key: 1, Broker: 1, Topics: []string{"orders"}, Records: 9, Answered: true, Start: now, Stages: sniff.Stages{Wait: ms(10), Ack: ms(2)}})
	step := func(m profileModel, msgs ...tea.Msg) profileModel {
		for _, msg := range msgs {
			next, _ := m.Update(msg)
			m = next.(profileModel)
		}
		return m
	}
	key := func(code rune) tea.Msg { return tea.KeyPressMsg{Code: code} }
	m = step(m, sniffTick(now))
	v := view(m)
	if !strings.Contains(v, "requests 6") || !strings.Contains(v, "polls hidden") || strings.Contains(v, "idle poll ") {
		t.Fatalf("after 6:\n%s", v)
	}
	// Idle polls hidden: produce's 33ms outweighs fetch's 12ms, so it is first.
	m = step(m, key(tea.KeyUp))
	if strings.Join(m.sel, ";") != "Produce" {
		t.Errorf("up from all: %v", m.sel)
	}
	m = step(m, key(tea.KeyRight))
	if strings.Join(m.sel, ";") != "Fetch" {
		t.Errorf("right: %v", m.sel)
	}
	m = step(m, key(tea.KeyLeft), key(tea.KeyUp), key(tea.KeyUp), key(tea.KeyUp))
	if strings.Join(m.sel, ";") != "Produce;broker 1;orders;wait" {
		t.Errorf("up to the stage: %v", m.sel)
	}
	v = view(m)
	if !strings.Contains(v, "▶ wait") || !strings.Contains(v, "4 requests · avg 6.5ms") || !strings.Contains(v, "all → Produce → broker 1 → orders") {
		t.Errorf("selected wait:\n%s", v)
	}
	// Zoom into the topic, then out.
	m = step(m, key(tea.KeyDown), key(tea.KeyEnter))
	if v = view(m); strings.Join(m.zoom, ";") != "Produce;broker 1;orders" || !strings.Contains(v, "zoomed into") || strings.Contains(v, "Fetch") {
		t.Errorf("zoomed: %v\n%s", m.zoom, v)
	}
	m = step(m, key(tea.KeyEscape))
	if len(m.zoom) != 2 {
		t.Errorf("esc: %v", m.zoom)
	}
	// Idle polls shown: fetch now outweighs produce.
	m = step(m, tea.KeyPressMsg{Code: 'i', Text: "i"})
	full, _ := m.view(m.prof.Root)
	if !m.showIdle || full.Kids[0].Name != "Fetch" || !strings.Contains(view(m), "polls shown") {
		t.Errorf("idle shown: first %s", full.Kids[0].Name)
	}
	m = step(m, tea.KeyPressMsg{Code: 'r', Text: "r"})
	if m.prof.Requests != 0 || len(m.zoom) != 0 {
		t.Error("reset kept the profile")
	}
}
