package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/core/engine"
	"github.com/IshaanNene/Kafka-as-a-Service/core/snapshot"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/kit"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

// toastLife is how long an event card stays on screen.
const toastLife = time.Minute

// maxRates bounds each sparkline's history.
const maxRates = 120

// watchView is kaas watch on a terminal: watched topics as sparklines of
// records/sec, cluster events as a stack of toasts, redrawn in place.
type watchView struct {
	lv      *live
	topics  []string
	cfg     engine.Config
	snap    *snapshot.Snapshot
	stats   engine.Stats
	dropped int
	series  map[string]*screens.Series
	seen    map[string]offsetMark
	events  []stamped
}

type offsetMark struct {
	end int64
	at  time.Time
}

type stamped struct {
	engine.Event
	at time.Time
}

func newWatchView(s surface, topics []string, cfg engine.Config) *watchView {
	v := &watchView{lv: &live{s: s}, topics: topics, cfg: cfg, series: map[string]*screens.Series{}, seen: map[string]offsetMark{}}
	for _, t := range topics {
		v.series[t] = &screens.Series{Name: t}
	}
	return v
}

// update folds one engine update in: new events become toasts, and each
// watched topic whose offsets were re-read gains a rate sample.
func (v *watchView) update(u engine.Update, st engine.Stats, now time.Time) {
	v.snap, v.stats = u.Snapshot, st
	v.dropped += u.DroppedEvents
	for _, e := range u.Events {
		v.events = append([]stamped{{e, now}}, v.events...)
	}
	if len(v.events) > 50 {
		v.events = v.events[:50]
	}
	for _, name := range v.topics {
		t, ok := u.Snapshot.Topic(name)
		if !ok || t.OffsetsAt.IsZero() {
			continue
		}
		end, retained, ok := topicOffsets(t)
		if !ok {
			continue
		}
		s := v.series[name]
		s.Known, s.Retained = true, retained
		prev, had := v.seen[name]
		if had && t.OffsetsAt.After(prev.at) {
			rate := float64(end-prev.end) / t.OffsetsAt.Sub(prev.at).Seconds()
			s.Rates = append(s.Rates, max(0, rate))
			if len(s.Rates) > maxRates {
				s.Rates = s.Rates[len(s.Rates)-maxRates:]
			}
		}
		if !had || t.OffsetsAt.After(prev.at) {
			v.seen[name] = offsetMark{end, t.OffsetsAt}
		}
	}
}

// topicOffsets sums a topic's end offsets (records ever produced) and what is
// still retained.
func topicOffsets(t snapshot.Topic) (end, retained int64, ok bool) {
	for _, p := range t.Partitions {
		if p.StartOffset < 0 || p.EndOffset < 0 {
			return 0, 0, false
		}
		end += p.EndOffset
		retained += p.EndOffset - p.StartOffset
	}
	return end, retained, true
}

func (v *watchView) draw(now time.Time) {
	s := v.snap
	w := screens.Watch{
		Title: "watch",
		Empty: "no topics followed · add -t <topic> to chart its records/sec",
		Quiet: "no cluster events yet · leader moves, ISR changes and topic changes appear here",
	}
	if s != nil {
		w.Title = "watch · cluster " + s.ClusterID
		w.Right = fmt.Sprintf("gen %d · %s · %s", s.Generation, plural(len(s.Brokers), "broker"), plural(len(s.Topics), "topic"))
	}
	for _, t := range v.topics {
		w.Series = append(w.Series, *v.series[t])
	}
	b := v.stats.Budget
	w.Foot = fmt.Sprintf("budget %.1f · %s granted · %d denied · offsets every %s · metadata every %s",
		b.Tokens, screens.Num(float64(b.Granted)), b.Denied, v.cfg.FastInterval, v.cfg.MetadataInterval)
	if v.stats.LastError != nil {
		w.Foot = "refresh failing: " + v.stats.LastError.Error()
	}
	if v.dropped > 0 {
		w.Foot += fmt.Sprintf(" · %d events dropped", v.dropped)
	}
	for _, e := range v.events {
		age := now.Sub(e.at)
		if age > toastLife {
			break
		}
		t := toastFor(e.Event)
		t.Age, t.Life = age, 1-age.Seconds()/toastLife.Seconds()
		w.Toasts = append(w.Toasts, t)
	}
	v.lv.draw(w.Render(v.lv.s.kit, v.lv.s.width, v.lv.s.height-1))
}

// toastFor gives each event kind its glyph, colour and wording.
func toastFor(e engine.Event) screens.Toast {
	part := fmt.Sprintf("%s/%d", e.Topic, e.Partition)
	switch e.Kind {
	case engine.BrokerAdded:
		return screens.Toast{Role: kit.OK, Glyph: "✓", Title: "Broker joined", Subject: fmt.Sprintf("broker %d", e.Broker), Detail: "now serving metadata"}
	case engine.BrokerRemoved:
		return screens.Toast{Role: kit.Err, Glyph: "✗", Title: "Broker left", Subject: fmt.Sprintf("broker %d", e.Broker), Detail: "its partitions need new leaders"}
	case engine.ControllerMoved:
		return screens.Toast{Role: kit.Warn, Glyph: "▲", Title: "Controller moved", Subject: fmt.Sprintf("%d → %d", e.From, e.To), Detail: "a controller election took place"}
	case engine.TopicCreated:
		return screens.Toast{Role: kit.OK, Glyph: "✓", Title: "Topic created", Subject: e.Topic, Detail: plural(e.Count, "partition")}
	case engine.TopicDeleted:
		return screens.Toast{Role: kit.Err, Glyph: "✗", Title: "Topic deleted", Subject: e.Topic, Detail: "its data is gone"}
	case engine.PartitionsAdded:
		return screens.Toast{Role: kit.Info, Glyph: "•", Title: "Partitions added", Subject: e.Topic, Detail: fmt.Sprintf("+%d; keyed records may now land elsewhere", e.Count)}
	case engine.LeaderMoved:
		return screens.Toast{Role: kit.Info, Glyph: "•", Title: "Leader moved", Subject: part, Detail: fmt.Sprintf("broker %d → broker %d", e.From, e.To)}
	case engine.PartitionOffline:
		return screens.Toast{Role: kit.Err, Glyph: "✗", Title: "Partition offline", Subject: part, Detail: fmt.Sprintf("no leader; was broker %d", e.From)}
	case engine.ISRShrunk:
		return screens.Toast{Role: kit.Warn, Glyph: "▲", Title: "ISR shrunk", Subject: part, Detail: fmt.Sprintf("replicas %v fell out of sync", e.Replicas)}
	case engine.ISRExpanded:
		return screens.Toast{Role: kit.OK, Glyph: "✓", Title: "ISR expanded", Subject: part, Detail: fmt.Sprintf("replicas %v back in sync", e.Replicas)}
	}
	return screens.Toast{Role: kit.Muted, Glyph: "•", Title: string(e.Kind), Subject: e.Topic}
}

func plural(n int, s string) string {
	if n == 1 {
		return "1 " + s
	}
	if strings.HasSuffix(s, "x") || strings.HasSuffix(s, "s") || strings.HasSuffix(s, "ch") {
		return fmt.Sprintf("%d %ses", n, s)
	}
	return fmt.Sprintf("%d %ss", n, s)
}
