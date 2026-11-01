//go:build integration

package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/IshaanNene/Kafka-as-a-Service/test/kafkatest"
)

// TestIntegrationCLI drives kaas end to end against a real broker of
// each version CI runs, as an operator would: create a topic, load it,
// read it back, move it through a pipe as a consumer group, rewind the
// group and replay, change a config, and snapshot the result.
func TestIntegrationCLI(t *testing.T) {
	b := strings.Join(kafkatest.Real(t), ",")
	kaas := func(args ...string) string { return runKaas(t, append(args, "-b", b)...) }

	kaas("topics", "create", "orders", "-p", "3")
	kaas("topics", "create", "big", "-p", "3")
	kaas("pipe", "--from", "gen:orders?count=500&seed=2", "--to", "kafka:orders")

	lines := strings.Split(strings.TrimSpace(kaas("consume", "orders", "--from", "beginning", "--exit", "-o", "json")), "\n")
	if len(lines) != 500 {
		t.Fatalf("consumed %d records, produced 500", len(lines))
	}

	where := "value.amount > 250.0"
	var st pipeStatsJSON
	if err := json.Unmarshal([]byte(kaas("pipe", "--from", "kafka:orders?group=etl&from=start&stop=end", "--to", "kafka:big", "--where", where, "-o", "json")), &st); err != nil {
		t.Fatal(err)
	}
	if st.Read != 500 || st.Written == 0 || st.Written+st.Filtered != 500 {
		t.Fatalf("first pass %+v", st)
	}
	// The group committed what it wrote: a second run has nothing to do.
	var again pipeStatsJSON
	_ = json.Unmarshal([]byte(kaas("pipe", "--from", "kafka:big?group=check&from=start&stop=end", "--to", "file:"+t.TempDir()+"/big.jsonl", "-o", "json")), &again)
	if again.Read != st.Written {
		t.Fatalf("big has %d records, the pipe wrote %d", again.Read, st.Written)
	}
	var none pipeStatsJSON
	_ = json.Unmarshal([]byte(kaas("pipe", "--from", "kafka:orders?group=etl&stop=end", "--to", "kafka:big", "-o", "json")), &none)
	if none.Read != 0 {
		t.Fatalf("a caught-up group read %d", none.Read)
	}
	// Rewind it and the pipe replays the topic.
	kaas("offsets", "reset", "etl", "-t", "orders", "--to", "earliest", "--yes")
	var replay pipeStatsJSON
	_ = json.Unmarshal([]byte(kaas("pipe", "--from", "kafka:orders?group=etl&stop=end", "--to", "kafka:big", "--where", where, "-o", "json")), &replay)
	if replay.Read != 500 || replay.Written != st.Written {
		t.Fatalf("replay %+v, first pass wrote %d", replay, st.Written)
	}

	kaas("config", "edit", "orders", "--set", "retention.ms=3600000")
	if out := kaas("config", "orders", "-o", "json"); !strings.Contains(out, `"retention.ms"`) || !strings.Contains(out, "3600000") {
		t.Errorf("retention.ms not set:\n%s", out)
	}
	if out := kaas("snapshot", "-o", "json"); !strings.Contains(out, `"big"`) || !strings.Contains(out, `"orders"`) {
		t.Errorf("snapshot:\n%s", out)
	}
}
