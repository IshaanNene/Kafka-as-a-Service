// Package produce builds records from what a person typed or piped and
// writes them: header and line parsing, JSON checking with a line and
// column for the first problem, and a send that reports where each record
// landed.
package produce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Header is one record header.
type Header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Record is one record to write. A nil Key has no key; a nil Value is a
// tombstone.
type Record struct {
	Key       []byte
	Value     []byte
	Headers   []Header
	Partition int32 // -1 lets the partitioner choose
}

// ParseHeader reads "key=value"; the value may hold more '='.
func ParseHeader(s string) (Header, error) {
	k, v, ok := strings.Cut(s, "=")
	if !ok || strings.TrimSpace(k) == "" {
		return Header{}, fmt.Errorf("%q: want key=value", s)
	}
	return Header{Key: strings.TrimSpace(k), Value: v}, nil
}

// ParseHeaders reads one header per non-blank line, as the editor's
// headers tab holds them. The error names the first bad line, from 1.
func ParseHeaders(text string) ([]Header, int, error) {
	var out []Header
	for i, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		h, err := ParseHeader(line)
		if err != nil {
			return nil, i + 1, err
		}
		out = append(out, h)
	}
	return out, 0, nil
}

// ParseLine reads one piped line as kafka-console-producer does: the whole
// line is the value, or with keySep set, the part before its first
// occurrence is the key. A line without the separator is an error then,
// as a silently keyless record would land on the wrong partition.
func ParseLine(line, keySep string) (Record, error) {
	r := Record{Partition: -1}
	if keySep == "" {
		r.Value = []byte(line)
		return r, nil
	}
	k, v, ok := strings.Cut(line, keySep)
	if !ok {
		return r, fmt.Errorf("no %q between key and value", keySep)
	}
	r.Key, r.Value = []byte(k), []byte(v)
	return r, nil
}

// Problem is where text fails to parse as JSON, from 1.
type Problem struct {
	Line, Col int
	Msg       string
}

// CheckJSON reports the first JSON problem in text, or nil when it is
// valid. Blank text is not JSON, but it is not a problem either: the
// editor's starting state.
func CheckJSON(text string) *Problem {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(text))
	var v any
	err := dec.Decode(&v)
	if err == nil {
		end := int(dec.InputOffset())
		rest := strings.TrimLeft(text[end:], " \t\r\n")
		if rest == "" {
			return nil
		}
		l, c := position(text, len(text)-len(rest))
		return &Problem{Line: l, Col: c, Msg: "more after the end of the JSON value"}
	}
	offset := len(text)
	var se *json.SyntaxError
	var te *json.UnmarshalTypeError
	switch {
	case errors.As(err, &se):
		offset = int(se.Offset)
	case errors.As(err, &te):
		offset = int(te.Offset)
	}
	l, c := position(text, max(0, offset-1))
	return &Problem{Line: l, Col: c, Msg: strings.TrimPrefix(err.Error(), "invalid character ")}
}

// LooksJSON is whether text should be checked as JSON: it opens an object
// or an array.
func LooksJSON(text string) bool {
	t := strings.TrimSpace(text)
	return strings.HasPrefix(t, "{") || strings.HasPrefix(t, "[")
}

// Compact is text without the editor's indentation when it is valid JSON,
// so a pretty-printed value is not stored with its newlines.
func Compact(text string) []byte {
	var b bytes.Buffer
	if err := json.Compact(&b, []byte(text)); err != nil {
		return []byte(text)
	}
	return b.Bytes()
}

func position(text string, offset int) (line, col int) {
	line, col = 1, 1
	for i, r := range text {
		if i >= offset {
			break
		}
		if r == '\n' {
			line, col = line+1, 1
			continue
		}
		col++
	}
	return line, col
}

// Result is where a record landed, or why it did not.
type Result struct {
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Err       error     `json:"-"`
}

// Send writes recs to topic in order and waits for every ack. A record's
// Partition is used only when cl was built with kgo.ManualPartitioner.
func Send(ctx context.Context, cl *kgo.Client, topic string, recs []Record) []Result {
	out := make([]Result, len(recs))
	krs := make([]*kgo.Record, len(recs))
	for i, r := range recs {
		kr := &kgo.Record{Topic: topic, Key: r.Key, Value: r.Value, Partition: r.Partition}
		for _, h := range r.Headers {
			kr.Headers = append(kr.Headers, kgo.RecordHeader{Key: h.Key, Value: []byte(h.Value)})
		}
		krs[i] = kr
	}
	// ProduceSync returns results as acks arrive, not in recs order, so each
	// is matched back to its record.
	at := make(map[*kgo.Record]int, len(krs))
	for i, kr := range krs {
		at[kr] = i
	}
	for _, pr := range cl.ProduceSync(ctx, krs...) {
		i := at[pr.Record]
		out[i] = Result{Partition: pr.Record.Partition, Offset: pr.Record.Offset, Timestamp: pr.Record.Timestamp, Err: pr.Err}
		if pr.Err != nil {
			out[i].Partition, out[i].Offset = -1, -1
		}
	}
	return out
}
