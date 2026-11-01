package pipe

import (
	"bufio"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
	"unicode/utf8"
)

// Formats records are read and written in.
const (
	FormatJSONL = "jsonl" // a JSON object a line, as kaas consume -o json writes
	FormatText  = "text"  // a value a line
	FormatCSV   = "csv"   // key,value with a header line
)

// RecordJSON is a record as a JSON line: the shape kaas consume -o json
// writes, so a sample of a topic replays through a pipe unchanged. A key
// or value that isn't UTF-8 text is base64, and encoding says so.
type RecordJSON struct {
	Topic     string    `json:"topic,omitempty"`
	Partition *int32    `json:"partition,omitempty"`
	Offset    *int64    `json:"offset,omitempty"`
	Timestamp time.Time `json:"timestamp,omitzero"`
	Key       *string   `json:"key"`
	Value     *string   `json:"value"`
	Encoding  string    `json:"encoding,omitempty"`
	Headers   []Header  `json:"headers,omitempty"`
}

// ToJSON is r as a JSON line's object.
func ToJSON(r Record) RecordJSON {
	j := RecordJSON{Topic: r.Topic, Timestamp: r.Time, Headers: r.Headers}
	if r.Topic != "" {
		p, o := r.Partition, r.Offset
		j.Partition, j.Offset = &p, &o
	}
	binary := (r.Key != nil && !utf8.Valid(r.Key)) || (r.Value != nil && !utf8.Valid(r.Value))
	enc := func(b []byte) *string {
		if b == nil {
			return nil
		}
		s := string(b)
		if binary {
			s = base64.StdEncoding.EncodeToString(b)
		}
		return &s
	}
	if binary {
		j.Encoding = "base64"
	}
	j.Key, j.Value = enc(r.Key), enc(r.Value)
	return j
}

// FromJSON is a JSON line's record.
func FromJSON(j RecordJSON) (Record, error) {
	r := Record{Topic: j.Topic, Time: j.Timestamp, Headers: j.Headers}
	if j.Partition != nil {
		r.Partition = *j.Partition
	}
	if j.Offset != nil {
		r.Offset = *j.Offset
	}
	dec := func(s *string) ([]byte, error) {
		switch {
		case s == nil:
			return nil, nil
		case j.Encoding == "base64":
			return base64.StdEncoding.DecodeString(*s)
		case j.Encoding == "" || j.Encoding == "utf8" || j.Encoding == "text":
			return []byte(*s), nil
		}
		return nil, fmt.Errorf("unknown encoding %q: want base64 or none", j.Encoding)
	}
	var err error
	if r.Key, err = dec(j.Key); err != nil {
		return r, fmt.Errorf("key: %w", err)
	}
	if r.Value, err = dec(j.Value); err != nil {
		return r, fmt.Errorf("value: %w", err)
	}
	return r, nil
}

// decoder reads records from r in a format.
type decoder struct {
	format string
	br     *bufio.Reader
	csv    *csv.Reader
	line   int
	header []string
}

func newDecoder(r io.Reader, format string) (*decoder, error) {
	d := &decoder{format: format, br: bufio.NewReaderSize(r, 1<<16)}
	switch format {
	case FormatJSONL, FormatText:
	case FormatCSV:
		d.csv = csv.NewReader(d.br)
		d.csv.FieldsPerRecord = -1
	default:
		return nil, fmt.Errorf("format %q: want jsonl, text or csv", format)
	}
	return d, nil
}

// next is the next record, io.EOF at the end.
func (d *decoder) next() (Record, error) {
	if d.format == FormatCSV {
		return d.nextCSV()
	}
	for {
		line, err := d.br.ReadBytes('\n')
		if len(line) == 0 && err != nil {
			return Record{}, err
		}
		d.line++
		line = trimEOL(line)
		if d.format == FormatText {
			return Record{Value: append([]byte(nil), line...)}, nil
		}
		if len(line) == 0 {
			if err != nil {
				return Record{}, err
			}
			continue // a blank line between objects
		}
		var j RecordJSON
		if jerr := json.Unmarshal(line, &j); jerr != nil {
			return Record{}, fmt.Errorf("line %d: not a JSON record: %w", d.line, jerr)
		}
		r, cerr := FromJSON(j)
		if cerr != nil {
			return Record{}, fmt.Errorf("line %d: %w", d.line, cerr)
		}
		return r, nil
	}
}

// nextCSV reads key,value rows under a header naming key and value, and
// any other column as a header of the record.
func (d *decoder) nextCSV() (Record, error) {
	if d.header == nil {
		h, err := d.csv.Read()
		if err != nil {
			return Record{}, err
		}
		d.header = h
	}
	row, err := d.csv.Read()
	if err != nil {
		return Record{}, err
	}
	var r Record
	for i, col := range row {
		if i >= len(d.header) {
			break
		}
		switch name := d.header[i]; name {
		case "key":
			r.Key = []byte(col)
		case "value":
			r.Value = []byte(col)
		case "timestamp":
			if t, err := time.Parse(time.RFC3339Nano, col); err == nil {
				r.Time = t
			}
		default:
			r.Headers = append(r.Headers, Header{Key: name, Value: col})
		}
	}
	return r, nil
}

// Decode reads every record in r.
func Decode(r io.Reader, format string) ([]Record, error) {
	d, err := newDecoder(r, format)
	if err != nil {
		return nil, err
	}
	var out []Record
	for {
		rec, err := d.next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
}

// MaxLine is the longest line a plugin may write.
const MaxLine = 32 << 20

func trimEOL(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// encoder writes records to w in a format.
type encoder struct {
	format string
	w      *bufio.Writer
	csv    *csv.Writer
	wrote  bool
}

func newEncoder(w io.Writer, format string) (*encoder, error) {
	e := &encoder{format: format, w: bufio.NewWriterSize(w, 1<<16)}
	switch format {
	case FormatJSONL, FormatText:
	case FormatCSV:
		e.csv = csv.NewWriter(e.w)
	default:
		return nil, fmt.Errorf("format %q: want jsonl, text or csv", format)
	}
	return e, nil
}

func (e *encoder) write(rs []Record) error {
	for _, r := range rs {
		switch e.format {
		case FormatText:
			// A bufio.Writer keeps its first error for Flush, below.
			_, _ = e.w.Write(r.Value)
			_ = e.w.WriteByte('\n')
		case FormatCSV:
			if !e.wrote {
				if err := e.csv.Write([]string{"key", "value", "timestamp"}); err != nil {
					return err
				}
			}
			ts := ""
			if !r.Time.IsZero() {
				ts = r.Time.UTC().Format(time.RFC3339Nano)
			}
			if err := e.csv.Write([]string{string(r.Key), string(r.Value), ts}); err != nil {
				return err
			}
		default:
			b, err := json.Marshal(ToJSON(r))
			if err != nil {
				return err
			}
			_, _ = e.w.Write(b)
			_ = e.w.WriteByte('\n')
		}
		e.wrote = true
	}
	if e.csv != nil {
		e.csv.Flush()
		if err := e.csv.Error(); err != nil {
			return err
		}
	}
	return e.w.Flush()
}

// formatOf is a file's format: its format= parameter, else its extension.
func formatOf(path, param string) string {
	if param != "" {
		return param
	}
	for _, ext := range []struct{ suffix, format string }{
		{".jsonl", FormatJSONL}, {".ndjson", FormatJSONL}, {".json", FormatJSONL}, {".csv", FormatCSV},
	} {
		if len(path) > len(ext.suffix) && path[len(path)-len(ext.suffix):] == ext.suffix {
			return ext.format
		}
	}
	return FormatText
}

func batchSize(s string, def int) (int, error) {
	if s == "" {
		return def, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("batch=%s: want a positive number", s)
	}
	return n, nil
}
