package pipe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
)

func init() {
	Register(Kind{
		Scheme:  "std",
		Summary: "the process's stdin or stdout, as JSON lines, text lines or CSV",
		Example: "stdin?format=text · stdout",
		Source: func(u *url.URL, env Env) (Source, error) {
			in := env.Stdin
			if in == nil {
				in = os.Stdin
			}
			return newStreamSource(io.NopCloser(in), query(u, "format", FormatJSONL), query(u, "batch", ""))
		},
		Sink: func(u *url.URL, env Env) (Sink, error) {
			out := env.Stdout
			if out == nil {
				out = os.Stdout
			}
			return newStreamSink(nopWriteCloser{out}, query(u, "format", FormatJSONL))
		},
	})
	Register(Kind{
		Scheme:  "file",
		Summary: "a file of JSON lines (.jsonl), CSV (.csv) or text; written files are appended to",
		Example: "file:orders.jsonl · file:/data/export.csv?format=csv",
		Source: func(u *url.URL, env Env) (Source, error) {
			path := pathOf(u)
			f, err := os.Open(path)
			if err != nil {
				return nil, err
			}
			return newStreamSource(f, formatOf(path, u.Query().Get("format")), query(u, "batch", ""))
		},
		Sink: func(u *url.URL, env Env) (Sink, error) {
			path := pathOf(u)
			flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
			if u.Query().Get("truncate") == "true" {
				flags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
			}
			f, err := os.OpenFile(path, flags, 0o644)
			if err != nil {
				return nil, err
			}
			return newStreamSink(f, formatOf(path, u.Query().Get("format")))
		},
	})
}

// streamSource reads a stream to its end in batches. It doesn't
// checkpoint: a stream read again starts over.
type streamSource struct {
	rc    io.ReadCloser
	dec   *decoder
	batch int
	done  bool
}

func newStreamSource(rc io.ReadCloser, format, batch string) (*streamSource, error) {
	n, err := batchSize(batch, 500)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}
	d, err := newDecoder(rc, format)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}
	return &streamSource{rc: rc, dec: d, batch: n}, nil
}

func (s *streamSource) Next(ctx context.Context) (Batch, error) {
	if s.done {
		return Batch{}, io.EOF
	}
	var b Batch
	for len(b.Records) < s.batch {
		if err := ctx.Err(); err != nil {
			return b, err
		}
		r, err := s.dec.next()
		if errors.Is(err, io.EOF) {
			s.done = true
			break
		}
		if err != nil {
			return b, err
		}
		b.Records = append(b.Records, r)
	}
	if len(b.Records) == 0 {
		return b, io.EOF
	}
	return b, nil
}

func (s *streamSource) Close() error { return s.rc.Close() }

type streamSink struct {
	wc  io.WriteCloser
	enc *encoder
}

func newStreamSink(wc io.WriteCloser, format string) (*streamSink, error) {
	e, err := newEncoder(wc, format)
	if err != nil {
		_ = wc.Close()
		return nil, err
	}
	return &streamSink{wc: wc, enc: e}, nil
}

func (s *streamSink) Write(_ context.Context, rs []Record) error {
	if err := s.enc.write(rs); err != nil {
		return fmt.Errorf("writing: %w", err)
	}
	if f, ok := s.wc.(*os.File); ok {
		return f.Sync() // durable before the source is told
	}
	return nil
}

func (s *streamSink) Close() error { return s.wc.Close() }

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
