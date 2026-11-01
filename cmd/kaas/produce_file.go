package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/IshaanNene/Kafka-as-a-Service/core/produce"
	"github.com/IshaanNene/Kafka-as-a-Service/ui/screens"
)

// uploadParallel is how many files send at once.
const uploadParallel = 4

// upload is kaas produce --file's state, shared by the file workers and
// the screen.
type upload struct {
	mu      sync.Mutex
	files   []screens.UploadFile
	paths   []string
	started []time.Time
	errs    []error
}

func (u *upload) update(i int, fn func(f *screens.UploadFile)) {
	u.mu.Lock()
	defer u.mu.Unlock()
	fn(&u.files[i])
}

// files sends each file's lines as records, uploadParallel at a time,
// drawing the upload-dropzone list on a terminal and the tray at the end.
func (p producer) files(ctx context.Context, paths []string, keySep string, surf surface, cluster string) error {
	return p.run(ctx, newUpload(paths), keySep, surf, cluster)
}

func newUpload(paths []string) *upload {
	u := &upload{paths: paths, started: make([]time.Time, len(paths)), errs: make([]error, len(paths))}
	for _, path := range paths {
		f := screens.UploadFile{Name: filepath.Base(path), State: screens.Queued}
		if st, err := os.Stat(path); err == nil {
			f.Size = st.Size()
		}
		u.files = append(u.files, f)
	}
	return u
}

// run sends u's files and reports them.
func (p producer) run(ctx context.Context, u *upload, keySep string, surf surface, cluster string) error {
	paths := u.paths
	var enc *json.Encoder
	if p.json {
		enc = json.NewEncoder(p.out)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		sem := make(chan struct{}, uploadParallel)
		for i := range paths {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
			}
			if ctx.Err() != nil {
				u.update(i, func(f *screens.UploadFile) { f.State, f.Err = screens.Failed, "not started: stopped" })
				u.errs[i] = ctx.Err()
				continue
			}
			wg.Add(1)
			go func() {
				defer func() { <-sem; wg.Done() }()
				p.sendFile(ctx, u, i, keySep, enc)
			}()
		}
		wg.Wait()
	}()

	rich := surf.rich && !p.json
	if rich {
		lv := &live{s: surf}
		defer lv.done()
		frame := func() []string {
			return u.screen(p, keySep, cluster).Render(surf.kit, surf.width, min(surf.height-1, 26))
		}
		lv.draw(frame())
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
	loop:
		for {
			select {
			case <-done:
				break loop
			case <-t.C:
				lv.draw(frame())
			}
		}
		if ctx.Err() != nil {
			lv.draw(frame()) // where it stopped, not the tray
		} else {
			lv.draw(u.tray(p.topic).Render(surf.kit, surf.width))
		}
	} else {
		<-done
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !rich && !p.json {
		for i, f := range u.files {
			if u.errs[i] == nil {
				fmt.Fprintf(p.out, "%s: %s\n", u.paths[i], plural(int(f.Records), "record"))
			}
		}
	}
	err := u.outcome()
	if err != nil && rich {
		return shown("upload_failed", err) // the red tray says which and why
	}
	return err
}

// sendFile sends one file in batches, keeping its progress current.
func (p producer) sendFile(ctx context.Context, u *upload, i int, keySep string, enc *json.Encoder) {
	start := time.Now()
	u.started[i] = start
	u.update(i, func(f *screens.UploadFile) { f.State = screens.Sending })
	fail := func(err error) {
		u.errs[i] = err
		u.update(i, func(f *screens.UploadFile) { f.State, f.Err = screens.Failed, err.Error() })
	}
	fh, err := os.Open(u.paths[i])
	if err != nil {
		fail(err)
		return
	}
	defer func() { _ = fh.Close() }() // read-only: nothing to lose

	var batch []produce.Record
	var lines []int
	var read int64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		res := produce.Send(ctx, p.cl, p.topic, batch)
		var sent int64
		var first error
		for j, r := range res {
			if r.Err != nil {
				if first == nil {
					first = fmt.Errorf("line %d: %w", lines[j], r.Err)
				}
				continue
			}
			sent++
			if enc != nil {
				u.mu.Lock()
				err := enc.Encode(resultJSON{Topic: p.topic, Partition: r.Partition, Offset: r.Offset, Timestamp: r.Timestamp, File: u.paths[i], Line: lines[j]})
				u.mu.Unlock()
				if err != nil && first == nil {
					first = err
				}
			}
		}
		at := read
		u.update(i, func(f *screens.UploadFile) {
			f.Records += sent
			f.Read = at
			if secs := time.Since(start).Seconds(); secs > 0 {
				f.Rate = float64(f.Records) / secs
			}
		})
		batch, lines = batch[:0], lines[:0]
		return first
	}
	err = produce.EachLine(fh, keySep, func(n int, r produce.Record, at int64) error {
		r.Headers, r.Partition = p.headers, p.partition
		batch, lines, read = append(batch, r), append(lines, n), at
		if len(batch) == produceBatch {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		fail(err)
		return
	}
	u.update(i, func(f *screens.UploadFile) { f.State, f.Read = screens.Sent, f.Size })
}

// totals is the files sent, bytes read and records sent so far.
func (u *upload) totals() (sent int, read, size, records int64) {
	for _, f := range u.files {
		if f.State == screens.Sent {
			sent++
		}
		read += f.Read
		size += f.Size
		records += f.Records
	}
	return sent, read, size, records
}

// screen is the upload-dropzone list now.
func (u *upload) screen(p producer, keySep, cluster string) screens.Upload {
	u.mu.Lock()
	defer u.mu.Unlock()
	sent, read, size, records := u.totals()
	v := screens.Upload{
		Title: "Upload to " + p.topic, Right: "cluster " + short(cluster),
		Zone:   fmt.Sprintf("%s · %s · each line becomes a record", plural(len(u.files), "file"), screens.Bytes(size)),
		Files:  append([]screens.UploadFile(nil), u.files...),
		Foot:   fmt.Sprintf("%d of %d sent · %s of %s · %s records", sent, len(u.files), screens.Bytes(read), screens.Bytes(size), screens.Num(float64(records))),
		Action: "ctrl-c to stop",
	}
	var hint []string
	if keySep != "" {
		hint = append(hint, fmt.Sprintf("key before %q", keySep))
	}
	if len(p.headers) > 0 {
		hint = append(hint, plural(len(p.headers), "header")+" on every record")
	}
	if p.partition >= 0 {
		hint = append(hint, fmt.Sprintf("all to partition %d", p.partition))
	}
	hint = append(hint, fmt.Sprintf("%d files at a time", uploadParallel), "records sent stay sent if you stop")
	v.Hint = strings.Join(hint, " · ")
	return v
}

// tray is the empty-upload-tray card for the finished upload.
func (u *upload) tray(topic string) screens.Tray {
	u.mu.Lock()
	defer u.mu.Unlock()
	sent, _, size, records := u.totals()
	first := u.files[0].Name
	if len(u.files) > 1 {
		first += fmt.Sprintf(" and %d more", len(u.files)-1)
	}
	t := screens.Tray{
		Count:  fmt.Sprintf(" %s ", plural(len(u.files), "file")),
		Title:  "Upload complete",
		Detail: fmt.Sprintf("%s · %s · %s records", first, screens.Bytes(size), screens.Num(float64(records))),
		Action: "kaas consume " + topic + " →",
	}
	if failed := len(u.files) - sent; failed > 0 {
		for i, f := range u.files {
			if f.State == screens.Failed {
				t.Detail = f.Name + " " + u.errs[i].Error()
				break
			}
		}
		t.Count, t.Title, t.Failed = fmt.Sprintf(" %d of %d ", sent, len(u.files)), plural(failed, "file")+" failed", true
		t.Action = fmt.Sprintf("%s records from the rest are in %s", screens.Num(float64(records)), topic)
	}
	return t
}

// outcome is the error to exit with: the first failed file, and how many.
func (u *upload) outcome() error {
	var failed []string
	var first error
	for i, err := range u.errs {
		if err != nil {
			failed = append(failed, u.paths[i])
			if first == nil {
				first = fmt.Errorf("%s: %w", u.paths[i], err)
			}
		}
	}
	switch len(failed) {
	case 0:
		return nil
	case 1:
		return first
	}
	return fmt.Errorf("%d of %s failed: %w", len(failed), plural(len(u.paths), "file"), first)
}
