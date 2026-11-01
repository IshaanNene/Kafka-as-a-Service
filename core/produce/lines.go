package produce

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// MaxLine is the longest line EachLine reads as one record: Kafka's
// default message.max.bytes is about 1 MiB, so 16 MiB is room to spare.
const MaxLine = 16 << 20

// EachLine reads r as one record per line, as ParseLine does, and calls
// fn with each line's number from 1 and the bytes of r read so far,
// which is exact: progress on a file can't run ahead. A trailing \r is
// dropped, so files written on Windows read the same. fn's error stops
// the read and is returned as it is; a line that doesn't parse stops it
// with an error naming the line.
func EachLine(r io.Reader, keySep string, fn func(line int, rec Record, read int64) error) error {
	br := bufio.NewReaderSize(r, 64<<10)
	var read int64
	for n := 1; ; n++ {
		raw, err := readLine(br)
		if len(raw) == 0 && err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		read += int64(len(raw))
		text := string(raw)
		if l := len(text); l > 0 && text[l-1] == '\n' {
			text = text[:l-1]
		}
		if l := len(text); l > 0 && text[l-1] == '\r' {
			text = text[:l-1]
		}
		rec, perr := ParseLine(text, keySep)
		if perr != nil {
			return fmt.Errorf("line %d: %w", n, perr)
		}
		if err := fn(n, rec, read); err != nil {
			return err
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// readLine is one line with its newline, or what is left at the end.
func readLine(br *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		chunk, err := br.ReadSlice('\n')
		line = append(line, chunk...)
		if len(line) > MaxLine {
			return nil, fmt.Errorf("a line longer than %d MiB", MaxLine>>20)
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return line, err
		}
	}
}
