package base

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

func ReverseFileGo(threadIdx int, files chan map[string]string, lengths map[string][][]int, keepTrx bool, wg *sync.WaitGroup, cfg *ConfCmd) {
	defer wg.Done()
	for pair := range files {
		if cfg.Err() != nil {
			continue
		}
		err := reverseFile(pair["tmp"], pair["rollback"], lengths[pair["tmp"]], keepTrx, cfg.newOutput)
		if err != nil {
			cfg.RecordError(err)
			continue
		}
		// The source is the only complete copy on any read/write/close failure.
		if err := os.Remove(pair["tmp"]); err != nil {
			cfg.RecordError(fmt.Errorf("remove tmp %s: %w", pair["tmp"], err))
		}
	}
}

func ReverseFileToNewFileOneByOneLineAndKeepTrxBatchRead(src, dest string, positions [][]int, keepTrx bool) error {
	return reverseFile(src, dest, positions, keepTrx, (&ConfCmd{}).newOutput)
}

func reverseFile(src, dest string, positions [][]int, keepTrx bool, open func(string) (io.WriteCloser, error)) (result error) {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open rollback tmp %s: %w", src, err)
	}
	defer func() {
		if err := in.Close(); result == nil && err != nil {
			result = fmt.Errorf("close rollback tmp: %w", err)
		}
	}()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	var total int64
	for _, p := range positions {
		if len(p) != 2 || p[0] <= 0 {
			return fmt.Errorf("invalid rollback block metadata")
		}
		total += int64(p[0])
	}
	if total != info.Size() {
		return fmt.Errorf("rollback metadata size %d != tmp size %d", total, info.Size())
	}
	out, err := open(dest)
	if err != nil {
		return fmt.Errorf("open rollback %s: %w", dest, err)
	}
	defer func() {
		if err := out.Close(); result == nil && err != nil {
			result = fmt.Errorf("close rollback %s: %w", dest, err)
		}
	}()
	lastTrx := 0
	for i := len(positions) - 1; i >= 0; i-- {
		p := positions[i]
		total -= int64(p[0])
		buf := make([]byte, p[0])
		if _, err := in.ReadAt(buf, total); err != nil {
			return fmt.Errorf("read rollback tmp: %w", err)
		}
		if keepTrx && lastTrx != p[1] {
			if err := writeOutput(out, "commit;\nbegin;\n"); err != nil {
				return fmt.Errorf("write rollback transaction: %w", err)
			}
		}
		lastTrx = p[1]
		lines := strings.Split(string(buf), "\n")
		for j := len(lines) - 1; j >= 0; j-- {
			if lines[j] == "" {
				continue
			}
			if err := writeOutput(out, lines[j]+"\n"); err != nil {
				return fmt.Errorf("write rollback %s: %w", dest, err)
			}
		}
	}
	if keepTrx {
		if err := writeOutput(out, "commit;\n"); err != nil {
			return fmt.Errorf("write rollback commit: %w", err)
		}
	}
	return nil
}
