package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/siddontang/go-log/log"
	my "my2sql/base"
)

func TestPipelineResultIncludesEveryWorkerAndClose(t *testing.T) {
	oldLocation := my.GBinlogTimeLocation
	my.GBinlogTimeLocation = time.UTC
	defer func() { my.GBinlogTimeLocation = oldLocation }()
	for _, work := range []string{"2sql", "rollback", "stats"} {
		t.Run(work, func(t *testing.T) {
			for _, mode := range []string{"success", "parse-error", "connection-error", "stats-write", "stats-close", "generator-error"} {
				if work == "stats" && mode == "generator-error" {
					continue
				}
				t.Run(mode, func(t *testing.T) {
					cfg := &my.ConfCmd{WorkType: work, OutputDir: t.TempDir(), Threads: 3, PrintInterval: 1}
					if err := cfg.InitOutput(); err != nil {
						t.Fatal(err)
					}
					defer cfg.CloseResources()
					if strings.HasPrefix(mode, "stats-") {
						if err := cfg.StatFH.Close(); err != nil {
							t.Fatal(err)
						}
					}
					var logs bytes.Buffer
					handler, err := log.NewStreamHandler(&logs)
					if err != nil {
						t.Fatal(err)
					}
					log.SetDefaultLogger(log.NewDefault(handler))
					defer func() { h, _ := log.NewStreamHandler(os.Stderr); log.SetDefaultLogger(log.NewDefault(h)) }()
					connectionErr := &net.OpError{Op: "read", Net: "tcp", Err: io.ErrUnexpectedEOF}
					done := make(chan error, 1)
					go func() {
						done <- runPipeline(cfg, func() error {
							defer cfg.CloseChan()
							if mode == "parse-error" {
								return context.DeadlineExceeded
							}
							if mode == "connection-error" {
								cfg.StatChan <- my.BinEventStats{Timestamp: 100, Binlog: "mysql-bin.000001", Database: "db", Table: "t", QueryType: "insert", RowCnt: 1}
								return fmt.Errorf("未到物理边界，读取 binlog 失败（结果不完整）: %w", connectionErr)
							}
							if mode == "stats-write" {
								for i := 0; i < 40; i++ {
									cfg.StatChan <- my.BinEventStats{Timestamp: uint32(100 + i), Binlog: "mysql-bin.000001", Database: "db", Table: "t", QueryType: "insert", RowCnt: 1}
								}
							}
							if mode == "generator-error" {
								for i := 1; i < 40; i++ {
									cfg.EventChan <- my.MyBinEvent{EventIdx: uint64(i)}
								}
							}
							return nil
						})
					}()
					select {
					case err = <-done:
					case <-time.After(3 * time.Second):
						t.Fatal("main pipeline deadlocked")
					}
					success := mode == "success"
					if (err == nil) != success {
						t.Fatalf("result=%v", err)
					}
					if mode == "connection-error" && (!errors.Is(err, connectionErr) || !errors.Is(err, io.ErrUnexpectedEOF)) {
						t.Fatalf("流水线丢失连接错误原因: %v", err)
					}
					if strings.Contains(logs.String(), "finish parsing and writing all output files") != success {
						t.Fatalf("incorrect completion marker: %s", logs.String())
					}
				})
			}
		})
	}
}
