package base

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
)

func testTableInfo(_ *ConfCmd, db, tb string) (*TblInfoJson, error) {
	return &TblInfoJson{Database: db, Table: tb, Columns: []FieldInfo{{FieldName: "id", FieldType: "int"}}, PrimaryKey: KeyInfo{"id"}}, nil
}

func installTestTables(t *testing.T) {
	t.Helper()
	G_TablesColumnsInfo.lock.Lock()
	oldMap, oldLoad := G_TablesColumnsInfo.tableInfos, G_TablesColumnsInfo.load
	G_TablesColumnsInfo.tableInfos = nil
	G_TablesColumnsInfo.load = testTableInfo
	G_TablesColumnsInfo.lock.Unlock()
	oldLocation := GBinlogTimeLocation
	GBinlogTimeLocation = time.UTC
	t.Cleanup(func() {
		G_TablesColumnsInfo.lock.Lock()
		G_TablesColumnsInfo.tableInfos = oldMap
		G_TablesColumnsInfo.load = oldLoad
		G_TablesColumnsInfo.lock.Unlock()
		GBinlogTimeLocation = oldLocation
	})
}

func waitWorkers(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("pipeline deadlocked")
	}
}

func sqlBlock(n int) ForwardRollbackSqlOfPrint {
	return ForwardRollbackSqlOfPrint{sqls: []string{fmt.Sprintf("delete %d", n)}, sqlInfo: ExtraSqlInfoOfPrint{schema: "db", table: "t", binlog: "mysql-bin.000001", trxIndex: uint64(n)}}
}

func TestForwardRollbackOutputOrder(t *testing.T) {
	installTestTables(t)
	for _, work := range []string{"2sql", "rollback"} {
		t.Run(work, func(t *testing.T) {
			cfg := &ConfCmd{WorkType: work, OutputDir: t.TempDir(), Threads: 3, EventChan: make(chan MyBinEvent), SqlChan: make(chan ForwardRollbackSqlOfPrint)}
			G_HandlingBinEventIndex = &BinEventHandlingIndx{EventIdx: 1}
			var gen, out sync.WaitGroup
			out.Add(1)
			go PrintExtraInfoForForwardRollbackupSql(cfg, &out)
			for i := uint(1); i <= 3; i++ {
				gen.Add(1)
				go GenForwardRollbackSqlFromBinEvent(i, cfg, &gen)
			}
			for i := 1; i <= 3; i++ {
				cfg.EventChan <- sqlRow(i)
			}
			close(cfg.EventChan)
			waitWorkers(t, &gen)
			close(cfg.SqlChan)
			waitWorkers(t, &out)
			if err := cfg.Err(); err != nil {
				t.Fatal(err)
			}
			name := GetForwardRollbackSqlFileName("db", "t", false, cfg.OutputDir, work == "rollback", "mysql-bin.000001", false)
			data, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			expected := []string{"100", "200", "300"}
			if work == "rollback" {
				expected = []string{"300", "200", "100"}
			}
			lines := strings.Split(strings.TrimSpace(string(data)), "\n")
			if len(lines) != 3 {
				t.Fatalf("SQL=%s", data)
			}
			for i, number := range expected {
				if !strings.Contains(lines[i], number) {
					t.Fatalf("order: %s", data)
				}
			}
		})
	}
}

func TestGeneratorFailureDoesNotStrandEventIndex(t *testing.T) {
	installTestTables(t)
	cfg := &ConfCmd{WorkType: "2sql", EventChan: make(chan MyBinEvent), SqlChan: make(chan ForwardRollbackSqlOfPrint)}
	G_HandlingBinEventIndex = &BinEventHandlingIndx{EventIdx: 1}
	var wg sync.WaitGroup
	for i := uint(1); i <= 3; i++ {
		wg.Add(1)
		go GenForwardRollbackSqlFromBinEvent(i, cfg, &wg)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Index 2 can be waiting while index 1 fails, then all must drain.
		cfg.EventChan <- sqlRow(2)
		bad := sqlRow(1)
		bad.SqlType = "invalid"
		cfg.EventChan <- bad
		for i := 3; i < 100; i++ {
			cfg.EventChan <- sqlRow(i)
		}
		close(cfg.EventChan)
	}()
	waitWorkers(t, &wg)
	<-done
	if cfg.Err() == nil {
		t.Fatal("generation failure lost")
	}
}

func sqlRow(i int) MyBinEvent {
	e := rowEvent("db", 100, uint32(i*100))
	return MyBinEvent{MyPos: mysql.Position{Name: "mysql-bin.000001", Pos: uint32(i * 100)}, EventIdx: uint64(i), IfRowsEvent: true, BinEvent: e.Event.(*replication.RowsEvent), SqlType: "insert", Timestamp: 100, TrxIndex: uint64(i)}
}

type faultOutput struct {
	data                        bytes.Buffer
	failWrite, short, failClose bool
	closed                      bool
}

func (w *faultOutput) Write(p []byte) (int, error) {
	if w.failWrite {
		return 0, io.ErrClosedPipe
	}
	if w.short {
		return len(p) - 1, nil
	}
	return w.data.Write(p)
}
func (w *faultOutput) Close() error {
	w.closed = true
	if w.failClose {
		return io.ErrClosedPipe
	}
	return nil
}

func TestSQLWriteFailuresDrainAndNeverReportSuccess(t *testing.T) {
	for _, mode := range []string{"open", "write", "flush", "short-flush", "close", "reverse-open", "reverse-write", "reverse-close"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &ConfCmd{WorkType: "2sql", OutputDir: t.TempDir(), Threads: 2, SqlChan: make(chan ForwardRollbackSqlOfPrint)}
			if strings.HasPrefix(mode, "reverse") {
				cfg.WorkType = "rollback"
			}
			bad := &faultOutput{failWrite: mode == "write" || mode == "flush" || mode == "reverse-write", short: mode == "short-flush", failClose: mode == "close" || mode == "reverse-close"}
			cfg.openOutput = func(name string) (io.WriteCloser, error) {
				if cfg.WorkType == "rollback" && strings.HasPrefix(filepath.Base(name), ".") {
					return os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
				}
				if mode == "open" || mode == "reverse-open" {
					return nil, os.ErrPermission
				}
				return bad, nil
			}
			var logs bytes.Buffer
			handler, err := log.NewStreamHandler(&logs)
			if err != nil {
				t.Fatal(err)
			}
			log.SetDefaultLogger(log.NewDefault(handler))
			defer func() { h, _ := log.NewStreamHandler(os.Stderr); log.SetDefaultLogger(log.NewDefault(h)) }()
			var wg sync.WaitGroup
			wg.Add(1)
			go PrintExtraInfoForForwardRollbackupSql(cfg, &wg)
			producer := make(chan struct{})
			go func() {
				defer close(producer)
				for i := 0; i < 40; i++ {
					sc := sqlBlock(i)
					if mode == "write" {
						sc.sqls = []string{strings.Repeat("x", 8192)}
					}
					cfg.SqlChan <- sc
				}
				close(cfg.SqlChan)
			}()
			waitWorkers(t, &wg)
			<-producer
			if cfg.Err() == nil {
				t.Fatal("output failure returned success")
			}
			if strings.Contains(logs.String(), "finish") {
				t.Fatalf("success marker on failure: %s", logs.String())
			}
			if strings.HasPrefix(mode, "reverse") {
				tmp := GetForwardRollbackSqlFileName("db", "t", false, cfg.OutputDir, true, "mysql-bin.000001", true)
				if info, err := os.Stat(tmp); err != nil || info.Size() == 0 {
					t.Fatalf("lost only tmp: %v", err)
				}
			}
		})
	}
}

func TestRollbackTransactionOrderAndInvalidMetadata(t *testing.T) {
	dir := t.TempDir()
	src, dest := filepath.Join(dir, "tmp"), filepath.Join(dir, "rollback")
	if err := os.WriteFile(src, []byte("one;\ntwo;\nthree;\n"), 0600); err != nil {
		t.Fatal(err)
	}
	positions := [][]int{{5, 1}, {5, 1}, {7, 2}}
	if err := ReverseFileToNewFileOneByOneLineAndKeepTrxBatchRead(src, dest, positions, true); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "commit;\nbegin;\nthree;\ncommit;\nbegin;\ntwo;\none;\ncommit;\n" {
		t.Fatalf("rollback=%q", data)
	}
	if err := ReverseFileToNewFileOneByOneLineAndKeepTrxBatchRead(src, dest, positions[:2], false); err == nil {
		t.Fatal("accepted incomplete metadata")
	}
	preserved, err := os.ReadFile(dest)
	if err != nil || !bytes.Equal(data, preserved) {
		t.Fatal("invalid metadata truncated destination")
	}
}

func TestStatsWriteAndCloseFailures(t *testing.T) {
	installTestTables(t)
	for _, mode := range []string{"write", "close"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &ConfCmd{WorkType: "stats", OutputDir: t.TempDir(), Threads: 1}
			if err := cfg.InitOutput(); err != nil {
				t.Fatal(err)
			}
			if err := cfg.StatFH.Close(); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			wg.Add(1)
			go ProcessBinEventStats(cfg, &wg)
			if mode == "write" {
				for i := 0; i < 20; i++ {
					cfg.StatChan <- BinEventStats{Timestamp: 100, Binlog: "mysql-bin.000001", Database: "db", Table: "t", QueryType: "insert", RowCnt: 1}
				}
			}
			close(cfg.StatChan)
			waitWorkers(t, &wg)
			cfg.CloseFH()
			if cfg.Err() == nil {
				t.Fatal("stats failure lost")
			}
		})
	}
}

func TestTableCacheConcurrentCompletePublication(t *testing.T) {
	var columns, keys atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	db := sql.OpenDB(&offlineConnector{query: func(q string) (driver.Rows, error) {
		if strings.HasPrefix(q, "SHOW COLUMNS") {
			columns.Add(1)
			return &catalogRows{columns: []string{"Field", "Type", "Null", "Key", "Default", "Extra"}, values: [][]driver.Value{{"id", "int", "NO", "PRI", nil, ""}}}, nil
		}
		if strings.HasPrefix(q, "SHOW INDEX") {
			if keys.Add(1) == 1 {
				close(entered)
				<-release
			}
			return &catalogRows{columns: []string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name"}, values: [][]driver.Value{{"t", "0", "PRIMARY", "1", "id"}}}, nil
		}
		return nil, fmt.Errorf("unexpected query %s", q)
	}})
	defer db.Close()
	cfg := &ConfCmd{FromDB: db}
	cache := &TablesColumnsInfo{}
	var wg sync.WaitGroup
	get := func(table string) {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			info, err := cache.getTableInfo(cfg, "db", table)
			if err != nil || info == nil || len(info.Columns) != 1 || len(info.PrimaryKey) != 1 {
				t.Errorf("partial cache: %+v %v", info, err)
				return
			}
		}
	}
	wg.Add(1)
	go get("t0")
	<-entered
	cache.lock.RLock()
	count := len(cache.tableInfos)
	cache.lock.RUnlock()
	if count != 0 {
		t.Fatal("published columns before keys")
	}
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go get(fmt.Sprintf("t%d", i%8))
	}
	close(release)
	waitWorkers(t, &wg)
	if columns.Load() != 8 || keys.Load() != 8 {
		t.Fatalf("loads columns=%d keys=%d", columns.Load(), keys.Load())
	}
}

func TestTableCacheFailedLoadIsNotPublished(t *testing.T) {
	tries := 0
	cache := &TablesColumnsInfo{load: func(c *ConfCmd, db, tb string) (*TblInfoJson, error) {
		tries++
		if tries == 1 {
			return &TblInfoJson{Columns: []FieldInfo{{FieldName: "id"}}}, io.ErrUnexpectedEOF
		}
		return testTableInfo(c, db, tb)
	}}
	if _, err := cache.GetTableInfoJson("db", "t"); err == nil {
		t.Fatal("lost load error")
	}
	info, err := cache.GetTableInfoJson("db", "t")
	if err != nil || len(info.PrimaryKey) != 1 || tries != 2 {
		t.Fatalf("retry: %+v %v", info, err)
	}
}
