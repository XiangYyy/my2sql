package base

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func boundedConfig(pos uint32) *ConfCmd {
	return &ConfCmd{WorkType: "stats", StartFile: "mysql-bin.000001", StartPos: 4,
		StopFilePos: mysql.Position{Name: "mysql-bin.000001", Pos: pos}, IfSetStopFilePos: true,
		StatChan: make(chan BinEventStats, 32), EventChan: make(chan MyBinEvent, 32)}
}

func rowEvent(db string, timestamp, pos uint32) *replication.BinlogEvent {
	e := probeEvent(replication.WRITE_ROWS_EVENTv2, timestamp, pos)
	e.Header.EventSize = 20
	e.Event = &replication.RowsEvent{Table: &replication.TableMapEvent{Schema: []byte(db), Table: []byte("t"), ColumnType: []byte{mysql.MYSQL_TYPE_LONG}, ColumnMeta: []uint16{0}}, Rows: [][]interface{}{{int32(pos)}}}
	return e
}

func TestReplicationInclusivePhysicalBoundary(t *testing.T) {
	for _, kind := range []string{"rows", "time-filter", "database-filter", "xid", "rotate", "format"} {
		t.Run(kind, func(t *testing.T) {
			cfg := boundedConfig(100)
			cfg.IfSetStopDateTime, cfg.StopDatetime = true, 200
			last := rowEvent("wanted", 150, 100)
			want := 1
			switch kind {
			case "time-filter":
				last.Header.Timestamp = 200
				want = 0
			case "database-filter":
				cfg.Databases = []string{"other"}
				want = 0
			case "xid":
				last = probeEvent(replication.XID_EVENT, 150, 100)
			case "rotate":
				last = rotateEvent("mysql-bin.000002")
				last.Header.LogPos = 100
				want = 0
			case "format":
				last = probeEvent(replication.FORMAT_DESCRIPTION_EVENT, 0, 100)
				want = 0
			}
			reader := &testEventReader{events: []*replication.BinlogEvent{last}, err: context.DeadlineExceeded}
			if err := sendBinlogEvents(cfg, reader); err != nil {
				t.Fatal(err)
			}
			if reader.reads != 1 || len(cfg.StatChan) != want {
				t.Fatalf("reads=%d stats=%d", reader.reads, len(cfg.StatChan))
			}
		})
	}
}

func TestReplicationLastRowIsSentToSQLWorkers(t *testing.T) {
	installTestTables(t)
	cfg := boundedConfig(100)
	cfg.WorkType = "2sql"
	reader := &testEventReader{events: []*replication.BinlogEvent{rowEvent("db", 150, 100)}}
	if err := sendBinlogEvents(cfg, reader); err != nil {
		t.Fatal(err)
	}
	if reader.reads != 1 || len(cfg.EventChan) != 1 {
		t.Fatalf("reads=%d queued=%d", reader.reads, len(cfg.EventChan))
	}
	if event := <-cfg.EventChan; event.MyPos.Pos != 100 || event.EventIdx != 1 {
		t.Fatalf("last row=%+v", event)
	}
}

func TestRotateToExclusiveFileStart(t *testing.T) {
	cfg := boundedConfig(4)
	cfg.StopFilePos.Name = "mysql-bin.000002"
	reader := &testEventReader{events: []*replication.BinlogEvent{rotateEvent("mysql-bin.000002")}}
	if err := sendBinlogEvents(cfg, reader); err != nil || reader.reads != 1 {
		t.Fatalf("rotate boundary: %v reads=%d", err, reader.reads)
	}
}

func TestReplicationUnorderedTimeAndForeignDatabase(t *testing.T) {
	cfg := boundedConfig(140)
	cfg.IfSetStopDateTime, cfg.StopDatetime = true, 200
	cfg.Databases = []string{"wanted"}
	reader := &testEventReader{events: []*replication.BinlogEvent{
		rowEvent("wanted", 100, 40), rowEvent("other", 999, 60), rowEvent("wanted", 200, 80),
		rowEvent("wanted", 150, 120), probeEvent(replication.XID_EVENT, 300, 140),
	}}
	if err := sendBinlogEvents(cfg, reader); err != nil {
		t.Fatal(err)
	}
	if reader.reads != 5 || len(cfg.StatChan) != 2 {
		t.Fatalf("reads=%d stats=%d", reader.reads, len(cfg.StatChan))
	}
	for _, pos := range []uint32{40, 120} {
		if got := (<-cfg.StatChan).StopPos; got != pos {
			t.Fatalf("pos=%d", got)
		}
	}
}

func TestReplicationIncompleteNeverSucceeds(t *testing.T) {
	artificial := probeEvent(replication.FORMAT_DESCRIPTION_EVENT, 150, 100)
	artificial.Header.Flags = 0x20
	fakeRotate := rotateEvent("mysql-bin.000002")
	fakeRotate.Header.Timestamp = 0
	for _, tc := range []struct {
		name   string
		events []*replication.BinlogEvent
		cause  error
	}{
		{"timeout", []*replication.BinlogEvent{rowEvent("wanted", 100, 60)}, context.DeadlineExceeded},
		{"connection-error", []*replication.BinlogEvent{rowEvent("wanted", 100, 60)}, &net.OpError{Op: "read", Net: "tcp", Err: io.ErrUnexpectedEOF}},
		{"cancel", nil, context.Canceled}, {"eof", nil, io.EOF},
		{"past-P", []*replication.BinlogEvent{rowEvent("wanted", 100, 101)}, nil},
		{"fake-FDE", []*replication.BinlogEvent{artificial}, context.DeadlineExceeded},
		{"heartbeat", []*replication.BinlogEvent{probeEvent(replication.HEARTBEAT_EVENT, 150, 100)}, context.DeadlineExceeded},
		{"rotate-new-file-not-old-proof", []*replication.BinlogEvent{fakeRotate, rowEvent("wanted", 100, 100)}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := boundedConfig(100)
			reader := &testEventReader{events: tc.events, err: tc.cause}
			err := sendBinlogEvents(cfg, reader)
			if err == nil || (tc.cause != nil && !errors.Is(err, tc.cause)) {
				t.Fatalf("err=%v", err)
			}
			if tc.name == "past-P" && len(cfg.StatChan) != 0 {
				t.Fatal("processed >P")
			}
			if tc.name == "connection-error" {
				var opErr *net.OpError
				if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.As(err, &opErr) || opErr != tc.cause {
					t.Fatalf("连接错误原因未保留: %v", err)
				}
				if reader.reads != 2 || len(cfg.StatChan) != 1 {
					t.Fatalf("断连后读取次数或统计错误: reads=%d stats=%d", reader.reads, len(cfg.StatChan))
				}
				if stat := <-cfg.StatChan; stat.StopPos != 60 {
					t.Fatalf("断连前事件未正常处理: %+v", stat)
				}
			}
		})
	}
	cfg := boundedConfig(100)
	cfg.IfSetStopFilePos = false
	if err := sendBinlogEvents(cfg, &testEventReader{}); err == nil {
		t.Fatal("unbounded parse succeeded")
	}
}

func rawEvent(kind replication.EventType, timestamp, pos uint32, body []byte) []byte {
	data := make([]byte, replication.EventHeaderSize+len(body))
	binary.LittleEndian.PutUint32(data, timestamp)
	data[4] = byte(kind)
	binary.LittleEndian.PutUint32(data[5:], 1)
	binary.LittleEndian.PutUint32(data[9:], uint32(len(data)))
	binary.LittleEndian.PutUint32(data[13:], pos)
	copy(data[replication.EventHeaderSize:], body)
	return data
}

func TestLocalPhysicalBoundaryAndUnorderedTime(t *testing.T) {
	var data []byte
	for i, ts := range []uint32{100, 999, 150} {
		data = append(data, rawEvent(replication.XID_EVENT, ts, uint32(4+(i+1)*27), make([]byte, 8))...)
	}
	cfg := boundedConfig(85)
	cfg.IfSetStopDateTime = true
	cfg.StopDatetime = 200
	r := bytes.NewReader(append(data, []byte("must not be read")...))
	name := cfg.StartFile
	code, err := (BinFileParser{Parser: replication.NewBinlogParser()}).MyParseReader(cfg, r, &name)
	if err != nil || code != C_reBreak || len(cfg.StatChan) != 2 || r.Len() != 16 {
		t.Fatalf("code=%d err=%v stats=%d remaining=%d", code, err, len(cfg.StatChan), r.Len())
	}
	for _, pos := range []uint32{31, 85} {
		if got := (<-cfg.StatChan).StopPos; got != pos {
			t.Fatalf("pos=%d", got)
		}
	}
	// A filtered final event still terminates immediately.
	cfg = boundedConfig(58)
	cfg.IfSetStopDateTime = true
	cfg.StopDatetime = 200
	r = bytes.NewReader(data)
	if _, err := (BinFileParser{Parser: replication.NewBinlogParser()}).MyParseReader(cfg, r, &name); err != nil || r.Len() != 27 {
		t.Fatalf("filtered end: %v remaining=%d", err, r.Len())
	}
}

func TestLocalHeaderOnlyEventBoundary(t *testing.T) {
	cfg := boundedConfig(4 + replication.EventHeaderSize)
	data := rawEvent(replication.STOP_EVENT, 100, cfg.StopFilePos.Pos, nil)
	reader := bytes.NewReader(append(data, byte(1)))
	name := cfg.StartFile
	code, err := (BinFileParser{Parser: replication.NewBinlogParser()}).MyParseReader(cfg, reader, &name)
	if err != nil || code != C_reBreak || reader.Len() != 1 {
		t.Fatalf("header-only event: code=%d err=%v unread=%d", code, err, reader.Len())
	}
}

func TestLocalRotateZeroLogPosUsesFileOffset(t *testing.T) {
	body := append(make([]byte, 8), []byte("mysql-bin.000002")...)
	binary.LittleEndian.PutUint64(body, 4)
	data := rawEvent(replication.ROTATE_EVENT, 100, 0, body)
	cfg := boundedConfig(uint32(4 + len(data)))
	name := cfg.StartFile
	reader := bytes.NewReader(append(data, byte(1)))
	_, err := (BinFileParser{Parser: replication.NewBinlogParser()}).MyParseReader(cfg, reader, &name)
	if err != nil || name != cfg.StartFile || reader.Len() != 1 {
		t.Fatalf("rotate: %v name=%s unread=%d", err, name, reader.Len())
	}
}

func TestLocalExclusiveFileStartBoundary(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stopFile  string
		stopPos   uint32
		next      string
		truncate  string
		wantStats int
		wantError bool
		cause     error
	}{
		{name: "valid-next", stopFile: "mysql-bin.000002", stopPos: 4, next: "valid", wantStats: 1},
		{name: "missing-excluded-file", stopFile: "mysql-bin.000002", stopPos: 4, wantStats: 1},
		{name: "invalid-excluded-file", stopFile: "mysql-bin.000002", stopPos: 4, next: "invalid", wantStats: 1},
		{name: "missing-middle", stopFile: "mysql-bin.000003", stopPos: 4, wantStats: 1, wantError: true, cause: os.ErrNotExist},
		{name: "truncated-header", stopFile: "mysql-bin.000002", stopPos: 4, truncate: "header", wantStats: 1, wantError: true, cause: io.ErrUnexpectedEOF},
		{name: "truncated-body", stopFile: "mysql-bin.000002", stopPos: 4, truncate: "body", wantStats: 1, wantError: true, cause: io.ErrUnexpectedEOF},
		{name: "inclusive-event-end", stopFile: "mysql-bin.000001", stopPos: 31, wantStats: 1},
		{name: "inside-event", stopFile: "mysql-bin.000001", stopPos: 30, wantError: true},
		{name: "past-file", stopFile: "mysql-bin.000000", stopPos: 4, wantError: true},
		{name: "empty-range", stopFile: "mysql-bin.000001", stopPos: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			data := append([]byte{}, replication.BinLogFileHeader...)
			data = append(data, rawEvent(replication.XID_EVENT, 100, 31, make([]byte, 8))...)
			if tc.truncate != "" {
				event := rawEvent(replication.XID_EVENT, 200, 58, make([]byte, 8))
				if tc.truncate == "header" {
					event = event[:replication.EventHeaderSize-1]
				} else {
					event = event[:len(event)-1]
				}
				data = append(data, event...)
			}
			if err := os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), data, 0600); err != nil {
				t.Fatal(err)
			}
			if tc.next != "" {
				next := []byte("invalid binlog")
				if tc.next == "valid" {
					next = append([]byte{}, replication.BinLogFileHeader...)
					next = append(next, rawEvent(replication.XID_EVENT, 200, 31, make([]byte, 8))...)
				}
				if err := os.WriteFile(filepath.Join(dir, "mysql-bin.000002"), next, 0600); err != nil {
					t.Fatal(err)
				}
			}
			cfg := boundedConfig(tc.stopPos)
			cfg.Mode, cfg.BinlogDir = "file", dir
			cfg.StopFilePos.Name = tc.stopFile
			err := (BinFileParser{Parser: replication.NewBinlogParser()}).MyParseAllBinlogFiles(cfg)
			if (err != nil) != tc.wantError || (tc.cause != nil && !errors.Is(err, tc.cause)) {
				t.Fatalf("解析结果错误: %v", err)
			}
			if !errors.Is(cfg.Err(), err) {
				t.Fatalf("解析错误未汇总: %v，返回值: %v", cfg.Err(), err)
			}
			count := 0
			for stat := range cfg.StatChan {
				count++
				if stat.Binlog != "mysql-bin.000001" || stat.StopPos != 31 {
					t.Fatalf("处理了范围外事件: %+v", stat)
				}
			}
			if count != tc.wantStats {
				t.Fatalf("统计事件数=%d，期望 %d", count, tc.wantStats)
			}
		})
	}
}

func TestLocalSnapshotAndTruncatedFiles(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "mysql-bin.000001")
	data := append([]byte{}, replication.BinLogFileHeader...)
	data = append(data, rawEvent(replication.XID_EVENT, 100, 31, make([]byte, 8))...)
	if err := os.WriteFile(name, data, 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"snapshot", "missing-next", "truncated", "bad-header"} {
		t.Run(mode, func(t *testing.T) {
			cfg := boundedConfig(31)
			cfg.Mode = "file"
			cfg.BinlogDir = dir
			switch mode {
			case "snapshot":
				cfg.IfSetStopFilePos = false
			case "missing-next":
				cfg.StopFilePos = mysql.Position{Name: "mysql-bin.000002", Pos: 31}
			case "truncated":
				cfg.StopFilePos.Pos = 32
			case "bad-header":
				if err := os.WriteFile(name, []byte("oops"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			err := (BinFileParser{Parser: replication.NewBinlogParser()}).MyParseAllBinlogFiles(cfg)
			if (err != nil) != (mode != "snapshot") {
				t.Fatalf("err=%v", err)
			}
			if mode == "snapshot" && (cfg.StopFilePos.Pos != 31 || len(cfg.StatChan) != 1) {
				t.Fatal("bad local snapshot")
			}
		})
	}
}

type offlineConnector struct {
	query func(string) (driver.Rows, error)
}

func (c *offlineConnector) Connect(context.Context) (driver.Conn, error) { return &offlineConn{c}, nil }
func (*offlineConnector) Driver() driver.Driver                          { return catalogDriver{} }

type offlineConn struct{ c *offlineConnector }

func (*offlineConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected Prepare")
}
func (*offlineConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("unexpected Begin") }
func (*offlineConn) Close() error              { return nil }
func (c *offlineConn) QueryContext(ctx context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.c.query(q)
}

func TestPrepareReplSnapshotAndQuerySeparation(t *testing.T) {
	queries := 0
	db := sql.OpenDB(&offlineConnector{query: func(q string) (driver.Rows, error) {
		queries++
		switch q {
		case "SHOW BINARY LOG STATUS":
			return &catalogRows{columns: []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}, values: [][]driver.Value{{"mysql-bin.000003", "123", "", "", ""}}}, nil
		case "SHOW BINARY LOGS":
			return &catalogRows{columns: []string{"Log_name", "File_size"}, values: [][]driver.Value{{"mysql-bin.000001", "200"}, {"mysql-bin.000003", "123"}}}, nil
		}
		return nil, fmt.Errorf("unexpected query %s", q)
	}})
	defer db.Close()
	cfg := &ConfCmd{WorkType: "stats", FromDB: db}
	if err := cfg.prepareReplBoundary(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cfg.StopFilePos != (mysql.Position{Name: "mysql-bin.000003", Pos: 123}) || cfg.StartFile != "mysql-bin.000001" || queries != 2 {
		t.Fatalf("snapshot=%s start=%s queries=%d", cfg.StopFilePos, cfg.StartFile, queries)
	}
	queries = 0
	explicit := boundedConfig(100)
	explicit.FromDB = db
	if err := explicit.prepareReplBoundary(context.Background()); err != nil || queries != 0 || explicit.StopFilePos.Pos != 100 {
		t.Fatalf("explicit boundary overridden: %v", err)
	}
	explicit.WorkType = "binlogs"
	if err := explicit.prepareReplBoundary(context.Background()); err == nil || queries != 0 {
		t.Fatal("query started parsing")
	}
}

func TestPrepareReplSnapshotFallback(t *testing.T) {
	for _, code := range []uint16{1064, 1227} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var queries []string
			serverErr := &mysqldriver.MySQLError{Number: code, Message: "snapshot query failed"}
			db := sql.OpenDB(&offlineConnector{query: func(query string) (driver.Rows, error) {
				queries = append(queries, query)
				if query == "SHOW BINARY LOG STATUS" {
					return nil, serverErr
				}
				if query != "SHOW MASTER STATUS" {
					return nil, fmt.Errorf("unexpected query %s", query)
				}
				return &catalogRows{columns: []string{"File", "Position"}, values: [][]driver.Value{{"mysql-bin.000001", "100"}}}, nil
			}})
			defer db.Close()
			cfg := &ConfCmd{WorkType: "stats", FromDB: db, StartFile: "mysql-bin.000001", StartPos: 4}
			err := cfg.prepareReplBoundary(context.Background())
			if code == 1064 {
				if err != nil || len(queries) != 2 || cfg.StopFilePos.Pos != 100 {
					t.Fatalf("fallback: queries=%v pos=%s err=%v", queries, cfg.StopFilePos, err)
				}
			} else if !errors.Is(err, serverErr) || len(queries) != 1 || cfg.IfSetStopFilePos {
				t.Fatalf("permission error must not retry: queries=%v err=%v", queries, err)
			}
		})
	}
}
