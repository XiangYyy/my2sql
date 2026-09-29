package base

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func sampleCatalog(times ...uint32) []BinlogInfo {
	files := make([]BinlogInfo, len(times))
	for i, timestamp := range times {
		// 使用不连续编号，验证相邻关系取自目录而非文件名加一。
		files[i] = BinlogInfo{Name: fmt.Sprintf("mysql-bin.%06d", 2*i+1), Size: 1000,
			SampleTime: timestamp, Readable: true, Status: "sampled"}
	}
	return files
}

func timeRange(start, stop uint32) *ConfCmd {
	return &ConfCmd{Mode: "repl", MysqlType: "mysql", WorkType: "2sql", AutoPosition: true,
		StartDatetime: start, StopDatetime: stop, IfSetStartDateTime: start != 0, IfSetStopDateTime: stop != 0}
}

func TestSelectBinlogs(t *testing.T) {
	files := sampleCatalog(100, 200, 300, 400, 500, 600, 700)
	for _, tc := range []struct {
		name        string
		start, stop uint32
		first, last int
		warning     bool
	}{
		{"全部", 0, 0, 0, 6, false},
		{"中间范围", 350, 450, 1, 4, false},
		{"相等边界", 300, 400, 0, 4, false},
		{"仅开始", 350, 0, 1, 6, false},
		{"仅结束", 0, 350, 0, 3, false},
		{"早于保留", 1, 50, 0, 1, true},
		{"晚于最新", 800, 900, 5, 6, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := SelectBinlogs(files, timeRange(tc.start, tc.stop))
			if s.First != tc.first || s.Last != tc.last || (len(s.Warnings) > 0) != tc.warning {
				t.Fatalf("候选范围错误: %+v", s)
			}
			wantStop := ""
			if tc.last+1 < len(files) {
				wantStop = files[tc.last+1].Name
			}
			if s.StopBefore != wantStop {
				t.Fatalf("排他终点=%s，期望 %s", s.StopBefore, wantStop)
			}
		})
	}
	if s := SelectBinlogs(nil, timeRange(10, 20)); s.Last != -1 || s.StopBefore != "" {
		t.Fatalf("空目录: %+v", s)
	}
	if s := SelectBinlogs(sampleCatalog(100), timeRange(200, 300)); s.First != 0 || s.Last != 0 || s.StopBefore != "" {
		t.Fatalf("单文件: %+v", s)
	}
	s := SelectBinlogs(sampleCatalog(100, 200, 200, 200, 300, 400, 400, 400, 500, 600), timeRange(320, 330))
	if s.First != 1 || s.Last != 7 {
		t.Fatalf("同秒组被拆分: %+v", s)
	}
}

func TestSelectBinlogsFallback(t *testing.T) {
	for _, change := range []func([]BinlogInfo){
		func(f []BinlogInfo) { f[2].SampleTime = 0 },
		func(f []BinlogInfo) { f[2].Readable = false },
		func(f []BinlogInfo) { f[2].ProbeError = errors.New("无复制权限") },
		func(f []BinlogInfo) { f[2].SampleTime = 1 },
	} {
		files := sampleCatalog(100, 200, 300, 400, 500, 600)
		change(files)
		s := SelectBinlogs(files, timeRange(310, 320))
		if s.First != 0 || s.Last != 5 || s.StopBefore != "" || len(s.Warnings) == 0 {
			t.Fatalf("未保守回退: %+v", s)
		}
	}
}

func TestOrderedIntervalsRemainSelected(t *testing.T) {
	for _, files := range [][]BinlogInfo{sampleCatalog(10, 20, 30, 40, 50), sampleCatalog(10, 20, 20, 20, 40)} {
		for start := uint32(1); start < 60; start++ {
			for stop := start + 1; stop < 61; stop++ {
				s := SelectBinlogs(files, timeRange(start, stop))
				for i := range files {
					intersects := (i == 0 || files[i].SampleTime < stop) && (i == len(files)-1 || files[i+1].SampleTime >= start)
					if intersects && (i < s.First || i > s.Last) {
						t.Fatalf("[%d,%d) 遗漏文件 %d: %+v", start, stop, i, s)
					}
				}
			}
		}
	}
}

type testEventReader struct {
	events []*replication.BinlogEvent
	reads  int
	err    error
}

func (r *testEventReader) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.reads++
	if len(r.events) == 0 {
		if r.err != nil {
			return nil, r.err
		}
		return nil, io.EOF
	}
	event := r.events[0]
	r.events = r.events[1:]
	return event, nil
}

func probeEvent(kind replication.EventType, timestamp, pos uint32) *replication.BinlogEvent {
	return &replication.BinlogEvent{Header: &replication.EventHeader{EventType: kind, Timestamp: timestamp, LogPos: pos}}
}

func rotateEvent(name string) *replication.BinlogEvent {
	e := probeEvent(replication.ROTATE_EVENT, 9999, 0)
	e.Event = &replication.RotateEvent{NextLogName: []byte(name), Position: 4}
	return e
}

func TestProbeBinlogEvents(t *testing.T) {
	fde := probeEvent(replication.FORMAT_DESCRIPTION_EVENT, 9999, 100)
	for _, tc := range []struct {
		name             string
		events           []*replication.BinlogEvent
		size             uint64
		status           string
		timestamp        uint32
		readable, failed bool
	}{
		{"忽略控制和零时间", []*replication.BinlogEvent{rotateEvent("mysql-bin.000001"), fde,
			probeEvent(replication.PREVIOUS_GTIDS_EVENT, 9999, 150), probeEvent(replication.GTID_EVENT, 9999, 160),
			probeEvent(replication.HEARTBEAT_EVENT, 9999, 0), probeEvent(replication.QUERY_EVENT, 0, 180),
			probeEvent(replication.TABLE_MAP_EVENT, 123, 200)}, 1000, "sampled", 123, true, false},
		{"快照仅文件头", []*replication.BinlogEvent{fde}, 100, "header-only", 0, true, false},
		{"只有零时间业务", []*replication.BinlogEvent{fde, probeEvent(replication.QUERY_EVENT, 0, 200)}, 200, "unknown", 0, true, false},
		{"业务末事件", []*replication.BinlogEvent{fde, probeEvent(replication.XID_EVENT, 123, 200)}, 200, "sampled", 123, true, false},
		{"轮转停止", []*replication.BinlogEvent{fde, rotateEvent("mysql-bin.000003"), probeEvent(replication.QUERY_EVENT, 456, 200)}, 1000, "header-only", 0, true, false},
		{"忽略快照之后写入", []*replication.BinlogEvent{fde, probeEvent(replication.QUERY_EVENT, 456, 201)}, 200, "header-only", 0, true, false},
		{"缺少文件头", []*replication.BinlogEvent{probeEvent(replication.QUERY_EVENT, 123, 200)}, 1000, "error", 0, false, true},
		{"无头即轮转", []*replication.BinlogEvent{rotateEvent("mysql-bin.000003")}, 1000, "error", 0, false, true},
		{"空事件", []*replication.BinlogEvent{nil}, 1000, "error", 0, false, true},
		{"空头", []*replication.BinlogEvent{{}}, 1000, "error", 0, false, true},
		{"非法轮转", []*replication.BinlogEvent{probeEvent(replication.ROTATE_EVENT, 0, 0)}, 1000, "error", 0, false, true},
		{"活动文件读取失败", []*replication.BinlogEvent{fde}, 1000, "error", 0, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &testEventReader{events: tc.events}
			got := probeBinlogEvents(context.Background(), BinlogInfo{Name: "mysql-bin.000001", Size: tc.size}, reader)
			if got.Status != tc.status || got.SampleTime != tc.timestamp || got.Readable != tc.readable || (got.ProbeError != nil) != tc.failed {
				t.Fatalf("探测结果不符: %+v", got)
			}
		})
	}
	for _, kind := range []replication.EventType{replication.QUERY_EVENT, replication.TABLE_MAP_EVENT, replication.XID_EVENT,
		replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2} {
		reader := &testEventReader{events: []*replication.BinlogEvent{fde, probeEvent(kind, 123, 200)}}
		got := probeBinlogEvents(context.Background(), BinlogInfo{Size: 1000}, reader)
		if got.SampleTime != 123 {
			t.Fatalf("未识别业务类型 %v: %+v", kind, got)
		}
	}
}

func TestProbeLimitAndCancellation(t *testing.T) {
	reader := &testEventReader{events: []*replication.BinlogEvent{probeEvent(replication.FORMAT_DESCRIPTION_EVENT, 1, 100)}}
	for i := 0; i < binlogProbeEventLimit; i++ {
		reader.events = append(reader.events, probeEvent(replication.GTID_EVENT, 100, 0))
	}
	got := probeBinlogEvents(context.Background(), BinlogInfo{Size: 1000}, reader)
	if got.Status != "probe-limit" || got.SampleTime != 0 || reader.reads != 64 || !got.Readable {
		t.Fatalf("事件上限无效: %+v, reads=%d", got, reader.reads)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got = probeBinlogEvents(ctx, BinlogInfo{Size: 1000}, &testEventReader{})
	if !errors.Is(got.ProbeError, context.Canceled) {
		t.Fatalf("取消错误未传播: %+v", got)
	}
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	got = probeBinlogEvents(ctx, BinlogInfo{Size: 1000}, &testEventReader{})
	if !errors.Is(got.ProbeError, context.DeadlineExceeded) {
		t.Fatalf("超时错误未传播: %+v", got)
	}
}

type testBinlogSource struct {
	lists                 [][]BinlogInfo
	listCalls, probeCalls int
	listErr               error
}

func (s *testBinlogSource) List(context.Context) ([]BinlogInfo, error) {
	i := s.listCalls
	s.listCalls++
	if s.listErr != nil {
		return nil, s.listErr
	}
	if i >= len(s.lists) {
		i = len(s.lists) - 1
	}
	return append([]BinlogInfo(nil), s.lists[i]...), nil
}

func (s *testBinlogSource) Probe(_ context.Context, file BinlogInfo) BinlogInfo {
	s.probeCalls++
	return file
}

func TestLoadCatalogRetention(t *testing.T) {
	files := sampleCatalog(100, 200, 300)
	grown := append([]BinlogInfo(nil), files...)
	grown[2].Size++
	grown = append(grown, BinlogInfo{Name: "mysql-bin.000007", Size: 100})
	shrunk := append([]BinlogInfo(nil), files...)
	shrunk[0].Size--
	for _, tc := range []struct {
		name          string
		lists         [][]BinlogInfo
		probes, count int
		failed        bool
	}{
		{"空目录", [][]BinlogInfo{nil}, 0, 0, false},
		{"稳定目录", [][]BinlogInfo{files}, 3, 3, false},
		{"正常增长轮转", [][]BinlogInfo{files, grown}, 3, 3, false},
		{"清理重试", [][]BinlogInfo{files, files[1:]}, 5, 2, false},
		{"变小重试", [][]BinlogInfo{files, shrunk}, 6, 3, false},
		{"连续清理", [][]BinlogInfo{files, files[1:], files[2:]}, 5, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &testBinlogSource{lists: tc.lists}
			got, err := LoadBinlogCatalog(context.Background(), src)
			if (err != nil) != tc.failed || len(got) != tc.count || src.probeCalls != tc.probes {
				t.Fatalf("files=%v err=%v probes=%d", got, err, src.probeCalls)
			}
		})
	}
	src := &testBinlogSource{listErr: errors.New("无 REPLICATION CLIENT 权限")}
	if _, err := LoadBinlogCatalog(context.Background(), src); err == nil || src.probeCalls != 0 {
		t.Fatal("目录错误未传播")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	src = &testBinlogSource{lists: [][]BinlogInfo{files}}
	if _, err := LoadBinlogCatalog(ctx, src); !errors.Is(err, context.Canceled) || src.probeCalls != 0 {
		t.Fatal("取消后仍在探测")
	}
}

func TestDiscoverBinlogs(t *testing.T) {
	files := sampleCatalog(100, 200, 300, 400, 500, 600, 700)
	cfg := timeRange(350, 450)
	src := &testBinlogSource{lists: [][]BinlogInfo{files}}
	var out, diagnostics bytes.Buffer
	proceed, err := DiscoverBinlogs(context.Background(), cfg, src, &out, &diagnostics)
	if err != nil || !proceed || cfg.StartFile != files[0].Name || cfg.StartPos != 4 || !cfg.IfSetStartFilePos || cfg.StartFilePos != (mysql.Position{Name: files[0].Name, Pos: 4}) || cfg.AutoStopBeforeFile != "" || cfg.IfSetStopFilePos {
		t.Fatalf("自动定位配置不符: %+v, %v", cfg, err)
	}
	if out.Len() != 0 || !strings.Contains(diagnostics.String(), "启发式") {
		t.Fatal("自动定位污染 stdout 或缺少风险提示")
	}
	for _, work := range []string{"binlogs", "2sql"} {
		for _, failed := range []bool{false, true} {
			cfg = timeRange(350, 450)
			cfg.WorkType = work
			f := append([]BinlogInfo(nil), files...)
			if failed {
				f[3].ProbeError = errors.New("无复制权限")
				f[3].Status = "error"
			}
			src = &testBinlogSource{lists: [][]BinlogInfo{f}}
			out.Reset()
			diagnostics.Reset()
			proceed, err = DiscoverBinlogs(context.Background(), cfg, src, &out, &diagnostics)
			if proceed != (work != "binlogs") || (err != nil) != (work == "binlogs" && failed) {
				t.Fatalf("分流错误: %s failed=%v err=%v", work, failed, err)
			}
			if cfg.EventChan != nil || cfg.StatChan != nil || cfg.SqlChan != nil || cfg.StatFH != nil || cfg.BiglongFH != nil || cfg.FromDB != nil {
				t.Fatal("探测初始化了输出或表结构连接")
			}
			if work == "binlogs" && !strings.Contains(out.String(), "SampleStartTime") {
				t.Fatal("缺少查询表格")
			}
			if failed && work == "binlogs" && !strings.Contains(out.String(), "无复制权限") {
				t.Fatal("未保留错误行")
			}
			if failed && work != "binlogs" && (cfg.StartFile != files[0].Name || cfg.AutoStopBeforeFile != "") {
				t.Fatal("失败未回退全范围")
			}
		}
	}
	for _, work := range []string{"binlogs", "2sql"} {
		cfg = timeRange(350, 450)
		cfg.WorkType = work
		proceed, err = DiscoverBinlogs(context.Background(), cfg, &testBinlogSource{lists: [][]BinlogInfo{nil}}, io.Discard, io.Discard)
		if proceed || err != nil || cfg.IfSetStartFilePos {
			t.Fatal("空目录不应启动解析")
		}
	}
	cfg = timeRange(350, 450)
	cfg.ExplicitFilePos = true
	cfg.StartPos = 4
	proceed, err = DiscoverBinlogs(context.Background(), cfg, nil, io.Discard, io.Discard)
	if !proceed || err != nil || cfg.StartFile != "" {
		t.Fatal("显式 Pos 被自动覆盖")
	}
}

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestPrintCatalog(t *testing.T) {
	files := sampleCatalog(3600, 7200)
	var out bytes.Buffer
	if err := PrintBinlogCatalog(&out, files, SelectBinlogs(files, &ConfCmd{}), time.FixedZone("UTC+08", 8*3600)); err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"最早保留文件", "最早探测可读文件", "不保证", "SampleStartTime", "EstimatedEndTime", "unknown/open", "1970-01-01 09:00:00", "UTC+08"} {
		if !strings.Contains(out.String(), part) {
			t.Fatalf("缺少 %q: %s", part, out.String())
		}
	}
	files[0].SampleTime = 0
	files[0].Readable = false
	files[0].Status = "error"
	files[0].ProbeError = errors.New("错误\n换行")
	out.Reset()
	if err := PrintBinlogCatalog(&out, files, SelectBinlogs(files, &ConfCmd{}), time.UTC); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "unknown") || strings.Contains(out.String(), "错误\n换行") || !strings.Contains(out.String(), "最早探测可读文件: "+files[1].Name) {
		t.Fatalf("未知时间或错误展示错误: %s", out.String())
	}
	for _, f := range [][]BinlogInfo{nil, files} {
		if err := PrintBinlogCatalog(failedWriter{}, f, SelectBinlogs(f, &ConfCmd{}), time.UTC); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal("输出错误未传播")
		}
	}
	cfg := &ConfCmd{WorkType: "binlogs"}
	if _, err := DiscoverBinlogs(context.Background(), cfg, &testBinlogSource{lists: [][]BinlogInfo{files}}, failedWriter{}, io.Discard); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("查询输出错误未传播")
	}
}

func TestAutoStopAndTimeFilter(t *testing.T) {
	cfg := timeRange(100, 200)
	cfg.AutoStopBeforeFile = "mysql-bin.000005"
	current := "mysql-bin.000003"
	for _, tc := range []struct {
		timestamp uint32
		want      int
	}{{99, C_reContinue}, {100, C_reProcess}, {199, C_reProcess}, {200, C_reContinue}} {
		event := probeEvent(replication.XID_EVENT, tc.timestamp, 1000)
		if got := (&MyBinEvent{}).CheckBinEvent(cfg, event, &current); got != tc.want {
			t.Fatalf("时间 %d: %d，期望 %d", tc.timestamp, got, tc.want)
		}
		if got := CheckBinHeaderCondition(cfg, event.Header, current); got != tc.want {
			t.Fatalf("本地过滤时间 %d: %d", tc.timestamp, got)
		}
	}
	if got := (&MyBinEvent{}).CheckBinEvent(cfg, rotateEvent("mysql-bin.000005"), &current); got != C_reBreak {
		t.Fatal("未在下一文件前停止")
	}
	current = "mysql-bin.000007"
	if got := (&MyBinEvent{}).CheckBinEvent(cfg, probeEvent(replication.XID_EVENT, 150, 100), &current); got != C_reBreak {
		t.Fatal("未拦截文件越界")
	}
	cfg.AutoStopBeforeFile = ""
	if got := (&MyBinEvent{}).CheckBinEvent(cfg, rotateEvent("mysql-bin.000009"), &current); got != C_reContinue {
		t.Fatal("无自动终点时轮转行为改变")
	}
	cfg.IfSetStopFilePos = true
	cfg.StopFilePos = mysql.Position{Name: current, Pos: 100}
	if got := (&MyBinEvent{}).CheckBinEvent(cfg, probeEvent(replication.XID_EVENT, 150, 100), &current); got != C_reProcess {
		t.Fatal("等于显式 Pos 的事件必须处理")
	}
}

// 用最小 database/sql 驱动模拟服务端返回，避免引入额外依赖。
type catalogRows struct {
	columns []string
	values  [][]driver.Value
	err     error
	closed  bool
}

func (r *catalogRows) Columns() []string { return r.columns }
func (r *catalogRows) Close() error      { r.closed = true; return nil }
func (r *catalogRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		if r.err != nil {
			return r.err
		}
		return io.EOF
	}
	copy(dest, r.values[0])
	r.values = r.values[1:]
	return nil
}

type catalogConnector struct {
	rows   *catalogRows
	closed bool
}

func (c *catalogConnector) Connect(context.Context) (driver.Conn, error) { return &catalogConn{c}, nil }
func (c *catalogConnector) Driver() driver.Driver                        { return catalogDriver{} }

type catalogDriver struct{}

func (catalogDriver) Open(string) (driver.Conn, error) { return nil, errors.New("仅支持 Connector") }

type catalogConn struct{ connector *catalogConnector }

func (c *catalogConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("不允许 Prepare")
}
func (c *catalogConn) Begin() (driver.Tx, error) { return nil, errors.New("不允许事务") }
func (c *catalogConn) Close() error              { c.connector.closed = true; return nil }
func (c *catalogConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query != "SHOW BINARY LOGS" || len(args) != 0 {
		return nil, fmt.Errorf("不应执行的查询: %s", query)
	}
	return c.connector.rows, nil
}

func TestRemoteCatalogColumns(t *testing.T) {
	for _, tc := range []struct {
		name    string
		columns []string
		values  [][]driver.Value
		readErr error
		failed  bool
	}{
		{"两列", []string{"Log_name", "File_size"}, [][]driver.Value{{"mysql-bin.000001", int64(100)}}, nil, false},
		{"额外列且重排", []string{"Encrypted", "FILE_SIZE", "LOG_NAME"}, [][]driver.Value{{"No", "100", "mysql-bin.000001"}}, nil, false},
		{"空列表", []string{"Log_name", "File_size"}, nil, nil, false},
		{"缺列", []string{"Log_name"}, nil, nil, true},
		{"错误大小", []string{"Log_name", "File_size"}, [][]driver.Value{{"mysql-bin.000001", "bad"}}, nil, true},
		{"负数大小", []string{"Log_name", "File_size"}, [][]driver.Value{{"mysql-bin.000001", int64(-1)}}, nil, true},
		{"空文件名", []string{"Log_name", "File_size"}, [][]driver.Value{{"", int64(100)}}, nil, true},
		{"读取失败", []string{"Log_name", "File_size"}, nil, io.ErrUnexpectedEOF, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			count := len(tc.values)
			rows := &catalogRows{columns: tc.columns, values: tc.values, err: tc.readErr}
			connector := &catalogConnector{rows: rows}
			cfg := &ConfCmd{FromDB: sql.OpenDB(connector)}
			defer cfg.CloseResources()
			files, err := (RemoteBinlogSource{Config: cfg}).List(context.Background())
			if (err != nil) != tc.failed {
				t.Fatalf("files=%v err=%v", files, err)
			}
			if !tc.failed && (len(files) != count || (count > 0 && (files[0].Name != "mysql-bin.000001" || files[0].Size != 100))) {
				t.Fatalf("列表内容错误: %+v", files)
			}
			if !rows.closed {
				t.Fatal("未关闭查询结果")
			}
			cfg.CloseResources()
			if !connector.closed {
				t.Fatal("未关闭数据库连接")
			}
		})
	}
}

func TestBinlogSyncerConfigDisablesRetry(t *testing.T) {
	cfg := &ConfCmd{Host: "127.0.0.1", Port: 3307, User: "reader", ServerId: 123, MysqlType: "mysql"}
	for _, probe := range []bool{false, true} {
		t.Run(fmt.Sprintf("probe=%t", probe), func(t *testing.T) {
			replCfg := buildBinlogSyncerConfig(context.Background(), cfg, probe)
			if !replCfg.DisableRetrySync {
				t.Fatal("自动重连未禁用，关闭可能与重连互相等待")
			}
			if replCfg.RawModeEnabled != probe {
				t.Fatal("raw 模式必须仅用于探测")
			}
			if replCfg.ServerID != uint32(cfg.ServerId) || replCfg.Host != cfg.Host || replCfg.Port != uint16(cfg.Port) || replCfg.User != cfg.User || replCfg.Flavor != cfg.MysqlType {
				t.Fatal("复制连接配置被改变")
			}
			if replCfg.ParseTime || replCfg.UseDecimal || replCfg.SemiSyncEnabled || replCfg.TimestampStringLocation != GBinlogTimeLocation {
				t.Fatal("事件解析配置被改变")
			}
			if replCfg.Dialer == nil || replCfg.Option == nil {
				t.Fatal("连接取消或握手期限处理缺失")
			}
		})
	}
}

func TestWatchConnectionCancellation(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watched := watchBinlogConnection(ctx, client)
	defer watched.Close()
	cancel()
	server.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := server.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("取消未关闭连接: %v", err)
	}
	if err := watched.Close(); err != nil {
		t.Fatalf("重复关闭失败: %v", err)
	}
}

func TestProbeHandshakeTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	closed := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			closed <- err
			return
		}
		defer conn.Close()
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err = conn.Read(make([]byte, 1))
		closed <- err
	}()
	cfg := &ConfCmd{Host: "127.0.0.1", Port: uint(listener.Addr().(*net.TCPAddr).Port), ServerId: 123, MysqlType: "mysql"}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, closeStream, err := startBinlogStream(ctx, cfg, mysql.Position{Name: "mysql-bin.000001", Pos: 4}, true)
	if closeStream != nil {
		closeStream()
	}
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("握手超时失效: %v", err)
	}
	select {
	case err := <-closed:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("握手失败未关闭连接: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("连接关闭超时")
	}
}
