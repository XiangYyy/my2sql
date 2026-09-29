package base

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
)

func gtidEvent(sid []byte, gno int64) *replication.BinlogEvent {
	e := probeEvent(replication.GTID_EVENT, 9999, 200)
	e.Header.EventSize = 20
	e.Event = &replication.GTIDEvent{SID: sid, GNO: gno}
	return e
}

func anonymousGtidEvent() *replication.BinlogEvent {
	return gtidEvent(make([]byte, 16), 0)
}

func mariadbGtidEvent(domain uint32) *replication.BinlogEvent {
	e := probeEvent(replication.MARIADB_GTID_EVENT, 9999, 200)
	e.Header.EventSize = 20
	e.Event = &replication.MariadbGTIDEvent{GTID: mysql.MariadbGTID{DomainID: domain, ServerID: 7, SequenceNumber: 42}}
	return e
}

func TestFormatBinlogGtid(t *testing.T) {
	sid := make([]byte, 16)
	for i := range sid {
		sid[i] = byte(i + 1)
	}
	want := uuid.Must(uuid.FromBytes(sid)).String() + ":23"
	if got := formatBinlogGtid(gtidEvent(sid, 23)); got != want {
		t.Fatalf("gtid=%s want=%s", got, want)
	}
	for name, ev := range map[string]*replication.BinlogEvent{
		"匿名事务":       anonymousGtidEvent(),
		"匿名短SID":     gtidEvent(sid[:8], 23),
		"GTID类型断言失败": probeEvent(replication.GTID_EVENT, 0, 0),
		"普通事件":       probeEvent(replication.QUERY_EVENT, 0, 0),
		"XID":        probeEvent(replication.XID_EVENT, 0, 0),
	} {
		if got := formatBinlogGtid(ev); got != "" {
			t.Fatalf("%s 应返回空 gtid，得到 %q", name, got)
		}
	}
	if got := formatBinlogGtid(mariadbGtidEvent(3)); got != "3-7-42" {
		t.Fatalf("mariadb gtid=%s", got)
	}
}

func TestExtraInfoLineContainsTrxAndGtid(t *testing.T) {
	sc := ForwardRollbackSqlOfPrint{sqls: []string{"delete from t where id=1"},
		sqlInfo: ExtraSqlInfoOfPrint{schema: "db", table: "t", binlog: "mysql-bin.000001",
			startpos: 100, endpos: 200, datetime: "2026-09-29 10:00:00", trxIndex: 3,
			gtid: "3e11fa47-71ca-11e1-9e33-c80aa9429562:23"}}
	line := GetForwardRollbackContentLineWithExtra(sc, true)
	if !strings.Contains(line, " trxindex=3 gtid=3e11fa47-71ca-11e1-9e33-c80aa9429562:23\n") {
		t.Fatalf("注释行缺少事务标识: %q", line)
	}
	if !strings.Contains(line, "stoppos=200 trxindex=3") {
		t.Fatalf("字段顺序错误: %q", line)
	}
	sc.sqlInfo.gtid = ""
	line = GetForwardRollbackContentLineWithExtra(sc, true)
	if !strings.Contains(line, " trxindex=3 gtid=\n") {
		t.Fatalf("gtid 为空时字段应保留: %q", line)
	}
	if line2 := GetForwardRollbackContentLineWithExtra(sc, false); strings.Contains(line2, "trxindex") {
		t.Fatalf("关闭 extraInfo 不应输出事务标识: %q", line2)
	}
}

func TestPipelineCarriesGtidToOutput(t *testing.T) {
	installTestTables(t)
	cfg := &ConfCmd{WorkType: "2sql", OutputDir: t.TempDir(), Threads: 2, PrintExtraInfo: true,
		EventChan: make(chan MyBinEvent, 8), SqlChan: make(chan ForwardRollbackSqlOfPrint, 8)}
	G_HandlingBinEventIndex = &BinEventHandlingIndx{EventIdx: 1}
	var wg sync.WaitGroup
	wg.Add(1)
	go PrintExtraInfoForForwardRollbackupSql(cfg, &wg)
	for i := 1; i <= 3; i++ {
		ev := sqlRow(i)
		ev.Gtid = "3e11fa47-71ca-11e1-9e33-c80aa9429562:23"
		cfg.EventChan <- ev
	}
	close(cfg.EventChan)
	var gen sync.WaitGroup
	for i := uint(1); i <= 2; i++ {
		gen.Add(1)
		go GenForwardRollbackSqlFromBinEvent(i, cfg, &gen)
	}
	waitWorkers(t, &gen)
	close(cfg.SqlChan)
	waitWorkers(t, &wg)
	if err := cfg.Err(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(cfg.OutputDir, "forward.1.sql"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"trxindex=1", "trxindex=2", "trxindex=3", "gtid=3e11fa47-71ca-11e1-9e33-c80aa9429562:23"} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("输出缺少 %s: %s", want, data)
		}
	}
}

func TestReplicationGtidContextAcrossTransactions(t *testing.T) {
	installTestTables(t)
	sid := make([]byte, 16)
	for i := range sid {
		sid[i] = byte(i + 1)
	}
	first := uuid.Must(uuid.FromBytes(sid)).String()
	cfg := boundedConfig(300)
	cfg.WorkType = "2sql"
	reader := &testEventReader{events: []*replication.BinlogEvent{
		gtidEvent(sid, 23), rowEvent("db", 100, 60), probeEvent(replication.XID_EVENT, 100, 80),
		anonymousGtidEvent(), rowEvent("db", 150, 140), probeEvent(replication.XID_EVENT, 150, 160),
		gtidEvent(sid, 24), rowEvent("db", 150, 220), probeEvent(replication.XID_EVENT, 150, 240),
		rowEvent("db", 150, 300),
	}}
	if err := sendBinlogEvents(cfg, reader); err != nil {
		t.Fatal(err)
	}
	if len(cfg.EventChan) != 4 {
		t.Fatalf("rows=%d", len(cfg.EventChan))
	}
	// 事务组开始事件被过滤跳过前已更新 GTID 上下文：gtid -> 匿名空 -> gtid 残留检查 -> 新 gtid。
	want := []string{first + ":23", "", first + ":24", first + ":24"}
	for i, gtid := range want {
		if ev := <-cfg.EventChan; ev.Gtid != gtid {
			t.Fatalf("第 %d 个事件 gtid=%q want=%q", i+1, ev.Gtid, gtid)
		}
	}
}
