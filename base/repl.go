package base

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
)

func ParserAllBinEventsFromRepl(cfg *ConfCmd) (result error) {
	defer cfg.CloseChan()
	defer func() { cfg.RecordError(result) }()
	if err := cfg.prepareReplBoundary(context.Background()); err != nil {
		return err
	}
	if cfg.StartFilePos.Compare(cfg.StopFilePos) == 0 {
		return nil
	}
	var err error
	cfg.BinlogStreamer, err = NewReplBinlogStreamer(cfg)
	if err != nil {
		return err
	}
	defer cfg.CloseReplication()
	log.Info("start to get binlog from mysql")
	err = SendBinlogEventRepl(cfg)
	return err
}

func NewReplBinlogStreamer(cfg *ConfCmd) (*replication.BinlogStreamer, error) {
	syncPosition := mysql.Position{Name: cfg.StartFile, Pos: uint32(cfg.StartPos)}
	_, stream, closeStream, err := startBinlogStream(context.Background(), cfg, syncPosition, false)
	if err != nil {
		return nil, fmt.Errorf("error replication from master %s:%d: %w", cfg.Host, cfg.Port, err)
	}
	cfg.CloseReplication = closeStream
	return stream, nil
}

func SendBinlogEventRepl(cfg *ConfCmd) error {
	return sendBinlogEvents(cfg, cfg.BinlogStreamer)
}

func sendBinlogEvents(cfg *ConfCmd, stream binlogEventReader) error {
	if !cfg.IfSetStopFilePos {
		return fmt.Errorf("没有物理结束边界")
	}
	stopAfter := false
	var (
		err           error
		ev            *replication.BinlogEvent
		chkRe         int
		currentBinlog string = cfg.StartFile
		binEventIdx   uint64 = 0
		trxIndex      uint64 = 0
		trxStatus     int    = 0
		sqlLower      string = ""

		db      string = ""
		tb      string = ""
		sql     string = ""
		sqlType string = ""
		rowCnt  uint32 = 0

		tbMapPos uint32 = 0

		//justStart   bool = true
		//orgSqlEvent *replication.RowsQueryEvent
	)
	for !stopAfter {
		if err := cfg.Err(); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), EventTimeout)
		ev, err = stream.GetEvent(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("未到物理边界 %s，读取 binlog 失败（结果不完整）: %w", cfg.StopFilePos, err)
		}
		if ev == nil || ev.Header == nil {
			return fmt.Errorf("读取到空 binlog 事件")
		}
		stopAfter, err = eventAtStop(cfg, currentBinlog, ev.Header)
		if err != nil {
			return err
		}
		if ev.Header.EventType == replication.ROTATE_EVENT {
			rotate, ok := ev.Event.(*replication.RotateEvent)
			if !ok || len(rotate.NextLogName) == 0 {
				return fmt.Errorf("无效的 ROTATE 事件")
			}
			if !stopAfter {
				next := mysql.Position{Name: string(rotate.NextLogName), Pos: 4}
				if next.Compare(cfg.StopFilePos) > 0 {
					return fmt.Errorf("ROTATE 越过尚未读取的边界 %s", cfg.StopFilePos)
				}
				if next.Compare(cfg.StopFilePos) == 0 {
					if ev.Header.Timestamp == 0 || ev.Header.Flags&0x20 != 0 {
						return fmt.Errorf("fake ROTATE 不能证明达到边界 %s", cfg.StopFilePos)
					}
					stopAfter = true
				}
			}
		}

		if ev.Header.EventType == replication.TABLE_MAP_EVENT {
			tbMapPos = ev.Header.LogPos - ev.Header.EventSize
			// avoid mysqlbing mask the row event as unknown table row event
		}
		ev.RawData = []byte{} // we donnot need raw data

		oneMyEvent := &MyBinEvent{MyPos: mysql.Position{Name: currentBinlog, Pos: ev.Header.LogPos}, StartPos: tbMapPos}
		chkRe = oneMyEvent.CheckBinEvent(cfg, ev, &currentBinlog)

		if chkRe == C_reContinue {
			continue
		} else if chkRe == C_reBreak {
			if !stopAfter {
				return fmt.Errorf("过滤器在物理边界 %s 之前结束读取", cfg.StopFilePos)
			}
			break
		} else if chkRe == C_reFileEnd {
			continue
		}

		db, tb, sqlType, sql, rowCnt = GetDbTbAndQueryAndRowCntFromBinevent(ev)
		//if find := strings.Contains(db, "#"); find {
		//	log.Fatalf(fmt.Sprintf("Unsupported database name %s contains special character '#'", db))
		//	break
		//}
		//if find := strings.Contains(tb, "#"); find {
		//	log.Fatalf(fmt.Sprintf("Unsupported table name %s.%s contains special character '#'", db, tb))
		//	break
		//}

		if sqlType == "query" {
			sqlLower = strings.ToLower(sql)
			if sqlLower == "begin" {
				trxStatus = C_trxBegin
				trxIndex++
			} else if sqlLower == "commit" {
				trxStatus = C_trxCommit
			} else if sqlLower == "rollback" {
				trxStatus = C_trxRollback
			} else if oneMyEvent.QuerySql != nil {
				trxStatus = C_trxProcess
				rowCnt = 1
			}

		} else {
			trxStatus = C_trxProcess
		}

		if cfg.WorkType != "stats" {
			ifSendEvent := false
			if oneMyEvent.IfRowsEvent {

				tbKey := GetAbsTableName(string(oneMyEvent.BinEvent.Table.Schema),
					string(oneMyEvent.BinEvent.Table.Table))
				_, err = G_TablesColumnsInfo.GetTableInfoJson(string(oneMyEvent.BinEvent.Table.Schema),
					string(oneMyEvent.BinEvent.Table.Table))
				if err != nil {
					return fmt.Errorf("no table struct found for %s, it maybe dropped. RowsEvent position:%s: %w",
						tbKey, oneMyEvent.MyPos.String(), err)
				}
				ifSendEvent = true
			}
			if ifSendEvent {
				binEventIdx++
				oneMyEvent.EventIdx = binEventIdx
				oneMyEvent.SqlType = sqlType
				oneMyEvent.Timestamp = ev.Header.Timestamp
				oneMyEvent.TrxIndex = trxIndex
				oneMyEvent.TrxStatus = trxStatus
				cfg.EventChan <- *oneMyEvent
			}
		}

		//output analysis result whatever the WorkType is
		if sqlType != "" {
			if sqlType == "query" {
				cfg.StatChan <- BinEventStats{Timestamp: ev.Header.Timestamp, Binlog: currentBinlog, StartPos: ev.Header.LogPos - ev.Header.EventSize, StopPos: ev.Header.LogPos,
					Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
			} else {
				cfg.StatChan <- BinEventStats{Timestamp: ev.Header.Timestamp, Binlog: currentBinlog, StartPos: tbMapPos, StopPos: ev.Header.LogPos,
					Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
			}
		}

	}
	return nil
}
